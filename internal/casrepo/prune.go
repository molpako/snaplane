package casrepo

import (
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"time"
)

type PruneRequest struct {
	RepositoryPath  string
	KeepManifestIDs []string
}

type PruneResult struct {
	KeptManifestIDs    []string
	DeletedManifestIDs []string
	Compaction         *CompactResult
}

func Prune(req PruneRequest) (*PruneResult, error) {
	repoPath := filepath.Clean(req.RepositoryPath)
	if req.RepositoryPath == "" {
		return nil, fmt.Errorf("repository path is required")
	}
	if !filepath.IsAbs(repoPath) {
		return nil, fmt.Errorf("repository path must be absolute: %q", req.RepositoryPath)
	}
	if err := ensureRepoDirs(repoPath); err != nil {
		return nil, err
	}

	unlock, err := acquireRepoLock(repoPath)
	if err != nil {
		return nil, err
	}
	defer unlock()

	if _, err := ensureRepoMeta(repoPath); err != nil {
		return nil, err
	}

	keep, err := resolveKeepClosure(repoPath, req.KeepManifestIDs)
	if err != nil {
		return nil, err
	}
	deleted, err := deleteUnkeptManifestDirs(repoPath, keep)
	if err != nil {
		return nil, err
	}
	if err := publishLatestKeptManifest(repoPath, keep); err != nil {
		return nil, err
	}

	compaction, err := compactLocked(repoPath)
	if err != nil {
		return nil, err
	}
	kept := sortedSetKeys(keep)
	return &PruneResult{
		KeptManifestIDs:    kept,
		DeletedManifestIDs: deleted,
		Compaction:         compaction,
	}, nil
}

func resolveKeepClosure(repoPath string, manifestIDs []string) (map[string]struct{}, error) {
	keep := map[string]struct{}{}
	for _, manifestID := range manifestIDs {
		chain, err := ResolveManifestChain(repoPath, manifestID)
		if err != nil {
			return nil, err
		}
		for _, chainManifestID := range chain {
			keep[chainManifestID] = struct{}{}
		}
	}
	return keep, nil
}

func deleteUnkeptManifestDirs(repoPath string, keep map[string]struct{}) ([]string, error) {
	snapshotsDir := filepath.Join(repoPath, "snapshots")
	entries, err := os.ReadDir(snapshotsDir)
	if err != nil {
		return nil, fmt.Errorf("read snapshots dir: %w", err)
	}

	deleted := []string{}
	for _, entry := range entries {
		if !entry.IsDir() {
			continue
		}
		manifestID := entry.Name()
		if _, ok := keep[manifestID]; ok {
			continue
		}
		if err := os.RemoveAll(filepath.Join(snapshotsDir, manifestID)); err != nil {
			return nil, fmt.Errorf("delete manifest dir %q: %w", manifestID, err)
		}
		deleted = append(deleted, manifestID)
	}
	sort.Strings(deleted)
	if err := syncDir(snapshotsDir); err != nil {
		return nil, err
	}
	return deleted, nil
}

func publishLatestKeptManifest(repoPath string, keep map[string]struct{}) error {
	if len(keep) == 0 {
		if err := os.Remove(filepath.Join(repoPath, "refs", "latest.txt")); err != nil && !os.IsNotExist(err) {
			return fmt.Errorf("remove latest ref: %w", err)
		}
		return syncDir(filepath.Join(repoPath, "refs"))
	}

	type candidate struct {
		manifestID string
		createdAt  time.Time
	}
	candidates := make([]candidate, 0, len(keep))
	for manifestID := range keep {
		mf, err := loadManifest(filepath.Join(repoPath, "snapshots", manifestID, "manifest.json"))
		if err != nil {
			return err
		}
		createdAt, _ := time.Parse(time.RFC3339, mf.CreatedAt)
		candidates = append(candidates, candidate{manifestID: manifestID, createdAt: createdAt.UTC()})
	}
	sort.Slice(candidates, func(i, j int) bool {
		if candidates[i].createdAt.Equal(candidates[j].createdAt) {
			return candidates[i].manifestID < candidates[j].manifestID
		}
		return candidates[i].createdAt.Before(candidates[j].createdAt)
	})
	return atomicWriteFile(filepath.Join(repoPath, "refs", "latest.txt"), []byte(candidates[len(candidates)-1].manifestID+"\n"), 0o640)
}

func sortedSetKeys(set map[string]struct{}) []string {
	keys := make([]string, 0, len(set))
	for key := range set {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	return keys
}

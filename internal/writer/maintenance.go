package writer

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"time"

	"k8s.io/klog/v2"

	"github.com/molpako/snaplane/internal/casrepo"
)

const DefaultCASMaintenanceInterval = 6 * time.Hour

type CASMaintenanceSnapshot struct {
	LastRunAt      time.Time
	LastError      string
	RepositoryRuns int
	ReclaimedBytes int64
}

var casMaintenanceState = struct {
	sync.Mutex
	snapshot CASMaintenanceSnapshot
}{}

func CurrentCASMaintenanceSnapshot() CASMaintenanceSnapshot {
	casMaintenanceState.Lock()
	defer casMaintenanceState.Unlock()
	return casMaintenanceState.snapshot
}

func StartCASMaintenanceLoop(ctx context.Context, rootDir string, interval time.Duration) {
	if interval <= 0 {
		return
	}

	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if err := runCASMaintenanceOnce(rootDir); err != nil {
				klog.Errorf("cas maintenance run failed: %v", err)
			}
		}
	}
}

func runCASMaintenanceOnce(rootDir string) error {
	repos, err := listRepositoryPaths(rootDir)
	if err != nil {
		recordCASMaintenanceSnapshot(CASMaintenanceSnapshot{
			LastRunAt: time.Now().UTC(),
			LastError: err.Error(),
		})
		return err
	}

	var runErr error
	repositoryRuns := 0
	var reclaimedBytes int64
	for _, repoPath := range repos {
		result, err := casrepo.Compact(repoPath)
		if err != nil {
			runErr = errors.Join(runErr, fmt.Errorf("%s: %w", repoPath, err))
			continue
		}
		klog.V(1).Infof(
			"compacted CAS repository path=%s manifests=%d reachableChunks=%d reclaimedChunks=%d reclaimedBytes=%d",
			repoPath,
			result.ManifestCount,
			result.ReachableChunkCount,
			result.ReclaimedChunkCount,
			result.ReclaimedBytes,
		)
		repositoryRuns++
		reclaimedBytes += result.ReclaimedBytes
	}

	snapshot := CASMaintenanceSnapshot{
		LastRunAt:      time.Now().UTC(),
		RepositoryRuns: repositoryRuns,
		ReclaimedBytes: reclaimedBytes,
	}
	if runErr != nil {
		snapshot.LastError = runErr.Error()
	}
	recordCASMaintenanceSnapshot(snapshot)

	return runErr
}

func recordCASMaintenanceSnapshot(snapshot CASMaintenanceSnapshot) {
	casMaintenanceState.Lock()
	defer casMaintenanceState.Unlock()
	casMaintenanceState.snapshot = snapshot
}

func listRepositoryPaths(rootDir string) ([]string, error) {
	entries, err := os.ReadDir(rootDir)
	if err != nil {
		return nil, fmt.Errorf("read backup root %q: %w", rootDir, err)
	}

	repos := make([]string, 0)
	for _, namespaceEntry := range entries {
		if !namespaceEntry.IsDir() {
			continue
		}
		namespacePath := filepath.Join(rootDir, namespaceEntry.Name())
		pvcs, err := os.ReadDir(namespacePath)
		if err != nil {
			return nil, fmt.Errorf("read namespace dir %q: %w", namespacePath, err)
		}
		for _, pvcEntry := range pvcs {
			if !pvcEntry.IsDir() {
				continue
			}
			repoPath := filepath.Join(namespacePath, pvcEntry.Name(), "repo")
			info, err := os.Stat(repoPath)
			if err != nil {
				if os.IsNotExist(err) {
					continue
				}
				return nil, fmt.Errorf("stat repo dir %q: %w", repoPath, err)
			}
			if info.IsDir() {
				repos = append(repos, repoPath)
			}
		}
	}

	sort.Strings(repos)
	return repos, nil
}

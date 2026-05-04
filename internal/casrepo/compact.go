package casrepo

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
)

// CompactResult describes the outcome of one repository compaction run.
type CompactResult struct {
	ManifestCount        int
	ReachableChunkCount  int
	ReclaimedChunkCount  int
	ReclaimedBytes       int64
	PreviousSegmentCount int
	NewSegmentCount      int
	PreviousPackCount    int
	NewPackCount         int
	Compacted            bool
}

// Compact rewrites active index and pack files so only chunks reachable from
// on-disk manifests remain referenced. All manifests currently present under
// snapshots/ are treated as restore roots.
func Compact(repositoryPath string) (*CompactResult, error) {
	repoPath := filepath.Clean(repositoryPath)
	if repoPath == "" {
		return nil, fmt.Errorf("repository path is required")
	}
	if !filepath.IsAbs(repoPath) {
		return nil, fmt.Errorf("repository path must be absolute: %q", repositoryPath)
	}
	if err := ensureRepoDirs(repoPath); err != nil {
		return nil, err
	}

	unlock, err := acquireRepoLock(repoPath)
	if err != nil {
		return nil, err
	}
	defer unlock()

	return compactLocked(repoPath)
}

func compactLocked(repoPath string) (*CompactResult, error) {
	meta, err := ensureRepoMeta(repoPath)
	if err != nil {
		return nil, err
	}
	currentIndex, active, err := loadGlobalIndex(repoPath)
	if err != nil {
		return nil, err
	}

	manifestIDs, reachableChunkIDs, err := collectReachableChunkIDs(repoPath)
	if err != nil {
		return nil, err
	}

	reachableEntries := make([]indexEntry, 0, len(reachableChunkIDs))
	for chunkID := range reachableChunkIDs {
		entry, ok := currentIndex[chunkID]
		if !ok {
			return nil, fmt.Errorf("reachable chunk %q missing from active index", chunkID)
		}
		reachableEntries = append(reachableEntries, entry)
	}
	sort.Slice(reachableEntries, func(i, j int) bool {
		return reachableEntries[i].ChunkID < reachableEntries[j].ChunkID
	})

	previousPackRefs := uniquePackIDs(currentIndex)
	nextPackID, err := currentPackID(repoPath)
	if err != nil {
		return nil, err
	}
	if len(previousPackRefs) > 0 {
		nextPackID++
	}

	nextSegmentID, err := nextCompactionSegmentID(repoPath)
	if err != nil {
		return nil, err
	}
	newSegments, newPackCount, err := rewriteReachableData(repoPath, meta, reachableEntries, nextPackID, nextSegmentID)
	if err != nil {
		return nil, err
	}

	if err := atomicWriteJSON(filepath.Join(repoPath, "indexes", "active.json"), &activeIndex{
		Version:   1,
		Segments:  newSegments,
		CreatedAt: active.CreatedAt,
	}, 0o640); err != nil {
		return nil, fmt.Errorf("write compacted active index: %w", err)
	}

	if err := deleteInactiveSegments(repoPath, newSegments); err != nil {
		return nil, err
	}
	if err := deleteInactivePacks(repoPath, nextPackID, newPackCount); err != nil {
		return nil, err
	}

	return &CompactResult{
		ManifestCount:        len(manifestIDs),
		ReachableChunkCount:  len(reachableEntries),
		ReclaimedChunkCount:  len(currentIndex) - len(reachableEntries),
		ReclaimedBytes:       reclaimedStoredBytes(currentIndex, reachableChunkIDs),
		PreviousSegmentCount: len(active.Segments),
		NewSegmentCount:      len(newSegments),
		PreviousPackCount:    len(previousPackRefs),
		NewPackCount:         newPackCount,
		Compacted:            true,
	}, nil
}

func collectReachableChunkIDs(repoPath string) ([]string, map[string]struct{}, error) {
	entries, err := os.ReadDir(filepath.Join(repoPath, "snapshots"))
	if err != nil {
		return nil, nil, fmt.Errorf("read snapshots dir: %w", err)
	}

	manifestIDs := make([]string, 0, len(entries))
	reachable := map[string]struct{}{}
	for _, entry := range entries {
		if !entry.IsDir() {
			continue
		}
		manifestID := entry.Name()
		mf, err := loadManifest(filepath.Join(repoPath, "snapshots", manifestID, "manifest.json"))
		if err != nil {
			return nil, nil, err
		}
		changes, err := loadChangeRecords(filepath.Join(repoPath, "snapshots", manifestID, mf.Segments.Change))
		if err != nil {
			return nil, nil, err
		}
		manifestIDs = append(manifestIDs, manifestID)
		for _, change := range changes {
			if err := validateChangeRecord(change); err != nil {
				return nil, nil, fmt.Errorf("invalid change record in manifest %q: %w", manifestID, err)
			}
			if change.State == stateAllocated {
				reachable[change.ChunkID] = struct{}{}
			}
		}
	}
	sort.Strings(manifestIDs)
	return manifestIDs, reachable, nil
}

func loadManifest(path string) (*manifest, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read manifest %q: %w", path, err)
	}
	var mf manifest
	if err := json.Unmarshal(data, &mf); err != nil {
		return nil, fmt.Errorf("decode manifest %q: %w", path, err)
	}
	if mf.Segments.Change == "" {
		return nil, fmt.Errorf("manifest %q has empty change segment", path)
	}
	if mf.Volume.ChunkSizeBytes <= 0 {
		return nil, fmt.Errorf("manifest %q has invalid chunk size %d", path, mf.Volume.ChunkSizeBytes)
	}
	if mf.Volume.SizeBytes < 0 {
		return nil, fmt.Errorf("manifest %q has invalid volume size %d", path, mf.Volume.SizeBytes)
	}
	return &mf, nil
}

func loadChangeRecords(path string) ([]changeRecord, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read change segment %q: %w", path, err)
	}
	records := []changeRecord{}
	if len(data) == 0 {
		return records, nil
	}
	if err := json.Unmarshal(data, &records); err != nil {
		return nil, fmt.Errorf("decode change segment %q: %w", path, err)
	}
	return records, nil
}

func validateChangeRecord(change changeRecord) error {
	switch change.State {
	case stateAllocated:
		if change.ChunkID == "" {
			return fmt.Errorf("allocated chunk %d has empty chunkID", change.ChunkIndex)
		}
	case stateFreed:
		if change.ChunkID != "" {
			return fmt.Errorf("freed chunk %d must not carry chunkID", change.ChunkIndex)
		}
	default:
		return fmt.Errorf("unknown change state %q", change.State)
	}
	return nil
}

func rewriteReachableData(repoPath string, meta *repoMeta, entries []indexEntry, nextPackID int, nextSegment int) ([]string, int, error) {
	if len(entries) == 0 {
		return []string{}, 0, nil
	}

	newEntries := make([]indexEntry, 0, len(entries))
	segments := []string{}
	var currentPackFile *os.File
	currentPackID := 0
	currentPackBytes := int64(0)
	currentPackOffsets := int64(0)
	currentPidx := []packIndexEntry{}
	packCount := 0
	closeCurrentPack := func() error {
		if currentPackFile == nil {
			return nil
		}
		if err := currentPackFile.Sync(); err != nil {
			_ = currentPackFile.Close()
			return fmt.Errorf("sync compacted pack: %w", err)
		}
		if err := currentPackFile.Close(); err != nil {
			return fmt.Errorf("close compacted pack: %w", err)
		}
		pidxPath := filepath.Join(repoPath, "packs", fmt.Sprintf("pack-%06d.pidx", currentPackID))
		sort.Slice(currentPidx, func(i, j int) bool {
			return currentPidx[i].ChunkID < currentPidx[j].ChunkID
		})
		if err := atomicWriteJSON(pidxPath, currentPidx, 0o640); err != nil {
			return fmt.Errorf("write compacted pack index: %w", err)
		}
		currentPackFile = nil
		currentPidx = nil
		currentPackBytes = 0
		currentPackOffsets = 0
		return nil
	}

	openNextPack := func() error {
		currentPackID = nextPackID + packCount
		packPath := filepath.Join(repoPath, "packs", fmt.Sprintf("pack-%06d.pack", currentPackID))
		f, err := os.OpenFile(packPath, os.O_CREATE|os.O_TRUNC|os.O_RDWR, 0o640)
		if err != nil {
			return fmt.Errorf("open compacted pack: %w", err)
		}
		currentPackFile = f
		currentPidx = []packIndexEntry{}
		currentPackBytes = 0
		currentPackOffsets = 0
		packCount++
		return nil
	}

	if err := openNextPack(); err != nil {
		return nil, 0, err
	}

	for _, entry := range entries {
		payload, err := readChunkPayload(repoPath, entry)
		if err != nil {
			return nil, 0, err
		}
		if currentPackBytes > 0 && currentPackBytes+entry.StoredLength > meta.PackTargetSizeBytes {
			if err := closeCurrentPack(); err != nil {
				return nil, 0, err
			}
			if err := openNextPack(); err != nil {
				return nil, 0, err
			}
		}

		if _, err := currentPackFile.Write(payload); err != nil {
			return nil, 0, fmt.Errorf("write compacted chunk payload: %w", err)
		}
		newEntry := indexEntry{
			ChunkID:      entry.ChunkID,
			PackID:       currentPackID,
			PackOffset:   currentPackOffsets,
			StoredLength: entry.StoredLength,
			RawLength:    entry.RawLength,
			Codec:        entry.Codec,
		}
		newEntries = append(newEntries, newEntry)
		currentPidx = append(currentPidx, packIndexEntry{
			ChunkID:      entry.ChunkID,
			Offset:       currentPackOffsets,
			StoredLength: entry.StoredLength,
			RawLength:    entry.RawLength,
			Codec:        entry.Codec,
		})
		currentPackBytes += entry.StoredLength
		currentPackOffsets += entry.StoredLength
	}

	if err := closeCurrentPack(); err != nil {
		return nil, 0, err
	}

	segmentName := fmt.Sprintf("idx-%06d.seg", nextSegment)
	segmentPath := filepath.Join(repoPath, "indexes", "segments", segmentName)
	if err := atomicWriteJSON(segmentPath, newEntries, 0o640); err != nil {
		return nil, 0, fmt.Errorf("write compacted index segment: %w", err)
	}
	segments = append(segments, segmentName)

	return segments, packCount, nil
}

func readChunkPayload(repoPath string, entry indexEntry) ([]byte, error) {
	if err := validateIndexEntry(entry); err != nil {
		return nil, err
	}
	packPath := filepath.Join(repoPath, "packs", fmt.Sprintf("pack-%06d.pack", entry.PackID))
	f, err := os.Open(filepath.Clean(packPath))
	if err != nil {
		return nil, fmt.Errorf("open pack %q: %w", packPath, err)
	}
	defer f.Close()

	payload := make([]byte, entry.StoredLength)
	if _, err := f.ReadAt(payload, entry.PackOffset); err != nil {
		return nil, fmt.Errorf("read chunk %q payload from %q: %w", entry.ChunkID, packPath, err)
	}
	sum := sha256.Sum256(payload)
	if !strings.EqualFold(hex.EncodeToString(sum[:]), entry.ChunkID) {
		return nil, fmt.Errorf("chunk checksum mismatch for chunkID %q", entry.ChunkID)
	}
	return payload, nil
}

func deleteInactiveSegments(repoPath string, keep []string) error {
	keepSet := map[string]struct{}{}
	for _, seg := range keep {
		keepSet[seg] = struct{}{}
	}
	entries, err := os.ReadDir(filepath.Join(repoPath, "indexes", "segments"))
	if err != nil {
		return fmt.Errorf("read segments dir: %w", err)
	}
	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}
		if _, ok := keepSet[entry.Name()]; ok {
			continue
		}
		if err := os.Remove(filepath.Join(repoPath, "indexes", "segments", entry.Name())); err != nil && !os.IsNotExist(err) {
			return fmt.Errorf("delete inactive segment %q: %w", entry.Name(), err)
		}
	}
	return syncDir(filepath.Join(repoPath, "indexes", "segments"))
}

func deleteInactivePacks(repoPath string, firstNewPackID int, newPackCount int) error {
	keep := map[string]struct{}{}
	for i := 0; i < newPackCount; i++ {
		packID := firstNewPackID + i
		keep[fmt.Sprintf("pack-%06d.pack", packID)] = struct{}{}
		keep[fmt.Sprintf("pack-%06d.pidx", packID)] = struct{}{}
	}

	entries, err := os.ReadDir(filepath.Join(repoPath, "packs"))
	if err != nil {
		return fmt.Errorf("read packs dir: %w", err)
	}
	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}
		if _, ok := keep[entry.Name()]; ok {
			continue
		}
		if err := os.Remove(filepath.Join(repoPath, "packs", entry.Name())); err != nil && !os.IsNotExist(err) {
			return fmt.Errorf("delete inactive pack file %q: %w", entry.Name(), err)
		}
	}
	return syncDir(filepath.Join(repoPath, "packs"))
}

func uniquePackIDs(index map[string]indexEntry) map[int]struct{} {
	out := map[int]struct{}{}
	for _, entry := range index {
		out[entry.PackID] = struct{}{}
	}
	return out
}

func reclaimedStoredBytes(index map[string]indexEntry, reachable map[string]struct{}) int64 {
	var reclaimed int64
	for chunkID, entry := range index {
		if _, ok := reachable[chunkID]; ok {
			continue
		}
		reclaimed += entry.StoredLength
	}
	return reclaimed
}

func nextCompactionSegmentID(repoPath string) (int, error) {
	entries, err := os.ReadDir(filepath.Join(repoPath, "indexes", "segments"))
	if err != nil {
		return 0, fmt.Errorf("read segments dir: %w", err)
	}
	maxID := 0
	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}
		var id int
		if _, scanErr := fmt.Sscanf(entry.Name(), "idx-%06d.seg", &id); scanErr == nil && id > maxID {
			maxID = id
		}
	}
	return maxID + 1, nil
}

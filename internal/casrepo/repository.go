package casrepo

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"
)

const (
	DefaultChunkSize      int64 = 4 * 1024 * 1024
	defaultPackTargetSize int64 = 512 * 1024 * 1024
	defaultLockStaleAfter       = 15 * time.Minute
	envCASLockStaleAfter        = "SNAPLANE_CAS_LOCK_STALE_AFTER"

	stateAllocated = "allocated"
	stateFreed     = "freed"
)

type SpanKind string

const (
	SpanKindData SpanKind = "data"
	SpanKindZero SpanKind = "zero"
)

type FrameSpan struct {
	Kind   SpanKind
	Offset int64
	Length int64
}

type CommitRequest struct {
	RepositoryPath string
	ManifestID     string
	PolicyName     string
	SpoolPath      string
	LogicalSize    int64
	Spans          []FrameSpan
}

type CommitResult struct {
	RepositoryPath string
	ManifestID     string
	RepoUUID       string
	VolumeSize     int64
	ChunkSize      int64
}

type repoMeta struct {
	RepoVersion         int    `json:"repoVersion"`
	RepoUUID            string `json:"repoUUID"`
	ChunkSizeBytes      int64  `json:"chunkSizeBytes"`
	ChunkHash           string `json:"chunkHash"`
	PackTargetSizeBytes int64  `json:"packTargetSizeBytes"`
	Compression         string `json:"compression"`
	CreatedAt           string `json:"createdAt"`
}

type activeIndex struct {
	Version   int      `json:"version"`
	Segments  []string `json:"segments"`
	CreatedAt string   `json:"createdAt"`
}

type indexEntry struct {
	ChunkID      string `json:"chunkID"`
	PackID       int    `json:"packID"`
	PackOffset   int64  `json:"packOffset"`
	StoredLength int64  `json:"storedLength"`
	RawLength    int64  `json:"rawLength"`
	Codec        string `json:"codec"`
}

type packIndexEntry struct {
	ChunkID      string `json:"chunkID"`
	Offset       int64  `json:"offset"`
	StoredLength int64  `json:"storedLength"`
	RawLength    int64  `json:"rawLength"`
	Codec        string `json:"codec"`
}

type changeRecord struct {
	ChunkIndex uint64 `json:"chunkIndex"`
	ChunkID    string `json:"chunkID,omitempty"`
	State      string `json:"state"`
}

type manifest struct {
	ManifestID       string `json:"manifestID"`
	ParentManifestID string `json:"parentManifestID,omitempty"`
	Policy           string `json:"policy,omitempty"`
	CreatedAt        string `json:"createdAt"`
	Volume           struct {
		SizeBytes      int64 `json:"sizeBytes"`
		ChunkSizeBytes int64 `json:"chunkSizeBytes"`
	} `json:"volume"`
	Segments struct {
		Alloc  string `json:"alloc"`
		Change string `json:"change"`
	} `json:"segments"`
	Stats struct {
		ChangedChunks int   `json:"changedChunks"`
		NewChunks     int   `json:"newChunks"`
		ReusedChunks  int   `json:"reusedChunks"`
		BytesWritten  int64 `json:"bytesWritten"`
	} `json:"stats"`
}

type repoLockFile struct {
	AcquiredAt string `json:"acquiredAt"`
	PID        int    `json:"pid,omitempty"`
}

// Commit publishes one backup generation into a repository.
func Commit(req CommitRequest) (*CommitResult, error) {
	if req.RepositoryPath == "" {
		return nil, fmt.Errorf("repository path is required")
	}
	if req.ManifestID == "" {
		return nil, fmt.Errorf("manifest ID is required")
	}
	if req.SpoolPath == "" {
		return nil, fmt.Errorf("spool path is required")
	}
	if req.LogicalSize < 0 {
		return nil, fmt.Errorf("logical size must be >= 0")
	}

	repoPath := filepath.Clean(req.RepositoryPath)
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

	meta, err := ensureRepoMeta(repoPath)
	if err != nil {
		return nil, err
	}

	parentManifestID, _ := readLatestManifestID(repoPath)

	manifestDir := filepath.Join(repoPath, "snapshots", req.ManifestID)
	if _, err := os.Stat(manifestDir); err == nil {
		return nil, fmt.Errorf("manifest %q already exists", req.ManifestID)
	} else if !os.IsNotExist(err) {
		return nil, fmt.Errorf("stat manifest dir: %w", err)
	}

	globalIndex, active, err := loadGlobalIndex(repoPath)
	if err != nil {
		return nil, err
	}

	spans := normalizeSpans(req.Spans)
	changedChunkNumbers := collectChangedChunkNumbers(spans, meta.ChunkSizeBytes)
	if len(changedChunkNumbers) == 0 && req.LogicalSize > 0 {
		changedChunkNumbers = append(changedChunkNumbers, 0)
	}

	spool, err := os.Open(filepath.Clean(req.SpoolPath))
	if err != nil {
		return nil, fmt.Errorf("open spool file: %w", err)
	}
	defer spool.Close()

	packID, err := currentPackID(repoPath)
	if err != nil {
		return nil, err
	}
	packPath := filepath.Join(repoPath, "packs", fmt.Sprintf("pack-%06d.pack", packID))
	packFile, err := os.OpenFile(packPath, os.O_CREATE|os.O_RDWR, 0o640)
	if err != nil {
		return nil, fmt.Errorf("open pack file: %w", err)
	}
	defer packFile.Close()

	packOffset, err := packFile.Seek(0, io.SeekEnd)
	if err != nil {
		return nil, fmt.Errorf("seek pack end: %w", err)
	}

	pidxPath := filepath.Join(repoPath, "packs", fmt.Sprintf("pack-%06d.pidx", packID))
	pidxEntries, err := loadPackIndex(pidxPath)
	if err != nil {
		return nil, err
	}

	changes := make([]changeRecord, 0, len(changedChunkNumbers))
	newIndexEntries := make([]indexEntry, 0)
	newBytesWritten := int64(0)
	newChunkCount := 0
	reusedChunkCount := 0

	for _, chunkNumber := range changedChunkNumbers {
		chunkStart := chunkNumber * meta.ChunkSizeBytes
		if chunkStart >= req.LogicalSize {
			continue
		}
		chunkLength := minInt64(meta.ChunkSizeBytes, req.LogicalSize-chunkStart)
		payload := make([]byte, chunkLength)
		if _, err := spool.ReadAt(payload, chunkStart); err != nil && err != io.EOF {
			return nil, fmt.Errorf("read chunk payload at offset %d: %w", chunkStart, err)
		}

		if isAllZero(payload) {
			changes = append(changes, changeRecord{ChunkIndex: uint64(chunkNumber), State: stateFreed})
			continue
		}

		digest := sha256.Sum256(payload)
		chunkID := hex.EncodeToString(digest[:])
		changes = append(changes, changeRecord{ChunkIndex: uint64(chunkNumber), ChunkID: chunkID, State: stateAllocated})

		if _, exists := globalIndex[chunkID]; exists {
			reusedChunkCount++
			continue
		}

		if _, err := packFile.Write(payload); err != nil {
			return nil, fmt.Errorf("append pack payload: %w", err)
		}
		entry := indexEntry{
			ChunkID:      chunkID,
			PackID:       packID,
			PackOffset:   packOffset,
			StoredLength: int64(len(payload)),
			RawLength:    int64(len(payload)),
			Codec:        "none",
		}
		newIndexEntries = append(newIndexEntries, entry)
		globalIndex[chunkID] = entry
		pidxEntries = append(pidxEntries, packIndexEntry{
			ChunkID:      chunkID,
			Offset:       packOffset,
			StoredLength: int64(len(payload)),
			RawLength:    int64(len(payload)),
			Codec:        "none",
		})
		packOffset += int64(len(payload))
		newBytesWritten += int64(len(payload))
		newChunkCount++
	}

	if err := packFile.Sync(); err != nil {
		return nil, fmt.Errorf("sync pack file: %w", err)
	}

	sort.Slice(pidxEntries, func(i, j int) bool {
		return pidxEntries[i].ChunkID < pidxEntries[j].ChunkID
	})
	if err := atomicWriteJSON(pidxPath, pidxEntries, 0o640); err != nil {
		return nil, fmt.Errorf("write pack index: %w", err)
	}

	segmentName := fmt.Sprintf("idx-%06d.seg", nextSegmentID(active.Segments))
	segmentPath := filepath.Join(repoPath, "indexes", "segments", segmentName)
	if err := atomicWriteJSON(segmentPath, newIndexEntries, 0o640); err != nil {
		return nil, fmt.Errorf("write index segment: %w", err)
	}

	active.Segments = append(active.Segments, segmentName)
	active.CreatedAt = time.Now().UTC().Format(time.RFC3339)
	if err := atomicWriteJSON(filepath.Join(repoPath, "indexes", "active.json"), active, 0o640); err != nil {
		return nil, fmt.Errorf("write active index: %w", err)
	}
	if err := syncDir(filepath.Join(repoPath, "indexes")); err != nil {
		return nil, err
	}

	if err := os.MkdirAll(manifestDir, 0o750); err != nil {
		return nil, fmt.Errorf("create manifest dir: %w", err)
	}
	allocPath := filepath.Join(manifestDir, "seg-alloc.bin.zst")
	changePath := filepath.Join(manifestDir, "seg-change.bin.zst")
	if err := atomicWriteJSON(allocPath, map[string]any{"ranges": []any{}}, 0o640); err != nil {
		return nil, fmt.Errorf("write alloc segment: %w", err)
	}
	if err := atomicWriteJSON(changePath, changes, 0o640); err != nil {
		return nil, fmt.Errorf("write change segment: %w", err)
	}

	mf := manifest{
		ManifestID:       req.ManifestID,
		ParentManifestID: parentManifestID,
		Policy:           req.PolicyName,
		CreatedAt:        time.Now().UTC().Format(time.RFC3339),
	}
	mf.Volume.SizeBytes = req.LogicalSize
	mf.Volume.ChunkSizeBytes = meta.ChunkSizeBytes
	mf.Segments.Alloc = "seg-alloc.bin.zst"
	mf.Segments.Change = "seg-change.bin.zst"
	mf.Stats.ChangedChunks = len(changes)
	mf.Stats.NewChunks = newChunkCount
	mf.Stats.ReusedChunks = reusedChunkCount
	mf.Stats.BytesWritten = newBytesWritten
	if err := atomicWriteJSON(filepath.Join(manifestDir, "manifest.json"), mf, 0o640); err != nil {
		return nil, fmt.Errorf("write manifest: %w", err)
	}
	if err := syncDir(manifestDir); err != nil {
		return nil, err
	}

	if err := atomicWriteFile(filepath.Join(repoPath, "refs", "latest.txt"), []byte(req.ManifestID+"\n"), 0o640); err != nil {
		return nil, fmt.Errorf("write latest ref: %w", err)
	}

	return &CommitResult{
		RepositoryPath: repoPath,
		ManifestID:     req.ManifestID,
		RepoUUID:       meta.RepoUUID,
		VolumeSize:     req.LogicalSize,
		ChunkSize:      meta.ChunkSizeBytes,
	}, nil
}

func ensureRepoDirs(repoPath string) error {
	dirs := []string{
		repoPath,
		filepath.Join(repoPath, "refs"),
		filepath.Join(repoPath, "snapshots"),
		filepath.Join(repoPath, "packs"),
		filepath.Join(repoPath, "indexes", "segments"),
		filepath.Join(repoPath, "locks"),
		filepath.Join(repoPath, "tmp"),
	}
	for _, dir := range dirs {
		if err := os.MkdirAll(dir, 0o750); err != nil {
			return fmt.Errorf("create repo dir %q: %w", dir, err)
		}
	}
	return nil
}

func ensureRepoMeta(repoPath string) (*repoMeta, error) {
	metaPath := filepath.Join(repoPath, "repo.json")
	meta := &repoMeta{}
	if data, err := os.ReadFile(metaPath); err == nil {
		if err := json.Unmarshal(data, meta); err != nil {
			return nil, fmt.Errorf("decode repo.json: %w", err)
		}
		if meta.ChunkSizeBytes <= 0 {
			meta.ChunkSizeBytes = DefaultChunkSize
		}
		if meta.PackTargetSizeBytes <= 0 {
			meta.PackTargetSizeBytes = defaultPackTargetSize
		}
		return meta, nil
	} else if !os.IsNotExist(err) {
		return nil, fmt.Errorf("read repo.json: %w", err)
	}

	meta = &repoMeta{
		RepoVersion:         1,
		RepoUUID:            randomHexID(),
		ChunkSizeBytes:      DefaultChunkSize,
		ChunkHash:           "sha256",
		PackTargetSizeBytes: defaultPackTargetSize,
		Compression:         "none",
		CreatedAt:           time.Now().UTC().Format(time.RFC3339),
	}
	if err := atomicWriteJSON(metaPath, meta, 0o640); err != nil {
		return nil, fmt.Errorf("write repo.json: %w", err)
	}
	return meta, nil
}

func readLatestManifestID(repoPath string) (string, error) {
	data, err := os.ReadFile(filepath.Join(repoPath, "refs", "latest.txt"))
	if err != nil {
		return "", err
	}
	return strings.TrimSpace(string(data)), nil
}

func loadGlobalIndex(repoPath string) (map[string]indexEntry, *activeIndex, error) {
	activePath := filepath.Join(repoPath, "indexes", "active.json")
	active := &activeIndex{Version: 1, Segments: []string{}}
	if data, err := os.ReadFile(activePath); err == nil {
		if err := json.Unmarshal(data, active); err != nil {
			return nil, nil, fmt.Errorf("decode active index: %w", err)
		}
	} else if !os.IsNotExist(err) {
		return nil, nil, fmt.Errorf("read active index: %w", err)
	}

	index := map[string]indexEntry{}
	for _, seg := range active.Segments {
		segPath := filepath.Join(repoPath, "indexes", "segments", seg)
		entries := []indexEntry{}
		data, err := os.ReadFile(segPath)
		if err != nil {
			if os.IsNotExist(err) {
				continue
			}
			return nil, nil, fmt.Errorf("read segment %q: %w", seg, err)
		}
		if len(data) == 0 {
			continue
		}
		if err := json.Unmarshal(data, &entries); err != nil {
			return nil, nil, fmt.Errorf("decode segment %q: %w", seg, err)
		}
		for _, e := range entries {
			index[e.ChunkID] = e
		}
	}

	return index, active, nil
}

func loadPackIndex(path string) ([]packIndexEntry, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		if os.IsNotExist(err) {
			return []packIndexEntry{}, nil
		}
		return nil, fmt.Errorf("read pack index: %w", err)
	}
	entries := []packIndexEntry{}
	if len(data) == 0 {
		return entries, nil
	}
	if err := json.Unmarshal(data, &entries); err != nil {
		return nil, fmt.Errorf("decode pack index: %w", err)
	}
	return entries, nil
}

func currentPackID(repoPath string) (int, error) {
	entries, err := os.ReadDir(filepath.Join(repoPath, "packs"))
	if err != nil {
		return 0, fmt.Errorf("read packs dir: %w", err)
	}
	maxID := 0
	for _, e := range entries {
		name := e.Name()
		if !strings.HasPrefix(name, "pack-") || !strings.HasSuffix(name, ".pack") {
			continue
		}
		var id int
		if _, scanErr := fmt.Sscanf(name, "pack-%06d.pack", &id); scanErr == nil && id > maxID {
			maxID = id
		}
	}
	if maxID == 0 {
		return 1, nil
	}
	return maxID, nil
}

func nextSegmentID(segments []string) int {
	maxID := 0
	for _, seg := range segments {
		var id int
		if _, err := fmt.Sscanf(seg, "idx-%06d.seg", &id); err == nil && id > maxID {
			maxID = id
		}
	}
	return maxID + 1
}

func normalizeSpans(spans []FrameSpan) []FrameSpan {
	out := make([]FrameSpan, 0, len(spans))
	for _, s := range spans {
		if s.Length <= 0 || s.Offset < 0 {
			continue
		}
		out = append(out, s)
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].Offset == out[j].Offset {
			return out[i].Length < out[j].Length
		}
		return out[i].Offset < out[j].Offset
	})
	return out
}

func collectChangedChunkNumbers(spans []FrameSpan, chunkSize int64) []int64 {
	set := map[int64]struct{}{}
	for _, s := range spans {
		start := s.Offset / chunkSize
		end := (s.Offset + s.Length - 1) / chunkSize
		for i := start; i <= end; i++ {
			set[i] = struct{}{}
		}
	}
	keys := make([]int64, 0, len(set))
	for k := range set {
		keys = append(keys, k)
	}
	sort.Slice(keys, func(i, j int) bool { return keys[i] < keys[j] })
	return keys
}

func isAllZero(buf []byte) bool {
	for _, b := range buf {
		if b != 0 {
			return false
		}
	}
	return true
}

func randomHexID() string {
	h := sha256.Sum256([]byte(fmt.Sprintf("%d", time.Now().UnixNano())))
	return hex.EncodeToString(h[:16])
}

func acquireRepoLock(repoPath string) (func(), error) {
	lockPath := filepath.Join(repoPath, "locks", "repo.lock")
	for i := 0; i < 2; i++ {
		f, err := os.OpenFile(lockPath, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o640)
		if err == nil {
			lock := repoLockFile{
				AcquiredAt: time.Now().UTC().Format(time.RFC3339Nano),
				PID:        os.Getpid(),
			}
			if encodeErr := json.NewEncoder(f).Encode(lock); encodeErr != nil {
				_ = f.Close()
				_ = os.Remove(lockPath)
				return nil, fmt.Errorf("write repository lock: %w", encodeErr)
			}
			if syncErr := f.Sync(); syncErr != nil {
				_ = f.Close()
				_ = os.Remove(lockPath)
				return nil, fmt.Errorf("sync repository lock: %w", syncErr)
			}
			if closeErr := f.Close(); closeErr != nil {
				_ = os.Remove(lockPath)
				return nil, fmt.Errorf("close repository lock: %w", closeErr)
			}
			return func() {
				_ = os.Remove(lockPath)
			}, nil
		}
		if !os.IsExist(err) {
			return nil, fmt.Errorf("create repository lock: %w", err)
		}

		stale, staleErr := isRepoLockStale(lockPath, time.Now().UTC(), lockStaleAfter())
		if staleErr != nil {
			return nil, staleErr
		}
		if !stale {
			return nil, fmt.Errorf("repository is locked")
		}
		stalePath := fmt.Sprintf("%s.stale-%d", lockPath, time.Now().UTC().UnixNano())
		if renameErr := os.Rename(lockPath, stalePath); renameErr != nil {
			if os.IsNotExist(renameErr) {
				continue
			}
			return nil, fmt.Errorf("reclaim stale repository lock: %w", renameErr)
		}
	}
	return nil, fmt.Errorf("repository is locked")
}

func lockStaleAfter() time.Duration {
	raw := strings.TrimSpace(os.Getenv(envCASLockStaleAfter))
	if raw == "" {
		return defaultLockStaleAfter
	}
	parsed, err := time.ParseDuration(raw)
	if err != nil || parsed <= 0 {
		return defaultLockStaleAfter
	}
	return parsed
}

func isRepoLockStale(lockPath string, now time.Time, staleAfter time.Duration) (bool, error) {
	data, err := os.ReadFile(lockPath)
	if err != nil {
		if os.IsNotExist(err) {
			return true, nil
		}
		return false, fmt.Errorf("read repository lock: %w", err)
	}
	acquiredAt := parseRepoLockTimestamp(strings.TrimSpace(string(data)))
	if acquiredAt.IsZero() {
		info, statErr := os.Stat(lockPath)
		if statErr != nil {
			return false, fmt.Errorf("stat repository lock: %w", statErr)
		}
		acquiredAt = info.ModTime().UTC()
	}
	return now.Sub(acquiredAt) > staleAfter, nil
}

func parseRepoLockTimestamp(raw string) time.Time {
	if raw == "" {
		return time.Time{}
	}
	lock := repoLockFile{}
	if err := json.Unmarshal([]byte(raw), &lock); err == nil && lock.AcquiredAt != "" {
		if parsed, parseErr := time.Parse(time.RFC3339Nano, lock.AcquiredAt); parseErr == nil {
			return parsed.UTC()
		}
	}
	if parsed, err := time.Parse(time.RFC3339Nano, raw); err == nil {
		return parsed.UTC()
	}
	return time.Time{}
}

func atomicWriteJSON(path string, value any, mode os.FileMode) error {
	data, err := json.MarshalIndent(value, "", "  ")
	if err != nil {
		return err
	}
	data = append(data, '\n')
	return atomicWriteFile(path, data, mode)
}

func atomicWriteFile(path string, data []byte, mode os.FileMode) error {
	dir := filepath.Dir(path)
	tmp, err := os.CreateTemp(dir, ".tmp-*")
	if err != nil {
		return fmt.Errorf("create temp file: %w", err)
	}
	tmpPath := tmp.Name()
	defer func() {
		_ = os.Remove(tmpPath)
	}()

	if _, err := tmp.Write(data); err != nil {
		_ = tmp.Close()
		return fmt.Errorf("write temp file: %w", err)
	}
	if err := tmp.Sync(); err != nil {
		_ = tmp.Close()
		return fmt.Errorf("sync temp file: %w", err)
	}
	if err := tmp.Close(); err != nil {
		return fmt.Errorf("close temp file: %w", err)
	}
	if err := os.Chmod(tmpPath, mode); err != nil {
		return fmt.Errorf("chmod temp file: %w", err)
	}
	if err := os.Rename(tmpPath, path); err != nil {
		return fmt.Errorf("rename temp file: %w", err)
	}
	if err := syncDir(dir); err != nil {
		return err
	}
	return nil
}

func syncDir(path string) error {
	d, err := os.Open(path)
	if err != nil {
		return fmt.Errorf("open dir %q: %w", path, err)
	}
	defer d.Close()
	if err := d.Sync(); err != nil {
		return fmt.Errorf("sync dir %q: %w", path, err)
	}
	return nil
}

func minInt64(a, b int64) int64 {
	if a < b {
		return a
	}
	return b
}

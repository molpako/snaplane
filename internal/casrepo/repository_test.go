package casrepo

import (
	"encoding/json"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"

	snaplanev1alpha1 "github.com/molpako/snaplane/api/v1alpha1"
	"github.com/molpako/snaplane/internal/restorepopulator"
)

func TestCommitInitializesRepoAndCanReopen(t *testing.T) {
	t.Parallel()

	repoPath := filepath.Join(t.TempDir(), "repo")
	spoolPath := writeSpoolFile(t, []byte("abc"), 4096)

	first, err := Commit(CommitRequest{
		RepositoryPath: repoPath,
		ManifestID:     "m1",
		PolicyName:     "policy-a",
		SpoolPath:      spoolPath,
		LogicalSize:    4099,
		Spans: []FrameSpan{
			{Kind: SpanKindData, Offset: 0, Length: 3},
			{Kind: SpanKindZero, Offset: 3, Length: 4096},
		},
	})
	if err != nil {
		t.Fatalf("first commit failed: %v", err)
	}
	if first.RepoUUID == "" {
		t.Fatalf("expected repo UUID")
	}

	second, err := Commit(CommitRequest{
		RepositoryPath: repoPath,
		ManifestID:     "m2",
		PolicyName:     "policy-a",
		SpoolPath:      spoolPath,
		LogicalSize:    4099,
		Spans: []FrameSpan{
			{Kind: SpanKindData, Offset: 0, Length: 3},
			{Kind: SpanKindZero, Offset: 3, Length: 4096},
		},
	})
	if err != nil {
		t.Fatalf("second commit failed: %v", err)
	}
	if second.RepoUUID != first.RepoUUID {
		t.Fatalf("expected same repo UUID, got %q and %q", first.RepoUUID, second.RepoUUID)
	}

	if _, err := os.Stat(filepath.Join(repoPath, "repo.json")); err != nil {
		t.Fatalf("expected repo.json: %v", err)
	}
	if _, err := os.Stat(filepath.Join(repoPath, "snapshots", "m1", "manifest.json")); err != nil {
		t.Fatalf("expected m1 manifest: %v", err)
	}
	if _, err := os.Stat(filepath.Join(repoPath, "snapshots", "m2", "manifest.json")); err != nil {
		t.Fatalf("expected m2 manifest: %v", err)
	}

	m2, err := readManifest(filepath.Join(repoPath, "snapshots", "m2", "manifest.json"))
	if err != nil {
		t.Fatalf("read manifest: %v", err)
	}
	if m2.ParentManifestID != "m1" {
		t.Fatalf("expected parent m1, got %q", m2.ParentManifestID)
	}
}

func TestCommitDedupsIdenticalChunk(t *testing.T) {
	t.Parallel()

	repoPath := filepath.Join(t.TempDir(), "repo")
	spoolPath := writeSpoolFile(t, []byte("abc"), 4096)

	_, err := Commit(CommitRequest{
		RepositoryPath: repoPath,
		ManifestID:     "m1",
		SpoolPath:      spoolPath,
		LogicalSize:    4099,
		Spans: []FrameSpan{
			{Kind: SpanKindData, Offset: 0, Length: 3},
			{Kind: SpanKindZero, Offset: 3, Length: 4096},
		},
	})
	if err != nil {
		t.Fatalf("first commit failed: %v", err)
	}

	packPath := filepath.Join(repoPath, "packs", "pack-000001.pack")
	firstInfo, err := os.Stat(packPath)
	if err != nil {
		t.Fatalf("stat first pack: %v", err)
	}

	_, err = Commit(CommitRequest{
		RepositoryPath: repoPath,
		ManifestID:     "m2",
		SpoolPath:      spoolPath,
		LogicalSize:    4099,
		Spans: []FrameSpan{
			{Kind: SpanKindData, Offset: 0, Length: 3},
			{Kind: SpanKindZero, Offset: 3, Length: 4096},
		},
	})
	if err != nil {
		t.Fatalf("second commit failed: %v", err)
	}

	secondInfo, err := os.Stat(packPath)
	if err != nil {
		t.Fatalf("stat second pack: %v", err)
	}
	if secondInfo.Size() != firstInfo.Size() {
		t.Fatalf("expected dedup to avoid pack growth: before=%d after=%d", firstInfo.Size(), secondInfo.Size())
	}
}

func TestCommitRejectsUnsupportedRepositoryVersion(t *testing.T) {
	t.Parallel()

	repoPath := filepath.Join(t.TempDir(), "repo")
	spoolPath := writeSpoolFile(t, []byte("abc"), 0)
	if _, err := Commit(CommitRequest{
		RepositoryPath: repoPath,
		ManifestID:     "m1",
		SpoolPath:      spoolPath,
		LogicalSize:    3,
		Spans:          []FrameSpan{{Kind: SpanKindData, Offset: 0, Length: 3}},
	}); err != nil {
		t.Fatalf("commit m1: %v", err)
	}

	metaPath := filepath.Join(repoPath, "repo.json")
	data, err := os.ReadFile(metaPath)
	if err != nil {
		t.Fatalf("read repo metadata: %v", err)
	}
	var meta map[string]any
	if err := json.Unmarshal(data, &meta); err != nil {
		t.Fatalf("decode repo metadata: %v", err)
	}
	meta["repoVersion"] = float64(2)
	encoded, err := json.MarshalIndent(meta, "", "  ")
	if err != nil {
		t.Fatalf("encode repo metadata: %v", err)
	}
	if err := os.WriteFile(metaPath, append(encoded, '\n'), 0o640); err != nil {
		t.Fatalf("write repo metadata: %v", err)
	}

	_, err = Commit(CommitRequest{
		RepositoryPath: repoPath,
		ManifestID:     "m2",
		SpoolPath:      spoolPath,
		LogicalSize:    3,
		Spans:          []FrameSpan{{Kind: SpanKindData, Offset: 0, Length: 3}},
	})
	if err == nil || !strings.Contains(err.Error(), "unsupported repository version") {
		t.Fatalf("expected unsupported repository version error, got %v", err)
	}
}

func TestCommitWritesManifestDeltaForChangedChunk(t *testing.T) {
	t.Parallel()

	repoPath := filepath.Join(t.TempDir(), "repo")
	spoolA := writeSpoolFile(t, []byte("abc"), 4096)
	spoolB := writeSpoolFile(t, []byte("xyz"), 4096)

	_, err := Commit(CommitRequest{
		RepositoryPath: repoPath,
		ManifestID:     "m1",
		SpoolPath:      spoolA,
		LogicalSize:    4099,
		Spans: []FrameSpan{
			{Kind: SpanKindData, Offset: 0, Length: 3},
			{Kind: SpanKindZero, Offset: 3, Length: 4096},
		},
	})
	if err != nil {
		t.Fatalf("first commit failed: %v", err)
	}

	_, err = Commit(CommitRequest{
		RepositoryPath: repoPath,
		ManifestID:     "m2",
		SpoolPath:      spoolB,
		LogicalSize:    4099,
		Spans: []FrameSpan{
			{Kind: SpanKindData, Offset: 0, Length: 3},
			{Kind: SpanKindZero, Offset: 3, Length: 4096},
		},
	})
	if err != nil {
		t.Fatalf("second commit failed: %v", err)
	}

	m2, err := readManifest(filepath.Join(repoPath, "snapshots", "m2", "manifest.json"))
	if err != nil {
		t.Fatalf("read manifest: %v", err)
	}
	if m2.ParentManifestID != "m1" {
		t.Fatalf("expected parent m1, got %q", m2.ParentManifestID)
	}
	if m2.Stats.NewChunks == 0 {
		t.Fatalf("expected at least one new chunk for changed payload")
	}
}

func TestResolveManifestChainReturnsRootToRequestedInclusive(t *testing.T) {
	t.Parallel()

	repoPath := filepath.Join(t.TempDir(), "repo")
	spoolA := writeSpoolFile(t, []byte("aaa"), 4096)
	spoolB := writeSpoolFile(t, []byte("bbb"), 4096)
	spoolC := writeSpoolFile(t, []byte("ccc"), 4096)

	for _, tc := range []struct {
		manifestID string
		spoolPath  string
	}{
		{manifestID: "m1", spoolPath: spoolA},
		{manifestID: "m2", spoolPath: spoolB},
		{manifestID: "m3", spoolPath: spoolC},
	} {
		if _, err := Commit(CommitRequest{
			RepositoryPath: repoPath,
			ManifestID:     tc.manifestID,
			SpoolPath:      tc.spoolPath,
			LogicalSize:    4099,
			Spans: []FrameSpan{
				{Kind: SpanKindData, Offset: 0, Length: 3},
				{Kind: SpanKindZero, Offset: 3, Length: 4096},
			},
		}); err != nil {
			t.Fatalf("commit %s: %v", tc.manifestID, err)
		}
	}

	chain, err := ResolveManifestChain(repoPath, "m3", ManifestChainOptions{})
	if err != nil {
		t.Fatalf("resolve chain: %v", err)
	}
	want := []string{"m1", "m2", "m3"}
	if !slices.Equal(chain, want) {
		t.Fatalf("unexpected chain: got %v want %v", chain, want)
	}
}

func TestResolveManifestChainValidatesRepositoryPath(t *testing.T) {
	t.Parallel()

	_, err := ResolveManifestChain("relative-repo", "m1", ManifestChainOptions{})
	if err == nil || !strings.Contains(err.Error(), "absolute") {
		t.Fatalf("expected absolute repository path error, got %v", err)
	}
}

func TestResolveManifestChainRejectsUnsafeManifestID(t *testing.T) {
	t.Parallel()

	repoPath := filepath.Join(t.TempDir(), "repo")
	for _, manifestID := range []string{"../escape", "m1/../m2", filepath.Join(string(filepath.Separator), "tmp", "m1")} {
		_, err := ResolveManifestChain(repoPath, manifestID, ManifestChainOptions{})
		if err == nil {
			t.Fatalf("expected unsafe manifest ID %q to fail", manifestID)
		}
	}
}

func TestResolveManifestChainRejectsUnsafeParentManifestID(t *testing.T) {
	t.Parallel()

	repoPath := filepath.Join(t.TempDir(), "repo")
	spoolPath := writeSpoolFile(t, []byte("abc"), 4096)
	if _, err := Commit(CommitRequest{
		RepositoryPath: repoPath,
		ManifestID:     "m1",
		SpoolPath:      spoolPath,
		LogicalSize:    4099,
		Spans:          []FrameSpan{{Kind: SpanKindData, Offset: 0, Length: 3}},
	}); err != nil {
		t.Fatalf("commit m1: %v", err)
	}
	setManifestParentForTest(t, repoPath, "m1", "../escape")

	_, err := ResolveManifestChain(repoPath, "m1", ManifestChainOptions{})
	if err == nil || !strings.Contains(err.Error(), "manifest ID") {
		t.Fatalf("expected unsafe parent manifest ID error, got %v", err)
	}
}

func TestResolveManifestChainDetectsParentCycle(t *testing.T) {
	t.Parallel()

	repoPath := filepath.Join(t.TempDir(), "repo")
	spoolA := writeSpoolFile(t, []byte("abc"), 4096)
	spoolB := writeSpoolFile(t, []byte("xyz"), 4096)
	if _, err := Commit(CommitRequest{
		RepositoryPath: repoPath,
		ManifestID:     "m1",
		SpoolPath:      spoolA,
		LogicalSize:    4099,
		Spans:          []FrameSpan{{Kind: SpanKindData, Offset: 0, Length: 3}},
	}); err != nil {
		t.Fatalf("commit m1: %v", err)
	}
	if _, err := Commit(CommitRequest{
		RepositoryPath: repoPath,
		ManifestID:     "m2",
		SpoolPath:      spoolB,
		LogicalSize:    4099,
		Spans:          []FrameSpan{{Kind: SpanKindData, Offset: 0, Length: 3}},
	}); err != nil {
		t.Fatalf("commit m2: %v", err)
	}
	setManifestParentForTest(t, repoPath, "m1", "m2")

	_, err := ResolveManifestChain(repoPath, "m2", ManifestChainOptions{})
	if err == nil || !strings.Contains(err.Error(), "cycle") {
		t.Fatalf("expected cycle error, got %v", err)
	}
}

func TestResolveManifestChainDetectsDepthLimit(t *testing.T) {
	t.Parallel()

	repoPath := filepath.Join(t.TempDir(), "repo")
	spoolA := writeSpoolFile(t, []byte("abc"), 4096)
	spoolB := writeSpoolFile(t, []byte("xyz"), 4096)
	if _, err := Commit(CommitRequest{
		RepositoryPath: repoPath,
		ManifestID:     "m1",
		SpoolPath:      spoolA,
		LogicalSize:    4099,
		Spans:          []FrameSpan{{Kind: SpanKindData, Offset: 0, Length: 3}},
	}); err != nil {
		t.Fatalf("commit m1: %v", err)
	}
	if _, err := Commit(CommitRequest{
		RepositoryPath: repoPath,
		ManifestID:     "m2",
		SpoolPath:      spoolB,
		LogicalSize:    4099,
		Spans:          []FrameSpan{{Kind: SpanKindData, Offset: 0, Length: 3}},
	}); err != nil {
		t.Fatalf("commit m2: %v", err)
	}

	_, err := ResolveManifestChain(repoPath, "m2", ManifestChainOptions{MaxDepth: 1})
	if err == nil || !strings.Contains(err.Error(), "exceeds max depth") {
		t.Fatalf("expected max depth error, got %v", err)
	}
}

func TestCommitFailureDoesNotPublishNewLatest(t *testing.T) {
	t.Parallel()

	repoPath := filepath.Join(t.TempDir(), "repo")
	spoolPath := writeSpoolFile(t, []byte("abc"), 4096)

	_, err := Commit(CommitRequest{
		RepositoryPath: repoPath,
		ManifestID:     "m1",
		SpoolPath:      spoolPath,
		LogicalSize:    4099,
		Spans:          []FrameSpan{{Kind: SpanKindData, Offset: 0, Length: 3}},
	})
	if err != nil {
		t.Fatalf("first commit failed: %v", err)
	}

	_, err = Commit(CommitRequest{
		RepositoryPath: repoPath,
		ManifestID:     "m2",
		SpoolPath:      filepath.Join(t.TempDir(), "missing-spool"),
		LogicalSize:    4099,
		Spans:          []FrameSpan{{Kind: SpanKindData, Offset: 0, Length: 3}},
	})
	if err == nil {
		t.Fatalf("expected commit failure")
	}

	latest, err := os.ReadFile(filepath.Join(repoPath, "refs", "latest.txt"))
	if err != nil {
		t.Fatalf("read latest ref: %v", err)
	}
	if string(latest) != "m1\n" {
		t.Fatalf("expected latest ref to remain m1, got %q", string(latest))
	}
	if _, err := os.Stat(filepath.Join(repoPath, "snapshots", "m2", "manifest.json")); err == nil {
		t.Fatalf("unexpected m2 manifest after failed commit")
	}
}

func writeSpoolFile(t *testing.T, data []byte, zeroBytes int) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "spool.img")
	f, err := os.Create(path)
	if err != nil {
		t.Fatalf("create spool: %v", err)
	}
	if _, err := f.Write(data); err != nil {
		_ = f.Close()
		t.Fatalf("write data: %v", err)
	}
	if zeroBytes > 0 {
		if _, err := f.Write(make([]byte, zeroBytes)); err != nil {
			_ = f.Close()
			t.Fatalf("write zeros: %v", err)
		}
	}
	if err := f.Sync(); err != nil {
		_ = f.Close()
		t.Fatalf("sync spool: %v", err)
	}
	if err := f.Close(); err != nil {
		t.Fatalf("close spool: %v", err)
	}
	return path
}

func readManifest(path string) (*manifest, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	var m manifest
	if err := json.Unmarshal(data, &m); err != nil {
		return nil, err
	}
	return &m, nil
}

func setManifestParentForTest(t *testing.T, repoPath string, manifestID string, parentManifestID string) {
	t.Helper()
	manifestPath := filepath.Join(repoPath, "snapshots", manifestID, "manifest.json")
	mf, err := readManifest(manifestPath)
	if err != nil {
		t.Fatalf("read manifest %s: %v", manifestID, err)
	}
	mf.ParentManifestID = parentManifestID
	data, err := json.MarshalIndent(mf, "", "  ")
	if err != nil {
		t.Fatalf("marshal manifest %s: %v", manifestID, err)
	}
	if err := os.WriteFile(manifestPath, append(data, '\n'), 0o640); err != nil {
		t.Fatalf("write manifest %s: %v", manifestID, err)
	}
}

func TestCommitReclaimsStaleLock(t *testing.T) {
	t.Parallel()

	repoPath := filepath.Join(t.TempDir(), "repo")
	spoolPath := writeSpoolFile(t, []byte("abc"), 4096)
	if err := ensureRepoDirs(repoPath); err != nil {
		t.Fatalf("ensure repo dirs: %v", err)
	}

	old := os.Getenv(envCASLockStaleAfter)
	t.Cleanup(func() {
		_ = os.Setenv(envCASLockStaleAfter, old)
	})
	_ = os.Setenv(envCASLockStaleAfter, "1s")

	lockPath := filepath.Join(repoPath, "locks", "repo.lock")
	lock := repoLockFile{
		AcquiredAt: time.Now().UTC().Add(-time.Hour).Format(time.RFC3339Nano),
		PID:        1,
	}
	data, err := json.Marshal(lock)
	if err != nil {
		t.Fatalf("marshal lock: %v", err)
	}
	if err := os.WriteFile(lockPath, append(data, '\n'), 0o640); err != nil {
		t.Fatalf("write lock: %v", err)
	}

	if _, err := Commit(CommitRequest{
		RepositoryPath: repoPath,
		ManifestID:     "m1",
		SpoolPath:      spoolPath,
		LogicalSize:    4099,
		Spans: []FrameSpan{
			{Kind: SpanKindData, Offset: 0, Length: 3},
		},
	}); err != nil {
		t.Fatalf("commit should reclaim stale lock: %v", err)
	}

	staleMatches, err := filepath.Glob(lockPath + ".stale-*")
	if err != nil {
		t.Fatalf("glob stale lock: %v", err)
	}
	if len(staleMatches) == 0 {
		t.Fatalf("expected stale lock archive")
	}
}

func TestCommitRejectsFreshLock(t *testing.T) {
	t.Parallel()

	repoPath := filepath.Join(t.TempDir(), "repo")
	spoolPath := writeSpoolFile(t, []byte("abc"), 4096)
	if err := ensureRepoDirs(repoPath); err != nil {
		t.Fatalf("ensure repo dirs: %v", err)
	}

	old := os.Getenv(envCASLockStaleAfter)
	t.Cleanup(func() {
		_ = os.Setenv(envCASLockStaleAfter, old)
	})
	_ = os.Setenv(envCASLockStaleAfter, "1h")

	lockPath := filepath.Join(repoPath, "locks", "repo.lock")
	lock := repoLockFile{
		AcquiredAt: time.Now().UTC().Format(time.RFC3339Nano),
		PID:        1,
	}
	data, err := json.Marshal(lock)
	if err != nil {
		t.Fatalf("marshal lock: %v", err)
	}
	if err := os.WriteFile(lockPath, append(data, '\n'), 0o640); err != nil {
		t.Fatalf("write lock: %v", err)
	}

	_, err = Commit(CommitRequest{
		RepositoryPath: repoPath,
		ManifestID:     "m1",
		SpoolPath:      spoolPath,
		LogicalSize:    4099,
		Spans: []FrameSpan{
			{Kind: SpanKindData, Offset: 0, Length: 3},
		},
	})
	if err == nil {
		t.Fatalf("expected fresh lock to block commit")
	}
	if !strings.Contains(err.Error(), "repository is locked") {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestCompactDropsChunksOnlyReferencedByDeletedManifests(t *testing.T) {
	t.Parallel()

	repoPath := filepath.Join(t.TempDir(), "repo")
	spoolA := writeSpoolFile(t, []byte("abc"), 4096)
	spoolB := writeSpoolFile(t, []byte("xyz"), 4096)

	if _, err := Commit(CommitRequest{
		RepositoryPath: repoPath,
		ManifestID:     "m1",
		SpoolPath:      spoolA,
		LogicalSize:    4099,
		Spans:          []FrameSpan{{Kind: SpanKindData, Offset: 0, Length: 3}, {Kind: SpanKindZero, Offset: 3, Length: 4096}},
	}); err != nil {
		t.Fatalf("commit m1: %v", err)
	}
	if _, err := Commit(CommitRequest{
		RepositoryPath: repoPath,
		ManifestID:     "m2",
		SpoolPath:      spoolB,
		LogicalSize:    4099,
		Spans:          []FrameSpan{{Kind: SpanKindData, Offset: 0, Length: 3}, {Kind: SpanKindZero, Offset: 3, Length: 4096}},
	}); err != nil {
		t.Fatalf("commit m2: %v", err)
	}

	if err := os.RemoveAll(filepath.Join(repoPath, "snapshots", "m1")); err != nil {
		t.Fatalf("remove deleted manifest: %v", err)
	}

	result, err := Compact(repoPath)
	if err != nil {
		t.Fatalf("compact repo: %v", err)
	}
	if result.ManifestCount != 1 {
		t.Fatalf("expected 1 manifest after deletion, got %d", result.ManifestCount)
	}
	if result.ReachableChunkCount != 1 {
		t.Fatalf("expected 1 reachable chunk, got %d", result.ReachableChunkCount)
	}
	if result.ReclaimedChunkCount != 1 {
		t.Fatalf("expected 1 reclaimed chunk, got %d", result.ReclaimedChunkCount)
	}

	active := readActiveIndexForTest(t, filepath.Join(repoPath, "indexes", "active.json"))
	if len(active.Segments) != 1 {
		t.Fatalf("expected 1 active segment, got %d", len(active.Segments))
	}
	entries := readIndexSegmentForTest(t, filepath.Join(repoPath, "indexes", "segments", active.Segments[0]))
	if len(entries) != 1 {
		t.Fatalf("expected 1 index entry after compact, got %d", len(entries))
	}

	packFiles := globCount(t, filepath.Join(repoPath, "packs", "pack-*.pack"))
	if packFiles != 1 {
		t.Fatalf("expected 1 pack file after compact, got %d", packFiles)
	}
	pidxFiles := globCount(t, filepath.Join(repoPath, "packs", "pack-*.pidx"))
	if pidxFiles != 1 {
		t.Fatalf("expected 1 pack index after compact, got %d", pidxFiles)
	}
}

func TestCompactPreservesRestoreForRemainingManifests(t *testing.T) {
	t.Parallel()

	repoPath := filepath.Join(t.TempDir(), "repo")
	spoolA := writeSpoolFile(t, []byte("abc"), 4096)
	spoolB := writeSpoolFile(t, []byte("xyz"), 4096)

	if _, err := Commit(CommitRequest{
		RepositoryPath: repoPath,
		ManifestID:     "m1",
		SpoolPath:      spoolA,
		LogicalSize:    4099,
		Spans:          []FrameSpan{{Kind: SpanKindData, Offset: 0, Length: 3}, {Kind: SpanKindZero, Offset: 3, Length: 4096}},
	}); err != nil {
		t.Fatalf("commit m1: %v", err)
	}
	if _, err := Commit(CommitRequest{
		RepositoryPath: repoPath,
		ManifestID:     "m2",
		SpoolPath:      spoolB,
		LogicalSize:    4099,
		Spans:          []FrameSpan{{Kind: SpanKindData, Offset: 0, Length: 3}, {Kind: SpanKindZero, Offset: 3, Length: 4096}},
	}); err != nil {
		t.Fatalf("commit m2: %v", err)
	}

	result, err := Compact(repoPath)
	if err != nil {
		t.Fatalf("compact repo: %v", err)
	}
	if result.ReachableChunkCount != 2 {
		t.Fatalf("expected 2 reachable chunks, got %d", result.ReachableChunkCount)
	}
	if result.ReclaimedChunkCount != 0 {
		t.Fatalf("expected 0 reclaimed chunks, got %d", result.ReclaimedChunkCount)
	}

	restoreAndCompare(t, repoPath, "m1", []byte("abc"))
	restoreAndCompare(t, repoPath, "m2", []byte("xyz"))
}

func TestPruneKeepsManifestParentClosureAndCompacts(t *testing.T) {
	t.Parallel()

	repoPath := filepath.Join(t.TempDir(), "repo")
	spoolA := writeSpoolFile(t, []byte("aaa"), 4096)
	spoolB := writeSpoolFile(t, []byte("bbb"), 4096)
	spoolC := writeSpoolFile(t, []byte("ccc"), 4096)
	for _, tc := range []struct {
		manifestID string
		spoolPath  string
	}{
		{manifestID: "m1", spoolPath: spoolA},
		{manifestID: "m2", spoolPath: spoolB},
		{manifestID: "m3", spoolPath: spoolC},
	} {
		if _, err := Commit(CommitRequest{
			RepositoryPath: repoPath,
			ManifestID:     tc.manifestID,
			SpoolPath:      tc.spoolPath,
			LogicalSize:    4099,
			Spans:          []FrameSpan{{Kind: SpanKindData, Offset: 0, Length: 3}, {Kind: SpanKindZero, Offset: 3, Length: 4096}},
		}); err != nil {
			t.Fatalf("commit %s: %v", tc.manifestID, err)
		}
	}

	result, err := Prune(PruneRequest{
		RepositoryPath:  repoPath,
		KeepManifestIDs: []string{"m2"},
	})
	if err != nil {
		t.Fatalf("prune: %v", err)
	}
	if !slices.Equal(result.KeptManifestIDs, []string{"m1", "m2"}) {
		t.Fatalf("unexpected kept manifests: %v", result.KeptManifestIDs)
	}
	if !slices.Equal(result.DeletedManifestIDs, []string{"m3"}) {
		t.Fatalf("unexpected deleted manifests: %v", result.DeletedManifestIDs)
	}
	if result.Compaction == nil || result.Compaction.ManifestCount != 2 {
		t.Fatalf("expected compaction over 2 kept manifests, got %#v", result.Compaction)
	}
	if _, err := os.Stat(filepath.Join(repoPath, "snapshots", "m1", "manifest.json")); err != nil {
		t.Fatalf("expected parent manifest m1 to remain: %v", err)
	}
	if _, err := os.Stat(filepath.Join(repoPath, "snapshots", "m3", "manifest.json")); !os.IsNotExist(err) {
		t.Fatalf("expected m3 to be deleted, got %v", err)
	}
	latest, err := os.ReadFile(filepath.Join(repoPath, "refs", "latest.txt"))
	if err != nil {
		t.Fatalf("read latest ref: %v", err)
	}
	if string(latest) != "m2\n" {
		t.Fatalf("expected latest ref to be m2, got %q", string(latest))
	}

	restoreAndCompare(t, repoPath, "m2", []byte("bbb"))
}

func TestPruneCanDeleteAllManifestDirs(t *testing.T) {
	t.Parallel()

	repoPath := filepath.Join(t.TempDir(), "repo")
	spoolPath := writeSpoolFile(t, []byte("abc"), 4096)
	if _, err := Commit(CommitRequest{
		RepositoryPath: repoPath,
		ManifestID:     "m1",
		SpoolPath:      spoolPath,
		LogicalSize:    4099,
		Spans:          []FrameSpan{{Kind: SpanKindData, Offset: 0, Length: 3}, {Kind: SpanKindZero, Offset: 3, Length: 4096}},
	}); err != nil {
		t.Fatalf("commit m1: %v", err)
	}

	result, err := Prune(PruneRequest{RepositoryPath: repoPath})
	if err != nil {
		t.Fatalf("prune all: %v", err)
	}
	if len(result.KeptManifestIDs) != 0 {
		t.Fatalf("expected no kept manifests, got %v", result.KeptManifestIDs)
	}
	if !slices.Equal(result.DeletedManifestIDs, []string{"m1"}) {
		t.Fatalf("unexpected deleted manifests: %v", result.DeletedManifestIDs)
	}
	if _, err := os.Stat(filepath.Join(repoPath, "refs", "latest.txt")); !os.IsNotExist(err) {
		t.Fatalf("expected latest ref to be removed, got %v", err)
	}
	if result.Compaction == nil || result.Compaction.ReachableChunkCount != 0 {
		t.Fatalf("expected empty compaction result, got %#v", result.Compaction)
	}
}

func TestCompactRejectsCorruptedReachablePayload(t *testing.T) {
	t.Parallel()

	repoPath := filepath.Join(t.TempDir(), "repo")
	spoolPath := writeSpoolFile(t, []byte("abc"), 4096)
	if _, err := Commit(CommitRequest{
		RepositoryPath: repoPath,
		ManifestID:     "m1",
		SpoolPath:      spoolPath,
		LogicalSize:    4099,
		Spans:          []FrameSpan{{Kind: SpanKindData, Offset: 0, Length: 3}, {Kind: SpanKindZero, Offset: 3, Length: 4096}},
	}); err != nil {
		t.Fatalf("commit m1: %v", err)
	}

	packPath := filepath.Join(repoPath, "packs", "pack-000001.pack")
	if err := os.WriteFile(packPath, append([]byte("zzz"), make([]byte, 4096)...), 0o640); err != nil {
		t.Fatalf("corrupt pack: %v", err)
	}

	_, err := Compact(repoPath)
	if err == nil || !strings.Contains(err.Error(), "checksum mismatch") {
		t.Fatalf("expected checksum mismatch error, got %v", err)
	}
}

func restoreAndCompare(t *testing.T, repoPath, manifestID string, wantPrefix []byte) {
	t.Helper()

	targetPath := filepath.Join(t.TempDir(), manifestID+".img")
	target, err := os.Create(targetPath)
	if err != nil {
		t.Fatalf("create target: %v", err)
	}
	if err := target.Truncate(4099); err != nil {
		_ = target.Close()
		t.Fatalf("truncate target: %v", err)
	}
	if err := target.Close(); err != nil {
		t.Fatalf("close target: %v", err)
	}

	if err := restorepopulator.RunWorker(restorepopulator.WorkerOptions{
		SourceDir:       repoPath,
		ManifestID:      manifestID,
		TargetDevice:    targetPath,
		Format:          snaplanev1alpha1.RestoreSourceFormatCASV1,
		VolumeSizeBytes: 4099,
		ChunkSizeBytes:  DefaultChunkSize,
	}); err != nil {
		t.Fatalf("restore manifest %s: %v", manifestID, err)
	}

	data, err := os.ReadFile(targetPath)
	if err != nil {
		t.Fatalf("read restored target: %v", err)
	}
	if string(data[:len(wantPrefix)]) != string(wantPrefix) {
		t.Fatalf("restored prefix mismatch for %s: got %q want %q", manifestID, string(data[:len(wantPrefix)]), string(wantPrefix))
	}
}

func readActiveIndexForTest(t *testing.T, path string) *activeIndex {
	t.Helper()
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read active index: %v", err)
	}
	var active activeIndex
	if err := json.Unmarshal(data, &active); err != nil {
		t.Fatalf("decode active index: %v", err)
	}
	return &active
}

func readIndexSegmentForTest(t *testing.T, path string) []indexEntry {
	t.Helper()
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read index segment: %v", err)
	}
	var entries []indexEntry
	if err := json.Unmarshal(data, &entries); err != nil {
		t.Fatalf("decode index segment: %v", err)
	}
	return entries
}

func globCount(t *testing.T, pattern string) int {
	t.Helper()
	matches, err := filepath.Glob(pattern)
	if err != nil {
		t.Fatalf("glob %q: %v", pattern, err)
	}
	return len(matches)
}

package casrepo

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
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

package writer

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/molpako/snaplane/internal/casrepo"
)

func TestListRepositoryPathsFindsPVCScopedRepos(t *testing.T) {
	t.Parallel()

	root := t.TempDir()
	repoA := filepath.Join(root, "default", "pvc-a", "repo")
	repoB := filepath.Join(root, "kube-system", "pvc-b", "repo")
	for _, dir := range []string{
		repoB,
		repoA,
		filepath.Join(root, "default", "pvc-c"),
		filepath.Join(root, "not-a-namespace-file"),
	} {
		if err := os.MkdirAll(dir, 0o750); err != nil {
			t.Fatalf("mkdir %q: %v", dir, err)
		}
	}
	if err := os.WriteFile(filepath.Join(root, "default", "pvc-c", "repo"), []byte("not a dir"), 0o640); err != nil {
		t.Fatalf("write non-dir repo marker: %v", err)
	}

	repos, err := listRepositoryPaths(root)
	if err != nil {
		t.Fatalf("list repository paths: %v", err)
	}

	want := []string{repoA, repoB}
	if strings.Join(repos, "\n") != strings.Join(want, "\n") {
		t.Fatalf("unexpected repos:\ngot:  %v\nwant: %v", repos, want)
	}
}

func TestRunCASMaintenanceOnceCompactsValidReposAndReportsBrokenRepos(t *testing.T) {
	t.Parallel()

	root := t.TempDir()
	validRepo := filepath.Join(root, "default", "pvc-a", "repo")
	brokenRepo := filepath.Join(root, "default", "pvc-b", "repo")
	createCASGeneration(t, validRepo, "m1", []byte("abc"))
	createCASGeneration(t, validRepo, "m2", []byte("xyz"))
	if err := os.RemoveAll(filepath.Join(validRepo, "snapshots", "m1")); err != nil {
		t.Fatalf("remove stale manifest: %v", err)
	}
	if err := os.MkdirAll(brokenRepo, 0o750); err != nil {
		t.Fatalf("mkdir broken repo: %v", err)
	}
	if err := os.WriteFile(filepath.Join(brokenRepo, "repo.json"), []byte("{not-json"), 0o640); err != nil {
		t.Fatalf("write broken repo metadata: %v", err)
	}

	err := runCASMaintenanceOnce(root)
	if err == nil {
		t.Fatalf("expected broken repo to be reported")
	}
	if !strings.Contains(err.Error(), brokenRepo) {
		t.Fatalf("expected error to include broken repo path, got %v", err)
	}

	active := readActiveIndexForMaintenanceTest(t, filepath.Join(validRepo, "indexes", "active.json"))
	if len(active.Segments) != 1 {
		t.Fatalf("expected valid repo to be compacted despite broken peer, active segments=%v", active.Segments)
	}
	entries := readIndexSegmentForMaintenanceTest(t, filepath.Join(validRepo, "indexes", "segments", active.Segments[0]))
	if len(entries) != 1 {
		t.Fatalf("expected compacted valid repo to keep only one reachable chunk, got %d entries", len(entries))
	}
	snapshot := CurrentCASMaintenanceSnapshot()
	if snapshot.LastRunAt.IsZero() {
		t.Fatalf("expected maintenance snapshot timestamp")
	}
	if snapshot.RepositoryRuns != 1 {
		t.Fatalf("expected one successful repo run, got %d", snapshot.RepositoryRuns)
	}
	if !strings.Contains(snapshot.LastError, brokenRepo) {
		t.Fatalf("expected snapshot error to include broken repo path, got %q", snapshot.LastError)
	}
}

func createCASGeneration(t *testing.T, repoPath, manifestID string, payload []byte) {
	t.Helper()
	spoolPath := filepath.Join(t.TempDir(), "spool.img")
	if err := os.WriteFile(spoolPath, append(payload, make([]byte, 4096)...), 0o640); err != nil {
		t.Fatalf("write spool: %v", err)
	}
	if _, err := casrepo.Commit(casrepo.CommitRequest{
		RepositoryPath: repoPath,
		ManifestID:     manifestID,
		SpoolPath:      spoolPath,
		LogicalSize:    int64(len(payload) + 4096),
		Spans: []casrepo.FrameSpan{
			{Kind: casrepo.SpanKindData, Offset: 0, Length: int64(len(payload))},
			{Kind: casrepo.SpanKindZero, Offset: int64(len(payload)), Length: 4096},
		},
	}); err != nil {
		t.Fatalf("commit %s: %v", manifestID, err)
	}
}

func readActiveIndexForMaintenanceTest(t *testing.T, path string) struct {
	Segments []string `json:"segments"`
} {
	t.Helper()
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read active index: %v", err)
	}
	var active struct {
		Segments []string `json:"segments"`
	}
	if err := json.Unmarshal(data, &active); err != nil {
		t.Fatalf("decode active index: %v", err)
	}
	return active
}

func readIndexSegmentForMaintenanceTest(t *testing.T, path string) []struct {
	ChunkID string `json:"chunkID"`
} {
	t.Helper()
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read index segment: %v", err)
	}
	var entries []struct {
		ChunkID string `json:"chunkID"`
	}
	if err := json.Unmarshal(data, &entries); err != nil {
		t.Fatalf("decode index segment: %v", err)
	}
	return entries
}

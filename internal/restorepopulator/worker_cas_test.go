package restorepopulator

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"

	snaplanev1alpha1 "github.com/molpako/snaplane/api/v1alpha1"
	"github.com/molpako/snaplane/internal/casrepo"
)

func TestRunWorkerCASRestoresFirstGeneration(t *testing.T) {
	t.Parallel()

	repoPath := filepath.Join(t.TempDir(), "repo")
	expected := []byte("hello-phase2")
	commitCASGeneration(t, repoPath, "m1", expected)

	targetPath := filepath.Join(t.TempDir(), "target.img")
	prepareTargetFile(t, targetPath, len(expected), 0x7f)

	err := RunWorker(WorkerOptions{
		SourceDir:       repoPath,
		ManifestID:      "m1",
		TargetDevice:    targetPath,
		Format:          snaplanev1alpha1.RestoreSourceFormatCASV1,
		VolumeSizeBytes: int64(len(expected)),
		ChunkSizeBytes:  casrepo.DefaultChunkSize,
	})
	if err != nil {
		t.Fatalf("RunWorker returned error: %v", err)
	}

	got := readFile(t, targetPath)
	if string(got) != string(expected) {
		t.Fatalf("target payload mismatch: got %q want %q", string(got), string(expected))
	}
}

func TestRunWorkerCASRestoresSecondGeneration(t *testing.T) {
	t.Parallel()

	repoPath := filepath.Join(t.TempDir(), "repo")
	base := []byte("hello-phase2")
	second := []byte("HELLO-phase2")
	commitCASGeneration(t, repoPath, "m1", base)
	commitCASGeneration(t, repoPath, "m2", second)

	targetPath := filepath.Join(t.TempDir(), "target.img")
	prepareTargetFile(t, targetPath, len(second), 0x7f)

	err := RunWorker(WorkerOptions{
		SourceDir:       repoPath,
		ManifestID:      "m2",
		TargetDevice:    targetPath,
		Format:          snaplanev1alpha1.RestoreSourceFormatCASV1,
		VolumeSizeBytes: int64(len(second)),
		ChunkSizeBytes:  casrepo.DefaultChunkSize,
	})
	if err != nil {
		t.Fatalf("RunWorker returned error: %v", err)
	}

	got := readFile(t, targetPath)
	if string(got) != string(second) {
		t.Fatalf("target payload mismatch: got %q want %q", string(got), string(second))
	}
}

func TestRunWorkerCASZeroFillsFreedChunks(t *testing.T) {
	t.Parallel()

	repoPath := filepath.Join(t.TempDir(), "repo")
	original := []byte("hello-phase2")
	commitCASGeneration(t, repoPath, "m1", original)
	commitCASGeneration(t, repoPath, "m2", make([]byte, len(original)))

	targetPath := filepath.Join(t.TempDir(), "target.img")
	prepareTargetFile(t, targetPath, len(original), 0x7f)

	err := RunWorker(WorkerOptions{
		SourceDir:       repoPath,
		ManifestID:      "m2",
		TargetDevice:    targetPath,
		Format:          snaplanev1alpha1.RestoreSourceFormatCASV1,
		VolumeSizeBytes: int64(len(original)),
		ChunkSizeBytes:  casrepo.DefaultChunkSize,
	})
	if err != nil {
		t.Fatalf("RunWorker returned error: %v", err)
	}

	got := readFile(t, targetPath)
	for i := range got {
		if got[i] != 0 {
			t.Fatalf("expected zero-filled target, found non-zero byte at index %d", i)
		}
	}
}

func TestRunWorkerCASErrorsOnMissingManifest(t *testing.T) {
	t.Parallel()

	repoPath := filepath.Join(t.TempDir(), "repo")
	commitCASGeneration(t, repoPath, "m1", []byte("seed"))
	targetPath := filepath.Join(t.TempDir(), "target.img")
	prepareTargetFile(t, targetPath, 16, 0)

	err := RunWorker(WorkerOptions{
		SourceDir:       repoPath,
		ManifestID:      "missing-manifest",
		TargetDevice:    targetPath,
		Format:          snaplanev1alpha1.RestoreSourceFormatCASV1,
		VolumeSizeBytes: 16,
		ChunkSizeBytes:  casrepo.DefaultChunkSize,
	})
	if err == nil || !strings.Contains(err.Error(), "manifest") {
		t.Fatalf("expected missing manifest error, got %v", err)
	}
}

func TestRunWorkerCASRejectsUnsupportedRepoVersion(t *testing.T) {
	t.Parallel()

	repoPath := filepath.Join(t.TempDir(), "repo")
	payload := []byte("hello-phase2")
	commitCASGeneration(t, repoPath, "m1", payload)

	repoMetaPath := filepath.Join(repoPath, "repo.json")
	data := readFile(t, repoMetaPath)
	var decoded map[string]any
	if err := json.Unmarshal(data, &decoded); err != nil {
		t.Fatalf("decode repo metadata: %v", err)
	}
	decoded["repoVersion"] = float64(2)
	encoded, err := json.MarshalIndent(decoded, "", "  ")
	if err != nil {
		t.Fatalf("encode repo metadata: %v", err)
	}
	if err := os.WriteFile(repoMetaPath, append(encoded, '\n'), 0o600); err != nil {
		t.Fatalf("write repo metadata: %v", err)
	}

	targetPath := filepath.Join(t.TempDir(), "target.img")
	prepareTargetFile(t, targetPath, len(payload), 0x7f)

	err = RunWorker(WorkerOptions{
		SourceDir:       repoPath,
		ManifestID:      "m1",
		TargetDevice:    targetPath,
		Format:          snaplanev1alpha1.RestoreSourceFormatCASV1,
		VolumeSizeBytes: int64(len(payload)),
		ChunkSizeBytes:  casrepo.DefaultChunkSize,
	})
	if err == nil || !strings.Contains(err.Error(), "unsupported repository version") {
		t.Fatalf("expected unsupported repository version error, got %v", err)
	}
}

func TestRunWorkerCASErrorsOnCorruptedIndexOrPack(t *testing.T) {
	t.Parallel()

	t.Run("missing active index", func(t *testing.T) {
		t.Parallel()
		repoPath := filepath.Join(t.TempDir(), "repo")
		payload := []byte("hello-phase2")
		commitCASGeneration(t, repoPath, "m1", payload)
		targetPath := filepath.Join(t.TempDir(), "target.img")
		prepareTargetFile(t, targetPath, len(payload), 0x7f)

		if err := os.Remove(filepath.Join(repoPath, "indexes", "active.json")); err != nil {
			t.Fatalf("remove active index: %v", err)
		}

		err := RunWorker(WorkerOptions{
			SourceDir:       repoPath,
			ManifestID:      "m1",
			TargetDevice:    targetPath,
			Format:          snaplanev1alpha1.RestoreSourceFormatCASV1,
			VolumeSizeBytes: int64(len(payload)),
			ChunkSizeBytes:  casrepo.DefaultChunkSize,
		})
		if err == nil || !strings.Contains(err.Error(), "active index") {
			t.Fatalf("expected active index error, got %v", err)
		}
	})

	t.Run("missing pack file", func(t *testing.T) {
		t.Parallel()
		repoPath := filepath.Join(t.TempDir(), "repo")
		payload := []byte("hello-phase2")
		commitCASGeneration(t, repoPath, "m1", payload)
		targetPath := filepath.Join(t.TempDir(), "target.img")
		prepareTargetFile(t, targetPath, len(payload), 0x7f)

		if err := os.Remove(filepath.Join(repoPath, "packs", "pack-000001.pack")); err != nil {
			t.Fatalf("remove pack: %v", err)
		}

		err := RunWorker(WorkerOptions{
			SourceDir:       repoPath,
			ManifestID:      "m1",
			TargetDevice:    targetPath,
			Format:          snaplanev1alpha1.RestoreSourceFormatCASV1,
			VolumeSizeBytes: int64(len(payload)),
			ChunkSizeBytes:  casrepo.DefaultChunkSize,
		})
		if err == nil || !strings.Contains(err.Error(), "open pack file") {
			t.Fatalf("expected pack open error, got %v", err)
		}
	})
}

func commitCASGeneration(t *testing.T, repoPath string, manifestID string, payload []byte) {
	t.Helper()
	spool := writeSpool(t, payload)
	_, err := casrepo.Commit(casrepo.CommitRequest{
		RepositoryPath: repoPath,
		ManifestID:     manifestID,
		SpoolPath:      spool,
		LogicalSize:    int64(len(payload)),
		Spans: []casrepo.FrameSpan{
			{Kind: casrepo.SpanKindData, Offset: 0, Length: int64(len(payload))},
		},
	})
	if err != nil {
		t.Fatalf("commit cas generation %s: %v", manifestID, err)
	}
}

func writeSpool(t *testing.T, payload []byte) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "spool.img")
	if err := os.WriteFile(path, payload, 0o600); err != nil {
		t.Fatalf("write spool file: %v", err)
	}
	return path
}

func prepareTargetFile(t *testing.T, path string, size int, fill byte) {
	t.Helper()
	buf := make([]byte, size)
	for i := range buf {
		buf[i] = fill
	}
	if err := os.WriteFile(path, buf, 0o600); err != nil {
		t.Fatalf("prepare target: %v", err)
	}
}

func readFile(t *testing.T, path string) []byte {
	t.Helper()
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read file %q: %v", path, err)
	}
	return data
}

func TestRunWorkerCASErrorsOnParentCycle(t *testing.T) {
	t.Parallel()

	repoPath := filepath.Join(t.TempDir(), "repo")
	payload := []byte("hello-phase2")
	commitCASGeneration(t, repoPath, "m1", payload)

	manifestPath := filepath.Join(repoPath, "snapshots", "m1", "manifest.json")
	data := readFile(t, manifestPath)
	var decoded map[string]any
	if err := json.Unmarshal(data, &decoded); err != nil {
		t.Fatalf("decode manifest: %v", err)
	}
	decoded["parentManifestID"] = "m1"
	encoded, err := json.MarshalIndent(decoded, "", "  ")
	if err != nil {
		t.Fatalf("encode manifest: %v", err)
	}
	if err := os.WriteFile(manifestPath, append(encoded, '\n'), 0o600); err != nil {
		t.Fatalf("write manifest: %v", err)
	}

	targetPath := filepath.Join(t.TempDir(), "target.img")
	prepareTargetFile(t, targetPath, len(payload), 0x7f)

	err = RunWorker(WorkerOptions{
		SourceDir:       repoPath,
		ManifestID:      "m1",
		TargetDevice:    targetPath,
		Format:          snaplanev1alpha1.RestoreSourceFormatCASV1,
		VolumeSizeBytes: int64(len(payload)),
		ChunkSizeBytes:  casrepo.DefaultChunkSize,
	})
	if err == nil || !strings.Contains(err.Error(), "cycle") {
		t.Fatalf("expected parent cycle error, got %v", err)
	}
}

func TestRunWorkerCASErrorsOnChunkChecksumMismatch(t *testing.T) {
	t.Parallel()

	repoPath := filepath.Join(t.TempDir(), "repo")
	payload := []byte("hello-phase2")
	commitCASGeneration(t, repoPath, "m1", payload)

	segmentPath := filepath.Join(repoPath, "indexes", "segments", "idx-000001.seg")
	data := readFile(t, segmentPath)
	var entries []map[string]any
	if err := json.Unmarshal(data, &entries); err != nil {
		t.Fatalf("decode segment: %v", err)
	}
	if len(entries) == 0 {
		t.Fatalf("expected index entries")
	}
	badChunkID := "ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff"
	entries[0]["chunkID"] = badChunkID
	encoded, err := json.MarshalIndent(entries, "", "  ")
	if err != nil {
		t.Fatalf("encode segment: %v", err)
	}
	if err := os.WriteFile(segmentPath, append(encoded, '\n'), 0o600); err != nil {
		t.Fatalf("write segment: %v", err)
	}
	changePath := filepath.Join(repoPath, "snapshots", "m1", "seg-change.bin.zst")
	changeData := readFile(t, changePath)
	var changes []map[string]any
	if err := json.Unmarshal(changeData, &changes); err != nil {
		t.Fatalf("decode change segment: %v", err)
	}
	if len(changes) == 0 {
		t.Fatalf("expected change records")
	}
	changes[0]["chunkID"] = badChunkID
	changeEncoded, err := json.MarshalIndent(changes, "", "  ")
	if err != nil {
		t.Fatalf("encode change segment: %v", err)
	}
	if err := os.WriteFile(changePath, append(changeEncoded, '\n'), 0o600); err != nil {
		t.Fatalf("write change segment: %v", err)
	}

	targetPath := filepath.Join(t.TempDir(), "target.img")
	prepareTargetFile(t, targetPath, len(payload), 0x7f)

	err = RunWorker(WorkerOptions{
		SourceDir:       repoPath,
		ManifestID:      "m1",
		TargetDevice:    targetPath,
		Format:          snaplanev1alpha1.RestoreSourceFormatCASV1,
		VolumeSizeBytes: int64(len(payload)),
		ChunkSizeBytes:  casrepo.DefaultChunkSize,
	})
	if err == nil || !strings.Contains(err.Error(), "checksum mismatch") {
		t.Fatalf("expected checksum mismatch error, got %v", err)
	}
}

func TestRunWorkerCASErrorsWhenManifestDepthExceedsLimit(t *testing.T) {
	old := os.Getenv(envCASMaxChainDepth)
	t.Cleanup(func() {
		_ = os.Setenv(envCASMaxChainDepth, old)
	})
	_ = os.Setenv(envCASMaxChainDepth, "1")

	repoPath := filepath.Join(t.TempDir(), "repo")
	base := []byte("hello-phase2")
	next := []byte("HELLO-phase2")
	commitCASGeneration(t, repoPath, "m1", base)
	commitCASGeneration(t, repoPath, "m2", next)

	targetPath := filepath.Join(t.TempDir(), "target.img")
	prepareTargetFile(t, targetPath, len(next), 0x7f)
	err := RunWorker(WorkerOptions{
		SourceDir:       repoPath,
		ManifestID:      "m2",
		TargetDevice:    targetPath,
		Format:          snaplanev1alpha1.RestoreSourceFormatCASV1,
		VolumeSizeBytes: int64(len(next)),
		ChunkSizeBytes:  casrepo.DefaultChunkSize,
	})
	if err == nil || !strings.Contains(err.Error(), "exceeds max depth") {
		t.Fatalf("expected max depth error, got %v", err)
	}
}

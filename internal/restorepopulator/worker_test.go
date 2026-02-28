package restorepopulator

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestRunWorkerCopiesSourceToTarget(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	sourceDir := filepath.Join(dir, "repo")
	if err := os.MkdirAll(sourceDir, 0o755); err != nil {
		t.Fatalf("mkdir source dir: %v", err)
	}
	sourcePath := filepath.Join(sourceDir, "mock.img")
	sourcePayload := []byte("hello-snaplane")
	if err := os.WriteFile(sourcePath, sourcePayload, 0o600); err != nil {
		t.Fatalf("write source file: %v", err)
	}

	targetPath := filepath.Join(dir, "target.img")
	if err := os.WriteFile(targetPath, make([]byte, len(sourcePayload)), 0o600); err != nil {
		t.Fatalf("create target file: %v", err)
	}

	err := RunWorker(WorkerOptions{
		SourceDir:    sourceDir,
		ManifestID:   "mock.img",
		TargetDevice: targetPath,
	})
	if err != nil {
		t.Fatalf("RunWorker returned error: %v", err)
	}

	content, err := os.ReadFile(targetPath)
	if err != nil {
		t.Fatalf("read target file: %v", err)
	}
	if string(content) != string(sourcePayload) {
		t.Fatalf("target payload mismatch: got %q want %q", string(content), string(sourcePayload))
	}
}

func TestRunWorkerValidation(t *testing.T) {
	t.Parallel()

	testCases := []struct {
		name      string
		opts      WorkerOptions
		expectErr string
	}{
		{
			name:      "requires source dir",
			opts:      WorkerOptions{ManifestID: "m", TargetDevice: "/tmp/t"},
			expectErr: "source-dir is required",
		},
		{
			name:      "requires manifest id",
			opts:      WorkerOptions{SourceDir: "/tmp/s", TargetDevice: "/tmp/t"},
			expectErr: "manifest-id is required",
		},
		{
			name:      "requires target device",
			opts:      WorkerOptions{SourceDir: "/tmp/s", ManifestID: "m"},
			expectErr: "target-device is required",
		},
		{
			name:      "requires absolute source dir",
			opts:      WorkerOptions{SourceDir: "relative", ManifestID: "m", TargetDevice: "/tmp/t"},
			expectErr: "source-dir must be absolute",
		},
		{
			name:      "rejects manifest traversal",
			opts:      WorkerOptions{SourceDir: "/tmp/s", ManifestID: "../escape", TargetDevice: "/tmp/t"},
			expectErr: "escapes source-dir",
		},
	}

	for _, tc := range testCases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			err := RunWorker(tc.opts)
			if err == nil || !strings.Contains(err.Error(), tc.expectErr) {
				t.Fatalf("expected error containing %q, got %v", tc.expectErr, err)
			}
		})
	}
}

func TestRunWorkerExecutionErrors(t *testing.T) {
	t.Parallel()

	testCases := []struct {
		name    string
		setup   func(t *testing.T) WorkerOptions
		wantErr string
	}{
		{
			name: "fails when source missing",
			setup: func(t *testing.T) WorkerOptions {
				t.Helper()
				dir := t.TempDir()
				targetPath := filepath.Join(dir, "target.img")
				if err := os.WriteFile(targetPath, []byte("x"), 0o600); err != nil {
					t.Fatalf("create target file: %v", err)
				}
				return WorkerOptions{
					SourceDir:    dir,
					ManifestID:   "missing.img",
					TargetDevice: targetPath,
				}
			},
			wantErr: "open source data",
		},
		{
			name: "fails when target open returns error",
			setup: func(t *testing.T) WorkerOptions {
				t.Helper()
				dir := t.TempDir()
				sourcePath := filepath.Join(dir, "source.bin")
				if err := os.WriteFile(sourcePath, []byte("data"), 0o600); err != nil {
					t.Fatalf("write source file: %v", err)
				}
				return WorkerOptions{
					SourceDir:    dir,
					ManifestID:   filepath.Base(sourcePath),
					TargetDevice: filepath.Join(dir, "missing", "target.img"),
				}
			},
			wantErr: "open target device",
		},
	}

	for _, tc := range testCases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			opts := tc.setup(t)
			err := RunWorker(opts)
			if err == nil || !strings.Contains(err.Error(), tc.wantErr) {
				t.Fatalf("expected error containing %q, got %v", tc.wantErr, err)
			}
		})
	}
}

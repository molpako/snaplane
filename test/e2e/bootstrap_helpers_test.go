package e2e

import (
	"os"
	"testing"

	"sigs.k8s.io/e2e-framework/pkg/envconf"
)

func TestResolveTLSMode(t *testing.T) {
	t.Setenv(envE2ETLSMode, "")
	mode, err := resolveTLSMode()
	if err != nil {
		t.Fatalf("resolve default TLS mode: %v", err)
	}
	if mode != tlsModeStatic {
		t.Fatalf("expected default mode %q, got %q", tlsModeStatic, mode)
	}

	t.Setenv(envE2ETLSMode, "cert-manager")
	mode, err = resolveTLSMode()
	if err != nil {
		t.Fatalf("resolve cert-manager TLS mode: %v", err)
	}
	if mode != tlsModeCertManager {
		t.Fatalf("expected mode %q, got %q", tlsModeCertManager, mode)
	}

	t.Setenv(envE2ETLSMode, "invalid")
	if _, err := resolveTLSMode(); err == nil {
		t.Fatalf("expected invalid TLS mode to fail")
	}
}

func TestBindAndRestoreActiveKubeconfig(t *testing.T) {
	t.Cleanup(func() {
		restoreActiveKubeconfig()
	})

	t.Setenv("KUBECONFIG", "/tmp/original-kubeconfig")
	originalKubeconfig = ""
	hadOriginalKubeconfig = false
	boundActiveKubeconfig = false

	cfg := envconf.NewWithKubeConfig("/tmp/hermetic-kubeconfig")
	if err := bindActiveKubeconfig(cfg); err != nil {
		t.Fatalf("bind active kubeconfig: %v", err)
	}

	if got := os.Getenv("KUBECONFIG"); got != "/tmp/hermetic-kubeconfig" {
		t.Fatalf("expected bound KUBECONFIG %q, got %q", "/tmp/hermetic-kubeconfig", got)
	}

	restoreActiveKubeconfig()
	if got := os.Getenv("KUBECONFIG"); got != "/tmp/original-kubeconfig" {
		t.Fatalf("expected restored KUBECONFIG %q, got %q", "/tmp/original-kubeconfig", got)
	}
}

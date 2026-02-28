package e2e

import (
	"fmt"
	"os"
	"strings"

	"sigs.k8s.io/e2e-framework/pkg/envconf"
)

const envE2ETLSMode = "E2E_TLS_MODE"

type tlsMode string

const (
	tlsModeStatic      tlsMode = "static"
	tlsModeCertManager tlsMode = "cert-manager"
)

var (
	originalKubeconfig    string
	hadOriginalKubeconfig bool
	boundActiveKubeconfig bool
)

func resolveTLSMode() (tlsMode, error) {
	raw := strings.TrimSpace(strings.ToLower(os.Getenv(envE2ETLSMode)))
	switch raw {
	case "", string(tlsModeStatic), "fast":
		return tlsModeStatic, nil
	case string(tlsModeCertManager), "nightly":
		return tlsModeCertManager, nil
	default:
		return "", fmt.Errorf("invalid %s=%q (supported: %s, %s)", envE2ETLSMode, raw, tlsModeStatic, tlsModeCertManager)
	}
}

func bindActiveKubeconfig(cfg *envconf.Config) error {
	kubeconfig := strings.TrimSpace(cfg.KubeconfigFile())
	if kubeconfig == "" {
		return fmt.Errorf("bind active kubeconfig: empty kubeconfig file")
	}
	if !boundActiveKubeconfig {
		originalKubeconfig, hadOriginalKubeconfig = os.LookupEnv("KUBECONFIG")
		boundActiveKubeconfig = true
	}
	if err := os.Setenv("KUBECONFIG", kubeconfig); err != nil {
		return fmt.Errorf("bind active kubeconfig: %w", err)
	}
	return nil
}

func restoreActiveKubeconfig() {
	if !boundActiveKubeconfig {
		return
	}
	if hadOriginalKubeconfig {
		_ = os.Setenv("KUBECONFIG", originalKubeconfig)
	} else {
		_ = os.Unsetenv("KUBECONFIG")
	}
	originalKubeconfig = ""
	hadOriginalKubeconfig = false
	boundActiveKubeconfig = false
}

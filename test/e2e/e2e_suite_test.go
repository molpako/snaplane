//go:build e2e
// +build e2e

/*
Copyright 2026.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package e2e

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"fmt"
	"log"
	"math/big"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	smsv1alpha1 "github.com/kubernetes-csi/external-snapshot-metadata/client/apis/snapshotmetadataservice/v1alpha1"
	smsclientset "github.com/kubernetes-csi/external-snapshot-metadata/client/clientset/versioned"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	ctrlconfig "sigs.k8s.io/controller-runtime/pkg/client/config"
	"sigs.k8s.io/e2e-framework/klient/conf"
	k8sresources "sigs.k8s.io/e2e-framework/klient/k8s/resources"
	"sigs.k8s.io/e2e-framework/klient/wait"
	"sigs.k8s.io/e2e-framework/klient/wait/conditions"
	"sigs.k8s.io/e2e-framework/pkg/env"
	"sigs.k8s.io/e2e-framework/pkg/envconf"
	"sigs.k8s.io/e2e-framework/pkg/envfuncs"
	"sigs.k8s.io/e2e-framework/support/kind"

	"github.com/molpako/snaplane/test/utils"
)

const (
	namespace               = "snaplane-system"
	managerDeploymentName   = "snaplane-controller-manager"
	snapshotterVersion      = "v8.2.0"
	snapshotMetadataVersion = "v0.1.0"

	// hostPathRepoURL is the upstream source that supplies deploy manifests for the
	// nightly hostpath snapshot metadata e2e flow. The repository is cloned into a
	// local cache directory unless the user overrides it via SNAPLANE_HOSTPATH_REPO_ROOT.
	// The cached checkout is pinned to hostPathRepoRevision for reproducible nightly runs.
	hostPathRepoURL         = "https://github.com/kubernetes-csi/csi-driver-host-path.git"
	hostPathRepoRevision    = "v1.17.0"
	hostPathRepoRootEnv     = "SNAPLANE_HOSTPATH_REPO_ROOT"
	hostPathRepoURLEnv      = "SNAPLANE_HOSTPATH_REPO_URL"
	hostPathRepoRevisionEnv = "SNAPLANE_HOSTPATH_REPO_REVISION"
	hostPathRepoCacheDir    = "test/e2e/.hostpath-driver"
	managerLeaderLeaseName  = "b7cc5d87.molpako.github.io"
	realClusterEnv          = "E2E_REAL_CLUSTER"
	realCBTProviderEnv      = "E2E_USE_REAL_CBT_PROVIDER"
	storageClassEnv         = "E2E_STORAGE_CLASS"
	volumeSnapshotClassEnv  = "E2E_VOLUME_SNAPSHOT_CLASS"
)

var (
	testenv                     env.Environment
	installedCertManager        bool
	installedSnapshotController bool
	kubeAPIReachable            bool
	activeTLSMode               = tlsModeStatic
	activeKindClusterName       string
	hostPathInstalled           bool
	hostPathGitBefore           string
	hostPathRepoPath            string
	projectImage                = envOrDefault("IMG", "ghcr.io/molpako/snaplane:v0.0.1")
)

var snapshotCRDURLs = []string{
	fmt.Sprintf("https://raw.githubusercontent.com/kubernetes-csi/external-snapshotter/%s/client/config/crd/snapshot.storage.k8s.io_volumesnapshotclasses.yaml", snapshotterVersion),
	fmt.Sprintf("https://raw.githubusercontent.com/kubernetes-csi/external-snapshotter/%s/client/config/crd/snapshot.storage.k8s.io_volumesnapshotcontents.yaml", snapshotterVersion),
	fmt.Sprintf("https://raw.githubusercontent.com/kubernetes-csi/external-snapshotter/%s/client/config/crd/snapshot.storage.k8s.io_volumesnapshots.yaml", snapshotterVersion),
}

var snapshotMetadataCRDURL = fmt.Sprintf(
	"https://raw.githubusercontent.com/kubernetes-csi/external-snapshot-metadata/%s/client/config/crd/cbt.storage.k8s.io_snapshotmetadataservices.yaml",
	snapshotMetadataVersion,
)

var (
	snapshotMetadataServiceCRURL = fmt.Sprintf(
		"https://raw.githubusercontent.com/kubernetes-csi/external-snapshot-metadata/%s/deploy/example/csi-driver/testdata/snapshotmetadataservice.yaml",
		snapshotMetadataVersion,
	)
	snapshotMetadataServiceURL = fmt.Sprintf(
		"https://raw.githubusercontent.com/kubernetes-csi/external-snapshot-metadata/%s/deploy/example/csi-driver/testdata/csi-snapshot-metadata-service.yaml",
		snapshotMetadataVersion,
	)
	snapshotMetadataRBACURL = fmt.Sprintf(
		"https://raw.githubusercontent.com/kubernetes-csi/external-snapshot-metadata/%s/deploy/snapshot-metadata-cluster-role.yaml",
		snapshotMetadataVersion,
	)
	snapshotControllerRBACURL = fmt.Sprintf(
		"https://raw.githubusercontent.com/kubernetes-csi/external-snapshotter/%s/deploy/kubernetes/snapshot-controller/rbac-snapshot-controller.yaml",
		snapshotterVersion,
	)
	snapshotControllerSetupURL = fmt.Sprintf(
		"https://raw.githubusercontent.com/kubernetes-csi/external-snapshotter/%s/deploy/kubernetes/snapshot-controller/setup-snapshot-controller.yaml",
		snapshotterVersion,
	)
)

func TestMain(m *testing.M) {
	mode, err := resolveTLSMode()
	if err != nil {
		log.Printf("resolve e2e TLS mode: %v", err)
		os.Exit(1)
	}
	activeTLSMode = mode

	switch activeTLSMode {
	case tlsModeStatic:
		testenv = env.New()
		activeKindClusterName = envconf.RandomName("snaplane-fast", 16)
		testenv.Setup(
			envfuncs.CreateCluster(kind.NewProvider(), activeKindClusterName),
			setupEnvironment,
		)
		testenv.Finish(
			cleanupEnvironment,
			envfuncs.DestroyCluster(activeKindClusterName),
		)
	case tlsModeCertManager:
		path := conf.ResolveKubeConfigFile()
		cfg := envconf.NewWithKubeConfig(path)
		testenv = env.NewWithConfig(cfg)
		testenv.Setup(setupEnvironment)
		testenv.Finish(cleanupEnvironment)
	default:
		log.Printf("unsupported e2e TLS mode: %q", activeTLSMode)
		os.Exit(1)
	}

	os.Exit(testenv.Run(m))
}

func setupEnvironment(ctx context.Context, cfg *envconf.Config) (context.Context, error) {
	if err := bindActiveKubeconfig(cfg); err != nil {
		return ctx, err
	}
	if err := preflightE2EEnvironment(); err != nil {
		return ctx, err
	}

	log.Printf("e2e TLS mode: %s", activeTLSMode)

	if !isRealClusterLane() {
		if _, err := utils.Run(exec.Command("make", "docker-build", fmt.Sprintf("IMG=%s", projectImage))); err != nil {
			return ctx, fmt.Errorf("build manager image: %w", err)
		}

		if isHermeticFastLane() {
			ctx, err := envfuncs.LoadImageToCluster(activeKindClusterName, projectImage)(ctx, cfg)
			if err != nil {
				return ctx, fmt.Errorf("load image to hermetic kind cluster: %w", err)
			}
		} else {
			if err := utils.LoadImageToKindClusterWithName(projectImage); err != nil {
				return ctx, fmt.Errorf("load image to kind: %w", err)
			}
		}
	}

	if _, err := utils.Run(exec.Command("kubectl", "create", "ns", namespace)); err != nil {
		if !strings.Contains(err.Error(), "AlreadyExists") {
			return ctx, fmt.Errorf("create namespace: %w", err)
		}
		log.Printf("namespace %q already exists, continuing", namespace)
	}
	kubeAPIReachable = true

	if err := ensureSnapshotCRDsInstalled(); err != nil {
		return ctx, err
	}

	switch activeTLSMode {
	case tlsModeCertManager:
		if !isRealClusterLane() {
			if err := setupNightlyHostPathSMS(ctx); err != nil {
				return ctx, err
			}
		}
		if !utils.IsCertManagerCRDsInstalled() {
			if err := utils.InstallCertManager(); err != nil {
				return ctx, fmt.Errorf("install cert-manager: %w", err)
			}
			installedCertManager = true
		}
		if err := waitForCertManagerWebhookReady(); err != nil {
			return ctx, err
		}
	case tlsModeStatic:
		if err := ensureStaticMTLSSecrets(ctx, cfg); err != nil {
			return ctx, fmt.Errorf("ensure static mTLS secrets: %w", err)
		}
	default:
		return ctx, fmt.Errorf("unsupported TLS mode: %q", activeTLSMode)
	}

	if _, err := utils.Run(exec.Command("make", "install")); err != nil {
		return ctx, fmt.Errorf("install CRDs: %w", err)
	}

	deployTarget := deployTargetForMode(activeTLSMode)
	if _, err := utils.Run(exec.Command("make", deployTarget, fmt.Sprintf("IMG=%s", projectImage))); err != nil {
		return ctx, fmt.Errorf("deploy controller-manager via %s: %w", deployTarget, err)
	}
	if activeTLSMode == tlsModeCertManager {
		if err := waitForNightlyTLSSecrets(ctx, cfg); err != nil {
			return ctx, err
		}
	}
	if err := wait.For(
		conditions.New(cfg.Client().Resources()).DeploymentAvailable(managerDeploymentName, namespace),
		wait.WithTimeout(3*time.Minute),
		wait.WithInterval(2*time.Second),
	); err != nil {
		return ctx, fmt.Errorf("wait controller-manager availability: %w", err)
	}
	if activeTLSMode == tlsModeCertManager && (!isRealClusterLane() || useRealCBTProvider()) {
		if err := setRealCBTProviderOnManager(ctx); err != nil {
			return ctx, err
		}
	}

	return ctx, nil
}

func cleanupEnvironment(ctx context.Context, _ *envconf.Config) (context.Context, error) {
	defer restoreActiveKubeconfig()

	if !kubeAPIReachable {
		log.Printf("skip cleanup because kubernetes API was not reachable during setup")
		return ctx, nil
	}

	undeployTarget := undeployTargetForMode(activeTLSMode)
	if _, err := utils.Run(exec.Command("make", undeployTarget, "ignore-not-found=true")); err != nil {
		log.Printf("warning: %s failed: %v", undeployTarget, err)
	}
	if _, err := utils.Run(exec.Command("make", "uninstall", "ignore-not-found=true")); err != nil {
		log.Printf("warning: uninstall failed: %v", err)
	}
	if activeTLSMode == tlsModeCertManager && installedCertManager {
		utils.UninstallCertManager()
	}
	if activeTLSMode == tlsModeCertManager && installedSnapshotController {
		if err := uninstallSnapshotController(); err != nil {
			log.Printf("warning: snapshot-controller cleanup failed: %v", err)
		}
	}
	if hostPathInstalled {
		if err := destroyNightlyHostPathSMS(); err != nil {
			log.Printf("warning: host-path cleanup failed: %v", err)
		}
	}
	if _, err := utils.Run(exec.Command("kubectl", "delete", "ns", namespace, "--ignore-not-found=true")); err != nil {
		log.Printf("warning: namespace cleanup failed: %v", err)
	}

	return ctx, nil
}

func preflightE2EEnvironment() error {
	if !isRealClusterLane() {
		if _, err := utils.Run(exec.Command("docker", "info", "--format", "{{.ServerVersion}}")); err != nil {
			return fmt.Errorf("preflight docker check failed: %w", err)
		}
	}
	if activeTLSMode == tlsModeCertManager {
		if _, err := utils.Run(exec.Command("kubectl", "version", "--request-timeout=10s")); err != nil {
			return fmt.Errorf("preflight kubectl check failed: %w", err)
		}
		kubeAPIReachable = true
	}
	return nil
}

func isRealClusterLane() bool {
	return os.Getenv(realClusterEnv) == "true"
}

func useRealCBTProvider() bool {
	return os.Getenv(realCBTProviderEnv) == "true"
}

func envOrDefault(key, fallback string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return fallback
}

func isHermeticFastLane() bool {
	return activeTLSMode == tlsModeStatic && activeKindClusterName != ""
}

func deployTargetForMode(mode tlsMode) string {
	if mode == tlsModeCertManager {
		return "deploy"
	}
	return "deploy-e2e-fast"
}

func undeployTargetForMode(mode tlsMode) string {
	if mode == tlsModeCertManager {
		return "undeploy"
	}
	return "undeploy-e2e-fast"
}

type staticMTLSMaterial struct {
	caCertPEM     []byte
	serverCertPEM []byte
	serverKeyPEM  []byte
	clientCertPEM []byte
	clientKeyPEM  []byte
}

type serverTLSMaterial struct {
	caCertPEM     []byte
	serverCertPEM []byte
	serverKeyPEM  []byte
}

func ensureStaticMTLSSecrets(ctx context.Context, cfg *envconf.Config) error {
	resources := cfg.Client().Resources()
	if err := corev1.AddToScheme(resources.GetScheme()); err != nil {
		return fmt.Errorf("add corev1 scheme: %w", err)
	}

	material, err := generateStaticMTLSMaterial()
	if err != nil {
		return err
	}

	secrets := []*corev1.Secret{
		{
			ObjectMeta: metav1.ObjectMeta{Name: "writer-ca", Namespace: namespace},
			Type:       corev1.SecretTypeOpaque,
			Data: map[string][]byte{
				"tls.crt": material.caCertPEM,
			},
		},
		{
			ObjectMeta: metav1.ObjectMeta{Name: "writer-sidecar-server-cert", Namespace: namespace},
			Type:       corev1.SecretTypeTLS,
			Data: map[string][]byte{
				corev1.TLSCertKey:       material.serverCertPEM,
				corev1.TLSPrivateKeyKey: material.serverKeyPEM,
			},
		},
		{
			ObjectMeta: metav1.ObjectMeta{Name: "writer-client-cert", Namespace: namespace},
			Type:       corev1.SecretTypeTLS,
			Data: map[string][]byte{
				corev1.TLSCertKey:       material.clientCertPEM,
				corev1.TLSPrivateKeyKey: material.clientKeyPEM,
			},
		},
	}

	for _, secret := range secrets {
		if err := upsertSecret(ctx, resources, secret); err != nil {
			return err
		}
	}
	return nil
}

func upsertSecret(ctx context.Context, resources *k8sresources.Resources, desired *corev1.Secret) error {
	var current corev1.Secret
	err := resources.Get(ctx, desired.Name, desired.Namespace, &current)
	if err != nil {
		if apierrors.IsNotFound(err) {
			if createErr := resources.Create(ctx, desired); createErr != nil {
				return fmt.Errorf("create secret %s/%s: %w", desired.Namespace, desired.Name, createErr)
			}
			return nil
		}
		return fmt.Errorf("get secret %s/%s: %w", desired.Namespace, desired.Name, err)
	}

	if current.Type != desired.Type {
		// Secret type is immutable; recreate to switch between static/cert-manager lanes safely.
		if deleteErr := resources.Delete(ctx, &current); deleteErr != nil && !apierrors.IsNotFound(deleteErr) {
			return fmt.Errorf("delete secret %s/%s for type change: %w", desired.Namespace, desired.Name, deleteErr)
		}
		if createErr := resources.Create(ctx, desired); createErr != nil {
			return fmt.Errorf("recreate secret %s/%s: %w", desired.Namespace, desired.Name, createErr)
		}
		return nil
	}

	current.Type = desired.Type
	current.Data = desired.Data
	if updateErr := resources.Update(ctx, &current); updateErr != nil {
		return fmt.Errorf("update secret %s/%s: %w", desired.Namespace, desired.Name, updateErr)
	}
	return nil
}

func generateStaticMTLSMaterial() (*staticMTLSMaterial, error) {
	now := time.Now().UTC()
	validUntil := now.Add(24 * time.Hour)

	caKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		return nil, fmt.Errorf("generate ca key: %w", err)
	}
	caSerial, err := randomSerialNumber()
	if err != nil {
		return nil, fmt.Errorf("generate ca serial number: %w", err)
	}
	caTemplate := &x509.Certificate{
		SerialNumber:          caSerial,
		Subject:               pkix.Name{CommonName: "snaplane-e2e-ca"},
		NotBefore:             now.Add(-1 * time.Hour),
		NotAfter:              validUntil,
		KeyUsage:              x509.KeyUsageCertSign | x509.KeyUsageCRLSign | x509.KeyUsageDigitalSignature,
		BasicConstraintsValid: true,
		IsCA:                  true,
	}
	caDER, err := x509.CreateCertificate(rand.Reader, caTemplate, caTemplate, caKey.Public(), caKey)
	if err != nil {
		return nil, fmt.Errorf("create ca certificate: %w", err)
	}
	caCert, err := x509.ParseCertificate(caDER)
	if err != nil {
		return nil, fmt.Errorf("parse ca certificate: %w", err)
	}

	serverKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		return nil, fmt.Errorf("generate server key: %w", err)
	}
	serverSerial, err := randomSerialNumber()
	if err != nil {
		return nil, fmt.Errorf("generate server serial number: %w", err)
	}
	serverTemplate := &x509.Certificate{
		SerialNumber: serverSerial,
		Subject:      pkix.Name{CommonName: "writer-sidecar"},
		NotBefore:    now.Add(-1 * time.Hour),
		NotAfter:     validUntil,
		KeyUsage:     x509.KeyUsageDigitalSignature | x509.KeyUsageKeyEncipherment,
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
		DNSNames: []string{
			"writer-sidecar." + namespace + ".svc",
			"writer-sidecar." + namespace + ".svc.cluster.local",
		},
		IPAddresses: []net.IP{net.ParseIP("127.0.0.1")},
	}
	serverDER, err := x509.CreateCertificate(rand.Reader, serverTemplate, caCert, serverKey.Public(), caKey)
	if err != nil {
		return nil, fmt.Errorf("create server certificate: %w", err)
	}

	clientKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		return nil, fmt.Errorf("generate client key: %w", err)
	}
	clientSerial, err := randomSerialNumber()
	if err != nil {
		return nil, fmt.Errorf("generate client serial number: %w", err)
	}
	clientTemplate := &x509.Certificate{
		SerialNumber: clientSerial,
		Subject:      pkix.Name{CommonName: "backup-controller"},
		NotBefore:    now.Add(-1 * time.Hour),
		NotAfter:     validUntil,
		KeyUsage:     x509.KeyUsageDigitalSignature | x509.KeyUsageKeyEncipherment,
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth},
	}
	clientDER, err := x509.CreateCertificate(rand.Reader, clientTemplate, caCert, clientKey.Public(), caKey)
	if err != nil {
		return nil, fmt.Errorf("create client certificate: %w", err)
	}

	serverKeyPEM, err := marshalECPrivateKeyPEM(serverKey)
	if err != nil {
		return nil, fmt.Errorf("encode server key: %w", err)
	}
	clientKeyPEM, err := marshalECPrivateKeyPEM(clientKey)
	if err != nil {
		return nil, fmt.Errorf("encode client key: %w", err)
	}

	return &staticMTLSMaterial{
		caCertPEM:     pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: caDER}),
		serverCertPEM: pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: serverDER}),
		serverKeyPEM:  serverKeyPEM,
		clientCertPEM: pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: clientDER}),
		clientKeyPEM:  clientKeyPEM,
	}, nil
}

func generateSnapshotMetadataTLSMaterial() (*serverTLSMaterial, error) {
	now := time.Now().UTC()
	validUntil := now.Add(24 * time.Hour)

	caKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		return nil, fmt.Errorf("generate snapshot metadata ca key: %w", err)
	}
	caSerial, err := randomSerialNumber()
	if err != nil {
		return nil, fmt.Errorf("generate snapshot metadata ca serial number: %w", err)
	}
	caTemplate := &x509.Certificate{
		SerialNumber:          caSerial,
		Subject:               pkix.Name{CommonName: "csi-snapshot-metadata.default-ca"},
		NotBefore:             now.Add(-1 * time.Hour),
		NotAfter:              validUntil,
		KeyUsage:              x509.KeyUsageCertSign | x509.KeyUsageCRLSign | x509.KeyUsageDigitalSignature,
		BasicConstraintsValid: true,
		IsCA:                  true,
	}
	caDER, err := x509.CreateCertificate(rand.Reader, caTemplate, caTemplate, caKey.Public(), caKey)
	if err != nil {
		return nil, fmt.Errorf("create snapshot metadata ca certificate: %w", err)
	}
	caCert, err := x509.ParseCertificate(caDER)
	if err != nil {
		return nil, fmt.Errorf("parse snapshot metadata ca certificate: %w", err)
	}

	serverKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		return nil, fmt.Errorf("generate snapshot metadata server key: %w", err)
	}
	serverSerial, err := randomSerialNumber()
	if err != nil {
		return nil, fmt.Errorf("generate snapshot metadata server serial number: %w", err)
	}
	serverTemplate := &x509.Certificate{
		SerialNumber: serverSerial,
		Subject:      pkix.Name{CommonName: "csi-snapshot-metadata.default"},
		NotBefore:    now.Add(-1 * time.Hour),
		NotAfter:     validUntil,
		KeyUsage:     x509.KeyUsageDigitalSignature | x509.KeyUsageKeyEncipherment,
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
		DNSNames: []string{
			"csi-snapshot-metadata.default",
			"csi-snapshot-metadata.default.svc",
			"csi-snapshot-metadata.default.svc.cluster.local",
			".default",
		},
	}
	serverDER, err := x509.CreateCertificate(rand.Reader, serverTemplate, caCert, serverKey.Public(), caKey)
	if err != nil {
		return nil, fmt.Errorf("create snapshot metadata server certificate: %w", err)
	}
	serverKeyPEM, err := marshalECPrivateKeyPEM(serverKey)
	if err != nil {
		return nil, fmt.Errorf("encode snapshot metadata server key: %w", err)
	}

	return &serverTLSMaterial{
		caCertPEM:     pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: caDER}),
		serverCertPEM: pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: serverDER}),
		serverKeyPEM:  serverKeyPEM,
	}, nil
}

func marshalECPrivateKeyPEM(key *ecdsa.PrivateKey) ([]byte, error) {
	encoded, err := x509.MarshalECPrivateKey(key)
	if err != nil {
		return nil, err
	}
	return pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: encoded}), nil
}

func randomSerialNumber() (*big.Int, error) {
	limit := new(big.Int).Lsh(big.NewInt(1), 128)
	return rand.Int(rand.Reader, limit)
}

func waitForCertManagerWebhookReady() error {
	var lastErr error
	deadline := time.Now().Add(2 * time.Minute)
	const pollInterval = 5 * time.Second
	for time.Now().Before(deadline) {
		caBundleLen, err := certManagerWebhookCABundleLen()
		if err != nil {
			lastErr = err
			time.Sleep(pollInterval)
			continue
		}

		// cert-manager webhook deployment can be Available before cainjector
		// injects a trusted CA bundle into webhook configuration.
		if caBundleLen == 0 {
			lastErr = fmt.Errorf("cert-manager webhook CABundle is still empty")
			time.Sleep(pollInterval)
			continue
		}

		if err := probeCertManagerWebhook(); err == nil {
			return nil
		} else {
			lastErr = err
			time.Sleep(pollInterval)
		}
	}
	return fmt.Errorf("wait cert-manager webhook readiness: %w", lastErr)
}

func certManagerWebhookCABundleLen() (int, error) {
	cmd := exec.Command(
		"kubectl",
		"get",
		"validatingwebhookconfiguration",
		"cert-manager-webhook",
		"-o",
		"jsonpath={.webhooks[0].clientConfig.caBundle}",
	)

	out, err := cmd.CombinedOutput()
	if err != nil {
		return 0, fmt.Errorf("get cert-manager webhook CABundle: (%v) %s", err, strings.TrimSpace(string(out)))
	}
	return len(strings.TrimSpace(string(out))), nil
}

func probeCertManagerWebhook() error {
	probe := `apiVersion: cert-manager.io/v1
kind: Issuer
metadata:
  name: cert-manager-webhook-ready-probe
  namespace: cert-manager
spec:
  selfSigned: {}`

	cmd := exec.Command("kubectl", "apply", "--dry-run=server", "-f", "-")
	cmd.Stdin = strings.NewReader(probe)
	if out, err := cmd.CombinedOutput(); err != nil {
		return fmt.Errorf("probe cert-manager webhook: (%v) %s", err, strings.TrimSpace(string(out)))
	}
	return nil
}

type nightlyTLSSecretExpectation struct {
	name            string
	certificateName string
	requiredKeys    []string
}

func waitForNightlyTLSSecrets(ctx context.Context, cfg *envconf.Config) error {
	resources := cfg.Client().Resources()
	if err := corev1.AddToScheme(resources.GetScheme()); err != nil {
		return fmt.Errorf("add corev1 scheme for nightly tls wait: %w", err)
	}

	expectations := []nightlyTLSSecretExpectation{
		{name: "writer-ca", certificateName: "snaplane-writer-ca", requiredKeys: []string{"tls.crt"}},
		{name: "writer-client-cert", certificateName: "snaplane-writer-client", requiredKeys: []string{corev1.TLSCertKey, corev1.TLSPrivateKeyKey}},
		{name: "writer-sidecar-server-cert", certificateName: "snaplane-writer-sidecar-server", requiredKeys: []string{corev1.TLSCertKey, corev1.TLSPrivateKeyKey}},
	}

	for _, expectation := range expectations {
		if err := waitForNightlyTLSSecret(ctx, resources, expectation); err != nil {
			return err
		}
	}
	return nil
}

func waitForNightlyTLSSecret(ctx context.Context, resources *k8sresources.Resources, expectation nightlyTLSSecretExpectation) error {
	if err := wait.For(func(ctx context.Context) (bool, error) {
		var secret corev1.Secret
		if err := resources.Get(ctx, expectation.name, namespace, &secret); err != nil {
			if apierrors.IsNotFound(err) {
				return false, nil
			}
			return false, err
		}
		for _, key := range expectation.requiredKeys {
			if len(secret.Data[key]) == 0 {
				return false, nil
			}
		}
		return certManagerCertificateReady(expectation.certificateName) == nil, nil
	}, wait.WithTimeout(3*time.Minute), wait.WithInterval(2*time.Second)); err != nil {
		return fmt.Errorf("wait nightly tls secret %s/%s: %w", namespace, expectation.name, err)
	}
	return nil
}

func certManagerCertificateReady(name string) error {
	out, err := exec.Command(
		"kubectl",
		"-n", namespace,
		"get", "certificate", name,
		"-o", "jsonpath={range .status.conditions[?(@.type==\"Ready\")]}{.status}{end}",
	).CombinedOutput()
	if err != nil {
		return fmt.Errorf("get certificate %s readiness: (%v) %s", name, err, strings.TrimSpace(string(out)))
	}
	if strings.TrimSpace(string(out)) != "True" {
		return fmt.Errorf("certificate %s is not Ready", name)
	}
	return nil
}

func setupNightlyHostPathSMS(ctx context.Context) error {
	repoRoot, err := ensureHostPathRepo()
	if err != nil {
		return err
	}
	deployScript := filepath.Join(repoRoot, "deploy/kubernetes-latest/deploy.sh")
	destroyScript := filepath.Join(repoRoot, "deploy/kubernetes-latest/destroy.sh")
	storageClassManifest := filepath.Join(repoRoot, "examples/csi-storageclass.yaml")

	for _, p := range []string{deployScript, destroyScript, storageClassManifest} {
		if _, err := os.Stat(p); err != nil {
			return fmt.Errorf("nightly host-path dependency missing at %q: %w", p, err)
		}
	}

	before, err := gitStatusPorcelain(repoRoot)
	if err != nil {
		return fmt.Errorf("read host-path git status: %w", err)
	}
	hostPathGitBefore = before

	if err := ensureSnapshotMetadataCRDInstalled(); err != nil {
		return err
	}
	if err := ensureSnapshotControllerInstalled(); err != nil {
		return err
	}
	if err := ensureSnapshotMetadataRBACInstalled(); err != nil {
		return err
	}

	tlsMaterial, err := generateSnapshotMetadataTLSMaterial()
	if err != nil {
		return err
	}

	// deploy.sh waits for the hostpath plugin pod to become ready. If this TLS secret
	// is missing, snapshot-metadata sidecar mount blocks pod readiness and deploy hangs.
	if err := upsertHostPathSnapshotMetadataTLSSecret(ctx, tlsMaterial); err != nil {
		return err
	}

	cmd := exec.Command("bash", deployScript)
	cmd.Env = append(os.Environ(), "SNAPSHOT_METADATA_TESTS=true", "INSTALL_CRD=true")
	if _, err := utils.Run(cmd); err != nil {
		return fmt.Errorf("deploy host-path with snapshot metadata: %w", err)
	}
	hostPathInstalled = true

	if _, err := utils.Run(exec.Command("kubectl", "apply", "-f", storageClassManifest)); err != nil {
		return fmt.Errorf("apply host-path storageclass: %w", err)
	}
	if err := ensureHostPathSnapshotMetadataServiceComponents(); err != nil {
		return err
	}
	if err := upsertHostPathSnapshotMetadataTLSSecret(ctx, tlsMaterial); err != nil {
		return err
	}
	if err := upsertHostPathSnapshotMetadataServiceCR(ctx, tlsMaterial.caCertPEM); err != nil {
		return err
	}
	if err := restartHostPathSnapshotMetadataStatefulSet(); err != nil {
		return err
	}
	if _, err := utils.Run(exec.Command("kubectl", "wait", "--for=condition=ready", "pod", "-l", "app.kubernetes.io/instance=hostpath.csi.k8s.io", "--all-namespaces", "--timeout=5m")); err != nil {
		return fmt.Errorf("wait host-path pods ready: %w", err)
	}
	return nil
}

func ensureHostPathSnapshotMetadataServiceComponents() error {
	for _, manifestURL := range []string{snapshotMetadataServiceCRURL, snapshotMetadataServiceURL} {
		if _, err := utils.Run(exec.Command("kubectl", "apply", "-f", manifestURL)); err != nil {
			return fmt.Errorf("apply snapshot metadata component from %q: %w", manifestURL, err)
		}
	}
	return nil
}

func upsertHostPathSnapshotMetadataTLSSecret(ctx context.Context, material *serverTLSMaterial) error {
	kubeClient, _, err := hostPathSnapshotMetadataClients()
	if err != nil {
		return err
	}

	desired := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "csi-snapshot-metadata-certs",
			Namespace: "default",
		},
		Type: corev1.SecretTypeTLS,
		Data: map[string][]byte{
			corev1.TLSCertKey:       material.serverCertPEM,
			corev1.TLSPrivateKeyKey: material.serverKeyPEM,
		},
	}

	current, err := kubeClient.CoreV1().Secrets(desired.Namespace).Get(ctx, desired.Name, metav1.GetOptions{})
	if err != nil {
		if apierrors.IsNotFound(err) {
			if _, createErr := kubeClient.CoreV1().Secrets(desired.Namespace).Create(ctx, desired, metav1.CreateOptions{}); createErr != nil {
				return fmt.Errorf("create snapshot metadata tls secret %s/%s: %w", desired.Namespace, desired.Name, createErr)
			}
			return nil
		}
		return fmt.Errorf("get snapshot metadata tls secret %s/%s: %w", desired.Namespace, desired.Name, err)
	}

	if current.Type != desired.Type {
		if deleteErr := kubeClient.CoreV1().Secrets(desired.Namespace).Delete(ctx, desired.Name, metav1.DeleteOptions{}); deleteErr != nil && !apierrors.IsNotFound(deleteErr) {
			return fmt.Errorf("delete snapshot metadata tls secret %s/%s for type change: %w", desired.Namespace, desired.Name, deleteErr)
		}
		if _, createErr := kubeClient.CoreV1().Secrets(desired.Namespace).Create(ctx, desired, metav1.CreateOptions{}); createErr != nil {
			return fmt.Errorf("recreate snapshot metadata tls secret %s/%s: %w", desired.Namespace, desired.Name, createErr)
		}
		return nil
	}

	current.Type = desired.Type
	current.Data = desired.Data
	if _, updateErr := kubeClient.CoreV1().Secrets(desired.Namespace).Update(ctx, current, metav1.UpdateOptions{}); updateErr != nil {
		return fmt.Errorf("update snapshot metadata tls secret %s/%s: %w", desired.Namespace, desired.Name, updateErr)
	}
	return nil
}

func upsertHostPathSnapshotMetadataServiceCR(ctx context.Context, caCertPEM []byte) error {
	_, smsClient, err := hostPathSnapshotMetadataClients()
	if err != nil {
		return err
	}

	desired := &smsv1alpha1.SnapshotMetadataService{
		ObjectMeta: metav1.ObjectMeta{Name: "hostpath.csi.k8s.io"},
		Spec: smsv1alpha1.SnapshotMetadataServiceSpec{
			Audience: "test-backup-client",
			Address:  "csi-snapshot-metadata.default:6443",
			CACert:   caCertPEM,
		},
	}

	current, err := smsClient.CbtV1alpha1().SnapshotMetadataServices().Get(ctx, desired.Name, metav1.GetOptions{})
	if err != nil {
		if apierrors.IsNotFound(err) {
			if _, createErr := smsClient.CbtV1alpha1().SnapshotMetadataServices().Create(ctx, desired, metav1.CreateOptions{}); createErr != nil {
				return fmt.Errorf("create snapshot metadata service CR %s: %w", desired.Name, createErr)
			}
			return nil
		}
		return fmt.Errorf("get snapshot metadata service CR %s: %w", desired.Name, err)
	}

	current.Spec = desired.Spec
	if _, updateErr := smsClient.CbtV1alpha1().SnapshotMetadataServices().Update(ctx, current, metav1.UpdateOptions{}); updateErr != nil {
		return fmt.Errorf("update snapshot metadata service CR %s: %w", desired.Name, updateErr)
	}
	return nil
}

func restartHostPathSnapshotMetadataStatefulSet() error {
	if _, err := utils.Run(exec.Command("kubectl", "rollout", "restart", "statefulset/csi-hostpathplugin", "-n", "default")); err != nil {
		return fmt.Errorf("restart host-path snapshot metadata statefulset: %w", err)
	}
	return nil
}

func hostPathSnapshotMetadataClients() (kubernetes.Interface, *smsclientset.Clientset, error) {
	cfg, err := ctrlconfig.GetConfig()
	if err != nil {
		return nil, nil, fmt.Errorf("build kube config for host-path snapshot metadata setup: %w", err)
	}
	kubeClient, err := kubernetes.NewForConfig(cfg)
	if err != nil {
		return nil, nil, fmt.Errorf("build kubernetes client for host-path snapshot metadata setup: %w", err)
	}
	smsClient, err := smsclientset.NewForConfig(cfg)
	if err != nil {
		return nil, nil, fmt.Errorf("build snapshot metadata service client for host-path snapshot metadata setup: %w", err)
	}
	return kubeClient, smsClient, nil
}

func destroyNightlyHostPathSMS() error {
	repoRoot, err := requireHostPathRepoRoot()
	if err != nil {
		return err
	}
	destroyScript := filepath.Join(repoRoot, "deploy/kubernetes-latest/destroy.sh")
	if _, err := os.Stat(destroyScript); err != nil {
		return err
	}
	if _, err := utils.Run(exec.Command("bash", destroyScript)); err != nil {
		return err
	}
	after, err := gitStatusPorcelain(repoRoot)
	if err != nil {
		return err
	}
	if strings.TrimSpace(hostPathGitBefore) != strings.TrimSpace(after) {
		log.Printf("warning: host-path repo became dirty after deploy/destroy\nbefore:\n%s\nafter:\n%s", hostPathGitBefore, after)
	}
	for _, manifestURL := range []string{snapshotMetadataServiceURL, snapshotMetadataServiceCRURL} {
		if _, err := utils.Run(exec.Command("kubectl", "delete", "-f", manifestURL, "--ignore-not-found=true")); err != nil {
			log.Printf("warning: snapshot metadata component cleanup failed for %s: %v", manifestURL, err)
		}
	}
	return nil
}

func ensureHostPathRepo() (string, error) {
	if hostPathRepoPath != "" {
		return hostPathRepoPath, nil
	}
	repoRoot, err := resolveHostPathRepoRoot()
	if err != nil {
		return "", err
	}
	if err := syncHostPathRepo(repoRoot); err != nil {
		return "", err
	}
	hostPathRepoPath = repoRoot
	return repoRoot, nil
}

func requireHostPathRepoRoot() (string, error) {
	if hostPathRepoPath != "" {
		return hostPathRepoPath, nil
	}
	repoRoot, err := resolveHostPathRepoRoot()
	if err != nil {
		return "", err
	}
	hostPathRepoPath = repoRoot
	return repoRoot, nil
}

func resolveHostPathRepoRoot() (string, error) {
	if override := os.Getenv(hostPathRepoRootEnv); override != "" {
		abs, err := filepath.Abs(override)
		if err != nil {
			return "", fmt.Errorf("abs host-path repo override: %w", err)
		}
		return abs, nil
	}
	projectDir, err := utils.GetProjectDir()
	if err != nil {
		return "", fmt.Errorf("determine project root for host-path repo cache: %w", err)
	}
	abs, err := filepath.Abs(filepath.Join(projectDir, hostPathRepoCacheDir))
	if err != nil {
		return "", fmt.Errorf("abs host-path repo cache location: %w", err)
	}
	return abs, nil
}

func syncHostPathRepo(repoRoot string) error {
	if os.Getenv(hostPathRepoRootEnv) != "" {
		return nil
	}
	if err := os.MkdirAll(filepath.Dir(repoRoot), 0o755); err != nil {
		return fmt.Errorf("prepare host-path repo cache dir: %w", err)
	}
	repoURL := resolveHostPathRepoURL()
	if _, err := os.Stat(repoRoot); os.IsNotExist(err) {
		if _, err := utils.Run(exec.Command("git", "clone", "--depth", "1", repoURL, repoRoot)); err != nil {
			return fmt.Errorf("clone host-path repo from %q: %w", repoURL, err)
		}
	} else if err != nil {
		return fmt.Errorf("stat host-path repo cache: %w", err)
	}

	revision := resolveHostPathRepoRevision()
	if _, err := utils.Run(exec.Command("git", "-C", repoRoot, "remote", "set-url", "origin", repoURL)); err != nil {
		return fmt.Errorf("set host-path repo origin to %q: %w", repoURL, err)
	}
	if _, err := utils.Run(exec.Command("git", "-C", repoRoot, "fetch", "--depth", "1", "origin", revision)); err != nil {
		return fmt.Errorf("fetch host-path repo revision %q: %w", revision, err)
	}
	if _, err := utils.Run(exec.Command("git", "-C", repoRoot, "checkout", "--detach", "FETCH_HEAD")); err != nil {
		return fmt.Errorf("checkout host-path repo revision %q: %w", revision, err)
	}
	return nil
}

func resolveHostPathRepoURL() string {
	if override := strings.TrimSpace(os.Getenv(hostPathRepoURLEnv)); override != "" {
		return override
	}
	return hostPathRepoURL
}

func resolveHostPathRepoRevision() string {
	if override := strings.TrimSpace(os.Getenv(hostPathRepoRevisionEnv)); override != "" {
		return override
	}
	return hostPathRepoRevision
}

func gitStatusPorcelain(repo string) (string, error) {
	cmd := exec.Command("git", "-C", repo, "status", "--porcelain")
	out, err := cmd.CombinedOutput()
	if err != nil {
		return "", fmt.Errorf("git status --porcelain: (%v) %s", err, strings.TrimSpace(string(out)))
	}
	return string(out), nil
}

func ensureSnapshotCRDsInstalled() error {
	if snapshotCRDsAreInstalled() {
		return nil
	}
	for _, crdURL := range snapshotCRDURLs {
		if _, err := utils.Run(exec.Command("kubectl", "apply", "-f", crdURL)); err != nil {
			return fmt.Errorf("install external-snapshotter CRD from %q: %w", crdURL, err)
		}
	}
	return nil
}

func ensureSnapshotMetadataCRDInstalled() error {
	const crdName = "snapshotmetadataservices.cbt.storage.k8s.io"
	if _, err := utils.Run(exec.Command("kubectl", "get", "crd", crdName)); err == nil {
		return nil
	}
	if _, err := utils.Run(exec.Command("kubectl", "apply", "-f", snapshotMetadataCRDURL)); err != nil {
		return fmt.Errorf("install snapshot metadata CRD from %q: %w", snapshotMetadataCRDURL, err)
	}
	return nil
}

func ensureSnapshotMetadataRBACInstalled() error {
	const clusterRoleName = "external-snapshot-metadata-runner"
	if _, err := utils.Run(exec.Command("kubectl", "get", "clusterrole", clusterRoleName)); err == nil {
		return nil
	}
	if _, err := utils.Run(exec.Command("kubectl", "apply", "-f", snapshotMetadataRBACURL)); err != nil {
		return fmt.Errorf("install snapshot metadata RBAC from %q: %w", snapshotMetadataRBACURL, err)
	}
	return nil
}

func ensureSnapshotControllerInstalled() error {
	if _, err := utils.Run(exec.Command("kubectl", "get", "deployment", "snapshot-controller", "-n", "kube-system")); err == nil {
		return waitForSnapshotController()
	}
	for _, manifestURL := range []string{snapshotControllerRBACURL, snapshotControllerSetupURL} {
		if _, err := utils.Run(exec.Command("kubectl", "apply", "-f", manifestURL)); err != nil {
			return fmt.Errorf("install snapshot-controller from %q: %w", manifestURL, err)
		}
	}
	installedSnapshotController = true
	return waitForSnapshotController()
}

func waitForSnapshotController() error {
	if _, err := utils.Run(exec.Command(
		"kubectl",
		"wait",
		"deployment.apps/snapshot-controller",
		"--for", "condition=Available",
		"--namespace", "kube-system",
		"--timeout", "5m",
	)); err != nil {
		return fmt.Errorf("wait snapshot-controller availability: %w", err)
	}
	return nil
}

func uninstallSnapshotController() error {
	for _, manifestURL := range []string{snapshotControllerSetupURL, snapshotControllerRBACURL} {
		if _, err := utils.Run(exec.Command("kubectl", "delete", "-f", manifestURL, "--ignore-not-found=true")); err != nil {
			return err
		}
	}
	return nil
}

func snapshotCRDsAreInstalled() bool {
	required := []string{
		"volumesnapshotclasses.snapshot.storage.k8s.io",
		"volumesnapshotcontents.snapshot.storage.k8s.io",
		"volumesnapshots.snapshot.storage.k8s.io",
	}
	for _, name := range required {
		if _, err := utils.Run(exec.Command("kubectl", "get", "crd", name)); err != nil {
			return false
		}
	}
	return true
}

func setRealCBTProviderOnManager(ctx context.Context) error {
	kubeClient, err := operatorKubeClient()
	if err != nil {
		return err
	}

	existingDeployment, err := kubeClient.AppsV1().Deployments(namespace).Get(ctx, managerDeploymentName, metav1.GetOptions{})
	if err != nil {
		return fmt.Errorf("get controller-manager deployment before env update: %w", err)
	}

	existingUIDs, err := listManagerPodUIDs(ctx, kubeClient)
	if err != nil {
		return err
	}

	if _, err := utils.Run(exec.Command(
		"kubectl",
		"-n", namespace,
		"set", "env",
		"deployment/"+managerDeploymentName,
		"SNAPLANE_CBT_PROVIDER=snapshot-metadata",
		"SNAPLANE_SNAPSHOT_DATA_MODE=live",
		"SNAPLANE_CAS_MAX_CHAIN_DEPTH=1024",
	)); err != nil {
		return fmt.Errorf("set CBT provider env on manager: %w", err)
	}
	desiredEnv := map[string]string{
		"SNAPLANE_CBT_PROVIDER":        "snapshot-metadata",
		"SNAPLANE_SNAPSHOT_DATA_MODE":  "live",
		"SNAPLANE_CAS_MAX_CHAIN_DEPTH": "1024",
	}
	if err := waitForManagerDeploymentEnvRollout(ctx, kubeClient, existingDeployment.Generation, desiredEnv); err != nil {
		return fmt.Errorf("wait controller-manager rollout after CBT provider env change: %w", err)
	}
	_, err = waitForUsableManagerPod(ctx, kubeClient, existingUIDs, desiredEnv, false)
	if err != nil {
		return fmt.Errorf("wait updated manager pod after CBT provider env change: %w", err)
	}
	return nil
}

func operatorKubeClient() (kubernetes.Interface, error) {
	cfg, err := ctrlconfig.GetConfig()
	if err != nil {
		return nil, fmt.Errorf("build kube config for operator setup: %w", err)
	}
	kubeClient, err := kubernetes.NewForConfig(cfg)
	if err != nil {
		return nil, fmt.Errorf("build kubernetes client for operator setup: %w", err)
	}
	return kubeClient, nil
}

func listManagerPodUIDs(ctx context.Context, kubeClient kubernetes.Interface) (map[string]struct{}, error) {
	pods, err := kubeClient.CoreV1().Pods(namespace).List(ctx, metav1.ListOptions{
		LabelSelector: "control-plane=controller-manager",
	})
	if err != nil {
		return nil, fmt.Errorf("list controller-manager pods before env update: %w", err)
	}
	uids := make(map[string]struct{}, len(pods.Items))
	for _, pod := range pods.Items {
		uids[string(pod.UID)] = struct{}{}
	}
	return uids, nil
}

func waitForUsableManagerPod(
	ctx context.Context,
	kubeClient kubernetes.Interface,
	existingUIDs map[string]struct{},
	desiredEnv map[string]string,
	requireLeader bool,
) (*corev1.Pod, error) {
	var usablePod *corev1.Pod
	err := wait.For(func(ctx context.Context) (bool, error) {
		pods, listErr := kubeClient.CoreV1().Pods(namespace).List(ctx, metav1.ListOptions{
			LabelSelector: "control-plane=controller-manager",
		})
		if listErr != nil {
			return false, listErr
		}
		for i := range pods.Items {
			pod := pods.Items[i]
			if pod.DeletionTimestamp != nil || !podReady(&pod) {
				continue
			}
			if desiredEnv != nil && !managerPodHasEnv(&pod, desiredEnv) {
				continue
			}
			if existingUIDs != nil {
				if _, exists := existingUIDs[string(pod.UID)]; exists {
					continue
				}
			}
			if requireLeader {
				holderIdentity, leaseErr := managerLeaderHolderIdentity(ctx, kubeClient)
				if leaseErr != nil {
					return false, leaseErr
				}
				if !managerLeaderIdentityMatchesPod(holderIdentity, pod.Name) {
					continue
				}
			}
			usablePod = pod.DeepCopy()
			return true, nil
		}
		return false, nil
	}, wait.WithTimeout(3*time.Minute), wait.WithInterval(2*time.Second))
	if err != nil {
		return nil, err
	}
	return usablePod, nil
}

func waitForManagerDeploymentEnvRollout(
	ctx context.Context,
	kubeClient kubernetes.Interface,
	previousGeneration int64,
	desiredEnv map[string]string,
) error {
	return wait.For(func(ctx context.Context) (bool, error) {
		deployment, err := kubeClient.AppsV1().Deployments(namespace).Get(ctx, managerDeploymentName, metav1.GetOptions{})
		if err != nil {
			return false, err
		}
		if !deploymentHasEnv(deployment, desiredEnv) {
			return false, nil
		}
		if deployment.Generation <= previousGeneration {
			return false, nil
		}
		if deployment.Status.ObservedGeneration < deployment.Generation {
			return false, nil
		}
		replicas := int32(1)
		if deployment.Spec.Replicas != nil {
			replicas = *deployment.Spec.Replicas
		}
		if deployment.Status.UpdatedReplicas < replicas {
			return false, nil
		}
		if deployment.Status.AvailableReplicas < replicas {
			return false, nil
		}
		if deployment.Status.UnavailableReplicas > 0 {
			return false, nil
		}
		return true, nil
	}, wait.WithTimeout(3*time.Minute), wait.WithInterval(2*time.Second))
}

func managerLeaderHolderIdentity(ctx context.Context, kubeClient kubernetes.Interface) (string, error) {
	leaderLease, err := kubeClient.CoordinationV1().Leases(namespace).Get(ctx, managerLeaderLeaseName, metav1.GetOptions{})
	if err != nil {
		if apierrors.IsNotFound(err) {
			return "", nil
		}
		return "", err
	}
	if leaderLease.Spec.HolderIdentity == nil {
		return "", nil
	}
	return *leaderLease.Spec.HolderIdentity, nil
}

func managerLeaderIdentityMatchesPod(holderIdentity, podName string) bool {
	return holderIdentity == podName || strings.HasPrefix(holderIdentity, podName+"_")
}

func podReady(pod *corev1.Pod) bool {
	for _, condition := range pod.Status.Conditions {
		if condition.Type == corev1.PodReady {
			return condition.Status == corev1.ConditionTrue
		}
	}
	return false
}

func managerPodHasEnv(pod *corev1.Pod, desired map[string]string) bool {
	for _, container := range pod.Spec.Containers {
		if container.Name != "manager" {
			continue
		}
		actual := make(map[string]string, len(container.Env))
		for _, envVar := range container.Env {
			actual[envVar.Name] = envVar.Value
		}
		for key, value := range desired {
			if actual[key] != value {
				return false
			}
		}
		return true
	}
	return false
}

func deploymentHasEnv(deployment *appsv1.Deployment, desired map[string]string) bool {
	for _, container := range deployment.Spec.Template.Spec.Containers {
		if container.Name != "manager" {
			continue
		}
		actual := make(map[string]string, len(container.Env))
		for _, envVar := range container.Env {
			actual[envVar.Name] = envVar.Value
		}
		for key, value := range desired {
			if actual[key] != value {
				return false
			}
		}
		return true
	}
	return false
}

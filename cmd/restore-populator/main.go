package main

import (
	"flag"
	"fmt"
	"os"

	populatorMachinery "github.com/kubernetes-csi/lib-volume-populator/v3/populator-machinery"
	snaplanev1alpha1 "github.com/molpako/snaplane/api/v1alpha1"
	"github.com/molpako/snaplane/internal/restorepopulator"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/klog/v2"
)

const (
	modeController = "controller"
	modeWorker     = "worker"

	backupResource = "backups"
)

func main() {
	var (
		mode                  string
		namespace             string
		kubeconfigPath        string
		httpEndpoint          string
		metricsPath           string
		workerImage           string
		workerImagePullPolicy string
		backupRoot            string
		sourceDir             string
		manifestID            string
		targetDevice          string
		restoreFormat         string
		volumeSizeBytes       int64
		chunkSizeBytes        int64
	)

	klog.InitFlags(nil)
	flag.StringVar(&mode, "mode", modeController, "Mode to run (controller|worker)")
	flag.StringVar(&namespace, "namespace", envOrDefault("POD_NAMESPACE", "default"), "Namespace where restore populator controller runs")
	flag.StringVar(&kubeconfigPath, "kubeconfig-path", "", "Path to kubeconfig (for out-of-cluster controller run)")
	flag.StringVar(&httpEndpoint, "http-endpoint", "", "HTTP endpoint for diagnostics and metrics (example: :8080)")
	flag.StringVar(&metricsPath, "metrics-path", "/metrics", "HTTP path for metrics")
	flag.StringVar(&workerImage, "worker-image", envOrDefault("RESTORE_WORKER_IMAGE", "controller:latest"), "Container image used for restore worker pods")
	flag.StringVar(&workerImagePullPolicy, "worker-image-pull-policy", string(corev1.PullIfNotPresent), "Pull policy for restore worker pod image")
	flag.StringVar(&backupRoot, "backup-root", "/var/backup", "Host path root that stores backup payloads")

	flag.StringVar(&sourceDir, "source-dir", "", "[worker mode] source repository directory")
	flag.StringVar(&manifestID, "manifest-id", "", "[worker mode] manifest/object identifier")
	flag.StringVar(&targetDevice, "target-device", "/dev/restore-target", "[worker mode] destination block device path")
	flag.StringVar(&restoreFormat, "restore-format", "", "[worker mode] restore source format (mock-image-v1|cas-v1)")
	flag.Int64Var(&volumeSizeBytes, "volume-size-bytes", 0, "[worker mode] optional logical volume size")
	flag.Int64Var(&chunkSizeBytes, "chunk-size-bytes", 0, "[worker mode] optional chunk size hint")
	flag.Parse()

	switch mode {
	case modeController:
		if err := runController(namespace, kubeconfigPath, httpEndpoint, metricsPath, workerImage, workerImagePullPolicy, backupRoot, targetDevice); err != nil {
			klog.Fatalf("run restore populator controller: %v", err)
		}
	case modeWorker:
		if err := restorepopulator.RunWorker(restorepopulator.WorkerOptions{
			SourceDir:       sourceDir,
			ManifestID:      manifestID,
			TargetDevice:    targetDevice,
			Format:          restoreFormat,
			VolumeSizeBytes: volumeSizeBytes,
			ChunkSizeBytes:  chunkSizeBytes,
		}); err != nil {
			klog.Fatalf("run restore worker: %v", err)
		}
	default:
		klog.Fatalf("unsupported mode: %s", mode)
	}
}

func runController(
	namespace string,
	kubeconfigPath string,
	httpEndpoint string,
	metricsPath string,
	workerImage string,
	workerImagePullPolicy string,
	backupRoot string,
	targetDevice string,
) error {
	pullPolicy, err := parsePullPolicy(workerImagePullPolicy)
	if err != nil {
		return err
	}
	provider := &restorepopulator.Provider{
		WorkerImage:           workerImage,
		WorkerImagePullPolicy: pullPolicy,
		BackupRoot:            backupRoot,
		WorkerTargetDevice:    targetDevice,
	}

	groupKind := schema.GroupKind{Group: snaplanev1alpha1.GroupVersion.Group, Kind: "Backup"}
	versionResource := schema.GroupVersionResource{
		Group:    snaplanev1alpha1.GroupVersion.Group,
		Version:  snaplanev1alpha1.GroupVersion.Version,
		Resource: backupResource,
	}

	cfg := populatorMachinery.VolumePopulatorConfig{
		MasterURL:    "",
		Kubeconfig:   kubeconfigPath,
		HttpEndpoint: httpEndpoint,
		MetricsPath:  metricsPath,
		Namespace:    namespace,
		Prefix:       snaplanev1alpha1.RestorePopulatorPrefix,
		Gk:           groupKind,
		Gvr:          versionResource,
		ProviderFunctionConfig: &populatorMachinery.ProviderFunctionConfig{
			PopulateFn:         provider.PopulateFn,
			PopulateCompleteFn: provider.PopulateCompleteFn,
			PopulateCleanupFn:  provider.PopulateCleanupFn,
		},
		CrossNamespace: false,
	}

	populatorMachinery.RunControllerWithConfig(cfg)
	return nil
}

func parsePullPolicy(value string) (corev1.PullPolicy, error) {
	switch corev1.PullPolicy(value) {
	case corev1.PullAlways, corev1.PullIfNotPresent, corev1.PullNever:
		return corev1.PullPolicy(value), nil
	default:
		return "", fmt.Errorf("invalid worker-image-pull-policy %q", value)
	}
}

func envOrDefault(key, fallback string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return fallback
}

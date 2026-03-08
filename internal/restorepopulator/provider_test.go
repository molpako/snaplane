package restorepopulator

import (
	"context"
	"strings"
	"testing"

	populatorMachinery "github.com/kubernetes-csi/lib-volume-populator/v3/populator-machinery"
	snaplanev1alpha1 "github.com/molpako/snaplane/api/v1alpha1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	clientfake "k8s.io/client-go/kubernetes/fake"
)

func TestProviderPopulateFnCreatesWorkerPod(t *testing.T) {
	t.Parallel()

	provider := &Provider{
		WorkerImage:           "example.com/restore:latest",
		WorkerImagePullPolicy: corev1.PullIfNotPresent,
		BackupRoot:            "/var/backup",
		WorkerTargetDevice:    "/dev/restore-target",
	}
	params := newPopulatorParams(t, populatorParamsOptions{})

	if err := provider.PopulateFn(context.Background(), params); err != nil {
		t.Fatalf("PopulateFn returned error: %v", err)
	}

	podName := workerPodName(params.Pvc.UID)
	pod, err := params.KubeClient.CoreV1().Pods(params.PvcPrime.Namespace).Get(context.Background(), podName, metav1.GetOptions{})
	if err != nil {
		t.Fatalf("get worker pod: %v", err)
	}
	if pod.Spec.NodeName != "node-a" {
		t.Fatalf("expected pod nodeName node-a, got %q", pod.Spec.NodeName)
	}
	if len(pod.Spec.Containers) != 1 {
		t.Fatalf("expected one worker container, got %d", len(pod.Spec.Containers))
	}
	container := pod.Spec.Containers[0]
	if container.Image != provider.WorkerImage {
		t.Fatalf("expected image %q, got %q", provider.WorkerImage, container.Image)
	}
	if len(container.VolumeDevices) != 1 || container.VolumeDevices[0].DevicePath != provider.WorkerTargetDevice {
		t.Fatalf("expected block device path %q, got %+v", provider.WorkerTargetDevice, container.VolumeDevices)
	}
}

func TestProviderPopulateFnIdempotentWhenPodExists(t *testing.T) {
	t.Parallel()

	provider := &Provider{WorkerImage: "example.com/restore:latest"}
	params := newPopulatorParams(t, populatorParamsOptions{})

	podName := workerPodName(params.Pvc.UID)
	existing := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: params.PvcPrime.Namespace,
			Name:      podName,
		},
		Status: corev1.PodStatus{Phase: corev1.PodRunning},
	}
	if _, err := params.KubeClient.CoreV1().Pods(params.PvcPrime.Namespace).Create(context.Background(), existing, metav1.CreateOptions{}); err != nil {
		t.Fatalf("seed existing pod: %v", err)
	}

	if err := provider.PopulateFn(context.Background(), params); err != nil {
		t.Fatalf("PopulateFn returned error: %v", err)
	}

	pods, err := params.KubeClient.CoreV1().Pods(params.PvcPrime.Namespace).List(context.Background(), metav1.ListOptions{})
	if err != nil {
		t.Fatalf("list pods: %v", err)
	}
	if len(pods.Items) != 1 {
		t.Fatalf("expected one pod after idempotent call, got %d", len(pods.Items))
	}
}

func TestProviderPopulateCompleteFn(t *testing.T) {
	t.Parallel()

	testCases := []struct {
		name          string
		podPhase      corev1.PodPhase
		podMessage    string
		seedPod       bool
		expectDone    bool
		expectErrLike string
	}{
		{
			name:       "not found returns pending",
			seedPod:    false,
			expectDone: false,
		},
		{
			name:       "running returns pending",
			seedPod:    true,
			podPhase:   corev1.PodRunning,
			expectDone: false,
		},
		{
			name:       "succeeded returns done",
			seedPod:    true,
			podPhase:   corev1.PodSucceeded,
			expectDone: true,
		},
		{
			name:          "failed returns error",
			seedPod:       true,
			podPhase:      corev1.PodFailed,
			podMessage:    "restore copy failed",
			expectDone:    false,
			expectErrLike: "restore worker pod failed",
		},
	}

	for _, tc := range testCases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			provider := &Provider{WorkerImage: "example.com/restore:latest"}
			params := newPopulatorParams(t, populatorParamsOptions{})
			if tc.seedPod {
				pod := &corev1.Pod{
					ObjectMeta: metav1.ObjectMeta{
						Namespace: params.PvcPrime.Namespace,
						Name:      workerPodName(params.Pvc.UID),
					},
					Status: corev1.PodStatus{
						Phase:   tc.podPhase,
						Message: tc.podMessage,
					},
				}
				if _, err := params.KubeClient.CoreV1().Pods(params.PvcPrime.Namespace).Create(context.Background(), pod, metav1.CreateOptions{}); err != nil {
					t.Fatalf("seed pod: %v", err)
				}
			}

			done, err := provider.PopulateCompleteFn(context.Background(), params)
			if tc.expectErrLike == "" {
				if err != nil {
					t.Fatalf("PopulateCompleteFn returned error: %v", err)
				}
			} else {
				if err == nil || !strings.Contains(err.Error(), tc.expectErrLike) {
					t.Fatalf("expected error containing %q, got %v", tc.expectErrLike, err)
				}
			}
			if done != tc.expectDone {
				t.Fatalf("expected done=%v, got %v", tc.expectDone, done)
			}
		})
	}
}

func TestProviderPopulateCleanupFnDeletesWorkerPod(t *testing.T) {
	t.Parallel()

	provider := &Provider{WorkerImage: "example.com/restore:latest"}
	params := newPopulatorParams(t, populatorParamsOptions{})
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: params.PvcPrime.Namespace,
			Name:      workerPodName(params.Pvc.UID),
		},
	}
	if _, err := params.KubeClient.CoreV1().Pods(params.PvcPrime.Namespace).Create(context.Background(), pod, metav1.CreateOptions{}); err != nil {
		t.Fatalf("seed pod: %v", err)
	}

	if err := provider.PopulateCleanupFn(context.Background(), params); err != nil {
		t.Fatalf("PopulateCleanupFn returned error: %v", err)
	}
	if _, err := params.KubeClient.CoreV1().Pods(params.PvcPrime.Namespace).Get(context.Background(), pod.Name, metav1.GetOptions{}); err == nil {
		t.Fatalf("expected worker pod deletion")
	}
}

func TestProviderAcceptsCASFormat(t *testing.T) {
	t.Parallel()

	provider := &Provider{WorkerImage: "example.com/restore:latest", BackupRoot: "/var/backup"}
	params := newPopulatorParams(t, populatorParamsOptions{
		format:          snaplanev1alpha1.RestoreSourceFormatCASV1,
		volumeSizeBytes: 8192,
		chunkSizeBytes:  4096,
	})

	if err := provider.PopulateFn(context.Background(), params); err != nil {
		t.Fatalf("expected cas format acceptance, got %v", err)
	}

	podName := workerPodName(params.Pvc.UID)
	pod, err := params.KubeClient.CoreV1().Pods(params.PvcPrime.Namespace).Get(context.Background(), podName, metav1.GetOptions{})
	if err != nil {
		t.Fatalf("get worker pod: %v", err)
	}
	if len(pod.Spec.Containers) != 1 {
		t.Fatalf("expected one worker container, got %d", len(pod.Spec.Containers))
	}
	args := strings.Join(pod.Spec.Containers[0].Args, " ")
	if !strings.Contains(args, "--restore-format="+snaplanev1alpha1.RestoreSourceFormatCASV1) {
		t.Fatalf("expected restore-format arg in %q", args)
	}
	if !strings.Contains(args, "--volume-size-bytes=8192") {
		t.Fatalf("expected volume-size-bytes arg in %q", args)
	}
	if !strings.Contains(args, "--chunk-size-bytes=4096") {
		t.Fatalf("expected chunk-size-bytes arg in %q", args)
	}
}

func TestProviderAcceptsMockFormat(t *testing.T) {
	t.Parallel()

	provider := &Provider{WorkerImage: "example.com/restore:latest", BackupRoot: "/var/backup"}
	params := newPopulatorParams(t, populatorParamsOptions{format: snaplanev1alpha1.RestoreSourceFormatMockImageV1})
	if err := provider.PopulateFn(context.Background(), params); err != nil {
		t.Fatalf("expected mock format acceptance, got %v", err)
	}
}

func TestProviderAcceptsEmptyFormatForBackwardCompatibility(t *testing.T) {
	t.Parallel()

	provider := &Provider{WorkerImage: "example.com/restore:latest", BackupRoot: "/var/backup"}
	params := newPopulatorParams(t, populatorParamsOptions{format: ""})
	if err := provider.PopulateFn(context.Background(), params); err != nil {
		t.Fatalf("expected empty format acceptance, got %v", err)
	}
}

func TestProviderRejectsUnknownFormat(t *testing.T) {
	t.Parallel()

	provider := &Provider{WorkerImage: "example.com/restore:latest", BackupRoot: "/var/backup"}
	params := newPopulatorParams(t, populatorParamsOptions{format: "future-v9"})
	err := provider.PopulateFn(context.Background(), params)
	if err == nil || !strings.Contains(err.Error(), "is unknown") {
		t.Fatalf("expected unknown format rejection, got %v", err)
	}
}

func TestProviderValidation(t *testing.T) {
	t.Parallel()

	t.Run("rejects non-block pvc", func(t *testing.T) {
		t.Parallel()
		provider := &Provider{WorkerImage: "example.com/restore:latest"}
		params := newPopulatorParams(t, populatorParamsOptions{blockMode: boolPtr(false)})
		if err := provider.PopulateFn(context.Background(), params); err == nil {
			t.Fatalf("expected non-block PVC validation error")
		}
	})

	t.Run("rejects backup not succeeded", func(t *testing.T) {
		t.Parallel()
		provider := &Provider{WorkerImage: "example.com/restore:latest"}
		params := newPopulatorParams(t, populatorParamsOptions{completed: boolPtr(false)})
		if err := provider.PopulateFn(context.Background(), params); err == nil || !strings.Contains(err.Error(), "not succeeded") {
			t.Fatalf("expected not succeeded error, got %v", err)
		}
	})

	t.Run("rejects failed backup", func(t *testing.T) {
		t.Parallel()
		provider := &Provider{WorkerImage: "example.com/restore:latest"}
		params := newPopulatorParams(t, populatorParamsOptions{failed: boolPtr(true)})
		if err := provider.PopulateFn(context.Background(), params); err == nil || !strings.Contains(err.Error(), "is failed") {
			t.Fatalf("expected failed backup error, got %v", err)
		}
	})

	t.Run("rejects restore source outside backup root", func(t *testing.T) {
		t.Parallel()
		provider := &Provider{WorkerImage: "example.com/restore:latest", BackupRoot: "/var/backup"}
		params := newPopulatorParams(t, populatorParamsOptions{repositoryPath: "/tmp/other"})
		if err := provider.PopulateFn(context.Background(), params); err == nil || !strings.Contains(err.Error(), "outside backup root") {
			t.Fatalf("expected restoreSource path validation error, got %v", err)
		}
	})

	t.Run("rejects manifest traversal", func(t *testing.T) {
		t.Parallel()
		provider := &Provider{WorkerImage: "example.com/restore:latest", BackupRoot: "/var/backup"}
		params := newPopulatorParams(t, populatorParamsOptions{manifestID: "../escape"})
		if err := provider.PopulateFn(context.Background(), params); err == nil || !strings.Contains(err.Error(), "escapes repositoryPath") {
			t.Fatalf("expected manifest traversal validation error, got %v", err)
		}
	})

}

type populatorParamsOptions struct {
	blockMode       *bool
	completed       *bool
	failed          *bool
	repositoryPath  string
	manifestID      string
	format          string
	volumeSizeBytes int64
	chunkSizeBytes  int64
}

func newPopulatorParams(t *testing.T, opts populatorParamsOptions) populatorMachinery.PopulatorParams {
	t.Helper()

	blockMode := boolValue(opts.blockMode, true)
	completed := boolValue(opts.completed, true)
	failed := boolValue(opts.failed, false)

	volumeMode := corev1.PersistentVolumeBlock
	if !blockMode {
		volumeMode = corev1.PersistentVolumeFilesystem
	}
	pvc := &corev1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: "app-ns",
			Name:      "restore-pvc",
			UID:       types.UID("uid-1"),
		},
		Spec: corev1.PersistentVolumeClaimSpec{
			VolumeMode: &volumeMode,
		},
	}
	pvcPrime := &corev1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: "snaplane-system",
			Name:      "prime-pvc",
		},
	}

	u := newBackupUnstructured(
		t,
		completed,
		failed,
		opts.repositoryPath,
		opts.manifestID,
		opts.format,
		opts.volumeSizeBytes,
		opts.chunkSizeBytes,
	)
	client := clientfake.NewSimpleClientset()
	return populatorMachinery.PopulatorParams{
		KubeClient:   client,
		Pvc:          pvc,
		PvcPrime:     pvcPrime,
		Unstructured: u,
	}
}

func newBackupUnstructured(
	t *testing.T,
	completed bool,
	failed bool,
	repositoryPath string,
	manifestID string,
	format string,
	volumeSizeBytes int64,
	chunkSizeBytes int64,
) *unstructured.Unstructured {
	t.Helper()

	if repositoryPath == "" {
		repositoryPath = "/var/backup/app-ns/pvc-a/backup-a"
	}
	if manifestID == "" {
		manifestID = "mock.img"
	}

	succeededStatus := metav1.ConditionUnknown
	if completed {
		succeededStatus = metav1.ConditionTrue
	}
	if failed {
		succeededStatus = metav1.ConditionFalse
	}
	backup := &snaplanev1alpha1.Backup{
		TypeMeta: metav1.TypeMeta{
			APIVersion: snaplanev1alpha1.GroupVersion.String(),
			Kind:       backupKind,
		},
		ObjectMeta: metav1.ObjectMeta{
			Namespace: "app-ns",
			Name:      "backup-a",
			UID:       types.UID("backup-uid"),
		},
		Status: snaplanev1alpha1.BackupStatus{
			Conditions: []metav1.Condition{
				{
					Type:               snaplanev1alpha1.BackupConditionSucceeded,
					Status:             succeededStatus,
					Reason:             "UnitTest",
					ObservedGeneration: 1,
					LastTransitionTime: metav1.Now(),
				},
			},
			RestoreSource: snaplanev1alpha1.BackupRestoreSourceStatus{
				NodeName:        "node-a",
				RepositoryPath:  repositoryPath,
				ManifestID:      manifestID,
				RepoUUID:        "repo-1",
				Format:          format,
				VolumeSizeBytes: volumeSizeBytes,
				ChunkSizeBytes:  chunkSizeBytes,
			},
		},
	}
	obj, err := runtime.DefaultUnstructuredConverter.ToUnstructured(backup)
	if err != nil {
		t.Fatalf("convert backup to unstructured: %v", err)
	}
	return &unstructured.Unstructured{Object: obj}
}

func boolValue(value *bool, fallback bool) bool {
	if value == nil {
		return fallback
	}
	return *value
}

func boolPtr(v bool) *bool {
	return &v
}

//go:build e2e
// +build e2e

package e2e

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	populatorMachinery "github.com/kubernetes-csi/lib-volume-populator/v3/populator-machinery"
	snaplanev1alpha1 "github.com/molpako/snaplane/api/v1alpha1"
	"github.com/molpako/snaplane/internal/restorepopulator"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes"
	"sigs.k8s.io/e2e-framework/klient/wait"
	"sigs.k8s.io/e2e-framework/pkg/envconf"
	"sigs.k8s.io/e2e-framework/pkg/features"
)

func TestRealClusterRestoreWorkflow(t *testing.T) {
	if !isRealClusterLane() {
		t.Skip("real cluster restore workflow gate requires E2E_REAL_CLUSTER=true")
	}
	registerFailureDiagnostics(t)

	state := newSchedulerFeatureState("real-restore")
	const payload = "snaplane-real-cluster-restore\n"
	testenv.Test(t,
		features.New("real-cluster restore worker copies mock backup into pvcPrime block device").
			Setup(func(ctx context.Context, t *testing.T, cfg *envconf.Config) context.Context {
				c := newRuntimeClient(t, cfg)
				createFeatureNamespace(ctx, t, c, state.namespace)

				kubeClient, err := operatorKubeClient()
				if err != nil {
					t.Fatalf("build kube client: %v", err)
				}
				state.lowNode = firstReadyNodeName(ctx, t, kubeClient)
				seedMockBackupOnNode(ctx, t, kubeClient, state.namespace, state.lowNode, payload)
				return ctx
			}).
			Assess("restore provider worker succeeds on the seeded backup payload", func(ctx context.Context, t *testing.T, cfg *envconf.Config) context.Context {
				kubeClient, err := operatorKubeClient()
				if err != nil {
					t.Fatalf("build kube client: %v", err)
				}
				params := createRealClusterPopulatorParams(ctx, t, kubeClient, state.namespace, state.lowNode, int64(len(payload)))
				provider := &restorepopulator.Provider{
					WorkerImage:           projectImage,
					WorkerImagePullPolicy: corev1.PullIfNotPresent,
					BackupRoot:            "/var/backup",
					WorkerTargetDevice:    "/dev/restore-target",
				}
				if err := provider.PopulateFn(ctx, params); err != nil {
					t.Fatalf("populate restore worker: %v", err)
				}
				err = wait.For(func(ctx context.Context) (bool, error) {
					done, completeErr := provider.PopulateCompleteFn(ctx, params)
					if completeErr != nil {
						return false, completeErr
					}
					return done, nil
				}, wait.WithTimeout(5*time.Minute), wait.WithInterval(2*time.Second))
				if err != nil {
					t.Fatalf("wait restore worker completion: %v", err)
				}
				return ctx
			}).
			Teardown(func(ctx context.Context, t *testing.T, cfg *envconf.Config) context.Context {
				c := newRuntimeClient(t, cfg)
				cleanupSchedulerFeatureState(ctx, t, c, state)
				return ctx
			}).
			Feature(),
	)
}

func firstReadyNodeName(ctx context.Context, t *testing.T, kubeClient kubernetes.Interface) string {
	t.Helper()
	nodes, err := kubeClient.CoreV1().Nodes().List(ctx, metav1.ListOptions{})
	if err != nil {
		t.Fatalf("list nodes: %v", err)
	}
	for i := range nodes.Items {
		node := &nodes.Items[i]
		if node.Spec.Unschedulable {
			continue
		}
		for _, condition := range node.Status.Conditions {
			if condition.Type == corev1.NodeReady && condition.Status == corev1.ConditionTrue {
				return node.Name
			}
		}
	}
	t.Fatalf("no ready schedulable node found")
	return ""
}

func seedMockBackupOnNode(ctx context.Context, t *testing.T, kubeClient kubernetes.Interface, ns, nodeName, payload string) {
	t.Helper()
	podName := "seed-mock-backup"
	_ = kubeClient.CoreV1().Pods(ns).Delete(ctx, podName, metav1.DeleteOptions{})
	hostPathType := corev1.HostPathDirectoryOrCreate
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: ns,
			Name:      podName,
		},
		Spec: corev1.PodSpec{
			RestartPolicy: corev1.RestartPolicyNever,
			NodeName:      nodeName,
			Containers: []corev1.Container{
				{
					Name:    "seed",
					Image:   "busybox:1.36",
					Command: []string{"sh", "-c", fmt.Sprintf("mkdir -p /backup/%s/pvc-a/backup-a && printf %%s %q > /backup/%s/pvc-a/backup-a/mock.img", ns, payload, ns)},
					VolumeMounts: []corev1.VolumeMount{
						{Name: "backup-root", MountPath: "/backup"},
					},
				},
			},
			Volumes: []corev1.Volume{
				{
					Name: "backup-root",
					VolumeSource: corev1.VolumeSource{
						HostPath: &corev1.HostPathVolumeSource{
							Path: "/var/backup",
							Type: &hostPathType,
						},
					},
				},
			},
		},
	}
	if _, err := kubeClient.CoreV1().Pods(ns).Create(ctx, pod, metav1.CreateOptions{}); err != nil && !apierrors.IsAlreadyExists(err) {
		t.Fatalf("create seed pod: %v", err)
	}
	err := wait.For(func(ctx context.Context) (bool, error) {
		current, err := kubeClient.CoreV1().Pods(ns).Get(ctx, podName, metav1.GetOptions{})
		if err != nil {
			return false, err
		}
		switch current.Status.Phase {
		case corev1.PodSucceeded:
			return true, nil
		case corev1.PodFailed:
			return false, fmt.Errorf("seed pod failed: %s", podFailureMessage(current))
		default:
			return false, nil
		}
	}, wait.WithTimeout(2*time.Minute), wait.WithInterval(2*time.Second))
	if err != nil {
		t.Fatalf("wait seed pod: %v", err)
	}
}

func createRealClusterPopulatorParams(ctx context.Context, t *testing.T, kubeClient kubernetes.Interface, ns, nodeName string, volumeBytes int64) populatorMachinery.PopulatorParams {
	t.Helper()
	volumeMode := corev1.PersistentVolumeBlock
	storageClassName := e2eStorageClassName()
	pvcPrime := &corev1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: ns,
			Name:      "restore-prime",
		},
		Spec: corev1.PersistentVolumeClaimSpec{
			AccessModes: []corev1.PersistentVolumeAccessMode{corev1.ReadWriteOnce},
			Resources: corev1.VolumeResourceRequirements{
				Requests: corev1.ResourceList{
					corev1.ResourceStorage: resource.MustParse("16Mi"),
				},
			},
			VolumeMode:       &volumeMode,
			StorageClassName: &storageClassName,
		},
	}
	if _, err := kubeClient.CoreV1().PersistentVolumeClaims(ns).Create(ctx, pvcPrime, metav1.CreateOptions{}); err != nil && !apierrors.IsAlreadyExists(err) {
		t.Fatalf("create pvcPrime: %v", err)
	}
	currentPrime, err := kubeClient.CoreV1().PersistentVolumeClaims(ns).Get(ctx, pvcPrime.Name, metav1.GetOptions{})
	if err != nil {
		t.Fatalf("get pvcPrime: %v", err)
	}
	pvc := &corev1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: ns,
			Name:      "restore-pvc",
			UID:       types.UID("real-cluster-restore-pvc"),
		},
		Spec: corev1.PersistentVolumeClaimSpec{
			VolumeMode: &volumeMode,
		},
	}
	return populatorMachinery.PopulatorParams{
		KubeClient:   kubeClient,
		Pvc:          pvc,
		PvcPrime:     currentPrime,
		Unstructured: realClusterBackupUnstructured(t, ns, nodeName, volumeBytes),
	}
}

func realClusterBackupUnstructured(t *testing.T, ns, nodeName string, volumeBytes int64) *unstructured.Unstructured {
	t.Helper()
	backup := &snaplanev1alpha1.Backup{
		TypeMeta: metav1.TypeMeta{
			APIVersion: snaplanev1alpha1.GroupVersion.String(),
			Kind:       "Backup",
		},
		ObjectMeta: metav1.ObjectMeta{
			Namespace: ns,
			Name:      "backup-a",
		},
		Status: snaplanev1alpha1.BackupStatus{
			Conditions: []metav1.Condition{
				{
					Type:               snaplanev1alpha1.BackupConditionSucceeded,
					Status:             metav1.ConditionTrue,
					Reason:             "RealClusterSeed",
					Message:            "seeded mock backup for real-cluster restore e2e",
					ObservedGeneration: 1,
					LastTransitionTime: metav1.Now(),
				},
			},
			RestoreSource: snaplanev1alpha1.BackupRestoreSourceStatus{
				NodeName:        nodeName,
				RepositoryPath:  "/var/backup/" + ns + "/pvc-a/backup-a",
				ManifestID:      "mock.img",
				RepoUUID:        "real-cluster-seed",
				Format:          snaplanev1alpha1.RestoreSourceFormatMockImageV1,
				VolumeSizeBytes: volumeBytes,
			},
		},
	}
	if meta.FindStatusCondition(backup.Status.Conditions, snaplanev1alpha1.BackupConditionSucceeded) == nil {
		t.Fatalf("seed backup missing Succeeded condition")
	}
	obj, err := runtime.DefaultUnstructuredConverter.ToUnstructured(backup)
	if err != nil {
		t.Fatalf("convert backup: %v", err)
	}
	u := &unstructured.Unstructured{Object: obj}
	u.SetAPIVersion(snaplanev1alpha1.GroupVersion.String())
	u.SetKind("Backup")
	return u
}

func podFailureMessage(pod *corev1.Pod) string {
	messages := []string{}
	for _, status := range pod.Status.ContainerStatuses {
		if status.State.Terminated != nil && status.State.Terminated.Message != "" {
			messages = append(messages, status.State.Terminated.Message)
		}
	}
	if len(messages) > 0 {
		return strings.Join(messages, "; ")
	}
	return string(pod.Status.Phase)
}

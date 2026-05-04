//go:build e2e
// +build e2e

package e2e

import (
	"context"
	"fmt"
	"sort"
	"testing"
	"time"

	snaplanev1alpha1 "github.com/molpako/snaplane/api/v1alpha1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	ctrlclient "sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/e2e-framework/klient/wait"
	"sigs.k8s.io/e2e-framework/pkg/envconf"
	"sigs.k8s.io/e2e-framework/pkg/features"
)

const (
	incrementalGenerationCount = 4
	blockWriterScript          = `i=0
while true; do
  offset=$(( (i % 32) * 4096 ))
  printf "snaplane-generation-%02d-%s" "$i" "$(date +%s%N)" | dd of=/dev/source bs=1 seek=$offset conv=notrunc status=none
  i=$((i+1))
  sleep 60
done`
)

func TestIncrementalBackupGenerations(t *testing.T) {
	if !isRealClusterLane() || !useRealCBTProvider() {
		t.Skip("incremental generation coverage requires the Ceph nightly real-CBT lane")
	}
	registerFailureDiagnostics(t)

	testenv.Test(t, featureScheduledIncrementalGenerations())
}

func featureScheduledIncrementalGenerations() features.Feature {
	state := newSchedulerFeatureState("incremental-generations")
	writerPodName := state.prefixed("writer-cas")
	writerLeaseNode := state.prefixed("writer-node")
	sourceWriterPodName := state.prefixed("source-writer")

	return features.New("BackupPolicy creates four scheduled incremental CAS generations").
		Setup(func(ctx context.Context, t *testing.T, cfg *envconf.Config) context.Context {
			c := newRuntimeClient(t, cfg)
			createFeatureNamespace(ctx, t, c, state.namespace)

			state.lowNode = writerLeaseNode
			state.nodes = []string{state.lowNode}
			createBackupTargetNode(ctx, t, c, state.lowNode)
			upsertWriterLease(ctx, t, c, namespace, state.lowNode, 100, 20*1024*1024*1024, time.Now().UTC())

			state.pvcName = state.prefixed("pvc")
			ensurePVC(ctx, t, c, state.namespace, state.pvcName, "")
			createBlockWriterPod(ctx, t, c, state.namespace, sourceWriterPodName, state.pvcName)

			createStandaloneCASWriterPod(ctx, t, c, writerPodName, state.lowNode)
			endpoint := waitForStandaloneWriterEndpoint(ctx, t, c, writerPodName)
			updateWriterLeaseEndpoint(ctx, t, c, namespace, state.lowNode, endpoint)

			state.policyName = state.prefixed("policy")
			createScheduledBackupPolicy(ctx, t, c, state.namespace, state.policyName, state.pvcName, "*/5 * * * *")
			return ctx
		}).
		Assess("each 5 minute interval captures a data write and extends the CAS chain", func(ctx context.Context, t *testing.T, cfg *envconf.Config) context.Context {
			c := newRuntimeClient(t, cfg)
			for generation := 1; generation <= incrementalGenerationCount; generation++ {
				backups := waitForSucceededPolicyBackups(ctx, t, c, state.namespace, state.policyName, generation)
				latest := backups[generation-1]
				if latest.Status.RestoreSource.Format != snaplanev1alpha1.RestoreSourceFormatCASV1 {
					t.Fatalf("backup %s/%s used restore format %q, want %q", latest.Namespace, latest.Name, latest.Status.RestoreSource.Format, snaplanev1alpha1.RestoreSourceFormatCASV1)
				}
				if got := len(latest.Status.RestoreSource.ManifestChain); got != generation {
					t.Fatalf("backup %s/%s manifest chain length=%d, want %d", latest.Namespace, latest.Name, got, generation)
				}
				if latest.Status.Progress.IncrementalBytes <= 0 {
					t.Fatalf("backup %s/%s recorded no incremental bytes", latest.Namespace, latest.Name)
				}
			}
			return ctx
		}).
		Teardown(func(ctx context.Context, t *testing.T, cfg *envconf.Config) context.Context {
			c := newRuntimeClient(t, cfg)
			deleteStandaloneWriterPod(ctx, t, c, writerPodName)
			cleanupSchedulerFeatureState(ctx, t, c, state)
			return ctx
		}).
		Feature()
}

func createScheduledBackupPolicy(ctx context.Context, t *testing.T, c ctrlclient.Client, ns, policyName, pvcName, cronExpr string) {
	t.Helper()

	policy := &snaplanev1alpha1.BackupPolicy{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: ns,
			Name:      policyName,
		},
		Spec: snaplanev1alpha1.BackupPolicySpec{
			Source: snaplanev1alpha1.BackupSource{
				PersistentVolumeClaimName: pvcName,
				VolumeSnapshotClassName:   e2eVolumeSnapshotClassName(),
			},
			Schedule:  snaplanev1alpha1.BackupSchedule{Cron: cronExpr},
			Retention: snaplanev1alpha1.BackupRetention{KeepDays: 1},
		},
	}
	if err := c.Create(ctx, policy); err != nil {
		t.Fatalf("create scheduled BackupPolicy %s/%s: %v", ns, policyName, err)
	}
}

func createBlockWriterPod(ctx context.Context, t *testing.T, c ctrlclient.Client, ns, podName, pvcName string) {
	t.Helper()

	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: ns,
			Name:      podName,
		},
		Spec: corev1.PodSpec{
			RestartPolicy: corev1.RestartPolicyAlways,
			Containers: []corev1.Container{
				{
					Name:    "writer",
					Image:   "busybox:1.36",
					Command: []string{"sh", "-c", blockWriterScript},
					VolumeDevices: []corev1.VolumeDevice{
						{Name: "source", DevicePath: "/dev/source"},
					},
				},
			},
			Volumes: []corev1.Volume{
				{
					Name: "source",
					VolumeSource: corev1.VolumeSource{
						PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{
							ClaimName: pvcName,
						},
					},
				},
			},
		},
	}
	if err := c.Create(ctx, pod); err != nil && !apierrors.IsAlreadyExists(err) {
		t.Fatalf("create block writer pod %s/%s: %v", ns, podName, err)
	}
	waitForPodRunning(ctx, t, c, ns, podName)
}

func createStandaloneCASWriterPod(ctx context.Context, t *testing.T, c ctrlclient.Client, podName, nodeName string) {
	t.Helper()
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: namespace,
			Name:      podName,
			Labels: map[string]string{
				"app.kubernetes.io/name": "writer-cas",
			},
		},
		Spec: corev1.PodSpec{
			RestartPolicy:      corev1.RestartPolicyAlways,
			ServiceAccountName: "snaplane-writer-sidecar",
			Containers: []corev1.Container{
				{
					Name:            "writer-sidecar",
					Image:           projectImage,
					ImagePullPolicy: corev1.PullIfNotPresent,
					Command:         []string{"/writer-sidecar"},
					Args: []string{
						"--listen-address=:9443",
						"--backup-root=/var/backup",
						"--heartbeat-interval=30s",
						fmt.Sprintf("--lease-namespace=%s", namespace),
						fmt.Sprintf("--node-name=%s", nodeName),
						"--pod-ip=$(POD_IP)",
						"--tls-cert-file=/certs/server/tls.crt",
						"--tls-key-file=/certs/server/tls.key",
						"--client-ca-file=/certs/ca/tls.crt",
					},
					Env: []corev1.EnvVar{
						{
							Name: "POD_IP",
							ValueFrom: &corev1.EnvVarSource{
								FieldRef: &corev1.ObjectFieldSelector{FieldPath: "status.podIP"},
							},
						},
						{Name: "SNAPLANE_BACKUP_WRITE_MODE", Value: "cas"},
					},
					VolumeMounts: []corev1.VolumeMount{
						{Name: "backup-root", MountPath: "/var/backup"},
						{Name: "writer-sidecar-server-cert", MountPath: "/certs/server", ReadOnly: true},
						{Name: "writer-ca", MountPath: "/certs/ca", ReadOnly: true},
					},
				},
			},
			Volumes: []corev1.Volume{
				{
					Name: "backup-root",
					VolumeSource: corev1.VolumeSource{
						EmptyDir: &corev1.EmptyDirVolumeSource{},
					},
				},
				{
					Name: "writer-sidecar-server-cert",
					VolumeSource: corev1.VolumeSource{
						Secret: &corev1.SecretVolumeSource{SecretName: "writer-sidecar-server-cert"},
					},
				},
				{
					Name: "writer-ca",
					VolumeSource: corev1.VolumeSource{
						Secret: &corev1.SecretVolumeSource{SecretName: "writer-ca"},
					},
				},
			},
		},
	}
	if err := c.Create(ctx, pod); err != nil && !apierrors.IsAlreadyExists(err) {
		t.Fatalf("create standalone CAS writer pod %s/%s: %v", namespace, podName, err)
	}
	waitForPodRunning(ctx, t, c, namespace, podName)
}

func waitForPodRunning(ctx context.Context, t *testing.T, c ctrlclient.Client, ns, podName string) {
	t.Helper()

	err := wait.For(func(ctx context.Context) (bool, error) {
		var pod corev1.Pod
		if err := c.Get(ctx, types.NamespacedName{Namespace: ns, Name: podName}, &pod); err != nil {
			if apierrors.IsNotFound(err) {
				return false, nil
			}
			return false, err
		}
		return pod.Status.Phase == corev1.PodRunning && podReady(&pod), nil
	}, wait.WithTimeout(schedulerE2ETimeout), wait.WithInterval(schedulerE2EPoll))
	if err != nil {
		t.Fatalf("wait for pod running %s/%s: %v", ns, podName, err)
	}
}

func waitForSucceededPolicyBackups(
	ctx context.Context,
	t *testing.T,
	c ctrlclient.Client,
	ns, policyName string,
	want int,
) []snaplanev1alpha1.Backup {
	t.Helper()

	var succeeded []snaplanev1alpha1.Backup
	err := wait.For(func(ctx context.Context) (bool, error) {
		var list snaplanev1alpha1.BackupList
		if err := c.List(ctx, &list, ctrlclient.InNamespace(ns)); err != nil {
			return false, err
		}
		succeeded = succeeded[:0]
		for i := range list.Items {
			backup := list.Items[i]
			if backup.Spec.PolicyRef.Name != policyName {
				continue
			}
			cond := meta.FindStatusCondition(backup.Status.Conditions, snaplanev1alpha1.BackupConditionSucceeded)
			if cond == nil {
				continue
			}
			if cond.Status == metav1.ConditionFalse {
				return false, fmt.Errorf("backup %s/%s failed: reason=%s message=%s", backup.Namespace, backup.Name, cond.Reason, cond.Message)
			}
			if cond.Status != metav1.ConditionTrue {
				continue
			}
			succeeded = append(succeeded, backup)
		}
		sort.SliceStable(succeeded, func(i, j int) bool {
			ti := succeeded[i].CreationTimestamp.Time
			tj := succeeded[j].CreationTimestamp.Time
			if ti.Equal(tj) {
				return succeeded[i].Name < succeeded[j].Name
			}
			return ti.Before(tj)
		})
		return len(succeeded) >= want, nil
	}, wait.WithTimeout(30*time.Minute), wait.WithInterval(schedulerE2EPoll))
	if err != nil {
		t.Fatalf("wait for %d succeeded backups for policy %s/%s: %v", want, ns, policyName, err)
	}
	return append([]snaplanev1alpha1.Backup(nil), succeeded...)
}

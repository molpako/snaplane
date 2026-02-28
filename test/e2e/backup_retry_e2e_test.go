//go:build e2e
// +build e2e

package e2e

import (
	"context"
	"fmt"
	"testing"
	"time"

	volumesnapshotv1 "github.com/kubernetes-csi/external-snapshotter/client/v8/apis/volumesnapshot/v1"
	snaplanev1alpha1 "github.com/molpako/snaplane/api/v1alpha1"
	"github.com/molpako/snaplane/internal/writer"
	appsv1 "k8s.io/api/apps/v1"
	coordinationv1 "k8s.io/api/coordination/v1"
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

func isRetryableEndpointFailureReason(reason string) bool {
	return reason == snaplanev1alpha1.BackupFailureReasonWriterEndpointNotReady ||
		reason == snaplanev1alpha1.BackupFailureReasonWriteTimeout
}

func TestBackupRetryScenarios(t *testing.T) {
	registerFailureDiagnostics(t)

	testenv.Test(
		t,
		featureRetryableWriterEndpointErrorKeepsRunning(),
		featureRetryableWriterEndpointErrorExhaustsBackoff(),
		featureRetryableEndpointRecoveryCompletesBackup(),
		featureRetryRecoverySurvivesControllerRestart(),
	)
}

func featureRetryableWriterEndpointErrorKeepsRunning() features.Feature {
	state := newSchedulerFeatureState("backup-retry-running")
	return features.New("Backup keeps Running and updates retry status on retryable endpoint error").
		Setup(func(ctx context.Context, t *testing.T, cfg *envconf.Config) context.Context {
			c := newRuntimeClient(t, cfg)
			createFeatureNamespace(ctx, t, c, state.namespace)

			state.lowNode = state.prefixed("node-retry")
			state.nodes = []string{state.lowNode}
			createBackupTargetNode(ctx, t, c, state.lowNode)
			upsertWriterLease(ctx, t, c, namespace, state.lowNode, 100, 20*1024*1024*1024, time.Now().UTC())

			state.pvcName = state.prefixed("pvc")
			ensurePVC(ctx, t, c, state.namespace, state.pvcName, "")

			state.policyName = state.prefixed("policy")
			createManualBackupPolicy(ctx, t, c, state.namespace, state.policyName, state.pvcName)

			snapshotName := state.prefixed("snapshot")
			createManualSnapshot(ctx, t, c, state.namespace, snapshotName, state.pvcName)
			ensureSnapshotReady(ctx, t, c, state.namespace, snapshotName)

			backupName := state.prefixed("backup")
			createManualBackup(ctx, t, c, state.namespace, backupName, state.policyName, snapshotName, state.lowNode, 60)
			return ctx
		}).
		Assess("backup stays non-terminal and records retry status", func(ctx context.Context, t *testing.T, cfg *envconf.Config) context.Context {
			c := newRuntimeClient(t, cfg)
			backupName := state.prefixed("backup")

			var observed snaplanev1alpha1.Backup
			err := wait.For(func(ctx context.Context) (bool, error) {
				if err := c.Get(ctx, types.NamespacedName{Namespace: state.namespace, Name: backupName}, &observed); err != nil {
					if apierrors.IsNotFound(err) {
						return false, nil
					}
					return false, err
				}

				succeeded := meta.FindStatusCondition(observed.Status.Conditions, snaplanev1alpha1.BackupConditionSucceeded)
				if succeeded != nil && succeeded.Status == metav1.ConditionFalse {
					return false, fmt.Errorf("backup unexpectedly failed: reason=%s message=%s", succeeded.Reason, succeeded.Message)
				}
				if succeeded == nil || succeeded.Status != metav1.ConditionUnknown {
					return false, nil
				}
				if observed.Status.Retry.Attempt < 1 {
					return false, nil
				}
				if observed.Status.Retry.LastFailureTime == nil {
					return false, nil
				}
				if !isRetryableEndpointFailureReason(observed.Status.Retry.LastFailureReason) {
					return false, nil
				}
				return true, nil
			}, wait.WithTimeout(schedulerE2ETimeout), wait.WithInterval(schedulerE2EPoll))
			if err != nil {
				t.Fatalf("wait for running backup retry status: %v", err)
			}
			return ctx
		}).
		Teardown(func(ctx context.Context, t *testing.T, cfg *envconf.Config) context.Context {
			c := newRuntimeClient(t, cfg)
			cleanupSchedulerFeatureState(ctx, t, c, state)
			return ctx
		}).
		Feature()
}

func featureRetryableWriterEndpointErrorExhaustsBackoff() features.Feature {
	state := newSchedulerFeatureState("backup-retry-exhausted")
	return features.New("Backup transitions to Failed after retry exhaustion on retryable endpoint error").
		Setup(func(ctx context.Context, t *testing.T, cfg *envconf.Config) context.Context {
			c := newRuntimeClient(t, cfg)
			createFeatureNamespace(ctx, t, c, state.namespace)

			state.lowNode = state.prefixed("node-exhaust")
			state.nodes = []string{state.lowNode}
			createBackupTargetNode(ctx, t, c, state.lowNode)
			upsertWriterLease(ctx, t, c, namespace, state.lowNode, 100, 20*1024*1024*1024, time.Now().UTC())

			state.pvcName = state.prefixed("pvc")
			ensurePVC(ctx, t, c, state.namespace, state.pvcName, "")

			state.policyName = state.prefixed("policy")
			createManualBackupPolicy(ctx, t, c, state.namespace, state.policyName, state.pvcName)

			snapshotName := state.prefixed("snapshot")
			createManualSnapshot(ctx, t, c, state.namespace, snapshotName, state.pvcName)
			ensureSnapshotReady(ctx, t, c, state.namespace, snapshotName)

			backupName := state.prefixed("backup")
			createManualBackup(ctx, t, c, state.namespace, backupName, state.policyName, snapshotName, state.lowNode, 0)
			return ctx
		}).
		Assess("backup fails with WriterEndpointNotReady after retries are exhausted", func(ctx context.Context, t *testing.T, cfg *envconf.Config) context.Context {
			c := newRuntimeClient(t, cfg)
			backupName := state.prefixed("backup")
			waitForBackupFailedAnyReason(
				ctx,
				t,
				c,
				state.namespace,
				backupName,
				isRetryableEndpointFailureReason,
			)

			var observed snaplanev1alpha1.Backup
			err := wait.For(func(ctx context.Context) (bool, error) {
				if err := c.Get(ctx, types.NamespacedName{Namespace: state.namespace, Name: backupName}, &observed); err != nil {
					if apierrors.IsNotFound(err) {
						return false, nil
					}
					return false, err
				}
				return observed.Status.Retry.Attempt >= 1 &&
					observed.Status.Retry.LastFailureTime != nil &&
					isRetryableEndpointFailureReason(observed.Status.Retry.LastFailureReason) &&
					observed.Status.CompletionTime != nil, nil
			}, wait.WithTimeout(schedulerE2ETimeout), wait.WithInterval(schedulerE2EPoll))
			if err != nil {
				t.Fatalf("wait for exhausted retry status: %v", err)
			}
			return ctx
		}).
		Teardown(func(ctx context.Context, t *testing.T, cfg *envconf.Config) context.Context {
			c := newRuntimeClient(t, cfg)
			cleanupSchedulerFeatureState(ctx, t, c, state)
			return ctx
		}).
		Feature()
}

func featureRetryableEndpointRecoveryCompletesBackup() features.Feature {
	state := newSchedulerFeatureState("backup-retry-recovery")
	recoveryWriterPodName := state.prefixed("writer-recovery")
	recoveryWriterLeaseNode := state.prefixed("writer-helper")
	return features.New("Backup recovers to Completed after writer endpoint becomes reachable").
		Setup(func(ctx context.Context, t *testing.T, cfg *envconf.Config) context.Context {
			c := newRuntimeClient(t, cfg)
			createFeatureNamespace(ctx, t, c, state.namespace)

			state.lowNode = state.prefixed("node-recover")
			state.nodes = []string{state.lowNode}
			createBackupTargetNode(ctx, t, c, state.lowNode)
			upsertWriterLease(ctx, t, c, namespace, state.lowNode, 100, 20*1024*1024*1024, time.Now().UTC())

			state.pvcName = state.prefixed("pvc")
			ensurePVC(ctx, t, c, state.namespace, state.pvcName, "")

			state.policyName = state.prefixed("policy")
			createManualBackupPolicy(ctx, t, c, state.namespace, state.policyName, state.pvcName)

			snapshotName := state.prefixed("snapshot")
			createManualSnapshot(ctx, t, c, state.namespace, snapshotName, state.pvcName)
			ensureSnapshotReady(ctx, t, c, state.namespace, snapshotName)

			createStandaloneWriterPod(ctx, t, c, recoveryWriterPodName, recoveryWriterLeaseNode)

			backupName := state.prefixed("backup")
			createManualBackup(ctx, t, c, state.namespace, backupName, state.policyName, snapshotName, state.lowNode, 60)
			return ctx
		}).
		Assess("backup retries once, then succeeds after endpoint update", func(ctx context.Context, t *testing.T, cfg *envconf.Config) context.Context {
			c := newRuntimeClient(t, cfg)
			backupName := state.prefixed("backup")

			var retryObserved snaplanev1alpha1.Backup
			err := wait.For(func(ctx context.Context) (bool, error) {
				if err := c.Get(ctx, types.NamespacedName{Namespace: state.namespace, Name: backupName}, &retryObserved); err != nil {
					if apierrors.IsNotFound(err) {
						return false, nil
					}
					return false, err
				}
				succeeded := meta.FindStatusCondition(retryObserved.Status.Conditions, snaplanev1alpha1.BackupConditionSucceeded)
				if succeeded == nil || succeeded.Status != metav1.ConditionUnknown {
					return false, nil
				}
				return retryObserved.Status.Retry.Attempt >= 1 &&
					isRetryableEndpointFailureReason(retryObserved.Status.Retry.LastFailureReason), nil
			}, wait.WithTimeout(schedulerE2ETimeout), wait.WithInterval(schedulerE2EPoll))
			if err != nil {
				t.Fatalf("wait for retry before recovery: %v", err)
			}

			endpoint := waitForStandaloneWriterEndpoint(ctx, t, c, recoveryWriterPodName)
			updateWriterLeaseEndpoint(ctx, t, c, namespace, state.lowNode, endpoint)

			var completed snaplanev1alpha1.Backup
			err = wait.For(func(ctx context.Context) (bool, error) {
				if err := c.Get(ctx, types.NamespacedName{Namespace: state.namespace, Name: backupName}, &completed); err != nil {
					if apierrors.IsNotFound(err) {
						return false, nil
					}
					return false, err
				}
				succeeded := meta.FindStatusCondition(completed.Status.Conditions, snaplanev1alpha1.BackupConditionSucceeded)
				if succeeded == nil || succeeded.Status != metav1.ConditionTrue {
					return false, nil
				}
				return completed.Status.CompletionTime != nil &&
					completed.Status.Progress.BytesDone > 0 &&
					completed.Status.Progress.TotalBytes >= completed.Status.Progress.BytesDone, nil
			}, wait.WithTimeout(schedulerE2ETimeout), wait.WithInterval(schedulerE2EPoll))
			if err != nil {
				t.Fatalf("wait for completed backup after endpoint recovery: %v", err)
			}
			return ctx
		}).
		Teardown(func(ctx context.Context, t *testing.T, cfg *envconf.Config) context.Context {
			c := newRuntimeClient(t, cfg)
			deleteStandaloneWriterPod(ctx, t, c, recoveryWriterPodName)
			cleanupSchedulerFeatureState(ctx, t, c, state)
			return ctx
		}).
		Feature()
}

func featureRetryRecoverySurvivesControllerRestart() features.Feature {
	state := newSchedulerFeatureState("backup-retry-restart")
	recoveryWriterPodName := state.prefixed("writer-restart")
	recoveryWriterLeaseNode := state.prefixed("writer-helper")
	return features.New("Backup completes after controller-manager restart during retry recovery").
		Setup(func(ctx context.Context, t *testing.T, cfg *envconf.Config) context.Context {
			c := newRuntimeClient(t, cfg)
			createFeatureNamespace(ctx, t, c, state.namespace)

			state.lowNode = state.prefixed("node-restart")
			state.nodes = []string{state.lowNode}
			createBackupTargetNode(ctx, t, c, state.lowNode)
			upsertWriterLease(ctx, t, c, namespace, state.lowNode, 100, 20*1024*1024*1024, time.Now().UTC())

			state.pvcName = state.prefixed("pvc")
			ensurePVC(ctx, t, c, state.namespace, state.pvcName, "")

			state.policyName = state.prefixed("policy")
			createManualBackupPolicy(ctx, t, c, state.namespace, state.policyName, state.pvcName)

			snapshotName := state.prefixed("snapshot")
			createManualSnapshot(ctx, t, c, state.namespace, snapshotName, state.pvcName)
			ensureSnapshotReady(ctx, t, c, state.namespace, snapshotName)

			createStandaloneWriterPod(ctx, t, c, recoveryWriterPodName, recoveryWriterLeaseNode)

			backupName := state.prefixed("backup")
			createManualBackup(ctx, t, c, state.namespace, backupName, state.policyName, snapshotName, state.lowNode, 60)
			return ctx
		}).
		Assess("backup recovers and completes after manager restart", func(ctx context.Context, t *testing.T, cfg *envconf.Config) context.Context {
			c := newRuntimeClient(t, cfg)
			backupName := state.prefixed("backup")

			var retryObserved snaplanev1alpha1.Backup
			err := wait.For(func(ctx context.Context) (bool, error) {
				if err := c.Get(ctx, types.NamespacedName{Namespace: state.namespace, Name: backupName}, &retryObserved); err != nil {
					if apierrors.IsNotFound(err) {
						return false, nil
					}
					return false, err
				}
				succeeded := meta.FindStatusCondition(retryObserved.Status.Conditions, snaplanev1alpha1.BackupConditionSucceeded)
				if succeeded == nil || succeeded.Status != metav1.ConditionUnknown {
					return false, nil
				}
				return retryObserved.Status.Retry.Attempt >= 1 &&
					isRetryableEndpointFailureReason(retryObserved.Status.Retry.LastFailureReason), nil
			}, wait.WithTimeout(schedulerE2ETimeout), wait.WithInterval(schedulerE2EPoll))
			if err != nil {
				t.Fatalf("wait for retry before restart: %v", err)
			}

			endpoint := waitForStandaloneWriterEndpoint(ctx, t, c, recoveryWriterPodName)
			updateWriterLeaseEndpoint(ctx, t, c, namespace, state.lowNode, endpoint)
			restartControllerManagerPod(ctx, t, c)

			var completed snaplanev1alpha1.Backup
			err = wait.For(func(ctx context.Context) (bool, error) {
				if err := c.Get(ctx, types.NamespacedName{Namespace: state.namespace, Name: backupName}, &completed); err != nil {
					if apierrors.IsNotFound(err) {
						return false, nil
					}
					return false, err
				}
				succeeded := meta.FindStatusCondition(completed.Status.Conditions, snaplanev1alpha1.BackupConditionSucceeded)
				if succeeded == nil || succeeded.Status != metav1.ConditionTrue {
					return false, nil
				}
				return completed.Status.CompletionTime != nil, nil
			}, wait.WithTimeout(schedulerE2ETimeout), wait.WithInterval(schedulerE2EPoll))
			if err != nil {
				t.Fatalf("wait for backup completion after manager restart: %v", err)
			}
			return ctx
		}).
		Teardown(func(ctx context.Context, t *testing.T, cfg *envconf.Config) context.Context {
			c := newRuntimeClient(t, cfg)
			deleteStandaloneWriterPod(ctx, t, c, recoveryWriterPodName)
			cleanupSchedulerFeatureState(ctx, t, c, state)
			return ctx
		}).
		Feature()
}

func createStandaloneWriterPod(ctx context.Context, t *testing.T, c ctrlclient.Client, podName, nodeName string) {
	t.Helper()
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: namespace,
			Name:      podName,
			Labels: map[string]string{
				"app.kubernetes.io/name": "writer-recovery",
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
		t.Fatalf("create standalone writer pod %s/%s: %v", namespace, podName, err)
	}
}

func restartControllerManagerPod(ctx context.Context, t *testing.T, c ctrlclient.Client) {
	t.Helper()

	var pods corev1.PodList
	if err := c.List(
		ctx,
		&pods,
		ctrlclient.InNamespace(namespace),
		ctrlclient.MatchingLabels{"control-plane": "controller-manager"},
	); err != nil {
		t.Fatalf("list controller-manager pods: %v", err)
	}
	if len(pods.Items) == 0 {
		t.Fatalf("controller-manager pod not found")
	}
	target := pods.Items[0].DeepCopy()
	if err := c.Delete(ctx, target); err != nil && !apierrors.IsNotFound(err) {
		t.Fatalf("delete controller-manager pod %s/%s: %v", target.Namespace, target.Name, err)
	}

	err := wait.For(func(ctx context.Context) (bool, error) {
		var deploy appsv1.Deployment
		if err := c.Get(ctx, types.NamespacedName{Namespace: namespace, Name: managerDeploymentName}, &deploy); err != nil {
			if apierrors.IsNotFound(err) {
				return false, nil
			}
			return false, err
		}
		return deploy.Status.AvailableReplicas >= 1, nil
	}, wait.WithTimeout(schedulerE2ETimeout), wait.WithInterval(schedulerE2EPoll))
	if err != nil {
		t.Fatalf("wait controller-manager recovery after restart: %v", err)
	}
}

func waitForStandaloneWriterEndpoint(ctx context.Context, t *testing.T, c ctrlclient.Client, podName string) string {
	t.Helper()
	endpoint := ""
	err := wait.For(func(ctx context.Context) (bool, error) {
		var pod corev1.Pod
		if err := c.Get(ctx, types.NamespacedName{Namespace: namespace, Name: podName}, &pod); err != nil {
			if apierrors.IsNotFound(err) {
				return false, nil
			}
			return false, err
		}
		if pod.Status.Phase != corev1.PodRunning || pod.Status.PodIP == "" {
			return false, nil
		}
		endpoint = fmt.Sprintf("%s:9443", pod.Status.PodIP)
		return true, nil
	}, wait.WithTimeout(schedulerE2ETimeout), wait.WithInterval(schedulerE2EPoll))
	if err != nil {
		t.Fatalf("wait for standalone writer pod endpoint %s/%s: %v", namespace, podName, err)
	}
	return endpoint
}

func deleteStandaloneWriterPod(ctx context.Context, t *testing.T, c ctrlclient.Client, podName string) {
	t.Helper()
	pod := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Namespace: namespace, Name: podName}}
	if err := c.Delete(ctx, pod); err != nil && !apierrors.IsNotFound(err) {
		t.Fatalf("delete standalone writer pod %s/%s: %v", namespace, podName, err)
	}
}

func updateWriterLeaseEndpoint(
	ctx context.Context,
	t *testing.T,
	c ctrlclient.Client,
	leaseNS, nodeName, endpoint string,
) {
	t.Helper()

	leaseName := writer.LeaseNameForNode(nodeName)
	var lease coordinationv1.Lease
	if err := c.Get(ctx, types.NamespacedName{Namespace: leaseNS, Name: leaseName}, &lease); err != nil {
		t.Fatalf("get lease %s/%s: %v", leaseNS, leaseName, err)
	}
	base := lease.DeepCopy()
	if lease.Annotations == nil {
		lease.Annotations = map[string]string{}
	}
	lease.Annotations[snaplanev1alpha1.WriterEndpointAnnotationKey] = endpoint
	renew := metav1.MicroTime{Time: time.Now().UTC()}
	lease.Spec.RenewTime = &renew
	if err := c.Patch(ctx, &lease, ctrlclient.MergeFrom(base)); err != nil {
		t.Fatalf("patch lease %s/%s endpoint: %v", leaseNS, leaseName, err)
	}
}

func createManualBackupPolicy(ctx context.Context, t *testing.T, c ctrlclient.Client, ns, policyName, pvcName string) {
	t.Helper()

	suspend := true
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
			Schedule: snaplanev1alpha1.BackupSchedule{
				Cron:    "* * * * *",
				Suspend: &suspend,
			},
			Retention: snaplanev1alpha1.BackupRetention{KeepDays: 1},
		},
	}
	if err := c.Create(ctx, policy); err != nil {
		t.Fatalf("create manual BackupPolicy %s/%s: %v", ns, policyName, err)
	}
}

func createManualSnapshot(ctx context.Context, t *testing.T, c ctrlclient.Client, ns, snapshotName, pvcName string) {
	t.Helper()
	className := e2eVolumeSnapshotClassName()
	snapshot := &volumesnapshotv1.VolumeSnapshot{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: ns,
			Name:      snapshotName,
		},
		Spec: volumesnapshotv1.VolumeSnapshotSpec{
			VolumeSnapshotClassName: &className,
			Source: volumesnapshotv1.VolumeSnapshotSource{
				PersistentVolumeClaimName: &pvcName,
			},
		},
	}
	if err := c.Create(ctx, snapshot); err != nil {
		t.Fatalf("create manual VolumeSnapshot %s/%s: %v", ns, snapshotName, err)
	}
}

func createManualBackup(
	ctx context.Context,
	t *testing.T,
	c ctrlclient.Client,
	ns, backupName, policyName, snapshotName, destinationNode string,
	backoffLimit int32,
) {
	t.Helper()

	backup := &snaplanev1alpha1.Backup{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: ns,
			Name:      backupName,
		},
		Spec: snaplanev1alpha1.BackupSpec{
			PolicyRef: snaplanev1alpha1.LocalNameReference{Name: policyName},
			VolumeSnapshotRef: snaplanev1alpha1.VolumeSnapshotReference{
				Name: snapshotName,
			},
			Destination: snaplanev1alpha1.BackupDestination{
				NodeName: destinationNode,
			},
			Transfer: snaplanev1alpha1.TransferPolicy{
				BackoffLimit: &backoffLimit,
			},
		},
	}
	if err := c.Create(ctx, backup); err != nil {
		t.Fatalf("create manual Backup %s/%s: %v", ns, backupName, err)
	}
}

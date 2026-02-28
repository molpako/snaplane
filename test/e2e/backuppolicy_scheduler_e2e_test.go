//go:build e2e
// +build e2e

package e2e

import (
	"context"
	"fmt"
	"math/rand"
	"sort"
	"strings"
	"testing"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	coordinationv1 "k8s.io/api/coordination/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	ctrlclient "sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/e2e-framework/klient"
	"sigs.k8s.io/e2e-framework/klient/wait"
	"sigs.k8s.io/e2e-framework/pkg/envconf"
	"sigs.k8s.io/e2e-framework/pkg/features"

	volumesnapshotv1 "github.com/kubernetes-csi/external-snapshotter/client/v8/apis/volumesnapshot/v1"
	snaplanev1alpha1 "github.com/molpako/snaplane/api/v1alpha1"
	"github.com/molpako/snaplane/internal/writer"
)

const (
	schedulerE2ETimeout  = 5 * time.Minute
	schedulerE2EPoll     = 2 * time.Second
	unavailableNodeValue = "unavailable"
)

func TestBackupPolicySchedulerScenarios(t *testing.T) {
	registerFailureDiagnostics(t)

	testenv.Test(
		t,
		featureSelectsMinimumUsedNode(),
		featureUsesNodeNameTieBreak(),
		featureAssignedNodeUnavailableFallsBackToUnavailable(),
	)
}

func featureSelectsMinimumUsedNode() features.Feature {
	state := newSchedulerFeatureState("sched-min-used")
	return features.New("BackupPolicy scheduler selects minimum used-bytes node").
		Setup(func(ctx context.Context, t *testing.T, cfg *envconf.Config) context.Context {
			c := newRuntimeClient(t, cfg)
			createFeatureNamespace(ctx, t, c, state.namespace)

			state.lowNode = state.prefixed("node-low")
			state.highNode = state.prefixed("node-high")
			state.nodes = []string{state.lowNode, state.highNode}
			createBackupTargetNode(ctx, t, c, state.lowNode)
			createBackupTargetNode(ctx, t, c, state.highNode)
			upsertWriterLease(ctx, t, c, namespace, state.lowNode, 100, 20*1024*1024*1024, time.Now().UTC())
			upsertWriterLease(ctx, t, c, namespace, state.highNode, 1000, 20*1024*1024*1024, time.Now().UTC())

			state.pvcName = state.prefixed("pvc")
			ensurePVC(ctx, t, c, state.namespace, state.pvcName, "")

			state.policyName = state.prefixed("policy")
			createBackupPolicy(ctx, t, c, state.namespace, state.policyName, state.pvcName)
			return ctx
		}).
		Assess("snapshot becomes ready and backup destination chooses lowest usedBytes", func(ctx context.Context, t *testing.T, cfg *envconf.Config) context.Context {
			c := newRuntimeClient(t, cfg)
			snapshot := waitForPolicySnapshot(ctx, t, c, state.namespace, state.policyName)
			ensureSnapshotReady(ctx, t, c, snapshot.Namespace, snapshot.Name)

			backup := waitForPolicyBackup(ctx, t, c, state.namespace, state.policyName)
			if backup.Spec.Destination.NodeName != state.lowNode {
				t.Fatalf("expected destination node %q, got %q", state.lowNode, backup.Spec.Destination.NodeName)
			}

			var pvc corev1.PersistentVolumeClaim
			if err := c.Get(ctx, types.NamespacedName{Namespace: state.namespace, Name: state.pvcName}, &pvc); err != nil {
				t.Fatalf("get pvc: %v", err)
			}
			if pvc.Annotations[snaplanev1alpha1.AssignedBackupNodeAnnotationKey] != state.lowNode {
				t.Fatalf("expected pvc assignment %q, got %q", state.lowNode, pvc.Annotations[snaplanev1alpha1.AssignedBackupNodeAnnotationKey])
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

func featureUsesNodeNameTieBreak() features.Feature {
	state := newSchedulerFeatureState("sched-tie-break")
	return features.New("BackupPolicy scheduler tie-breaks by nodeName on equal used-bytes").
		Setup(func(ctx context.Context, t *testing.T, cfg *envconf.Config) context.Context {
			c := newRuntimeClient(t, cfg)
			createFeatureNamespace(ctx, t, c, state.namespace)

			state.lowNode = state.prefixed("node-a")
			state.highNode = state.prefixed("node-b")
			state.nodes = []string{state.lowNode, state.highNode}
			createBackupTargetNode(ctx, t, c, state.lowNode)
			createBackupTargetNode(ctx, t, c, state.highNode)
			upsertWriterLease(ctx, t, c, namespace, state.lowNode, 100, 20*1024*1024*1024, time.Now().UTC())
			upsertWriterLease(ctx, t, c, namespace, state.highNode, 100, 20*1024*1024*1024, time.Now().UTC())

			state.pvcName = state.prefixed("pvc")
			ensurePVC(ctx, t, c, state.namespace, state.pvcName, "")

			state.policyName = state.prefixed("policy")
			createBackupPolicy(ctx, t, c, state.namespace, state.policyName, state.pvcName)
			return ctx
		}).
		Assess("equal used-bytes selects lexicographically smallest node name", func(ctx context.Context, t *testing.T, cfg *envconf.Config) context.Context {
			c := newRuntimeClient(t, cfg)
			snapshot := waitForPolicySnapshot(ctx, t, c, state.namespace, state.policyName)
			ensureSnapshotReady(ctx, t, c, snapshot.Namespace, snapshot.Name)

			backup := waitForPolicyBackup(ctx, t, c, state.namespace, state.policyName)
			expected := state.lowNode
			if strings.Compare(state.highNode, state.lowNode) < 0 {
				expected = state.highNode
			}
			if backup.Spec.Destination.NodeName != expected {
				t.Fatalf("expected destination node %q, got %q", expected, backup.Spec.Destination.NodeName)
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

func featureAssignedNodeUnavailableFallsBackToUnavailable() features.Feature {
	state := newSchedulerFeatureState("sched-assigned-unavailable")
	return features.New("Assigned PVC node outside candidate set falls back to unavailable and fails backup").
		Setup(func(ctx context.Context, t *testing.T, cfg *envconf.Config) context.Context {
			c := newRuntimeClient(t, cfg)
			createFeatureNamespace(ctx, t, c, state.namespace)

			state.lowNode = state.prefixed("node-assigned")
			state.nodes = []string{state.lowNode}
			createBackupTargetNode(ctx, t, c, state.lowNode)
			upsertWriterLease(
				ctx,
				t,
				c,
				namespace,
				state.lowNode,
				100,
				20*1024*1024*1024,
				time.Now().UTC().Add(-2*time.Minute), // stale lease
			)

			state.pvcName = state.prefixed("pvc")
			ensurePVC(ctx, t, c, state.namespace, state.pvcName, state.lowNode)

			state.policyName = state.prefixed("policy")
			createBackupPolicy(ctx, t, c, state.namespace, state.policyName, state.pvcName)
			return ctx
		}).
		Assess("backup is created with unavailable destination and fails with AssignedNodeUnavailable", func(ctx context.Context, t *testing.T, cfg *envconf.Config) context.Context {
			c := newRuntimeClient(t, cfg)
			snapshot := waitForPolicySnapshot(ctx, t, c, state.namespace, state.policyName)
			ensureSnapshotReady(ctx, t, c, snapshot.Namespace, snapshot.Name)

			backup := waitForPolicyBackup(ctx, t, c, state.namespace, state.policyName)
			if backup.Spec.Destination.NodeName != unavailableNodeValue {
				t.Fatalf("expected destination node %q, got %q", unavailableNodeValue, backup.Spec.Destination.NodeName)
			}

			waitForBackupFailedReason(
				ctx,
				t,
				c,
				state.namespace,
				backup.Name,
				snaplanev1alpha1.BackupFailureReasonAssignedNodeUnavailable,
			)
			return ctx
		}).
		Teardown(func(ctx context.Context, t *testing.T, cfg *envconf.Config) context.Context {
			c := newRuntimeClient(t, cfg)
			cleanupSchedulerFeatureState(ctx, t, c, state)
			return ctx
		}).
		Feature()
}

type schedulerFeatureState struct {
	suffix     string
	namespace  string
	policyName string
	pvcName    string
	lowNode    string
	highNode   string
	nodes      []string
}

func newSchedulerFeatureState(prefix string) *schedulerFeatureState {
	suffix := randomSuffix()
	return &schedulerFeatureState{
		suffix:    suffix,
		namespace: fmt.Sprintf("%s-%s", prefix, suffix),
	}
}

func (s *schedulerFeatureState) prefixed(prefix string) string {
	return fmt.Sprintf("%s-%s", prefix, s.suffix)
}

func randomSuffix() string {
	return fmt.Sprintf("%d-%d", time.Now().UnixNano(), rand.Intn(10000))
}

func newRuntimeClient(t *testing.T, cfg *envconf.Config) ctrlclient.Client {
	t.Helper()

	scheme := runtime.NewScheme()
	mustAddScheme(t, appsv1.AddToScheme, scheme)
	mustAddScheme(t, corev1.AddToScheme, scheme)
	mustAddScheme(t, coordinationv1.AddToScheme, scheme)
	mustAddScheme(t, volumesnapshotv1.AddToScheme, scheme)
	mustAddScheme(t, snaplanev1alpha1.AddToScheme, scheme)

	c, err := klient.NewControllerRuntimeClient(cfg.Client().RESTConfig(), scheme)
	if err != nil {
		t.Fatalf("create controller-runtime client: %v", err)
	}
	return c
}

func mustAddScheme(t *testing.T, add func(*runtime.Scheme) error, scheme *runtime.Scheme) {
	t.Helper()
	if err := add(scheme); err != nil {
		t.Fatalf("add scheme failed: %v", err)
	}
}

func createFeatureNamespace(ctx context.Context, t *testing.T, c ctrlclient.Client, name string) {
	t.Helper()
	ns := &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: name}}
	if err := c.Create(ctx, ns); err != nil && !apierrors.IsAlreadyExists(err) {
		t.Fatalf("create namespace %q: %v", name, err)
	}
}

func ensurePVC(ctx context.Context, t *testing.T, c ctrlclient.Client, ns, pvcName, assignedNode string) {
	t.Helper()
	pvc := &corev1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: ns,
			Name:      pvcName,
		},
		Spec: corev1.PersistentVolumeClaimSpec{
			AccessModes: []corev1.PersistentVolumeAccessMode{corev1.ReadWriteOnce},
			Resources: corev1.VolumeResourceRequirements{
				Requests: corev1.ResourceList{
					corev1.ResourceStorage: resource.MustParse("1Gi"),
				},
			},
		},
	}
	if activeTLSMode == tlsModeCertManager {
		sc := "csi-hostpath-sc"
		pvc.Spec.StorageClassName = &sc
		vm := corev1.PersistentVolumeBlock
		pvc.Spec.VolumeMode = &vm
	}
	if err := c.Create(ctx, pvc); err != nil && !apierrors.IsAlreadyExists(err) {
		t.Fatalf("create pvc %s/%s: %v", ns, pvcName, err)
	}

	if activeTLSMode == tlsModeCertManager {
		waitForPVCBound(ctx, t, c, ns, pvcName)
	}

	if assignedNode == "" {
		return
	}
	var current corev1.PersistentVolumeClaim
	if err := c.Get(ctx, types.NamespacedName{Namespace: ns, Name: pvcName}, &current); err != nil {
		t.Fatalf("get pvc for assignment: %v", err)
	}
	base := current.DeepCopy()
	if current.Annotations == nil {
		current.Annotations = map[string]string{}
	}
	current.Annotations[snaplanev1alpha1.AssignedBackupNodeAnnotationKey] = assignedNode
	current.Annotations[snaplanev1alpha1.AssignedBackupNodeAtAnnotationKey] = time.Now().UTC().Format(time.RFC3339)
	if err := c.Patch(ctx, &current, ctrlclient.MergeFrom(base)); err != nil {
		t.Fatalf("patch pvc assignment: %v", err)
	}
}

func createBackupPolicy(ctx context.Context, t *testing.T, c ctrlclient.Client, ns, policyName, pvcName string) {
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
			Schedule: snaplanev1alpha1.BackupSchedule{Cron: "* * * * *"},
			Retention: snaplanev1alpha1.BackupRetention{
				KeepDays: 1,
			},
			Manual: snaplanev1alpha1.BackupPolicyManualSpec{
				RequestID: fmt.Sprintf("manual-%d", time.Now().UnixNano()),
			},
		},
	}
	if err := c.Create(ctx, policy); err != nil {
		t.Fatalf("create BackupPolicy %s/%s: %v", ns, policyName, err)
	}
}

func createBackupTargetNode(ctx context.Context, t *testing.T, c ctrlclient.Client, nodeName string) {
	t.Helper()

	node := &corev1.Node{
		ObjectMeta: metav1.ObjectMeta{
			Name: nodeName,
			Labels: map[string]string{
				snaplanev1alpha1.BackupTargetNodeLabelKey: "true",
			},
		},
	}
	if err := c.Create(ctx, node); err != nil && !apierrors.IsAlreadyExists(err) {
		t.Fatalf("create node %q: %v", nodeName, err)
	}

	var current corev1.Node
	if err := c.Get(ctx, types.NamespacedName{Name: nodeName}, &current); err != nil {
		t.Fatalf("get node %q: %v", nodeName, err)
	}
	base := current.DeepCopy()
	if current.Labels == nil {
		current.Labels = map[string]string{}
	}
	current.Labels[snaplanev1alpha1.BackupTargetNodeLabelKey] = "true"
	if err := c.Patch(ctx, &current, ctrlclient.MergeFrom(base)); err != nil {
		t.Fatalf("patch node %q labels: %v", nodeName, err)
	}

	err := wait.For(func(ctx context.Context) (bool, error) {
		var latest corev1.Node
		if getErr := c.Get(ctx, types.NamespacedName{Name: nodeName}, &latest); getErr != nil {
			return false, getErr
		}
		latest.Status.Conditions = []corev1.NodeCondition{
			{
				Type:   corev1.NodeReady,
				Status: corev1.ConditionTrue,
			},
		}
		if updateErr := c.Status().Update(ctx, &latest); updateErr != nil {
			if apierrors.IsConflict(updateErr) {
				return false, nil
			}
			return false, updateErr
		}
		return true, nil
	}, wait.WithTimeout(30*time.Second), wait.WithInterval(500*time.Millisecond))
	if err != nil {
		t.Fatalf("update node %q status: %v", nodeName, err)
	}
}

func upsertWriterLease(
	ctx context.Context,
	t *testing.T,
	c ctrlclient.Client,
	leaseNS, nodeName string,
	usedBytes, availableBytes int64,
	renewTime time.Time,
) {
	t.Helper()

	leaseName := writer.LeaseNameForNode(nodeName)
	var current coordinationv1.Lease
	err := c.Get(ctx, types.NamespacedName{Namespace: leaseNS, Name: leaseName}, &current)
	if err != nil && !apierrors.IsNotFound(err) {
		t.Fatalf("get lease %s/%s: %v", leaseNS, leaseName, err)
	}

	if apierrors.IsNotFound(err) {
		current = coordinationv1.Lease{
			ObjectMeta: metav1.ObjectMeta{
				Namespace: leaseNS,
				Name:      leaseName,
			},
		}
	}

	base := current.DeepCopy()
	if current.Annotations == nil {
		current.Annotations = map[string]string{}
	}
	current.Annotations[snaplanev1alpha1.WriterEndpointAnnotationKey] = "127.0.0.1:9443"
	current.Annotations[snaplanev1alpha1.LeaseUsedBytesAnnotationKey] = fmt.Sprintf("%d", usedBytes)
	current.Annotations[snaplanev1alpha1.LeaseAvailableBytesAnnotationKey] = fmt.Sprintf("%d", availableBytes)
	rt := metav1.MicroTime{Time: renewTime}
	current.Spec.RenewTime = &rt

	if apierrors.IsNotFound(err) {
		if createErr := c.Create(ctx, &current); createErr != nil {
			t.Fatalf("create lease %s/%s: %v", leaseNS, leaseName, createErr)
		}
		return
	}
	if patchErr := c.Patch(ctx, &current, ctrlclient.MergeFrom(base)); patchErr != nil {
		t.Fatalf("patch lease %s/%s: %v", leaseNS, leaseName, patchErr)
	}
}

func waitForPolicySnapshot(ctx context.Context, t *testing.T, c ctrlclient.Client, ns, policyName string) volumesnapshotv1.VolumeSnapshot {
	t.Helper()
	var snapshot volumesnapshotv1.VolumeSnapshot
	err := wait.For(func(ctx context.Context) (bool, error) {
		var list volumesnapshotv1.VolumeSnapshotList
		if err := c.List(ctx, &list, ctrlclient.InNamespace(ns), ctrlclient.MatchingLabels{
			snaplanev1alpha1.PolicyLabelKey: policyName,
		}); err != nil {
			return false, err
		}
		if len(list.Items) == 0 {
			return false, nil
		}
		sort.Slice(list.Items, func(i, j int) bool {
			return list.Items[i].Name < list.Items[j].Name
		})
		snapshot = list.Items[0]
		return true, nil
	}, wait.WithTimeout(schedulerE2ETimeout), wait.WithInterval(schedulerE2EPoll))
	if err != nil {
		t.Fatalf("wait for policy snapshot %s/%s: %v", ns, policyName, err)
	}
	return snapshot
}

func ensureSnapshotReady(ctx context.Context, t *testing.T, c ctrlclient.Client, ns, snapshotName string) {
	t.Helper()
	if activeTLSMode == tlsModeCertManager {
		if err := waitForSnapshotReady(ctx, c, ns, snapshotName); err != nil {
			t.Fatalf("wait for real snapshot ready %s/%s: %v", ns, snapshotName, err)
		}
		return
	}
	markSnapshotReady(ctx, t, c, ns, snapshotName)
}

func waitForSnapshotReady(ctx context.Context, c ctrlclient.Client, ns, snapshotName string) error {
	err := wait.For(func(ctx context.Context) (bool, error) {
		var snapshot volumesnapshotv1.VolumeSnapshot
		if err := c.Get(ctx, types.NamespacedName{Namespace: ns, Name: snapshotName}, &snapshot); err != nil {
			return false, err
		}
		return snapshot.Status != nil && snapshot.Status.ReadyToUse != nil && *snapshot.Status.ReadyToUse, nil
	}, wait.WithTimeout(2*time.Minute), wait.WithInterval(schedulerE2EPoll))
	if err != nil {
		return fmt.Errorf("wait for snapshot ready %s/%s: %w", ns, snapshotName, err)
	}
	return nil
}

func waitForPVCBound(ctx context.Context, t *testing.T, c ctrlclient.Client, ns, pvcName string) {
	t.Helper()
	err := wait.For(func(ctx context.Context) (bool, error) {
		var pvc corev1.PersistentVolumeClaim
		if err := c.Get(ctx, types.NamespacedName{Namespace: ns, Name: pvcName}, &pvc); err != nil {
			return false, err
		}
		return pvc.Status.Phase == corev1.ClaimBound, nil
	}, wait.WithTimeout(2*time.Minute), wait.WithInterval(schedulerE2EPoll))
	if err != nil {
		t.Fatalf("wait for pvc bound %s/%s: %v", ns, pvcName, err)
	}
}

func e2eVolumeSnapshotClassName() string {
	if activeTLSMode == tlsModeCertManager {
		return "csi-hostpath-snapclass"
	}
	return "csi-rbd-snapclass"
}

func markSnapshotReady(ctx context.Context, t *testing.T, c ctrlclient.Client, ns, snapshotName string) {
	t.Helper()
	var snapshot volumesnapshotv1.VolumeSnapshot
	if err := c.Get(ctx, types.NamespacedName{Namespace: ns, Name: snapshotName}, &snapshot); err != nil {
		t.Fatalf("get snapshot %s/%s: %v", ns, snapshotName, err)
	}
	if snapshot.Status == nil {
		snapshot.Status = &volumesnapshotv1.VolumeSnapshotStatus{}
	}
	snapshot.Status.ReadyToUse = boolPtr(true)
	if err := c.Status().Update(ctx, &snapshot); err != nil {
		t.Fatalf("update snapshot ready status %s/%s: %v", ns, snapshotName, err)
	}
}

func waitForPolicyBackup(ctx context.Context, t *testing.T, c ctrlclient.Client, ns, policyName string) snaplanev1alpha1.Backup {
	t.Helper()
	var backup snaplanev1alpha1.Backup
	err := wait.For(func(ctx context.Context) (bool, error) {
		var list snaplanev1alpha1.BackupList
		if err := c.List(ctx, &list, ctrlclient.InNamespace(ns)); err != nil {
			return false, err
		}
		for i := range list.Items {
			if list.Items[i].Spec.PolicyRef.Name == policyName {
				backup = list.Items[i]
				return true, nil
			}
		}
		return false, nil
	}, wait.WithTimeout(schedulerE2ETimeout), wait.WithInterval(schedulerE2EPoll))
	if err != nil {
		t.Fatalf("wait for backup for policy %s/%s: %v", ns, policyName, err)
	}
	return backup
}

func waitForBackupFailedReason(
	ctx context.Context,
	t *testing.T,
	c ctrlclient.Client,
	ns, backupName, reason string,
) {
	t.Helper()
	waitForBackupFailedAnyReason(ctx, t, c, ns, backupName, func(got string) bool { return got == reason })
}

func waitForBackupFailedAnyReason(
	ctx context.Context,
	t *testing.T,
	c ctrlclient.Client,
	ns, backupName string,
	match func(string) bool,
) {
	t.Helper()
	err := wait.For(func(ctx context.Context) (bool, error) {
		var backup snaplanev1alpha1.Backup
		if err := c.Get(ctx, types.NamespacedName{Namespace: ns, Name: backupName}, &backup); err != nil {
			if apierrors.IsNotFound(err) {
				return false, nil
			}
			return false, err
		}
		cond := meta.FindStatusCondition(backup.Status.Conditions, snaplanev1alpha1.BackupConditionSucceeded)
		if cond == nil {
			return false, nil
		}
		return cond.Status == metav1.ConditionFalse && match(cond.Reason), nil
	}, wait.WithTimeout(schedulerE2ETimeout), wait.WithInterval(schedulerE2EPoll))
	if err != nil {
		t.Fatalf("wait for backup %s/%s failed reason condition: %v", ns, backupName, err)
	}
}

func cleanupSchedulerFeatureState(ctx context.Context, t *testing.T, c ctrlclient.Client, state *schedulerFeatureState) {
	t.Helper()

	if state.namespace != "" {
		ns := &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: state.namespace}}
		_ = c.Delete(ctx, ns)
	}

	for _, nodeName := range state.nodes {
		if nodeName == "" {
			continue
		}
		lease := &coordinationv1.Lease{ObjectMeta: metav1.ObjectMeta{Namespace: namespace, Name: writer.LeaseNameForNode(nodeName)}}
		_ = c.Delete(ctx, lease)

		node := &corev1.Node{ObjectMeta: metav1.ObjectMeta{Name: nodeName}}
		_ = c.Delete(ctx, node)
	}
}

func boolPtr(v bool) *bool {
	return &v
}

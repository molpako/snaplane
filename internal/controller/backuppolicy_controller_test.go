package controller

import (
	"context"
	"errors"
	"fmt"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	coordinationv1 "k8s.io/api/coordination/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	meta "k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	volumesnapshotv1 "github.com/kubernetes-csi/external-snapshotter/client/v8/apis/volumesnapshot/v1"
	snaplanev1alpha1 "github.com/molpako/snaplane/api/v1alpha1"
	"github.com/molpako/snaplane/internal/writer"
)

type dispatchConflictClient struct {
	client.Client
	conflictSnapshotName string
	injected             bool
}

func (c *dispatchConflictClient) Patch(ctx context.Context, obj client.Object, patch client.Patch, opts ...client.PatchOption) error {
	vs, ok := obj.(*volumesnapshotv1.VolumeSnapshot)
	if ok && !c.injected && c.conflictSnapshotName != "" && vs.Name == c.conflictSnapshotName &&
		snapshotState(vs) == snaplanev1alpha1.SnapshotQueueStateDispatched {
		c.injected = true
		return apierrors.NewConflict(
			schema.GroupResource{Group: "snapshot.storage.k8s.io", Resource: "volumesnapshots"},
			vs.Name,
			errors.New("simulated conflict"),
		)
	}
	return c.Client.Patch(ctx, obj, patch, opts...)
}

func boolPtr(v bool) *bool {
	return &v
}

func newPolicy(name string) *snaplanev1alpha1.BackupPolicy {
	pvcName := fmt.Sprintf("pvc-%s", name)
	return &snaplanev1alpha1.BackupPolicy{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: "default",
		},
		Spec: snaplanev1alpha1.BackupPolicySpec{
			Source: snaplanev1alpha1.BackupSource{
				PersistentVolumeClaimName: pvcName,
				VolumeSnapshotClassName:   "csi-rbd-snapclass",
			},
			Schedule: snaplanev1alpha1.BackupSchedule{
				Cron: "* * * * *",
			},
			Retention: snaplanev1alpha1.BackupRetention{
				KeepDays: 1,
			},
		},
	}
}

func listPolicySnapshots(ctx context.Context, policyName string) []volumesnapshotv1.VolumeSnapshot {
	var list volumesnapshotv1.VolumeSnapshotList
	Expect(k8sClient.List(ctx, &list, client.InNamespace("default"))).To(Succeed())
	out := make([]volumesnapshotv1.VolumeSnapshot, 0, len(list.Items))
	for i := range list.Items {
		if list.Items[i].Labels != nil && list.Items[i].Labels[snaplanev1alpha1.PolicyLabelKey] == policyName {
			out = append(out, list.Items[i])
		}
	}
	return out
}

func listPolicyBackups(ctx context.Context, policyName string) []snaplanev1alpha1.Backup {
	var list snaplanev1alpha1.BackupList
	Expect(k8sClient.List(ctx, &list, client.InNamespace("default"))).To(Succeed())
	out := make([]snaplanev1alpha1.Backup, 0, len(list.Items))
	for i := range list.Items {
		if list.Items[i].Spec.PolicyRef.Name == policyName {
			out = append(out, list.Items[i])
		}
	}
	return out
}

func upsertSnapshotWithState(ctx context.Context, name, policyName string, queueTime time.Time, state snaplanev1alpha1.SnapshotQueueState, ready bool) {
	labels := map[string]string{
		snaplanev1alpha1.PolicyLabelKey:      policyName,
		snaplanev1alpha1.BackupStateLabelKey: string(state),
	}
	annotations := map[string]string{
		snaplanev1alpha1.QueueTimeAnnotationKey: queueTime.UTC().Format(time.RFC3339),
	}
	className := "csi-rbd-snapclass"
	pvcName := "pvc-a"
	snapshot := &volumesnapshotv1.VolumeSnapshot{
		ObjectMeta: metav1.ObjectMeta{
			Name:        name,
			Namespace:   "default",
			Labels:      labels,
			Annotations: annotations,
		},
		Spec: volumesnapshotv1.VolumeSnapshotSpec{
			VolumeSnapshotClassName: &className,
			Source: volumesnapshotv1.VolumeSnapshotSource{
				PersistentVolumeClaimName: &pvcName,
			},
		},
	}
	Expect(k8sClient.Create(ctx, snapshot)).To(Succeed())

	var current volumesnapshotv1.VolumeSnapshot
	Expect(k8sClient.Get(ctx, types.NamespacedName{Name: name, Namespace: "default"}, &current)).To(Succeed())
	if current.Status == nil {
		current.Status = &volumesnapshotv1.VolumeSnapshotStatus{}
	}
	current.Status.ReadyToUse = boolPtr(ready)
	Expect(k8sClient.Status().Update(ctx, &current)).To(Succeed())
}

func createNodeAndLease(ctx context.Context, nodeName string, usedBytes, availableBytes int64) {
	createNodeAndLeaseInNamespace(ctx, "default", nodeName, usedBytes, availableBytes)
}

func createNodeAndLeaseInNamespace(ctx context.Context, leaseNamespace, nodeName string, usedBytes, availableBytes int64) {
	node := &corev1.Node{
		ObjectMeta: metav1.ObjectMeta{
			Name: nodeName,
			Labels: map[string]string{
				snaplanev1alpha1.BackupTargetNodeLabelKey: "true",
			},
		},
		Status: corev1.NodeStatus{
			Conditions: []corev1.NodeCondition{
				{
					Type:   corev1.NodeReady,
					Status: corev1.ConditionTrue,
				},
			},
		},
	}
	_ = k8sClient.Create(ctx, node)
	var current corev1.Node
	Expect(k8sClient.Get(ctx, types.NamespacedName{Name: nodeName}, &current)).To(Succeed())
	baseNode := current.DeepCopy()
	if current.Labels == nil {
		current.Labels = map[string]string{}
	}
	current.Labels[snaplanev1alpha1.BackupTargetNodeLabelKey] = "true"
	Expect(k8sClient.Patch(ctx, &current, client.MergeFrom(baseNode))).To(Succeed())
	current.Status.Conditions = []corev1.NodeCondition{
		{
			Type:   corev1.NodeReady,
			Status: corev1.ConditionTrue,
		},
	}
	Expect(k8sClient.Status().Update(ctx, &current)).To(Succeed())

	now := metav1.MicroTime{Time: time.Now().UTC()}
	lease := &coordinationv1.Lease{
		ObjectMeta: metav1.ObjectMeta{
			Name:      writer.LeaseNameForNode(nodeName),
			Namespace: leaseNamespace,
			Annotations: map[string]string{
				snaplanev1alpha1.WriterEndpointAnnotationKey:      "127.0.0.1:9443",
				snaplanev1alpha1.LeaseUsedBytesAnnotationKey:      fmt.Sprintf("%d", usedBytes),
				snaplanev1alpha1.LeaseAvailableBytesAnnotationKey: fmt.Sprintf("%d", availableBytes),
			},
		},
		Spec: coordinationv1.LeaseSpec{
			RenewTime: &now,
		},
	}
	_ = k8sClient.Create(ctx, lease)
}

func useIsolatedLeaseNamespace(ctx context.Context, reconciler *BackupPolicyReconciler) string {
	leaseNamespace := fmt.Sprintf("lease-ns-%d", time.Now().UnixNano())
	Expect(k8sClient.Create(ctx, &corev1.Namespace{
		ObjectMeta: metav1.ObjectMeta{Name: leaseNamespace},
	})).To(Succeed())
	reconciler.LeaseNamespace = leaseNamespace
	return leaseNamespace
}

func ensurePolicyPVC(ctx context.Context, pvcName string) {
	pvc := &corev1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{
			Name:      pvcName,
			Namespace: "default",
		},
		Spec: corev1.PersistentVolumeClaimSpec{
			AccessModes: []corev1.PersistentVolumeAccessMode{corev1.ReadWriteOnce},
			Resources: corev1.VolumeResourceRequirements{
				Requests: corev1.ResourceList{
					corev1.ResourceStorage: resourceMustParse("1Gi"),
				},
			},
		},
	}
	_ = k8sClient.Create(ctx, pvc)
}

func resourceMustParse(value string) resource.Quantity {
	q, err := resource.ParseQuantity(value)
	Expect(err).NotTo(HaveOccurred())
	return q
}

var _ = Describe("BackupPolicy Controller", func() {
	var (
		ctx             context.Context
		reconciler      *BackupPolicyReconciler
		policyName      string
		policyNN        types.NamespacedName
		manualRequestID string
	)

	BeforeEach(func() {
		ctx = context.Background()
		reconciler = &BackupPolicyReconciler{
			Client: k8sClient,
			Scheme: k8sClient.Scheme(),
		}
		policyName = fmt.Sprintf("policy-%d", time.Now().UnixNano())
		policyNN = types.NamespacedName{Name: policyName, Namespace: "default"}
		manualRequestID = fmt.Sprintf("manual-%d", time.Now().UnixNano())
	})

	It("creates a queued snapshot on pending manual request and waits when oldest pending is not ready", func() {
		policy := newPolicy(policyName)
		policy.Spec.Manual.RequestID = manualRequestID
		Expect(k8sClient.Create(ctx, policy)).To(Succeed())

		_, err := reconciler.Reconcile(ctx, reconcile.Request{NamespacedName: policyNN})
		Expect(err).NotTo(HaveOccurred())

		snapshots := listPolicySnapshots(ctx, policyName)
		Expect(snapshots).To(HaveLen(1))
		Expect(snapshotState(&snapshots[0])).To(Equal(snaplanev1alpha1.SnapshotQueueStatePending))
		Expect(snapshots[0].Annotations[snaplanev1alpha1.QueueTimeAnnotationKey]).NotTo(BeEmpty())
		Expect(listPolicyBackups(ctx, policyName)).To(BeEmpty())
	})

	It("dispatches and creates backup when oldest pending snapshot becomes ready", func() {
		policy := newPolicy(policyName)
		policy.Spec.Manual.RequestID = manualRequestID
		Expect(k8sClient.Create(ctx, policy)).To(Succeed())
		ensurePolicyPVC(ctx, policy.Spec.Source.PersistentVolumeClaimName)
		createNodeAndLease(ctx, "node-a", 1024, 20*1024*1024*1024)

		_, err := reconciler.Reconcile(ctx, reconcile.Request{NamespacedName: policyNN})
		Expect(err).NotTo(HaveOccurred())

		snapshots := listPolicySnapshots(ctx, policyName)
		Expect(snapshots).To(HaveLen(1))

		var snapshot volumesnapshotv1.VolumeSnapshot
		Expect(k8sClient.Get(ctx, types.NamespacedName{Name: snapshots[0].Name, Namespace: "default"}, &snapshot)).To(Succeed())
		if snapshot.Status == nil {
			snapshot.Status = &volumesnapshotv1.VolumeSnapshotStatus{}
		}
		snapshot.Status.ReadyToUse = boolPtr(true)
		Expect(k8sClient.Status().Update(ctx, &snapshot)).To(Succeed())

		_, err = reconciler.Reconcile(ctx, reconcile.Request{NamespacedName: policyNN})
		Expect(err).NotTo(HaveOccurred())

		Expect(k8sClient.Get(ctx, types.NamespacedName{Name: snapshots[0].Name, Namespace: "default"}, &snapshot)).To(Succeed())
		Expect(snapshotState(&snapshot)).To(Equal(snaplanev1alpha1.SnapshotQueueStateDispatched))
		backups := listPolicyBackups(ctx, policyName)
		Expect(backups).To(HaveLen(1))
		Expect(backups[0].Spec.Destination.NodeName).To(Equal("node-a"))
	})

	It("keeps a manual request pending while non-done snapshots already exist", func() {
		policy := newPolicy(policyName)
		policy.Spec.Manual.RequestID = manualRequestID
		Expect(k8sClient.Create(ctx, policy)).To(Succeed())

		base := time.Now().UTC().Add(-2 * time.Minute)
		upsertSnapshotWithState(ctx, "snap-existing-pending", policyName, base, snaplanev1alpha1.SnapshotQueueStatePending, false)

		_, err := reconciler.Reconcile(ctx, reconcile.Request{NamespacedName: policyNN})
		Expect(err).NotTo(HaveOccurred())

		snapshots := listPolicySnapshots(ctx, policyName)
		Expect(snapshots).To(HaveLen(1))
		Expect(snapshots[0].Name).To(Equal("snap-existing-pending"))

		var refreshed snaplanev1alpha1.BackupPolicy
		Expect(k8sClient.Get(ctx, policyNN, &refreshed)).To(Succeed())
		Expect(refreshed.Status.Manual.LastHandledRequestID).To(BeEmpty())
		Expect(refreshed.Status.Manual.LastHandledTime).To(BeNil())
		queueHealthy := meta.FindStatusCondition(refreshed.Status.Conditions, snaplanev1alpha1.BackupPolicyConditionQueueHealthy)
		Expect(queueHealthy).NotTo(BeNil())
		Expect(queueHealthy.Status).To(Equal(metav1.ConditionTrue))
		Expect(queueHealthy.Reason).To(Equal("QueueObserved"))
	})

	It("enqueues once a pending manual request becomes the latest desired request", func() {
		policy := newPolicy(policyName)
		policy.Spec.Manual.RequestID = "manual-old"
		Expect(k8sClient.Create(ctx, policy)).To(Succeed())

		base := time.Now().UTC().Add(-2 * time.Minute)
		upsertSnapshotWithState(ctx, "snap-existing-pending-newest", policyName, base, snaplanev1alpha1.SnapshotQueueStatePending, false)

		_, err := reconciler.Reconcile(ctx, reconcile.Request{NamespacedName: policyNN})
		Expect(err).NotTo(HaveOccurred())
		Expect(listPolicySnapshots(ctx, policyName)).To(HaveLen(1))

		var current snaplanev1alpha1.BackupPolicy
		Expect(k8sClient.Get(ctx, policyNN, &current)).To(Succeed())
		policyBase := current.DeepCopy()
		current.Spec.Manual.RequestID = manualRequestID
		Expect(k8sClient.Patch(ctx, &current, client.MergeFrom(policyBase))).To(Succeed())

		var pending volumesnapshotv1.VolumeSnapshot
		Expect(k8sClient.Get(ctx, types.NamespacedName{Name: "snap-existing-pending-newest", Namespace: "default"}, &pending)).To(Succeed())
		pendingBase := pending.DeepCopy()
		pending.Labels[snaplanev1alpha1.BackupStateLabelKey] = string(snaplanev1alpha1.SnapshotQueueStateDone)
		Expect(k8sClient.Patch(ctx, &pending, client.MergeFrom(pendingBase))).To(Succeed())

		_, err = reconciler.Reconcile(ctx, reconcile.Request{NamespacedName: policyNN})
		Expect(err).NotTo(HaveOccurred())

		snapshots := listPolicySnapshots(ctx, policyName)
		Expect(snapshots).To(HaveLen(2))

		var refreshed snaplanev1alpha1.BackupPolicy
		Expect(k8sClient.Get(ctx, policyNN, &refreshed)).To(Succeed())
		Expect(refreshed.Status.Manual.LastHandledRequestID).To(Equal(manualRequestID))
		Expect(refreshed.Status.Manual.LastHandledTime).NotTo(BeNil())
		queueHealthy := meta.FindStatusCondition(refreshed.Status.Conditions, snaplanev1alpha1.BackupPolicyConditionQueueHealthy)
		Expect(queueHealthy).NotTo(BeNil())
		Expect(queueHealthy.Status).To(Equal(metav1.ConditionTrue))
		Expect(queueHealthy.Reason).To(Equal("QueueObserved"))
	})

	It("continues schedule enqueue while manual request is empty", func() {
		policy := newPolicy(policyName)
		Expect(k8sClient.Create(ctx, policy)).To(Succeed())

		var current snaplanev1alpha1.BackupPolicy
		Expect(k8sClient.Get(ctx, policyNN, &current)).To(Succeed())
		statusBase := current.DeepCopy()
		lastScheduled := metav1.NewTime(time.Now().UTC().Add(-2 * time.Minute))
		current.Status.LastScheduledTime = &lastScheduled
		Expect(k8sClient.Status().Patch(ctx, &current, client.MergeFrom(statusBase))).To(Succeed())

		_, err := reconciler.Reconcile(ctx, reconcile.Request{NamespacedName: policyNN})
		Expect(err).NotTo(HaveOccurred())

		snapshots := listPolicySnapshots(ctx, policyName)
		Expect(snapshots).To(HaveLen(1))

		var refreshed snaplanev1alpha1.BackupPolicy
		Expect(k8sClient.Get(ctx, policyNN, &refreshed)).To(Succeed())
		queueHealthy := meta.FindStatusCondition(refreshed.Status.Conditions, snaplanev1alpha1.BackupPolicyConditionQueueHealthy)
		Expect(queueHealthy).NotTo(BeNil())
		Expect(queueHealthy.Status).To(Equal(metav1.ConditionTrue))
		Expect(queueHealthy.Reason).To(Equal("QueueObserved"))
	})

	It("selects minimum used-bytes node and persists PVC assignment", func() {
		leaseNamespace := useIsolatedLeaseNamespace(ctx, reconciler)
		policy := newPolicy(policyName)
		policy.Spec.Manual.RequestID = manualRequestID
		Expect(k8sClient.Create(ctx, policy)).To(Succeed())
		ensurePolicyPVC(ctx, policy.Spec.Source.PersistentVolumeClaimName)
		createNodeAndLeaseInNamespace(ctx, leaseNamespace, "node-low", 100, 20*1024*1024*1024)
		createNodeAndLeaseInNamespace(ctx, leaseNamespace, "node-high", 1000, 20*1024*1024*1024)

		_, err := reconciler.Reconcile(ctx, reconcile.Request{NamespacedName: policyNN})
		Expect(err).NotTo(HaveOccurred())

		snapshots := listPolicySnapshots(ctx, policyName)
		Expect(snapshots).To(HaveLen(1))
		var snapshot volumesnapshotv1.VolumeSnapshot
		Expect(k8sClient.Get(ctx, types.NamespacedName{Name: snapshots[0].Name, Namespace: "default"}, &snapshot)).To(Succeed())
		snapshot.Status = &volumesnapshotv1.VolumeSnapshotStatus{ReadyToUse: boolPtr(true)}
		Expect(k8sClient.Status().Update(ctx, &snapshot)).To(Succeed())

		_, err = reconciler.Reconcile(ctx, reconcile.Request{NamespacedName: policyNN})
		Expect(err).NotTo(HaveOccurred())

		backups := listPolicyBackups(ctx, policyName)
		Expect(backups).To(HaveLen(1))
		Expect(backups[0].Spec.Destination.NodeName).To(Equal("node-low"))

		var pvc corev1.PersistentVolumeClaim
		Expect(k8sClient.Get(ctx, types.NamespacedName{Namespace: "default", Name: policy.Spec.Source.PersistentVolumeClaimName}, &pvc)).To(Succeed())
		Expect(pvc.Annotations[snaplanev1alpha1.AssignedBackupNodeAnnotationKey]).To(Equal("node-low"))
		Expect(pvc.Annotations[snaplanev1alpha1.AssignedBackupNodeAtAnnotationKey]).NotTo(BeEmpty())
	})

	It("selects nodeName ascending when used-bytes are equal", func() {
		leaseNamespace := useIsolatedLeaseNamespace(ctx, reconciler)
		policy := newPolicy(policyName)
		policy.Spec.Manual.RequestID = manualRequestID
		Expect(k8sClient.Create(ctx, policy)).To(Succeed())
		ensurePolicyPVC(ctx, policy.Spec.Source.PersistentVolumeClaimName)
		createNodeAndLeaseInNamespace(ctx, leaseNamespace, "node-b", 100, 20*1024*1024*1024)
		createNodeAndLeaseInNamespace(ctx, leaseNamespace, "node-a", 100, 20*1024*1024*1024)

		_, err := reconciler.Reconcile(ctx, reconcile.Request{NamespacedName: policyNN})
		Expect(err).NotTo(HaveOccurred())

		snapshots := listPolicySnapshots(ctx, policyName)
		Expect(snapshots).To(HaveLen(1))
		var snapshot volumesnapshotv1.VolumeSnapshot
		Expect(k8sClient.Get(ctx, types.NamespacedName{Name: snapshots[0].Name, Namespace: "default"}, &snapshot)).To(Succeed())
		snapshot.Status = &volumesnapshotv1.VolumeSnapshotStatus{ReadyToUse: boolPtr(true)}
		Expect(k8sClient.Status().Update(ctx, &snapshot)).To(Succeed())

		_, err = reconciler.Reconcile(ctx, reconcile.Request{NamespacedName: policyNN})
		Expect(err).NotTo(HaveOccurred())

		backups := listPolicyBackups(ctx, policyName)
		Expect(backups).To(HaveLen(1))
		Expect(backups[0].Spec.Destination.NodeName).To(Equal("node-a"))
	})

	It("excludes stale lease nodes when selecting unassigned PVC destination", func() {
		leaseNamespace := useIsolatedLeaseNamespace(ctx, reconciler)
		policy := newPolicy(policyName)
		policy.Spec.Manual.RequestID = manualRequestID
		Expect(k8sClient.Create(ctx, policy)).To(Succeed())
		ensurePolicyPVC(ctx, policy.Spec.Source.PersistentVolumeClaimName)
		createNodeAndLeaseInNamespace(ctx, leaseNamespace, "node-stale-low", 1, 20*1024*1024*1024)
		createNodeAndLeaseInNamespace(ctx, leaseNamespace, "node-fresh-high", 1000, 20*1024*1024*1024)

		var staleLease coordinationv1.Lease
		Expect(k8sClient.Get(ctx, types.NamespacedName{Namespace: leaseNamespace, Name: writer.LeaseNameForNode("node-stale-low")}, &staleLease)).To(Succeed())
		staleBase := staleLease.DeepCopy()
		old := metav1.MicroTime{Time: time.Now().UTC().Add(-2 * time.Minute)}
		staleLease.Spec.RenewTime = &old
		Expect(k8sClient.Patch(ctx, &staleLease, client.MergeFrom(staleBase))).To(Succeed())

		_, err := reconciler.Reconcile(ctx, reconcile.Request{NamespacedName: policyNN})
		Expect(err).NotTo(HaveOccurred())

		snapshots := listPolicySnapshots(ctx, policyName)
		Expect(snapshots).To(HaveLen(1))
		var snapshot volumesnapshotv1.VolumeSnapshot
		Expect(k8sClient.Get(ctx, types.NamespacedName{Name: snapshots[0].Name, Namespace: "default"}, &snapshot)).To(Succeed())
		snapshot.Status = &volumesnapshotv1.VolumeSnapshotStatus{ReadyToUse: boolPtr(true)}
		Expect(k8sClient.Status().Update(ctx, &snapshot)).To(Succeed())

		_, err = reconciler.Reconcile(ctx, reconcile.Request{NamespacedName: policyNN})
		Expect(err).NotTo(HaveOccurred())

		backups := listPolicyBackups(ctx, policyName)
		Expect(backups).To(HaveLen(1))
		Expect(backups[0].Spec.Destination.NodeName).To(Equal("node-fresh-high"))
	})

	It("uses assigned PVC node when assigned node is still a healthy candidate", func() {
		leaseNamespace := useIsolatedLeaseNamespace(ctx, reconciler)
		policy := newPolicy(policyName)
		policy.Spec.Manual.RequestID = manualRequestID
		Expect(k8sClient.Create(ctx, policy)).To(Succeed())
		ensurePolicyPVC(ctx, policy.Spec.Source.PersistentVolumeClaimName)
		createNodeAndLeaseInNamespace(ctx, leaseNamespace, "node-assigned", 100, 20*1024*1024*1024)

		var pvc corev1.PersistentVolumeClaim
		Expect(k8sClient.Get(ctx, types.NamespacedName{Namespace: "default", Name: policy.Spec.Source.PersistentVolumeClaimName}, &pvc)).To(Succeed())
		base := pvc.DeepCopy()
		if pvc.Annotations == nil {
			pvc.Annotations = map[string]string{}
		}
		pvc.Annotations[snaplanev1alpha1.AssignedBackupNodeAnnotationKey] = "node-assigned"
		pvc.Annotations[snaplanev1alpha1.AssignedBackupNodeAtAnnotationKey] = time.Now().UTC().Add(-time.Hour).Format(time.RFC3339)
		Expect(k8sClient.Patch(ctx, &pvc, client.MergeFrom(base))).To(Succeed())

		_, err := reconciler.Reconcile(ctx, reconcile.Request{NamespacedName: policyNN})
		Expect(err).NotTo(HaveOccurred())

		snapshots := listPolicySnapshots(ctx, policyName)
		Expect(snapshots).To(HaveLen(1))
		var snapshot volumesnapshotv1.VolumeSnapshot
		Expect(k8sClient.Get(ctx, types.NamespacedName{Name: snapshots[0].Name, Namespace: "default"}, &snapshot)).To(Succeed())
		snapshot.Status = &volumesnapshotv1.VolumeSnapshotStatus{ReadyToUse: boolPtr(true)}
		Expect(k8sClient.Status().Update(ctx, &snapshot)).To(Succeed())

		_, err = reconciler.Reconcile(ctx, reconcile.Request{NamespacedName: policyNN})
		Expect(err).NotTo(HaveOccurred())

		backups := listPolicyBackups(ctx, policyName)
		Expect(backups).To(HaveLen(1))
		Expect(backups[0].Spec.Destination.NodeName).To(Equal("node-assigned"))
	})

	It("returns unavailable destination when assigned PVC node is not a healthy candidate", func() {
		leaseNamespace := useIsolatedLeaseNamespace(ctx, reconciler)
		policy := newPolicy(policyName)
		policy.Spec.Manual.RequestID = manualRequestID
		Expect(k8sClient.Create(ctx, policy)).To(Succeed())
		ensurePolicyPVC(ctx, policy.Spec.Source.PersistentVolumeClaimName)
		createNodeAndLeaseInNamespace(ctx, leaseNamespace, "node-assigned", 100, 20*1024*1024*1024)

		var pvc corev1.PersistentVolumeClaim
		Expect(k8sClient.Get(ctx, types.NamespacedName{Namespace: "default", Name: policy.Spec.Source.PersistentVolumeClaimName}, &pvc)).To(Succeed())
		base := pvc.DeepCopy()
		if pvc.Annotations == nil {
			pvc.Annotations = map[string]string{}
		}
		pvc.Annotations[snaplanev1alpha1.AssignedBackupNodeAnnotationKey] = "node-assigned"
		pvc.Annotations[snaplanev1alpha1.AssignedBackupNodeAtAnnotationKey] = time.Now().UTC().Add(-time.Hour).Format(time.RFC3339)
		Expect(k8sClient.Patch(ctx, &pvc, client.MergeFrom(base))).To(Succeed())

		var staleLease coordinationv1.Lease
		Expect(k8sClient.Get(ctx, types.NamespacedName{Namespace: leaseNamespace, Name: writer.LeaseNameForNode("node-assigned")}, &staleLease)).To(Succeed())
		staleBase := staleLease.DeepCopy()
		old := metav1.MicroTime{Time: time.Now().UTC().Add(-2 * time.Minute)}
		staleLease.Spec.RenewTime = &old
		Expect(k8sClient.Patch(ctx, &staleLease, client.MergeFrom(staleBase))).To(Succeed())

		_, err := reconciler.Reconcile(ctx, reconcile.Request{NamespacedName: policyNN})
		Expect(err).NotTo(HaveOccurred())

		snapshots := listPolicySnapshots(ctx, policyName)
		Expect(snapshots).To(HaveLen(1))
		var snapshot volumesnapshotv1.VolumeSnapshot
		Expect(k8sClient.Get(ctx, types.NamespacedName{Name: snapshots[0].Name, Namespace: "default"}, &snapshot)).To(Succeed())
		snapshot.Status = &volumesnapshotv1.VolumeSnapshotStatus{ReadyToUse: boolPtr(true)}
		Expect(k8sClient.Status().Update(ctx, &snapshot)).To(Succeed())

		_, err = reconciler.Reconcile(ctx, reconcile.Request{NamespacedName: policyNN})
		Expect(err).NotTo(HaveOccurred())

		backups := listPolicyBackups(ctx, policyName)
		Expect(backups).To(HaveLen(1))
		Expect(backups[0].Spec.Destination.NodeName).To(Equal(unavailableNodeName))
	})

	It("creates backup with unavailable destination when no candidate nodes exist", func() {
		leaseNamespace := fmt.Sprintf("lease-ns-%d", time.Now().UnixNano())
		Expect(k8sClient.Create(ctx, &corev1.Namespace{
			ObjectMeta: metav1.ObjectMeta{Name: leaseNamespace},
		})).To(Succeed())
		reconciler.LeaseNamespace = leaseNamespace

		policy := newPolicy(policyName)
		policy.Spec.Manual.RequestID = manualRequestID
		Expect(k8sClient.Create(ctx, policy)).To(Succeed())
		ensurePolicyPVC(ctx, policy.Spec.Source.PersistentVolumeClaimName)

		_, err := reconciler.Reconcile(ctx, reconcile.Request{NamespacedName: policyNN})
		Expect(err).NotTo(HaveOccurred())

		snapshots := listPolicySnapshots(ctx, policyName)
		Expect(snapshots).To(HaveLen(1))
		var snapshot volumesnapshotv1.VolumeSnapshot
		Expect(k8sClient.Get(ctx, types.NamespacedName{Name: snapshots[0].Name, Namespace: "default"}, &snapshot)).To(Succeed())
		snapshot.Status = &volumesnapshotv1.VolumeSnapshotStatus{ReadyToUse: boolPtr(true)}
		Expect(k8sClient.Status().Update(ctx, &snapshot)).To(Succeed())

		_, err = reconciler.Reconcile(ctx, reconcile.Request{NamespacedName: policyNN})
		Expect(err).NotTo(HaveOccurred())

		backups := listPolicyBackups(ctx, policyName)
		Expect(backups).To(HaveLen(1))
		Expect(backups[0].Spec.Destination.NodeName).To(Equal(unavailableNodeName))
	})

	It("blocks dispatch when oldest non-done snapshot is failed", func() {
		policy := newPolicy(policyName)
		Expect(k8sClient.Create(ctx, policy)).To(Succeed())

		base := time.Now().UTC().Add(-2 * time.Minute)
		upsertSnapshotWithState(ctx, "snap-a-failed", policyName, base, snaplanev1alpha1.SnapshotQueueStateFailed, true)
		upsertSnapshotWithState(ctx, "snap-b-ready", policyName, base.Add(time.Minute), snaplanev1alpha1.SnapshotQueueStatePending, true)

		_, err := reconciler.Reconcile(ctx, reconcile.Request{NamespacedName: policyNN})
		Expect(err).NotTo(HaveOccurred())
		Expect(listPolicyBackups(ctx, policyName)).To(BeEmpty())

		var refreshed snaplanev1alpha1.BackupPolicy
		Expect(k8sClient.Get(ctx, policyNN, &refreshed)).To(Succeed())
		cond := meta.FindStatusCondition(refreshed.Status.Conditions, snaplanev1alpha1.BackupPolicyConditionSerialGuardSatisfied)
		Expect(cond).NotTo(BeNil())
		Expect(cond.Status).To(Equal(metav1.ConditionFalse))
		Expect(cond.Reason).To(Equal("BlockedByFailedSnapshot"))
	})

	It("keeps failed-head blocking while a manual request stays pending", func() {
		policy := newPolicy(policyName)
		policy.Spec.Manual.RequestID = manualRequestID
		Expect(k8sClient.Create(ctx, policy)).To(Succeed())

		base := time.Now().UTC().Add(-2 * time.Minute)
		upsertSnapshotWithState(ctx, "snap-a-failed-head", policyName, base, snaplanev1alpha1.SnapshotQueueStateFailed, true)
		upsertSnapshotWithState(ctx, "snap-b-pending", policyName, base.Add(time.Minute), snaplanev1alpha1.SnapshotQueueStatePending, true)

		_, err := reconciler.Reconcile(ctx, reconcile.Request{NamespacedName: policyNN})
		Expect(err).NotTo(HaveOccurred())

		snapshots := listPolicySnapshots(ctx, policyName)
		Expect(snapshots).To(HaveLen(2))
		Expect(listPolicyBackups(ctx, policyName)).To(BeEmpty())

		var refreshed snaplanev1alpha1.BackupPolicy
		Expect(k8sClient.Get(ctx, policyNN, &refreshed)).To(Succeed())
		cond := meta.FindStatusCondition(refreshed.Status.Conditions, snaplanev1alpha1.BackupPolicyConditionSerialGuardSatisfied)
		Expect(cond).NotTo(BeNil())
		Expect(cond.Status).To(Equal(metav1.ConditionFalse))
		Expect(cond.Reason).To(Equal("BlockedByFailedSnapshot"))
	})

	It("unblocks failed head after manual retry backup completes", func() {
		policy := newPolicy(policyName)
		Expect(k8sClient.Create(ctx, policy)).To(Succeed())
		ensurePolicyPVC(ctx, policy.Spec.Source.PersistentVolumeClaimName)
		createNodeAndLease(ctx, "node-a", 1024, 20*1024*1024*1024)

		failedSnapshotName := fmt.Sprintf("failed-%s", policyName)
		pendingSnapshotName := fmt.Sprintf("pending-%s", policyName)
		retryBackupName := fmt.Sprintf("manual-retry-%s", policyName)

		base := time.Now().UTC().Add(-2 * time.Minute)
		upsertSnapshotWithState(ctx, failedSnapshotName, policyName, base, snaplanev1alpha1.SnapshotQueueStateFailed, true)
		upsertSnapshotWithState(ctx, pendingSnapshotName, policyName, base.Add(time.Minute), snaplanev1alpha1.SnapshotQueueStatePending, true)

		retryBackup := &snaplanev1alpha1.Backup{
			ObjectMeta: metav1.ObjectMeta{
				Namespace: "default",
				Name:      retryBackupName,
			},
			Spec: snaplanev1alpha1.BackupSpec{
				PolicyRef: snaplanev1alpha1.LocalNameReference{Name: policyName},
				VolumeSnapshotRef: snaplanev1alpha1.VolumeSnapshotReference{
					Name: failedSnapshotName,
				},
				Destination: snaplanev1alpha1.BackupDestination{
					NodeName: "node-a",
				},
			},
		}
		Expect(k8sClient.Create(ctx, retryBackup)).To(Succeed())

		var retryCurrent snaplanev1alpha1.Backup
		Expect(k8sClient.Get(ctx, types.NamespacedName{Namespace: "default", Name: retryBackup.Name}, &retryCurrent)).To(Succeed())
		retryCurrent.Status.CompletionTime = func() *metav1.Time {
			now := metav1.Now()
			return &now
		}()
		retryCurrent.Status.Conditions = []metav1.Condition{
			{
				Type:               snaplanev1alpha1.BackupConditionSucceeded,
				Status:             metav1.ConditionTrue,
				Reason:             "ManualRetrySucceeded",
				Message:            "manual retry completed",
				LastTransitionTime: metav1.Now(),
				ObservedGeneration: retryCurrent.Generation,
			},
		}
		Expect(k8sClient.Status().Update(ctx, &retryCurrent)).To(Succeed())

		_, err := reconciler.Reconcile(ctx, reconcile.Request{NamespacedName: policyNN})
		Expect(err).NotTo(HaveOccurred())

		var failedSnapshot volumesnapshotv1.VolumeSnapshot
		Expect(k8sClient.Get(ctx, types.NamespacedName{Name: failedSnapshotName, Namespace: "default"}, &failedSnapshot)).To(Succeed())
		Expect(snapshotState(&failedSnapshot)).To(Equal(snaplanev1alpha1.SnapshotQueueStateDone))

		var pendingSnapshot volumesnapshotv1.VolumeSnapshot
		Expect(k8sClient.Get(ctx, types.NamespacedName{Name: pendingSnapshotName, Namespace: "default"}, &pendingSnapshot)).To(Succeed())
		Expect(snapshotState(&pendingSnapshot)).To(Equal(snaplanev1alpha1.SnapshotQueueStateDispatched))

		backups := listPolicyBackups(ctx, policyName)
		Expect(backups).To(HaveLen(2))
	})

	It("retries dispatch CAS patch conflict without overtaking", func() {
		policy := newPolicy(policyName)
		policy.Spec.Manual.RequestID = manualRequestID
		Expect(k8sClient.Create(ctx, policy)).To(Succeed())
		ensurePolicyPVC(ctx, policy.Spec.Source.PersistentVolumeClaimName)
		createNodeAndLease(ctx, "node-a", 1024, 20*1024*1024*1024)

		_, err := reconciler.Reconcile(ctx, reconcile.Request{NamespacedName: policyNN})
		Expect(err).NotTo(HaveOccurred())

		snapshots := listPolicySnapshots(ctx, policyName)
		Expect(snapshots).To(HaveLen(1))
		headSnapshotName := snapshots[0].Name
		laterSnapshotName := fmt.Sprintf("%s-later", headSnapshotName)
		upsertSnapshotWithState(
			ctx,
			laterSnapshotName,
			policyName,
			time.Now().UTC().Add(time.Minute),
			snaplanev1alpha1.SnapshotQueueStatePending,
			true,
		)

		var headSnapshot volumesnapshotv1.VolumeSnapshot
		Expect(k8sClient.Get(ctx, types.NamespacedName{Name: headSnapshotName, Namespace: "default"}, &headSnapshot)).To(Succeed())
		if headSnapshot.Status == nil {
			headSnapshot.Status = &volumesnapshotv1.VolumeSnapshotStatus{}
		}
		headSnapshot.Status.ReadyToUse = boolPtr(true)
		Expect(k8sClient.Status().Update(ctx, &headSnapshot)).To(Succeed())

		reconciler.Client = &dispatchConflictClient{
			Client:               k8sClient,
			conflictSnapshotName: headSnapshotName,
		}
		_, err = reconciler.Reconcile(ctx, reconcile.Request{NamespacedName: policyNN})
		Expect(err).NotTo(HaveOccurred())

		var headCurrent volumesnapshotv1.VolumeSnapshot
		Expect(k8sClient.Get(ctx, types.NamespacedName{Name: headSnapshotName, Namespace: "default"}, &headCurrent)).To(Succeed())
		Expect(snapshotState(&headCurrent)).To(Equal(snaplanev1alpha1.SnapshotQueueStateDispatched))

		var laterCurrent volumesnapshotv1.VolumeSnapshot
		Expect(k8sClient.Get(ctx, types.NamespacedName{Name: laterSnapshotName, Namespace: "default"}, &laterCurrent)).To(Succeed())
		Expect(snapshotState(&laterCurrent)).To(Equal(snaplanev1alpha1.SnapshotQueueStatePending))
	})

	It("retention keeps done snapshot and backup while restore PVC still references backup", func() {
		policy := newPolicy(policyName)
		Expect(k8sClient.Create(ctx, policy)).To(Succeed())

		queueTime := time.Now().UTC().Add(-48 * time.Hour)
		snapshotName := fmt.Sprintf("done-%s", policyName)
		backupName := fmt.Sprintf("backup-%s", policyName)
		upsertSnapshotWithState(ctx, snapshotName, policyName, queueTime, snaplanev1alpha1.SnapshotQueueStateDone, true)

		var snapshot volumesnapshotv1.VolumeSnapshot
		Expect(k8sClient.Get(ctx, types.NamespacedName{Name: snapshotName, Namespace: "default"}, &snapshot)).To(Succeed())
		snapshotBase := snapshot.DeepCopy()
		if snapshot.Annotations == nil {
			snapshot.Annotations = map[string]string{}
		}
		snapshot.Annotations[snaplanev1alpha1.BackupNameAnnotationKey] = backupName
		Expect(k8sClient.Patch(ctx, &snapshot, client.MergeFrom(snapshotBase))).To(Succeed())

		backup := &snaplanev1alpha1.Backup{
			ObjectMeta: metav1.ObjectMeta{
				Namespace: "default",
				Name:      backupName,
			},
			Spec: snaplanev1alpha1.BackupSpec{
				PolicyRef: snaplanev1alpha1.LocalNameReference{Name: policyName},
				VolumeSnapshotRef: snaplanev1alpha1.VolumeSnapshotReference{
					Name: snapshotName,
				},
				Destination: snaplanev1alpha1.BackupDestination{NodeName: "node-a"},
			},
			Status: snaplanev1alpha1.BackupStatus{
				CompletionTime: func() *metav1.Time {
					now := metav1.Now()
					return &now
				}(),
				Conditions: []metav1.Condition{
					{
						Type:               snaplanev1alpha1.BackupConditionSucceeded,
						Status:             metav1.ConditionTrue,
						Reason:             "Completed",
						ObservedGeneration: 1,
						LastTransitionTime: metav1.Now(),
					},
				},
			},
		}
		Expect(k8sClient.Create(ctx, backup)).To(Succeed())

		group := snaplanev1alpha1.GroupVersion.Group
		restorePVC := &corev1.PersistentVolumeClaim{
			ObjectMeta: metav1.ObjectMeta{
				Namespace: "default",
				Name:      fmt.Sprintf("restore-%s", policyName),
			},
			Spec: corev1.PersistentVolumeClaimSpec{
				AccessModes: []corev1.PersistentVolumeAccessMode{corev1.ReadWriteOnce},
				Resources: corev1.VolumeResourceRequirements{
					Requests: corev1.ResourceList{
						corev1.ResourceStorage: resourceMustParse("1Gi"),
					},
				},
				DataSourceRef: &corev1.TypedObjectReference{
					APIGroup: &group,
					Kind:     "Backup",
					Name:     backupName,
				},
			},
		}
		Expect(k8sClient.Create(ctx, restorePVC)).To(Succeed())

		_, err := reconciler.Reconcile(ctx, reconcile.Request{NamespacedName: policyNN})
		Expect(err).NotTo(HaveOccurred())

		Expect(k8sClient.Get(ctx, types.NamespacedName{Name: snapshotName, Namespace: "default"}, &volumesnapshotv1.VolumeSnapshot{})).To(Succeed())
		Expect(k8sClient.Get(ctx, types.NamespacedName{Name: backupName, Namespace: "default"}, &snaplanev1alpha1.Backup{})).To(Succeed())

		var restorePVCCurrent corev1.PersistentVolumeClaim
		Expect(k8sClient.Get(ctx, types.NamespacedName{Name: restorePVC.Name, Namespace: restorePVC.Namespace}, &restorePVCCurrent)).To(Succeed())
		restorePVCBase := restorePVCCurrent.DeepCopy()
		restorePVCCurrent.Spec.VolumeName = "pv-bound"
		restorePVCCurrent.Finalizers = nil
		Expect(k8sClient.Patch(ctx, &restorePVCCurrent, client.MergeFrom(restorePVCBase))).To(Succeed())
		_, err = reconciler.Reconcile(ctx, reconcile.Request{NamespacedName: policyNN})
		Expect(err).NotTo(HaveOccurred())

		Expect(apierrors.IsNotFound(k8sClient.Get(ctx, types.NamespacedName{Name: snapshotName, Namespace: "default"}, &volumesnapshotv1.VolumeSnapshot{}))).To(BeTrue())
		Expect(apierrors.IsNotFound(k8sClient.Get(ctx, types.NamespacedName{Name: backupName, Namespace: "default"}, &snaplanev1alpha1.Backup{}))).To(BeTrue())
	})
})

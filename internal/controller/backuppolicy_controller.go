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

package controller

import (
	"context"
	"crypto/sha1"
	"encoding/hex"
	"errors"
	"fmt"
	"sort"
	"strings"
	"time"

	volumesnapshotv1 "github.com/kubernetes-csi/external-snapshotter/client/v8/apis/volumesnapshot/v1"
	"github.com/robfig/cron/v3"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/equality"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	meta "k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/util/retry"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/builder"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/event"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	"sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/predicate"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	snaplanev1alpha1 "github.com/molpako/snaplane/api/v1alpha1"
)

const defaultRequeueAfter = time.Minute

const (
	queueHealthyReasonObserved  = "QueueObserved"
	queueHealthyMessageObserved = "queue metadata observed"
)

var (
	errDispatchGateNotPending = errors.New("dispatch gate is not pending")
	errDispatchGateNotReady   = errors.New("dispatch gate is not ready")
)

type serialGuard struct {
	satisfied bool
	reason    string
	message   string
}

type queueHealthStatus struct {
	healthy bool
	reason  string
	message string
}

// BackupPolicyReconciler reconciles a BackupPolicy object.
type BackupPolicyReconciler struct {
	client.Client
	Scheme         *runtime.Scheme
	LeaseNamespace string
}

// +kubebuilder:rbac:groups=snaplane.molpako.github.io,resources=backuppolicies,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=snaplane.molpako.github.io,resources=backuppolicies/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=snaplane.molpako.github.io,resources=backuppolicies/finalizers,verbs=update
// +kubebuilder:rbac:groups=snaplane.molpako.github.io,resources=backups,verbs=get;list;watch;create;delete;patch
// +kubebuilder:rbac:groups=snapshot.storage.k8s.io,resources=volumesnapshots,verbs=get;list;watch;create;delete;patch
// +kubebuilder:rbac:groups="",resources=nodes,verbs=get;list;watch
// +kubebuilder:rbac:groups="",resources=persistentvolumeclaims,verbs=get;list;watch;patch
// +kubebuilder:rbac:groups=coordination.k8s.io,resources=leases,verbs=get;list;watch

// Reconcile executes scheduler, dispatcher, completion observer and retention in one idempotent loop.
func (r *BackupPolicyReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	logger := log.FromContext(ctx).WithValues("policy", req.NamespacedName)

	var policy snaplanev1alpha1.BackupPolicy
	if err := r.Get(ctx, req.NamespacedName, &policy); err != nil {
		if apierrors.IsNotFound(err) {
			return ctrl.Result{}, nil
		}
		return ctrl.Result{}, err
	}
	if !policy.DeletionTimestamp.IsZero() {
		return ctrl.Result{}, nil
	}

	oldPolicy := policy.DeepCopy()
	now := time.Now().UTC()
	sg := serialGuard{satisfied: true, reason: "SerialGuardSatisfied", message: "serial guard is satisfied"}

	queueHealth, err := r.ensureScheduleAndManualSnapshots(ctx, &policy, now)
	if err != nil {
		return ctrl.Result{}, err
	}

	if err := r.reconcileSnapshotStateFromBackups(ctx, &policy, now); err != nil {
		return ctrl.Result{}, err
	}

	sg, err = r.dispatchOldestPending(ctx, &policy, now)
	if err != nil {
		return ctrl.Result{}, err
	}

	if err := r.applyRetention(ctx, &policy, now); err != nil {
		return ctrl.Result{}, err
	}

	if err := r.refreshPolicyStatus(ctx, oldPolicy, &policy, sg, queueHealth, now); err != nil {
		return ctrl.Result{}, err
	}

	requeueAfter := r.nextRequeueAfter(&policy, now)
	logger.V(1).Info("reconcile completed", "requeueAfter", requeueAfter)
	return ctrl.Result{RequeueAfter: requeueAfter}, nil
}

func (r *BackupPolicyReconciler) ensureScheduleAndManualSnapshots(
	ctx context.Context,
	policy *snaplanev1alpha1.BackupPolicy,
	now time.Time,
) (queueHealthStatus, error) {
	queueHealth := queueHealthStatus{
		healthy: true,
		reason:  queueHealthyReasonObserved,
		message: queueHealthyMessageObserved,
	}

	manualCreated, err := r.ensurePendingManualSnapshot(ctx, policy, now)
	if err != nil {
		return queueHealth, err
	}
	if manualCreated {
		return queueHealth, nil
	}

	if policy.Spec.Schedule.Suspend != nil && *policy.Spec.Schedule.Suspend {
		return queueHealth, nil
	}

	due, scheduledTime, err := nextScheduleTime(policy, now)
	if err != nil {
		return queueHealth, nil
	}
	if !due {
		return queueHealth, nil
	}

	if err := r.ensureQueuedSnapshot(ctx, policy, snapshotNameFor(policy.Name, scheduledTime), scheduledTime, true); err != nil {
		return queueHealth, err
	}
	mt := metav1.NewTime(scheduledTime)
	policy.Status.LastScheduledTime = &mt
	return queueHealth, nil
}

func nextScheduleTime(policy *snaplanev1alpha1.BackupPolicy, now time.Time) (bool, time.Time, error) {
	location := time.UTC
	if policy.Spec.Schedule.TimeZone != nil && *policy.Spec.Schedule.TimeZone != "" {
		loaded, err := time.LoadLocation(*policy.Spec.Schedule.TimeZone)
		if err != nil {
			return false, time.Time{}, err
		}
		location = loaded
	}

	sched, err := cron.ParseStandard(policy.Spec.Schedule.Cron)
	if err != nil {
		return false, time.Time{}, err
	}

	base := policy.CreationTimestamp.Time
	if policy.Status.LastScheduledTime != nil {
		base = policy.Status.LastScheduledTime.Time
	}
	next := sched.Next(base.In(location))
	if now.In(location).Before(next) {
		return false, next.UTC(), nil
	}
	return true, next.UTC(), nil
}

func (r *BackupPolicyReconciler) ensurePendingManualSnapshot(
	ctx context.Context,
	policy *snaplanev1alpha1.BackupPolicy,
	now time.Time,
) (bool, error) {
	requestID, pending := pendingManualRequest(policy)
	if !pending {
		return false, nil
	}

	snapshotName := manualSnapshotNameFor(policy.Name, requestID)
	nn := types.NamespacedName{Namespace: policy.Namespace, Name: snapshotName}
	var existing volumesnapshotv1.VolumeSnapshot
	if err := r.Get(ctx, nn, &existing); err == nil {
		if existing.Labels != nil {
			owner := existing.Labels[snaplanev1alpha1.PolicyLabelKey]
			if owner != "" && owner != policy.Name {
				return false, fmt.Errorf("manual snapshot %s/%s is owned by %q", existing.Namespace, existing.Name, owner)
			}
		}
		markManualRequestHandled(policy, requestID, now)
		return false, nil
	} else if !apierrors.IsNotFound(err) {
		return false, err
	}

	hasNonDone, err := r.hasNonDoneSnapshots(ctx, policy)
	if err != nil {
		return false, err
	}
	if hasNonDone {
		return false, nil
	}

	if err := r.ensureQueuedSnapshot(ctx, policy, snapshotName, now, false); err != nil {
		return false, err
	}
	markManualRequestHandled(policy, requestID, now)
	return true, nil
}

func (r *BackupPolicyReconciler) ensureQueuedSnapshot(
	ctx context.Context,
	policy *snaplanev1alpha1.BackupPolicy,
	snapshotName string,
	queueTime time.Time,
	strictName bool,
) error {
	nn := types.NamespacedName{Namespace: policy.Namespace, Name: snapshotName}
	queueTimeValue := queueTime.UTC().Format(time.RFC3339)

	var snapshot volumesnapshotv1.VolumeSnapshot
	err := r.Get(ctx, nn, &snapshot)
	if err != nil {
		if !apierrors.IsNotFound(err) {
			return err
		}

		labelsMap := map[string]string{
			snaplanev1alpha1.PolicyLabelKey:      policy.Name,
			snaplanev1alpha1.BackupStateLabelKey: string(snaplanev1alpha1.SnapshotQueueStatePending),
		}
		annotations := map[string]string{
			snaplanev1alpha1.QueueTimeAnnotationKey: queueTimeValue,
		}
		className := policy.Spec.Source.VolumeSnapshotClassName
		pvcName := policy.Spec.Source.PersistentVolumeClaimName
		snapshot = volumesnapshotv1.VolumeSnapshot{
			ObjectMeta: metav1.ObjectMeta{
				Namespace:   policy.Namespace,
				Name:        snapshotName,
				Labels:      labelsMap,
				Annotations: annotations,
			},
			Spec: volumesnapshotv1.VolumeSnapshotSpec{
				VolumeSnapshotClassName: &className,
				Source: volumesnapshotv1.VolumeSnapshotSource{
					PersistentVolumeClaimName: &pvcName,
				},
			},
		}
		return r.Create(ctx, &snapshot)
	}

	base := snapshot.DeepCopy()
	if snapshot.Labels == nil {
		snapshot.Labels = map[string]string{}
	}
	if snapshot.Annotations == nil {
		snapshot.Annotations = map[string]string{}
	}

	owner, hasOwner := snapshot.Labels[snaplanev1alpha1.PolicyLabelKey]
	if hasOwner && owner != "" && owner != policy.Name {
		return nil
	}

	state := snaplanev1alpha1.SnapshotQueueState(snapshot.Labels[snaplanev1alpha1.BackupStateLabelKey])
	switch state {
	case "":
		snapshot.Labels[snaplanev1alpha1.PolicyLabelKey] = policy.Name
		snapshot.Labels[snaplanev1alpha1.BackupStateLabelKey] = string(snaplanev1alpha1.SnapshotQueueStatePending)
		snapshot.Annotations[snaplanev1alpha1.QueueTimeAnnotationKey] = queueTimeValue
	case snaplanev1alpha1.SnapshotQueueStatePending:
		if snapshot.Labels[snaplanev1alpha1.PolicyLabelKey] == "" {
			snapshot.Labels[snaplanev1alpha1.PolicyLabelKey] = policy.Name
		}
		if snapshot.Annotations[snaplanev1alpha1.QueueTimeAnnotationKey] == "" {
			snapshot.Annotations[snaplanev1alpha1.QueueTimeAnnotationKey] = queueTimeValue
		}
	default:
		if strictName {
			return nil
		}
	}

	if equality.Semantic.DeepEqual(base.Labels, snapshot.Labels) && equality.Semantic.DeepEqual(base.Annotations, snapshot.Annotations) {
		return nil
	}
	return r.Patch(ctx, &snapshot, client.MergeFrom(base))
}

func (r *BackupPolicyReconciler) reconcileSnapshotStateFromBackups(ctx context.Context, policy *snaplanev1alpha1.BackupPolicy, now time.Time) error {
	backups, err := r.listPolicyBackups(ctx, policy)
	if err != nil {
		return err
	}

	for i := range backups {
		backup := &backups[i]
		snapshotNamespace := backup.Namespace
		if backup.Spec.VolumeSnapshotRef.Namespace != nil && *backup.Spec.VolumeSnapshotRef.Namespace != "" {
			snapshotNamespace = *backup.Spec.VolumeSnapshotRef.Namespace
		}

		var snapshot volumesnapshotv1.VolumeSnapshot
		err := r.Get(ctx, types.NamespacedName{Namespace: snapshotNamespace, Name: backup.Spec.VolumeSnapshotRef.Name}, &snapshot)
		if err != nil {
			if apierrors.IsNotFound(err) {
				continue
			}
			return err
		}

		base := snapshot.DeepCopy()
		if snapshot.Labels == nil {
			snapshot.Labels = map[string]string{}
		}
		if snapshot.Annotations == nil {
			snapshot.Annotations = map[string]string{}
		}

		if snapshot.Labels[snaplanev1alpha1.PolicyLabelKey] == "" {
			snapshot.Labels[snaplanev1alpha1.PolicyLabelKey] = policy.Name
		}

		state := snaplanev1alpha1.SnapshotQueueState(snapshot.Labels[snaplanev1alpha1.BackupStateLabelKey])
		if state == "" {
			snapshot.Labels[snaplanev1alpha1.BackupStateLabelKey] = string(snaplanev1alpha1.SnapshotQueueStatePending)
			if snapshot.Annotations[snaplanev1alpha1.QueueTimeAnnotationKey] == "" {
				snapshot.Annotations[snaplanev1alpha1.QueueTimeAnnotationKey] = now.UTC().Format(time.RFC3339)
			}
		}

		switch {
		case backupIsSucceeded(backup):
			snapshot.Labels[snaplanev1alpha1.BackupStateLabelKey] = string(snaplanev1alpha1.SnapshotQueueStateDone)
			snapshot.Annotations[snaplanev1alpha1.BackupNameAnnotationKey] = backup.Name
		case backupIsFailed(backup):
			snapshot.Labels[snaplanev1alpha1.BackupStateLabelKey] = string(snaplanev1alpha1.SnapshotQueueStateFailed)
			snapshot.Annotations[snaplanev1alpha1.BackupNameAnnotationKey] = backup.Name
		}

		if equality.Semantic.DeepEqual(base.Labels, snapshot.Labels) && equality.Semantic.DeepEqual(base.Annotations, snapshot.Annotations) {
			continue
		}
		if err := r.Patch(ctx, &snapshot, client.MergeFrom(base)); err != nil {
			return err
		}
	}

	return nil
}

func (r *BackupPolicyReconciler) dispatchOldestPending(
	ctx context.Context,
	policy *snaplanev1alpha1.BackupPolicy,
	now time.Time,
) (serialGuard, error) {
	snapshots, err := r.listPolicySnapshots(ctx, policy)
	if err != nil {
		return serialGuard{}, err
	}

	sort.SliceStable(snapshots, func(i, j int) bool {
		qi := queueTimeOf(&snapshots[i])
		qj := queueTimeOf(&snapshots[j])
		if qi.Equal(qj) {
			return snapshots[i].Name < snapshots[j].Name
		}
		return qi.Before(qj)
	})

	var gate *volumesnapshotv1.VolumeSnapshot
	for i := range snapshots {
		state := snapshotState(&snapshots[i])
		if state == snaplanev1alpha1.SnapshotQueueStateDone {
			continue
		}
		gate = &snapshots[i]
		break
	}

	if gate == nil {
		return serialGuard{satisfied: true, reason: "QueueEmpty", message: "no pending snapshots"}, nil
	}

	switch snapshotState(gate) {
	case snaplanev1alpha1.SnapshotQueueStateFailed:
		return serialGuard{
			satisfied: false,
			reason:    "BlockedByFailedSnapshot",
			message:   "oldest non-done snapshot is failed",
		}, nil
	case snaplanev1alpha1.SnapshotQueueStateDispatched:
		return serialGuard{satisfied: true, reason: "DispatchedInProgress", message: "waiting for dispatched snapshot completion"}, nil
	case snaplanev1alpha1.SnapshotQueueStatePending:
		if !snapshotIsReady(gate) {
			return serialGuard{satisfied: true, reason: "WaitingForReady", message: "oldest pending snapshot is not ready"}, nil
		}
	default:
		return serialGuard{satisfied: true, reason: "UnknownState", message: "waiting for snapshot state reconciliation"}, nil
	}

	backups, err := r.listPolicyBackups(ctx, policy)
	if err != nil {
		return serialGuard{}, err
	}
	for i := range backups {
		if backupIsActive(&backups[i]) {
			return serialGuard{satisfied: true, reason: "SingleFlightActive", message: "backup is already running"}, nil
		}
	}

	latest := &volumesnapshotv1.VolumeSnapshot{}
	dispatched, err := r.casPatchSnapshotToDispatched(ctx, types.NamespacedName{Namespace: gate.Namespace, Name: gate.Name}, latest)
	if err != nil {
		return serialGuard{}, err
	}
	if !dispatched {
		return serialGuard{satisfied: true, reason: "StateAdvanced", message: "snapshot state changed by another reconcile"}, nil
	}

	backupName, err := r.createOrGetBackup(ctx, policy, latest, now)
	if err != nil {
		return serialGuard{}, err
	}

	if err := r.patchSnapshotBackupName(ctx, types.NamespacedName{Namespace: latest.Namespace, Name: latest.Name}, backupName); err != nil {
		return serialGuard{}, err
	}

	policy.Status.LastDispatchedSnapshot = latest.Name
	policy.Status.ActiveBackupName = backupName
	_ = now
	return serialGuard{satisfied: true, reason: "Dispatched", message: "snapshot dispatched successfully"}, nil
}

func (r *BackupPolicyReconciler) casPatchSnapshotToDispatched(
	ctx context.Context,
	nn types.NamespacedName,
	into *volumesnapshotv1.VolumeSnapshot,
) (bool, error) {
	err := retry.RetryOnConflict(retry.DefaultRetry, func() error {
		if err := r.Get(ctx, nn, into); err != nil {
			return err
		}
		if snapshotState(into) != snaplanev1alpha1.SnapshotQueueStatePending {
			return errDispatchGateNotPending
		}
		if !snapshotIsReady(into) {
			return errDispatchGateNotReady
		}
		base := into.DeepCopy()
		if into.Labels == nil {
			into.Labels = map[string]string{}
		}
		into.Labels[snaplanev1alpha1.BackupStateLabelKey] = string(snaplanev1alpha1.SnapshotQueueStateDispatched)
		return r.Patch(ctx, into, client.MergeFrom(base))
	})
	if err == nil {
		return true, nil
	}
	if errors.Is(err, errDispatchGateNotPending) || errors.Is(err, errDispatchGateNotReady) {
		return false, nil
	}
	return false, err
}

func (r *BackupPolicyReconciler) patchSnapshotBackupName(ctx context.Context, nn types.NamespacedName, backupName string) error {
	return retry.RetryOnConflict(retry.DefaultRetry, func() error {
		var current volumesnapshotv1.VolumeSnapshot
		if err := r.Get(ctx, nn, &current); err != nil {
			return err
		}
		base := current.DeepCopy()
		if current.Annotations == nil {
			current.Annotations = map[string]string{}
		}
		if current.Annotations[snaplanev1alpha1.BackupNameAnnotationKey] == backupName {
			return nil
		}
		current.Annotations[snaplanev1alpha1.BackupNameAnnotationKey] = backupName
		return r.Patch(ctx, &current, client.MergeFrom(base))
	})
}

func (r *BackupPolicyReconciler) createOrGetBackup(
	ctx context.Context,
	policy *snaplanev1alpha1.BackupPolicy,
	snapshot *volumesnapshotv1.VolumeSnapshot,
	now time.Time,
) (string, error) {
	backupName := backupNameFor(policy.Name, snapshot.Name)
	nn := types.NamespacedName{Namespace: policy.Namespace, Name: backupName}
	var backup snaplanev1alpha1.Backup
	err := r.Get(ctx, nn, &backup)
	if err == nil {
		return backup.Name, nil
	}
	if !apierrors.IsNotFound(err) {
		return "", err
	}

	nodeName, err := r.resolveBackupDestinationNode(ctx, policy, now)
	if err != nil {
		return "", err
	}

	backup = snaplanev1alpha1.Backup{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: policy.Namespace,
			Name:      backupName,
		},
		Spec: snaplanev1alpha1.BackupSpec{
			PolicyRef: snaplanev1alpha1.LocalNameReference{Name: policy.Name},
			VolumeSnapshotRef: snaplanev1alpha1.VolumeSnapshotReference{
				Name:      snapshot.Name,
				Namespace: &snapshot.Namespace,
			},
			Destination: snaplanev1alpha1.BackupDestination{
				NodeName: nodeName,
			},
			Transfer: policy.Spec.Transfer,
		},
	}
	if err := r.Create(ctx, &backup); err != nil {
		return "", err
	}
	return backup.Name, nil
}

func (r *BackupPolicyReconciler) applyRetention(ctx context.Context, policy *snaplanev1alpha1.BackupPolicy, now time.Time) error {
	keepAfter := now.Add(-time.Duration(policy.Spec.Retention.KeepDays) * 24 * time.Hour)
	snapshots, err := r.listPolicySnapshots(ctx, policy)
	if err != nil {
		return err
	}

	candidates := make([]volumesnapshotv1.VolumeSnapshot, 0)
	for i := range snapshots {
		if snapshotState(&snapshots[i]) != snaplanev1alpha1.SnapshotQueueStateDone {
			continue
		}
		if queueTimeOf(&snapshots[i]).Before(keepAfter) {
			candidates = append(candidates, snapshots[i])
		}
	}
	if len(candidates) == 0 {
		return nil
	}

	protectedBackups, err := r.activeRestoreBackupReferences(ctx, policy.Namespace)
	if err != nil {
		return err
	}

	sort.SliceStable(candidates, func(i, j int) bool {
		qi := queueTimeOf(&candidates[i])
		qj := queueTimeOf(&candidates[j])
		if qi.Equal(qj) {
			return candidates[i].Name < candidates[j].Name
		}
		return qi.Before(qj)
	})

	for i := range candidates {
		snap := candidates[i]
		backupName := ""
		if snap.Annotations != nil {
			backupName = snap.Annotations[snaplanev1alpha1.BackupNameAnnotationKey]
		}
		if _, protected := protectedBackups[backupName]; protected {
			continue
		}

		if err := r.Delete(ctx, &snap); err != nil && !apierrors.IsNotFound(err) {
			return err
		}

		if backupName == "" {
			continue
		}
		backup := &snaplanev1alpha1.Backup{}
		nn := types.NamespacedName{Namespace: policy.Namespace, Name: backupName}
		if err := r.Get(ctx, nn, backup); err == nil {
			if err := r.Delete(ctx, backup); err != nil && !apierrors.IsNotFound(err) {
				return err
			}
		}
	}

	return nil
}

func (r *BackupPolicyReconciler) activeRestoreBackupReferences(ctx context.Context, namespace string) (map[string]struct{}, error) {
	var pvcList corev1.PersistentVolumeClaimList
	if err := r.List(ctx, &pvcList, &client.ListOptions{Namespace: namespace}); err != nil {
		return nil, err
	}

	protected := make(map[string]struct{})
	for i := range pvcList.Items {
		pvc := &pvcList.Items[i]
		if !pvcReferencesBackupDataSource(pvc, namespace) {
			continue
		}
		if pvc.Spec.VolumeName != "" && !stringSliceContains(pvc.Finalizers, snaplanev1alpha1.RestorePopulatorFinalizerKey) {
			continue
		}
		protected[pvc.Spec.DataSourceRef.Name] = struct{}{}
	}
	return protected, nil
}

func pvcReferencesBackupDataSource(pvc *corev1.PersistentVolumeClaim, namespace string) bool {
	if pvc == nil || pvc.Spec.DataSourceRef == nil {
		return false
	}
	ref := pvc.Spec.DataSourceRef
	if ref.APIGroup == nil || *ref.APIGroup != snaplanev1alpha1.GroupVersion.Group {
		return false
	}
	if ref.Kind != "Backup" || ref.Name == "" {
		return false
	}
	if ref.Namespace != nil && *ref.Namespace != "" && *ref.Namespace != namespace {
		return false
	}
	return true
}

func stringSliceContains(values []string, want string) bool {
	for i := range values {
		if values[i] == want {
			return true
		}
	}
	return false
}

func (r *BackupPolicyReconciler) refreshPolicyStatus(
	ctx context.Context,
	old *snaplanev1alpha1.BackupPolicy,
	policy *snaplanev1alpha1.BackupPolicy,
	sg serialGuard,
	queueHealth queueHealthStatus,
	now time.Time,
) error {
	snapshots, err := r.listPolicySnapshots(ctx, policy)
	if err != nil {
		return err
	}
	pendingCount := int32(0)
	for i := range snapshots {
		if snapshotState(&snapshots[i]) == snaplanev1alpha1.SnapshotQueueStatePending {
			pendingCount++
		}
	}
	policy.Status.PendingSnapshotCount = pendingCount

	backups, err := r.listPolicyBackups(ctx, policy)
	if err != nil {
		return err
	}
	active := ""
	for i := range backups {
		if backupIsActive(&backups[i]) {
			active = backups[i].Name
			break
		}
	}
	policy.Status.ActiveBackupName = active

	nowMeta := metav1.NewTime(now)
	setPolicyCondition(policy, snaplanev1alpha1.BackupPolicyConditionReady, metav1.ConditionTrue, "Reconciled", "policy reconciled", nowMeta)
	if queueHealth.healthy {
		setPolicyCondition(policy, snaplanev1alpha1.BackupPolicyConditionQueueHealthy, metav1.ConditionTrue, queueHealth.reason, queueHealth.message, nowMeta)
	} else {
		setPolicyCondition(policy, snaplanev1alpha1.BackupPolicyConditionQueueHealthy, metav1.ConditionFalse, queueHealth.reason, queueHealth.message, nowMeta)
	}
	if sg.satisfied {
		setPolicyCondition(policy, snaplanev1alpha1.BackupPolicyConditionSerialGuardSatisfied, metav1.ConditionTrue, sg.reason, sg.message, nowMeta)
	} else {
		setPolicyCondition(policy, snaplanev1alpha1.BackupPolicyConditionSerialGuardSatisfied, metav1.ConditionFalse, sg.reason, sg.message, nowMeta)
	}

	if equality.Semantic.DeepEqual(old.Status, policy.Status) {
		return nil
	}
	return r.Status().Patch(ctx, policy, client.MergeFrom(old))
}

func (r *BackupPolicyReconciler) listPolicySnapshots(ctx context.Context, policy *snaplanev1alpha1.BackupPolicy) ([]volumesnapshotv1.VolumeSnapshot, error) {
	selector := labels.SelectorFromSet(map[string]string{snaplanev1alpha1.PolicyLabelKey: policy.Name})
	var list volumesnapshotv1.VolumeSnapshotList
	if err := r.List(ctx, &list, &client.ListOptions{Namespace: policy.Namespace, LabelSelector: selector}); err != nil {
		return nil, err
	}
	return list.Items, nil
}

func (r *BackupPolicyReconciler) listPolicyBackups(ctx context.Context, policy *snaplanev1alpha1.BackupPolicy) ([]snaplanev1alpha1.Backup, error) {
	var list snaplanev1alpha1.BackupList
	if err := r.List(ctx, &list, &client.ListOptions{Namespace: policy.Namespace}); err != nil {
		return nil, err
	}

	out := make([]snaplanev1alpha1.Backup, 0, len(list.Items))
	for i := range list.Items {
		if list.Items[i].Spec.PolicyRef.Name == policy.Name {
			out = append(out, list.Items[i])
		}
	}
	return out, nil
}

func (r *BackupPolicyReconciler) hasNonDoneSnapshots(ctx context.Context, policy *snaplanev1alpha1.BackupPolicy) (bool, error) {
	snapshots, err := r.listPolicySnapshots(ctx, policy)
	if err != nil {
		return false, err
	}
	for i := range snapshots {
		if snapshotState(&snapshots[i]) != snaplanev1alpha1.SnapshotQueueStateDone {
			return true, nil
		}
	}
	return false, nil
}

func (r *BackupPolicyReconciler) nextRequeueAfter(policy *snaplanev1alpha1.BackupPolicy, now time.Time) time.Duration {
	if policy.Spec.Schedule.Suspend != nil && *policy.Spec.Schedule.Suspend {
		return defaultRequeueAfter
	}
	_, next, err := nextScheduleTime(policy, now)
	if err != nil {
		return defaultRequeueAfter
	}
	if !next.After(now) {
		return 5 * time.Second
	}
	wait := next.Sub(now)
	if wait < 5*time.Second {
		return 5 * time.Second
	}
	if wait > defaultRequeueAfter {
		return defaultRequeueAfter
	}
	return wait
}

func setPolicyCondition(
	policy *snaplanev1alpha1.BackupPolicy,
	conditionType string,
	status metav1.ConditionStatus,
	reason string,
	message string,
	now metav1.Time,
) {
	meta.SetStatusCondition(&policy.Status.Conditions, metav1.Condition{
		Type:               conditionType,
		Status:             status,
		Reason:             reason,
		Message:            message,
		ObservedGeneration: policy.Generation,
		LastTransitionTime: now,
	})
}

func snapshotState(snapshot *volumesnapshotv1.VolumeSnapshot) snaplanev1alpha1.SnapshotQueueState {
	if snapshot.Labels == nil {
		return ""
	}
	return snaplanev1alpha1.SnapshotQueueState(snapshot.Labels[snaplanev1alpha1.BackupStateLabelKey])
}

func queueTimeOf(snapshot *volumesnapshotv1.VolumeSnapshot) time.Time {
	if snapshot.Annotations == nil {
		return time.Unix(1<<62, 0).UTC()
	}
	value := snapshot.Annotations[snaplanev1alpha1.QueueTimeAnnotationKey]
	if value == "" {
		return time.Unix(1<<62, 0).UTC()
	}
	parsed, err := time.Parse(time.RFC3339, value)
	if err != nil {
		return time.Unix(1<<62, 0).UTC()
	}
	return parsed.UTC()
}

func snapshotIsReady(snapshot *volumesnapshotv1.VolumeSnapshot) bool {
	if snapshot == nil || snapshot.Status == nil || snapshot.Status.ReadyToUse == nil {
		return false
	}
	return *snapshot.Status.ReadyToUse
}

func snapshotNameFor(policyName string, queueTime time.Time) string {
	name := fmt.Sprintf("%s-%s", policyName, queueTime.UTC().Format("20060102-150405"))
	return dns1123Name(name)
}

func manualSnapshotNameFor(policyName, requestID string) string {
	h := sha1.Sum([]byte(policyName + "\x00" + requestID))
	suffix := hex.EncodeToString(h[:])[:10]
	return dns1123Name(fmt.Sprintf("%s-manual-%s", policyName, suffix))
}

func backupNameFor(policyName, snapshotName string) string {
	h := sha1.Sum([]byte(policyName + "/" + snapshotName))
	suffix := hex.EncodeToString(h[:])[:8]
	base := fmt.Sprintf("%s-%s-%s", policyName, snapshotName, suffix)
	return dns1123Name(base)
}

func dns1123Name(name string) string {
	n := strings.ToLower(name)
	n = strings.ReplaceAll(n, "_", "-")
	n = strings.ReplaceAll(n, ".", "-")
	n = strings.Trim(n, "-")
	if n == "" {
		return "x"
	}
	if len(n) <= 63 {
		return n
	}
	return strings.TrimRight(n[:63], "-")
}

func (r *BackupPolicyReconciler) mapSnapshotToPolicy(_ context.Context, obj client.Object) []reconcile.Request {
	snapshot, ok := obj.(*volumesnapshotv1.VolumeSnapshot)
	if !ok || snapshot.Labels == nil {
		return nil
	}
	policyName := snapshot.Labels[snaplanev1alpha1.PolicyLabelKey]
	if policyName == "" {
		return nil
	}
	return []reconcile.Request{{NamespacedName: types.NamespacedName{Namespace: snapshot.Namespace, Name: policyName}}}
}

func (r *BackupPolicyReconciler) mapBackupToPolicy(_ context.Context, obj client.Object) []reconcile.Request {
	backup, ok := obj.(*snaplanev1alpha1.Backup)
	if !ok {
		return nil
	}
	if backup.Spec.PolicyRef.Name == "" {
		return nil
	}
	return []reconcile.Request{{NamespacedName: types.NamespacedName{Namespace: backup.Namespace, Name: backup.Spec.PolicyRef.Name}}}
}

func pendingManualRequest(policy *snaplanev1alpha1.BackupPolicy) (string, bool) {
	if policy == nil {
		return "", false
	}
	requestID := policy.Spec.Manual.RequestID
	if requestID == "" || requestID == policy.Status.Manual.LastHandledRequestID {
		return "", false
	}
	return requestID, true
}

func markManualRequestHandled(policy *snaplanev1alpha1.BackupPolicy, requestID string, now time.Time) {
	policy.Status.Manual.LastHandledRequestID = requestID
	handledAt := metav1.NewTime(now.UTC())
	policy.Status.Manual.LastHandledTime = &handledAt
}

func queueMetadataChanged(oldObj, newObj *volumesnapshotv1.VolumeSnapshot) bool {
	var oldLabels map[string]string
	var newLabels map[string]string
	var oldAnnotations map[string]string
	var newAnnotations map[string]string
	if oldObj != nil {
		oldLabels = oldObj.GetLabels()
		oldAnnotations = oldObj.GetAnnotations()
	}
	if newObj != nil {
		newLabels = newObj.GetLabels()
		newAnnotations = newObj.GetAnnotations()
	}
	return oldLabels[snaplanev1alpha1.BackupStateLabelKey] != newLabels[snaplanev1alpha1.BackupStateLabelKey] ||
		oldLabels[snaplanev1alpha1.PolicyLabelKey] != newLabels[snaplanev1alpha1.PolicyLabelKey] ||
		oldAnnotations[snaplanev1alpha1.QueueTimeAnnotationKey] != newAnnotations[snaplanev1alpha1.QueueTimeAnnotationKey] ||
		oldAnnotations[snaplanev1alpha1.BackupNameAnnotationKey] != newAnnotations[snaplanev1alpha1.BackupNameAnnotationKey]
}

func (r *BackupPolicyReconciler) backupPolicyPredicate() predicate.Predicate {
	return predicate.Funcs{
		CreateFunc: func(e event.CreateEvent) bool {
			return true
		},
		UpdateFunc: func(e event.UpdateEvent) bool {
			if e.ObjectOld == nil || e.ObjectNew == nil {
				return false
			}
			if e.ObjectOld.GetGeneration() != e.ObjectNew.GetGeneration() {
				return true
			}
			return e.ObjectOld.GetDeletionTimestamp().IsZero() != e.ObjectNew.GetDeletionTimestamp().IsZero()
		},
		DeleteFunc: func(e event.DeleteEvent) bool {
			return false
		},
		GenericFunc: func(e event.GenericEvent) bool {
			return false
		},
	}
}

func (r *BackupPolicyReconciler) volumeSnapshotPredicate() predicate.Predicate {
	return predicate.Funcs{
		CreateFunc: func(e event.CreateEvent) bool {
			return false
		},
		UpdateFunc: func(e event.UpdateEvent) bool {
			oldVS, okOld := e.ObjectOld.(*volumesnapshotv1.VolumeSnapshot)
			newVS, okNew := e.ObjectNew.(*volumesnapshotv1.VolumeSnapshot)
			if !okOld || !okNew {
				return false
			}

			oldReady := snapshotIsReady(oldVS)
			newReady := snapshotIsReady(newVS)
			if !oldReady && newReady {
				return true
			}

			if queueMetadataChanged(oldVS, newVS) {
				return true
			}

			return oldVS.GetDeletionTimestamp().IsZero() != newVS.GetDeletionTimestamp().IsZero()
		},
		DeleteFunc: func(e event.DeleteEvent) bool {
			return true
		},
		GenericFunc: func(e event.GenericEvent) bool {
			return false
		},
	}
}

func (r *BackupPolicyReconciler) backupPredicate() predicate.Predicate {
	return predicate.Funcs{
		CreateFunc: func(e event.CreateEvent) bool {
			backup, ok := e.Object.(*snaplanev1alpha1.Backup)
			return ok && backup.Spec.PolicyRef.Name != ""
		},
		UpdateFunc: func(e event.UpdateEvent) bool {
			oldBackup, okOld := e.ObjectOld.(*snaplanev1alpha1.Backup)
			newBackup, okNew := e.ObjectNew.(*snaplanev1alpha1.Backup)
			if !okOld || !okNew {
				return false
			}
			if newBackup.Spec.PolicyRef.Name == "" {
				return false
			}
			if oldBackup.Spec.PolicyRef.Name != newBackup.Spec.PolicyRef.Name {
				return true
			}
			return !backupIsTerminal(oldBackup) && backupIsTerminal(newBackup)
		},
		DeleteFunc: func(e event.DeleteEvent) bool {
			return false
		},
		GenericFunc: func(e event.GenericEvent) bool {
			return false
		},
	}
}

// SetupWithManager sets up the controller with the Manager.
func (r *BackupPolicyReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&snaplanev1alpha1.BackupPolicy{}, builder.WithPredicates(r.backupPolicyPredicate())).
		Watches(
			&volumesnapshotv1.VolumeSnapshot{},
			handler.EnqueueRequestsFromMapFunc(r.mapSnapshotToPolicy),
			builder.WithPredicates(r.volumeSnapshotPredicate()),
		).
		Watches(
			&snaplanev1alpha1.Backup{},
			handler.EnqueueRequestsFromMapFunc(r.mapBackupToPolicy),
			builder.WithPredicates(r.backupPredicate()),
		).
		Named("backuppolicy").
		Complete(r)
}

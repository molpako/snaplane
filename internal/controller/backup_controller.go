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
	"errors"
	"fmt"
	"hash/crc32"
	"os"
	"sort"
	"time"

	volumesnapshotv1 "github.com/kubernetes-csi/external-snapshotter/client/v8/apis/volumesnapshot/v1"
	coordinationv1 "k8s.io/api/coordination/v1"
	"k8s.io/apimachinery/pkg/api/equality"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	meta "k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/record"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/builder"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/event"
	"sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/predicate"

	writerv1 "github.com/molpako/snaplane/api/proto/writer/v1"
	snaplanev1alpha1 "github.com/molpako/snaplane/api/v1alpha1"
	"github.com/molpako/snaplane/internal/backupsource"
	"github.com/molpako/snaplane/internal/casrepo"
	"github.com/molpako/snaplane/internal/writer"
)

const (
	defaultBackupRetryMaxAttempts int32         = 6
	backupRetryBaseDelay          time.Duration = time.Second
	backupRetryCapDelay           time.Duration = 60 * time.Second
	backupRetryJitterRatio                      = 0.2
)

var errFramePlanInvalid = errors.New("frame plan invalid")

// BackupReconciler reconciles a Backup object.
type BackupReconciler struct {
	client.Client
	Scheme              *runtime.Scheme
	WriterClientFactory WriterClientFactory
	LeaseNamespace      string
	Recorder            record.EventRecorder
}

// +kubebuilder:rbac:groups=snaplane.molpako.github.io,resources=backups,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=snaplane.molpako.github.io,resources=backups/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=snaplane.molpako.github.io,resources=backups/finalizers,verbs=update
// +kubebuilder:rbac:groups=snapshot.storage.k8s.io,resources=volumesnapshots,verbs=get;list;watch
// +kubebuilder:rbac:groups=snapshot.storage.k8s.io,resources=volumesnapshotcontents,verbs=get;list;watch
// +kubebuilder:rbac:groups=snaplane.molpako.github.io,resources=backuppolicies,verbs=get;list;watch
// +kubebuilder:rbac:groups=coordination.k8s.io,resources=leases,verbs=get;list;watch
// +kubebuilder:rbac:groups=cbt.storage.k8s.io,resources=snapshotmetadataservices,verbs=get;list;watch
// +kubebuilder:rbac:groups="",resources=persistentvolumeclaims,verbs=create;delete;get;list;patch;update;watch
// +kubebuilder:rbac:groups="",resources=pods,verbs=create;delete;get;list;watch
// +kubebuilder:rbac:groups="",resources=pods/exec,verbs=create
// +kubebuilder:rbac:groups=authentication.k8s.io,resources=selfsubjectreviews,verbs=create
// +kubebuilder:rbac:groups="",resources=serviceaccounts/token,verbs=create

// Reconcile drives a single transfer run object with immutable spec, cluster state,
// and minimal retry/completion checkpoints.
func (r *BackupReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	logger := log.FromContext(ctx).WithValues("backup", req.NamespacedName)

	var backup snaplanev1alpha1.Backup
	if err := r.Get(ctx, req.NamespacedName, &backup); err != nil {
		if apierrors.IsNotFound(err) {
			return ctrl.Result{}, nil
		}
		return ctrl.Result{}, err
	}
	if !backup.DeletionTimestamp.IsZero() {
		return ctrl.Result{}, nil
	}
	if backup.Status.CompletionTime != nil {
		return ctrl.Result{}, nil
	}

	old := backup.DeepCopy()
	now := metav1.Now()
	if backup.Status.StartTime == nil {
		backup.Status.StartTime = &now
	}

	snapshotNamespace := backup.Namespace
	if backup.Spec.VolumeSnapshotRef.Namespace != nil && *backup.Spec.VolumeSnapshotRef.Namespace != "" {
		snapshotNamespace = *backup.Spec.VolumeSnapshotRef.Namespace
	}

	var snapshot volumesnapshotv1.VolumeSnapshot
	err := r.Get(ctx, types.NamespacedName{Namespace: snapshotNamespace, Name: backup.Spec.VolumeSnapshotRef.Name}, &snapshot)
	if err != nil {
		if apierrors.IsNotFound(err) {
			if patchErr := r.markBackupFailed(ctx, old, &backup, "VolumeSnapshotNotFound", fmt.Sprintf("VolumeSnapshot %s/%s not found", snapshotNamespace, backup.Spec.VolumeSnapshotRef.Name)); patchErr != nil {
				return ctrl.Result{}, patchErr
			}
			return ctrl.Result{}, nil
		}
		return ctrl.Result{}, err
	}

	maxAttempts := resolveRetryMaxAttempts(&backup)
	if wait, ok := shouldWaitForRetry(now.Time, &backup); ok {
		setBackupCondition(
			&backup,
			snaplanev1alpha1.BackupConditionSucceeded,
			metav1.ConditionUnknown,
			"WaitingForRetry",
			fmt.Sprintf("retry attempt %d/%d waiting after %s", backup.Status.Retry.Attempt, maxAttempts, backup.Status.Retry.LastFailureReason),
			now,
		)
		changed, patchErr := r.patchBackupStatusIfChanged(ctx, old, &backup)
		if patchErr != nil {
			return ctrl.Result{}, patchErr
		}
		if changed {
			return ctrl.Result{RequeueAfter: wait}, nil
		}
		return ctrl.Result{RequeueAfter: wait}, nil
	}

	setBackupCondition(
		&backup,
		snaplanev1alpha1.BackupConditionSucceeded,
		metav1.ConditionUnknown,
		"InProgress",
		"backup transfer in progress",
		now,
	)

	transferCtx := ctx
	cancelTransfer := func() {}
	timeoutSeconds := resolveTransferTimeoutSeconds(&backup)
	if timeoutSeconds > 0 {
		transferCtx, cancelTransfer = context.WithTimeout(ctx, time.Duration(timeoutSeconds)*time.Second)
	}
	writeErr := normalizeTimeoutWriteError(r.executeBackupTransfer(transferCtx, &backup, &snapshot))
	cancelTransfer()
	if writeErr != nil {
		if writeErr.retryable {
			delay, exhausted := recordRetryAttempt(&backup, writeErr, maxAttempts)
			if exhausted {
				r.emitRetryExhaustedEvent(&backup, writeErr, maxAttempts)
				if patchErr := r.markBackupFailed(ctx, old, &backup, writeErr.reason, writeErr.message); patchErr != nil {
					return ctrl.Result{}, patchErr
				}
				return ctrl.Result{}, nil
			}
			r.emitRetryScheduledEvent(&backup, writeErr, delay, maxAttempts)
			setBackupCondition(
				&backup,
				snaplanev1alpha1.BackupConditionSucceeded,
				metav1.ConditionUnknown,
				"WaitingForRetry",
				fmt.Sprintf("retry attempt %d/%d scheduled: %s", backup.Status.Retry.Attempt, maxAttempts, writeErr.message),
				metav1.Now(),
			)
			changed, patchErr := r.patchBackupStatusIfChanged(ctx, old, &backup)
			if patchErr != nil {
				return ctrl.Result{}, patchErr
			}
			if changed {
				return ctrl.Result{RequeueAfter: delay}, nil
			}
			return ctrl.Result{RequeueAfter: delay}, nil
		}
		if patchErr := r.markBackupFailed(ctx, old, &backup, writeErr.reason, writeErr.message); patchErr != nil {
			return ctrl.Result{}, patchErr
		}
		return ctrl.Result{}, nil
	}
	if !backupRestoreSourceReady(&backup) {
		if patchErr := r.markBackupFailed(
			ctx,
			old,
			&backup,
			snaplanev1alpha1.BackupFailureReasonRestoreSourceInvalid,
			"restore source is not ready",
		); patchErr != nil {
			return ctrl.Result{}, patchErr
		}
		return ctrl.Result{}, nil
	}

	markBackupSucceeded(&backup, metav1.Now())
	changed, patchErr := r.patchBackupStatusIfChanged(ctx, old, &backup)
	if patchErr != nil {
		return ctrl.Result{}, patchErr
	}
	if changed {
		logger.Info("backup marked completed", "snapshot", fmt.Sprintf("%s/%s", snapshotNamespace, snapshot.Name))
	}
	return ctrl.Result{}, nil
}

type backupWriteError struct {
	reason  string
	message string
	err     error

	retryable bool
}

func (e *backupWriteError) Error() string {
	if e == nil {
		return ""
	}
	if e.err == nil {
		return e.message
	}
	return fmt.Sprintf("%s: %v", e.message, e.err)
}

func newBackupWriteError(reason, message string, err error, retryable bool) *backupWriteError {
	return &backupWriteError{
		reason:    reason,
		message:   message,
		err:       err,
		retryable: retryable,
	}
}

func normalizeTimeoutWriteError(err *backupWriteError) *backupWriteError {
	if err == nil {
		return nil
	}
	if errors.Is(err.err, context.DeadlineExceeded) {
		return newBackupWriteError(
			snaplanev1alpha1.BackupFailureReasonWriteTimeout,
			"backup transfer timed out",
			err.err,
			true,
		)
	}
	return err
}

func assignedNodeUnavailableWriteError(nodeName string, err error) *backupWriteError {
	if nodeName == "" || nodeName == unavailableNodeName {
		return newBackupWriteError(
			snaplanev1alpha1.BackupFailureReasonAssignedNodeUnavailable,
			"assigned backup node is unavailable",
			err,
			false,
		)
	}
	return newBackupWriteError(
		snaplanev1alpha1.BackupFailureReasonAssignedNodeUnavailable,
		fmt.Sprintf("writer endpoint for node %q is unavailable", nodeName),
		err,
		false,
	)
}

func (r *BackupReconciler) resolvedWriterFactory() WriterClientFactory {
	if r.WriterClientFactory != nil {
		return r.WriterClientFactory
	}
	return NewGRPCWriterClientFactoryFromEnv()
}

func abortWriteSessionSilently(ctx context.Context, client WriterClient, sessionID, reason string) {
	if client == nil || sessionID == "" {
		return
	}
	_ = client.AbortWrite(ctx, &writerv1.AbortWriteRequest{
		SessionId: sessionID,
		Reason:    reason,
	})
}

func (r *BackupReconciler) executeBackupTransfer(
	ctx context.Context,
	backup *snaplanev1alpha1.Backup,
	snapshot *volumesnapshotv1.VolumeSnapshot,
) *backupWriteError {
	if backup.Spec.Destination.NodeName == "" || backup.Spec.Destination.NodeName == unavailableNodeName {
		return assignedNodeUnavailableWriteError(backup.Spec.Destination.NodeName, nil)
	}

	writerEndpoint, err := r.resolveWriterEndpoint(ctx, backup.Namespace, backup.Spec.Destination.NodeName, time.Now().UTC())
	if err != nil {
		return assignedNodeUnavailableWriteError(backup.Spec.Destination.NodeName, err)
	}

	pvcName, err := r.resolvePolicyPVCName(ctx, backup.Namespace, backup.Spec.PolicyRef.Name)
	if err != nil {
		return newBackupWriteError(
			snaplanev1alpha1.BackupFailureReasonWriteSessionStartFailed,
			"failed to resolve policy PVC name",
			err,
			false,
		)
	}

	client, err := r.resolvedWriterFactory().New(ctx, writerEndpoint)
	if err != nil {
		return newBackupWriteError(
			snaplanev1alpha1.BackupFailureReasonWriterEndpointNotReady,
			fmt.Sprintf("failed to connect writer endpoint %q", writerEndpoint),
			err,
			true,
		)
	}
	defer client.Close()

	startResp, err := client.StartWrite(ctx, &writerv1.StartWriteRequest{
		BackupUid:  string(backup.UID),
		BackupName: backup.Name,
		Namespace:  backup.Namespace,
		PvcName:    pvcName,
	})
	if err != nil {
		return newBackupWriteError(
			snaplanev1alpha1.BackupFailureReasonWriteSessionStartFailed,
			"failed to start write session",
			err,
			true,
		)
	}
	setProgressBytesDone(backup, startResp.GetAcceptedOffset())

	sourceProvider := backupsource.NewProviderFromEnv()
	rangeReq := backupsource.RangeRequest{
		BackupName:   backup.Name,
		BackupUID:    string(backup.UID),
		Namespace:    backup.Namespace,
		PVCName:      pvcName,
		SnapshotName: snapshot.Name,
	}
	if cleanupProvider, ok := sourceProvider.(backupsource.ReaderCleanup); ok {
		defer func() {
			cleanupCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer cancel()
			if cleanupErr := cleanupProvider.Cleanup(cleanupCtx, rangeReq); cleanupErr != nil {
				log.FromContext(ctx).Error(cleanupErr, "cleanup snapshot reader failed", "snapshot", rangeReq.SnapshotName)
			}
		}()
	}
	frameProducer, logicalSize, incrementalBytes, err := buildFrameProducer(ctx, sourceProvider, rangeReq, startResp.GetAcceptedOffset())
	if err != nil {
		abortWriteSessionSilently(ctx, client, startResp.GetSessionId(), "frame plan build failed")
		return newBackupWriteError(
			snaplanev1alpha1.BackupFailureReasonWriteStreamFailed,
			fmt.Sprintf("failed to build write frames: %v", err),
			err,
			!errors.Is(err, errFramePlanInvalid),
		)
	}
	backup.Status.Progress.TotalBytes = logicalSize
	backup.Status.Progress.IncrementalBytes = incrementalBytes

	summary, err := client.WriteFrames(ctx, startResp.GetSessionId(), frameProducer)
	if err != nil {
		abortWriteSessionSilently(ctx, client, startResp.GetSessionId(), "write stream failed")
		return newBackupWriteError(
			snaplanev1alpha1.BackupFailureReasonWriteStreamFailed,
			fmt.Sprintf("failed to stream write frames: %v", err),
			err,
			!errors.Is(err, errFramePlanInvalid),
		)
	}

	backup.Status.Stats.BytesTransferred = summary.GetDataBytes()
	setProgressBytesDone(backup, summary.GetLastAckedOffset())

	commitResp, err := client.CommitWrite(ctx, &writerv1.CommitWriteRequest{SessionId: startResp.GetSessionId()})
	if err != nil {
		abortWriteSessionSilently(ctx, client, startResp.GetSessionId(), "commit failed")
		return newBackupWriteError(
			snaplanev1alpha1.BackupFailureReasonWriteCommitFailed,
			"failed to commit write session",
			err,
			true,
		)
	}

	if err := populateBackupRestoreSource(backup, summary, commitResp, logicalSize); err != nil {
		return newBackupWriteError(
			snaplanev1alpha1.BackupFailureReasonRestoreSourceInvalid,
			"failed to build restore source status",
			err,
			false,
		)
	}
	return nil
}

func buildFrameProducer(
	ctx context.Context,
	provider backupsource.Provider,
	req backupsource.RangeRequest,
	startOffset int64,
) (FrameProducer, int64, int64, error) {
	if provider == nil {
		return nil, 0, 0, fmt.Errorf("%w: backup source provider is nil", errFramePlanInvalid)
	}
	if startOffset < 0 {
		return nil, 0, 0, fmt.Errorf("%w: start offset must be >= 0", errFramePlanInvalid)
	}
	ranges, err := provider.GetChangedRanges(ctx, req)
	if err != nil {
		return nil, 0, 0, err
	}
	sort.Slice(ranges, func(i, j int) bool {
		if ranges[i].Offset == ranges[j].Offset {
			return ranges[i].Length < ranges[j].Length
		}
		return ranges[i].Offset < ranges[j].Offset
	})

	logicalSize := int64(0)
	incrementalBytes := int64(0)
	for i := range ranges {
		if ranges[i].Offset < 0 || ranges[i].Length < 0 {
			return nil, 0, 0, fmt.Errorf("%w: invalid range values", errFramePlanInvalid)
		}
		end, overflowErr := rangeEnd(ranges[i].Offset, ranges[i].Length)
		if overflowErr != nil {
			return nil, 0, 0, fmt.Errorf("%w: %v", errFramePlanInvalid, overflowErr)
		}
		if end > logicalSize {
			logicalSize = end
		}
		if ranges[i].Kind == backupsource.RangeKindData {
			incrementalBytes += ranges[i].Length
		}
	}

	producer := func(send func(frame *writerv1.WriteFrame) error) error {
		cursor := startOffset
		table := crc32.MakeTable(crc32.Castagnoli)

		for i := range ranges {
			rg := ranges[i]
			endOffset, overflowErr := rangeEnd(rg.Offset, rg.Length)
			if overflowErr != nil {
				return fmt.Errorf("%w: %v", errFramePlanInvalid, overflowErr)
			}
			if endOffset <= startOffset || rg.Length == 0 {
				continue
			}
			effectiveOffset := rg.Offset
			effectiveLength := rg.Length
			if effectiveOffset < startOffset {
				effectiveLength -= startOffset - effectiveOffset
				effectiveOffset = startOffset
			}
			if effectiveOffset < cursor {
				effectiveLength -= cursor - effectiveOffset
				effectiveOffset = cursor
			}
			if effectiveLength <= 0 {
				continue
			}

			if effectiveOffset > cursor {
				if err := send(&writerv1.WriteFrame{
					Body: &writerv1.WriteFrame_Zero{Zero: &writerv1.ZeroFrame{
						Offset: cursor,
						Length: effectiveOffset - cursor,
					}},
				}); err != nil {
					return err
				}
				cursor = effectiveOffset
			}

			switch rg.Kind {
			case backupsource.RangeKindData:
				payload, readErr := provider.ReadBlock(ctx, backupsource.ReadRequest{
					RangeRequest: req,
					Offset:       effectiveOffset,
					Length:       effectiveLength,
				})
				if readErr != nil {
					return readErr
				}
				if int64(len(payload)) != effectiveLength {
					return fmt.Errorf(
						"%w: payload length mismatch: expected %d, got %d",
						errFramePlanInvalid,
						effectiveLength,
						len(payload),
					)
				}
				if err := send(&writerv1.WriteFrame{
					Body: &writerv1.WriteFrame_Data{Data: &writerv1.DataFrame{
						Offset:  effectiveOffset,
						Payload: payload,
						Crc32C:  crc32.Checksum(payload, table),
					}},
				}); err != nil {
					return err
				}
			case backupsource.RangeKindZero:
				if err := send(&writerv1.WriteFrame{
					Body: &writerv1.WriteFrame_Zero{Zero: &writerv1.ZeroFrame{
						Offset: effectiveOffset,
						Length: effectiveLength,
					}},
				}); err != nil {
					return err
				}
			default:
				return fmt.Errorf("%w: unsupported range kind %q", errFramePlanInvalid, rg.Kind)
			}
			cursor = effectiveOffset + effectiveLength
		}

		if err := send(&writerv1.WriteFrame{
			Body: &writerv1.WriteFrame_Eof{Eof: &writerv1.EofFrame{LogicalSize: logicalSize}},
		}); err != nil {
			return err
		}
		return nil
	}

	return producer, logicalSize, incrementalBytes, nil
}

func populateBackupRestoreSource(
	backup *snaplanev1alpha1.Backup,
	summary *writerv1.WriteFramesSummary,
	commitResp *writerv1.CommitWriteResponse,
	logicalSize int64,
) error {
	repositoryPath := commitResp.GetRepositoryPath()
	manifestID := commitResp.GetManifestId()
	repoUUID := commitResp.GetRepoUuid()
	format := commitResp.GetRestoreFormat()
	volumeSizeBytes := commitResp.GetVolumeSizeBytes()
	chunkSizeBytes := commitResp.GetChunkSizeBytes()

	if repositoryPath == "" || repositoryPath == "." {
		return fmt.Errorf("repository path is empty")
	}
	if manifestID == "" || manifestID == "." {
		return fmt.Errorf("manifest ID is empty")
	}
	if backup.Spec.Destination.NodeName == "" {
		return fmt.Errorf("destination node name is empty")
	}

	if repoUUID == "" {
		repoUUID = string(backup.UID)
	}
	if format == "" {
		format = snaplanev1alpha1.RestoreSourceFormatMockImageV1
	}
	if volumeSizeBytes == 0 {
		volumeSizeBytes = summary.GetDataBytes() + summary.GetZeroBytes()
	}
	if volumeSizeBytes == 0 {
		volumeSizeBytes = logicalSize
	}
	if volumeSizeBytes < 0 {
		volumeSizeBytes = 0
	}
	if chunkSizeBytes <= 0 {
		chunkSizeBytes = 4096
		if format == snaplanev1alpha1.RestoreSourceFormatCASV1 {
			chunkSizeBytes = casrepo.DefaultChunkSize
		}
	}

	backup.Status.RestoreSource = snaplanev1alpha1.BackupRestoreSourceStatus{
		NodeName:        backup.Spec.Destination.NodeName,
		RepositoryPath:  repositoryPath,
		ManifestID:      manifestID,
		RepoUUID:        repoUUID,
		Format:          format,
		VolumeSizeBytes: volumeSizeBytes,
		ChunkSizeBytes:  chunkSizeBytes,
	}
	return nil
}

func (r *BackupReconciler) resolvePolicyPVCName(ctx context.Context, namespace, policyName string) (string, error) {
	policy, err := r.resolvePolicy(ctx, namespace, policyName)
	if err != nil {
		return "", err
	}
	return policy.Spec.Source.PersistentVolumeClaimName, nil
}

func (r *BackupReconciler) resolvePolicy(ctx context.Context, namespace, policyName string) (*snaplanev1alpha1.BackupPolicy, error) {
	var policy snaplanev1alpha1.BackupPolicy
	if err := r.Get(ctx, types.NamespacedName{Namespace: namespace, Name: policyName}, &policy); err != nil {
		return nil, err
	}
	return &policy, nil
}

func resolveRetryMaxAttempts(backup *snaplanev1alpha1.Backup) int32 {
	if backup.Spec.Transfer.BackoffLimit != nil {
		return *backup.Spec.Transfer.BackoffLimit
	}
	return defaultBackupRetryMaxAttempts
}

func resolveTransferTimeoutSeconds(backup *snaplanev1alpha1.Backup) int64 {
	if backup.Spec.Transfer.TimeoutSeconds != nil {
		return *backup.Spec.Transfer.TimeoutSeconds
	}
	return 0
}

func (r *BackupReconciler) resolveWriterEndpoint(
	ctx context.Context,
	backupNamespace string,
	nodeName string,
	now time.Time,
) (string, error) {
	leaseNS := r.getLeaseNamespace(backupNamespace)
	var lease coordinationv1.Lease
	if err := r.Get(ctx, types.NamespacedName{
		Namespace: leaseNS,
		Name:      writer.LeaseNameForNode(nodeName),
	}, &lease); err != nil {
		return "", err
	}

	if lease.Spec.RenewTime == nil {
		return "", fmt.Errorf("lease renew time is empty")
	}
	if now.Sub(lease.Spec.RenewTime.Time.UTC()) > backupNodeLeaseStaleThreshold {
		return "", fmt.Errorf("lease is stale")
	}

	if lease.Annotations == nil {
		return "", fmt.Errorf("lease annotations are empty")
	}
	endpoint := lease.Annotations[snaplanev1alpha1.WriterEndpointAnnotationKey]
	if endpoint == "" {
		return "", fmt.Errorf("writer endpoint annotation is missing")
	}
	return endpoint, nil
}

func (r *BackupReconciler) getLeaseNamespace(fallbackNamespace string) string {
	if r.LeaseNamespace != "" {
		return r.LeaseNamespace
	}
	if ns := os.Getenv("POD_NAMESPACE"); ns != "" {
		return ns
	}
	if fallbackNamespace != "" {
		return fallbackNamespace
	}
	return metav1.NamespaceDefault
}

func (r *BackupReconciler) markBackupFailed(
	ctx context.Context,
	old *snaplanev1alpha1.Backup,
	backup *snaplanev1alpha1.Backup,
	reason string,
	message string,
) error {
	if backupIsSucceeded(backup) {
		return nil
	}
	now := metav1.Now()
	if backup.Status.StartTime == nil {
		backup.Status.StartTime = &now
	}
	backup.Status.CompletionTime = &now
	backup.Status.LastError = message
	setBackupCondition(backup, snaplanev1alpha1.BackupConditionSucceeded, metav1.ConditionFalse, reason, message, now)
	_, err := r.patchBackupStatusIfChanged(ctx, old, backup)
	return err
}

func markBackupSucceeded(backup *snaplanev1alpha1.Backup, now metav1.Time) {
	if backup.Status.StartTime == nil {
		backup.Status.StartTime = &now
	}
	backup.Status.CompletionTime = &now
	backup.Status.LastError = ""
	backup.Status.Retry = snaplanev1alpha1.BackupRetryStatus{}
	setBackupCondition(
		backup,
		snaplanev1alpha1.BackupConditionSucceeded,
		metav1.ConditionTrue,
		"TransferSucceeded",
		"backup completed successfully",
		now,
	)
}

func (r *BackupReconciler) patchBackupStatusIfChanged(
	ctx context.Context,
	old *snaplanev1alpha1.Backup,
	current *snaplanev1alpha1.Backup,
) (bool, error) {
	if equality.Semantic.DeepEqual(old.Status, current.Status) {
		return false, nil
	}
	if err := r.Status().Patch(ctx, current, client.MergeFrom(old)); err != nil {
		return false, err
	}
	return true, nil
}

func setBackupCondition(
	backup *snaplanev1alpha1.Backup,
	conditionType string,
	status metav1.ConditionStatus,
	reason string,
	message string,
	now metav1.Time,
) {
	meta.SetStatusCondition(&backup.Status.Conditions, metav1.Condition{
		Type:               conditionType,
		Status:             status,
		Reason:             reason,
		Message:            message,
		ObservedGeneration: backup.Generation,
		LastTransitionTime: now,
	})
}

func shouldWaitForRetry(now time.Time, backup *snaplanev1alpha1.Backup) (time.Duration, bool) {
	if backup == nil || backup.Status.Retry.Attempt <= 0 || backup.Status.Retry.LastFailureTime == nil {
		return 0, false
	}
	nextAttempt := backup.Status.Retry.LastFailureTime.Time.Add(computeRetryBackoffDelay(string(backup.UID), backup.Status.Retry.Attempt))
	wait := nextAttempt.Sub(now.UTC())
	if wait <= 0 {
		return 0, false
	}
	return wait, true
}

func recordRetryAttempt(
	backup *snaplanev1alpha1.Backup,
	writeErr *backupWriteError,
	maxAttempts int32,
) (time.Duration, bool) {
	now := metav1.Now()
	attempt := backup.Status.Retry.Attempt + 1
	backup.Status.Retry.Attempt = attempt
	backup.Status.Retry.LastFailureReason = writeErr.reason
	backup.Status.Retry.LastFailureTime = &now
	backup.Status.LastError = writeErr.message
	if attempt > maxAttempts {
		return 0, true
	}

	delay := computeRetryBackoffDelay(string(backup.UID), attempt)
	return delay, false
}

func computeRetryBackoffDelay(backupUID string, attempt int32) time.Duration {
	if attempt <= 0 {
		return backupRetryBaseDelay
	}

	delay := backupRetryBaseDelay
	for i := int32(1); i < attempt; i++ {
		delay *= 2
		if delay >= backupRetryCapDelay {
			delay = backupRetryCapDelay
			break
		}
	}

	if delay > backupRetryCapDelay {
		delay = backupRetryCapDelay
	}

	seed := crc32.ChecksumIEEE([]byte(fmt.Sprintf("%s:%d", backupUID, attempt)))
	unit := float64(seed%10000) / 9999.0
	factor := 1 + ((unit*2)-1)*backupRetryJitterRatio
	jittered := time.Duration(float64(delay) * factor)
	if jittered < time.Millisecond {
		return time.Millisecond
	}
	if jittered > backupRetryCapDelay {
		return backupRetryCapDelay
	}
	return jittered
}

func rangeEnd(offset, length int64) (int64, error) {
	if offset < 0 || length < 0 {
		return 0, fmt.Errorf("offset and length must be >= 0")
	}
	if offset > (1<<63-1)-length {
		return 0, fmt.Errorf("range end overflow")
	}
	return offset + length, nil
}

func setProgressBytesDone(backup *snaplanev1alpha1.Backup, ack int64) {
	if ack < 0 {
		ack = 0
	}
	if total := backup.Status.Progress.TotalBytes; total > 0 && ack > total {
		ack = total
	}
	if ack < backup.Status.Progress.BytesDone {
		ack = backup.Status.Progress.BytesDone
	}
	backup.Status.Progress.BytesDone = ack
}

func (r *BackupReconciler) emitRetryScheduledEvent(backup *snaplanev1alpha1.Backup, writeErr *backupWriteError, delay time.Duration, maxAttempts int32) {
	if r.Recorder == nil {
		return
	}
	r.Recorder.Eventf(
		backup,
		"Warning",
		"RetryScheduled",
		"retry attempt=%d/%d scheduled after=%s reason=%s message=%s",
		backup.Status.Retry.Attempt,
		maxAttempts,
		delay.Round(time.Millisecond),
		writeErr.reason,
		writeErr.message,
	)
}

func (r *BackupReconciler) emitRetryExhaustedEvent(backup *snaplanev1alpha1.Backup, writeErr *backupWriteError, maxAttempts int32) {
	if r.Recorder == nil {
		return
	}
	r.Recorder.Eventf(
		backup,
		"Warning",
		"RetryExhausted",
		"retry exhausted attempt=%d maxAttempts=%d reason=%s message=%s",
		backup.Status.Retry.Attempt,
		maxAttempts,
		writeErr.reason,
		writeErr.message,
	)
}

func (r *BackupReconciler) backupPredicate() predicate.Predicate {
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

// SetupWithManager sets up the controller with the Manager.
func (r *BackupReconciler) SetupWithManager(mgr ctrl.Manager) error {
	if r.Recorder == nil {
		r.Recorder = mgr.GetEventRecorderFor("backup-controller")
	}
	return ctrl.NewControllerManagedBy(mgr).
		For(&snaplanev1alpha1.Backup{}, builder.WithPredicates(r.backupPredicate())).
		Named("backup").
		Complete(r)
}

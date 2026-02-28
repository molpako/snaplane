# CRD Schema Draft (kubebuilder, v1alpha1 Locked)

## Scope

This schema draft covers:
- `BackupPolicy`
- `Backup`

`VolumeSnapshot` は既存 CRD を利用し、queue state は metadata に保持する。

## API Group

- group: `snaplane.molpako.github.io`
- version: `v1alpha1`

## Shared Constants

```go
const (
    BackupStateLabelKey     = "snaplane.molpako.github.io/backup-state"
    QueueTimeAnnotationKey  = "snaplane.molpako.github.io/queueTime"
    BackupNameAnnotationKey = "snaplane.molpako.github.io/backupName"
    PolicyLabelKey          = "snaplane.molpako.github.io/policy"
    BackupTargetNodeLabelKey = "snaplane.molpako.github.io/backup-target"

    AssignedBackupNodeAnnotationKey   = "snaplane.molpako.github.io/assigned-backup-node"
    AssignedBackupNodeAtAnnotationKey = "snaplane.molpako.github.io/assigned-backup-node-at"

    WriterEndpointAnnotationKey      = "snaplane.molpako.github.io/writer-endpoint"
    LeaseUsedBytesAnnotationKey      = "snaplane.molpako.github.io/used-bytes"
    LeaseAvailableBytesAnnotationKey = "snaplane.molpako.github.io/available-bytes"

    ReconcileRequestedAtKey = "reconcile.snaplane.molpako.github.io/requestedAt"
)

type SnapshotQueueState string

const (
    SnapshotQueueStatePending    SnapshotQueueState = "Pending"
    SnapshotQueueStateDispatched SnapshotQueueState = "Dispatched"
    SnapshotQueueStateDone       SnapshotQueueState = "Done"
    SnapshotQueueStateFailed     SnapshotQueueState = "Failed"
)

const (
    BackupFailureReasonAssignedNodeUnavailable = "AssignedNodeUnavailable"
    BackupFailureReasonWriterEndpointNotReady  = "WriterEndpointNotReady"
    BackupFailureReasonWriteSessionStartFailed = "WriteSessionStartFailed"
    BackupFailureReasonWriteStreamFailed       = "WriteStreamFailed"
    BackupFailureReasonWriteCommitFailed       = "WriteCommitFailed"
)
```

## BackupPolicy Types

```go
type SerialDispatchMode string

const (
    SerialDispatchModeOnly SerialDispatchMode = "SerialOnly"
)

type BackupSource struct {
    // +kubebuilder:validation:Required
    // +kubebuilder:validation:MinLength=1
    // +kubebuilder:validation:XValidation:rule="self == oldSelf",message="persistentVolumeClaimName is immutable"
    PersistentVolumeClaimName string `json:"persistentVolumeClaimName"`

    // +kubebuilder:validation:Required
    // +kubebuilder:validation:MinLength=1
    // +kubebuilder:validation:XValidation:rule="self == oldSelf",message="volumeSnapshotClassName is immutable"
    VolumeSnapshotClassName string `json:"volumeSnapshotClassName"`
}

type BackupSchedule struct {
    // +kubebuilder:validation:Required
    // +kubebuilder:validation:MinLength=1
    Cron string `json:"cron"`

    // +optional
    TimeZone *string `json:"timeZone,omitempty"`

    // +optional
    // +kubebuilder:default:=false
    Suspend *bool `json:"suspend,omitempty"`
}

type BackupDispatch struct {
    // +optional
    // +kubebuilder:validation:Enum=SerialOnly
    // +kubebuilder:default:=SerialOnly
    Mode SerialDispatchMode `json:"mode,omitempty"`
}

type BackupRetention struct {
    // +kubebuilder:validation:Required
    // +kubebuilder:validation:Minimum=1
    KeepDays int32 `json:"keepDays"`
}

type TransferPolicy struct {
    // +optional
    // +kubebuilder:validation:Minimum=1
    TimeoutSeconds *int64 `json:"timeoutSeconds,omitempty"`

    // +optional
    // +kubebuilder:validation:Minimum=0
    // +kubebuilder:validation:Maximum=60
    // +kubebuilder:default:=6
    BackoffLimit *int32 `json:"backoffLimit,omitempty"`
}

type BackupPolicySpec struct {
    // +kubebuilder:validation:Required
    Source BackupSource `json:"source"`

    // +kubebuilder:validation:Required
    Schedule BackupSchedule `json:"schedule"`

    // +optional
    Dispatch BackupDispatch `json:"dispatch,omitempty"`

    // +kubebuilder:validation:Required
    Retention BackupRetention `json:"retention"`

    // +optional
    Transfer TransferPolicy `json:"transfer,omitempty"`
}

type BackupPolicyStatus struct {
    Conditions []metav1.Condition `json:"conditions,omitempty"`

    LastScheduledTime *metav1.Time `json:"lastScheduledTime,omitempty"`
    LastDispatchedSnapshot string `json:"lastDispatchedSnapshot,omitempty"`
    ActiveBackupName string `json:"activeBackupName,omitempty"`
    PendingSnapshotCount int32 `json:"pendingSnapshotCount,omitempty"`
    LastRequestedAtSeen string `json:"lastRequestedAtSeen,omitempty"`
}

// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
// +kubebuilder:printcolumn:JSONPath=".spec.schedule.cron",name=Schedule,type=string
// +kubebuilder:printcolumn:JSONPath=".status.activeBackupName",name=ActiveBackup,type=string
// +kubebuilder:printcolumn:JSONPath=".status.pendingSnapshotCount",name=PendingVS,type=integer
// +kubebuilder:printcolumn:JSONPath=".metadata.creationTimestamp",name=Age,type=date
type BackupPolicy struct {
    metav1.TypeMeta   `json:",inline"`
    metav1.ObjectMeta `json:"metadata,omitempty"`

    // +kubebuilder:validation:Required
    Spec BackupPolicySpec `json:"spec"`
    Status BackupPolicyStatus `json:"status,omitempty"`
}
```

## Backup Types

```go
type LocalNameReference struct {
    // +kubebuilder:validation:Required
    // +kubebuilder:validation:MinLength=1
    Name string `json:"name"`
}

type VolumeSnapshotReference struct {
    // +kubebuilder:validation:Required
    // +kubebuilder:validation:MinLength=1
    Name string `json:"name"`

    // +optional
    Namespace *string `json:"namespace,omitempty"`
}

type BackupDestination struct {
    // +kubebuilder:validation:Required
    // +kubebuilder:validation:MinLength=1
    NodeName string `json:"nodeName"`
}

type BackupSpec struct {
    // +kubebuilder:validation:Required
    // +kubebuilder:validation:XValidation:rule="self == oldSelf",message="policyRef is immutable"
    PolicyRef LocalNameReference `json:"policyRef"`

    // +kubebuilder:validation:Required
    // +kubebuilder:validation:XValidation:rule="self == oldSelf",message="volumeSnapshotRef is immutable"
    VolumeSnapshotRef VolumeSnapshotReference `json:"volumeSnapshotRef"`

    // +kubebuilder:validation:Required
    // +kubebuilder:validation:XValidation:rule="self == oldSelf",message="destination is immutable"
    Destination BackupDestination `json:"destination"`

    // +optional
    Transfer TransferPolicy `json:"transfer,omitempty"`
}

type BackupStats struct {
    BytesTransferred int64 `json:"bytesTransferred,omitempty"`
}

type BackupWriterStatus struct {
    SessionID string `json:"sessionID,omitempty"`
    LastAckedOffset int64 `json:"lastAckedOffset,omitempty"`
    TargetPath string `json:"targetPath,omitempty"`
}

type BackupRestoreSourceStatus struct {
    NodeName string `json:"nodeName,omitempty"`
    RepositoryPath string `json:"repositoryPath,omitempty"`
    ManifestID string `json:"manifestID,omitempty"`
    RepoUUID string `json:"repoUUID,omitempty"`
    VolumeSizeBytes int64 `json:"volumeSizeBytes,omitempty"`
    ChunkSizeBytes int64 `json:"chunkSizeBytes,omitempty"`
}

type BackupStatus struct {
    Conditions []metav1.Condition `json:"conditions,omitempty"`

    StartTime *metav1.Time `json:"startTime,omitempty"`
    CompletionTime *metav1.Time `json:"completionTime,omitempty"`
    LastError string `json:"lastError,omitempty"`
    Stats BackupStats `json:"stats,omitempty"`
    Writer BackupWriterStatus `json:"writer,omitempty"`
    RestoreSource BackupRestoreSourceStatus `json:"restoreSource,omitempty"`
}

// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
// +kubebuilder:printcolumn:JSONPath=".spec.volumeSnapshotRef.name",name=Snapshot,type=string
// +kubebuilder:printcolumn:JSONPath=".metadata.creationTimestamp",name=Age,type=date
type Backup struct {
    metav1.TypeMeta   `json:",inline"`
    metav1.ObjectMeta `json:"metadata,omitempty"`

    // +kubebuilder:validation:Required
    Spec BackupSpec `json:"spec"`
    Status BackupStatus `json:"status,omitempty"`
}
```

## Status and Conditions Guidance

condition vocabulary は固定する。
- `BackupPolicy`: `Ready`, `QueueHealthy`, `SerialGuardSatisfied`
- `Backup`: `Accepted`, `Running`, `Completed`, `Failed`

運用ルール:
- status 更新は差分がある時だけ行う
- retry は自動で行わず、手動 `Backup` 作成でのみ再投入する
- restore populator が受理できる `Backup` は `Completed=True` かつ `restoreSource.repositoryPath/manifestID` が揃っているものに限定する
- oldest non-`Done` snapshot が `Failed` の間は
  `SerialGuardSatisfied=False`, reason `BlockedByFailedSnapshot` を使う

## Notes

- queue ownership は `BackupPolicy` controller に固定（single writer）。
- `Pending -> Dispatched` は CAS（`resourceVersion`）でのみ遷移。
- metadata 更新は `Patch`、status は `Status().Patch` or `Status().Update` を使う。
- `BackupPolicy` controller は secondary watch として `VolumeSnapshot` を監視する。
- restore の入口は PVC `dataSourceRef -> Backup` + `VolumePopulator(sourceKind=Backup)` を使う。

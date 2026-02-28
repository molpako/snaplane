package v1alpha1

type SnapshotQueueState string

const (
	BackupStateLabelKey      = "snaplane.molpako.github.io/backup-state"
	QueueTimeAnnotationKey   = "snaplane.molpako.github.io/queueTime"
	BackupNameAnnotationKey  = "snaplane.molpako.github.io/backupName"
	PolicyLabelKey           = "snaplane.molpako.github.io/policy"
	BackupTargetNodeLabelKey = "snaplane.molpako.github.io/backup-target"

	AssignedBackupNodeAnnotationKey   = "snaplane.molpako.github.io/assigned-backup-node"
	AssignedBackupNodeAtAnnotationKey = "snaplane.molpako.github.io/assigned-backup-node-at"

	WriterEndpointAnnotationKey      = "snaplane.molpako.github.io/writer-endpoint"
	LeaseUsedBytesAnnotationKey      = "snaplane.molpako.github.io/used-bytes"
	LeaseAvailableBytesAnnotationKey = "snaplane.molpako.github.io/available-bytes"

	SnapshotQueueStatePending    SnapshotQueueState = "Pending"
	SnapshotQueueStateDispatched SnapshotQueueState = "Dispatched"
	SnapshotQueueStateDone       SnapshotQueueState = "Done"
	SnapshotQueueStateFailed     SnapshotQueueState = "Failed"
)

const (
	BackupPolicyConditionReady                = "Ready"
	BackupPolicyConditionQueueHealthy         = "QueueHealthy"
	BackupPolicyConditionSerialGuardSatisfied = "SerialGuardSatisfied"
)

const (
	BackupConditionSucceeded = "Succeeded"
)

const (
	BackupFailureReasonAssignedNodeUnavailable = "AssignedNodeUnavailable"
	BackupFailureReasonWriterEndpointNotReady  = "WriterEndpointNotReady"
	BackupFailureReasonWriteSessionStartFailed = "WriteSessionStartFailed"
	BackupFailureReasonWriteStreamFailed       = "WriteStreamFailed"
	BackupFailureReasonWriteCommitFailed       = "WriteCommitFailed"
	BackupFailureReasonWriteTimeout            = "WriteTimeout"
	BackupFailureReasonRestoreSourceInvalid    = "RestoreSourceInvalid"
)

const (
	RestoreSourceFormatMockImageV1 = "mock-image-v1"
	RestoreSourceFormatCASV1       = "cas-v1"
)

var (
	RestorePopulatorPrefix       = GroupVersion.Group
	RestorePopulatorFinalizerKey = RestorePopulatorPrefix + "/populate-target-protection"
)

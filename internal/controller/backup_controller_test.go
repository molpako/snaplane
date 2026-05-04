package controller

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	coordinationv1 "k8s.io/api/coordination/v1"
	meta "k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/event"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	volumesnapshotv1 "github.com/kubernetes-csi/external-snapshotter/client/v8/apis/volumesnapshot/v1"
	writerv1 "github.com/molpako/snaplane/api/proto/writer/v1"
	snaplanev1alpha1 "github.com/molpako/snaplane/api/v1alpha1"
	"github.com/molpako/snaplane/internal/backupsource"
	"github.com/molpako/snaplane/internal/writer"
)

func createBackupWithSnapshotRef(name, snapshotName string) *snaplanev1alpha1.Backup {
	return &snaplanev1alpha1.Backup{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: "default",
		},
		Spec: snaplanev1alpha1.BackupSpec{
			PolicyRef: snaplanev1alpha1.LocalNameReference{Name: "policy-a"},
			VolumeSnapshotRef: snaplanev1alpha1.VolumeSnapshotReference{
				Name: snapshotName,
			},
			Destination: snaplanev1alpha1.BackupDestination{
				NodeName: "node-a",
			},
		},
	}
}

func createSnapshotForBackup(name string) {
	className := "csi-rbd-snapclass"
	pvcName := "pvc-a"
	snapshot := &volumesnapshotv1.VolumeSnapshot{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: "default",
		},
		Spec: volumesnapshotv1.VolumeSnapshotSpec{
			VolumeSnapshotClassName: &className,
			Source: volumesnapshotv1.VolumeSnapshotSource{
				PersistentVolumeClaimName: &pvcName,
			},
		},
	}
	Expect(k8sClient.Create(context.Background(), snapshot)).To(Succeed())
}

func createPolicyForBackup() {
	policy := &snaplanev1alpha1.BackupPolicy{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "policy-a",
			Namespace: "default",
		},
		Spec: snaplanev1alpha1.BackupPolicySpec{
			Source: snaplanev1alpha1.BackupSource{
				PersistentVolumeClaimName: "pvc-a",
				VolumeSnapshotClassName:   "csi-rbd-snapclass",
			},
			Schedule: snaplanev1alpha1.BackupSchedule{
				Cron: "* * * * *",
			},
			Retention: snaplanev1alpha1.BackupRetention{KeepDays: 1},
		},
	}
	_ = k8sClient.Create(context.Background(), policy)
}

func createLeaseForBackupNode(nodeName string) {
	now := metav1.MicroTime{Time: time.Now().UTC()}
	lease := &coordinationv1.Lease{
		ObjectMeta: metav1.ObjectMeta{
			Name:      writer.LeaseNameForNode(nodeName),
			Namespace: "default",
			Annotations: map[string]string{
				snaplanev1alpha1.WriterEndpointAnnotationKey:      "127.0.0.1:9443",
				snaplanev1alpha1.LeaseUsedBytesAnnotationKey:      "1024",
				snaplanev1alpha1.LeaseAvailableBytesAnnotationKey: "21474836480",
			},
		},
		Spec: coordinationv1.LeaseSpec{
			RenewTime: &now,
		},
	}
	_ = k8sClient.Create(context.Background(), lease)
}

type fakeWriterFactory struct{}

func (f *fakeWriterFactory) New(_ context.Context, _ string) (WriterClient, error) {
	return &fakeWriterClient{}, nil
}

type fakeWriterClient struct{}

func (c *fakeWriterClient) StartWrite(_ context.Context, req *writerv1.StartWriteRequest) (*writerv1.StartWriteResponse, error) {
	return &writerv1.StartWriteResponse{
		SessionId:      req.GetBackupUid(),
		AcceptedOffset: 0,
		TargetPath:     "/var/backup/default/pvc-a/" + req.GetBackupName() + "/mock.img",
	}, nil
}

func (c *fakeWriterClient) WriteFrames(_ context.Context, _ string, producer FrameProducer) (*writerv1.WriteFramesSummary, error) {
	if producer == nil {
		return nil, errors.New("producer is nil")
	}
	var dataBytes int64
	var zeroBytes int64
	var lastAcked int64
	if err := producer(func(frame *writerv1.WriteFrame) error {
		switch body := frame.Body.(type) {
		case *writerv1.WriteFrame_Data:
			size := int64(len(body.Data.GetPayload()))
			dataBytes += size
			lastAcked = body.Data.GetOffset() + size
		case *writerv1.WriteFrame_Zero:
			zeroBytes += body.Zero.GetLength()
			lastAcked = body.Zero.GetOffset() + body.Zero.GetLength()
		case *writerv1.WriteFrame_Eof:
			lastAcked = body.Eof.GetLogicalSize()
		}
		return nil
	}); err != nil {
		return nil, err
	}
	return &writerv1.WriteFramesSummary{
		LastAckedOffset: lastAcked,
		DataBytes:       dataBytes,
		ZeroBytes:       zeroBytes,
	}, nil
}

func (c *fakeWriterClient) CommitWrite(_ context.Context, _ *writerv1.CommitWriteRequest) (*writerv1.CommitWriteResponse, error) {
	return &writerv1.CommitWriteResponse{
		TargetPath:      "/var/backup/default/pvc-a/backup/mock.img",
		RepositoryPath:  "/var/backup/default/pvc-a/backup",
		ManifestId:      "mock.img",
		RepoUuid:        "repo-mock-1",
		VolumeSizeBytes: 4105,
		ChunkSizeBytes:  4096,
		RestoreFormat:   snaplanev1alpha1.RestoreSourceFormatMockImageV1,
	}, nil
}

func (c *fakeWriterClient) AbortWrite(_ context.Context, _ *writerv1.AbortWriteRequest) error {
	return nil
}

func (c *fakeWriterClient) Close() error {
	return nil
}

type scriptedWriterFactory struct {
	newErr error
	client *scriptedWriterClient
}

func (f *scriptedWriterFactory) New(_ context.Context, _ string) (WriterClient, error) {
	if f.newErr != nil {
		return nil, f.newErr
	}
	if f.client == nil {
		f.client = &scriptedWriterClient{}
	}
	return f.client, nil
}

type scriptedWriterClient struct {
	startErr         error
	streamErr        error
	streamDelay      time.Duration
	commitErr        error
	startAccepted    int64
	startTargetPath  *string
	commitTargetPath *string
	commitResp       *writerv1.CommitWriteResponse
	streamCalls      int
	failStreamCalls  int
	seenFrameOffsets []int64
}

func (c *scriptedWriterClient) StartWrite(_ context.Context, req *writerv1.StartWriteRequest) (*writerv1.StartWriteResponse, error) {
	if c.startErr != nil {
		return nil, c.startErr
	}
	targetPath := "/var/backup/default/pvc-a/" + req.GetBackupName() + "/mock.img"
	if c.startTargetPath != nil {
		targetPath = *c.startTargetPath
	}
	return &writerv1.StartWriteResponse{
		SessionId:      req.GetBackupUid(),
		AcceptedOffset: c.startAccepted,
		TargetPath:     targetPath,
	}, nil
}

func (c *scriptedWriterClient) WriteFrames(ctx context.Context, _ string, producer FrameProducer) (*writerv1.WriteFramesSummary, error) {
	return c.writeFrames(ctx, producer)
}

func (c *scriptedWriterClient) writeFrames(ctx context.Context, producer FrameProducer) (*writerv1.WriteFramesSummary, error) {
	if c.streamDelay > 0 {
		select {
		case <-time.After(c.streamDelay):
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
	c.streamCalls++
	if c.failStreamCalls > 0 && c.streamCalls <= c.failStreamCalls {
		return nil, errors.New("scripted transient stream failure")
	}
	if c.streamErr != nil {
		return nil, c.streamErr
	}
	if producer == nil {
		return nil, errors.New("producer is nil")
	}
	var dataBytes int64
	var zeroBytes int64
	var lastAcked int64
	if err := producer(func(frame *writerv1.WriteFrame) error {
		switch body := frame.Body.(type) {
		case *writerv1.WriteFrame_Data:
			size := int64(len(body.Data.GetPayload()))
			dataBytes += size
			lastAcked = body.Data.GetOffset() + size
			c.seenFrameOffsets = append(c.seenFrameOffsets, body.Data.GetOffset())
		case *writerv1.WriteFrame_Zero:
			zeroBytes += body.Zero.GetLength()
			lastAcked = body.Zero.GetOffset() + body.Zero.GetLength()
			c.seenFrameOffsets = append(c.seenFrameOffsets, body.Zero.GetOffset())
		case *writerv1.WriteFrame_Eof:
			lastAcked = body.Eof.GetLogicalSize()
		}
		return nil
	}); err != nil {
		return nil, err
	}
	return &writerv1.WriteFramesSummary{
		LastAckedOffset: lastAcked,
		DataBytes:       dataBytes,
		ZeroBytes:       zeroBytes,
	}, nil
}

func (c *scriptedWriterClient) CommitWrite(_ context.Context, _ *writerv1.CommitWriteRequest) (*writerv1.CommitWriteResponse, error) {
	if c.commitErr != nil {
		return nil, c.commitErr
	}
	if c.commitResp != nil {
		return c.commitResp, nil
	}
	targetPath := "/var/backup/default/pvc-a/backup/mock.img"
	if c.commitTargetPath != nil {
		targetPath = *c.commitTargetPath
	}
	resp := &writerv1.CommitWriteResponse{
		TargetPath:      targetPath,
		RepoUuid:        "repo-mock-scripted",
		VolumeSizeBytes: 4105,
		ChunkSizeBytes:  4096,
		RestoreFormat:   snaplanev1alpha1.RestoreSourceFormatMockImageV1,
	}
	if targetPath != "" {
		resp.RepositoryPath = filepath.Dir(targetPath)
		resp.ManifestId = filepath.Base(targetPath)
	}
	return resp, nil
}

func (c *scriptedWriterClient) AbortWrite(_ context.Context, _ *writerv1.AbortWriteRequest) error {
	return nil
}

func (c *scriptedWriterClient) Close() error {
	return nil
}

type overlapRangeProvider struct{}

func (p *overlapRangeProvider) GetChangedRanges(_ context.Context, _ backupsource.RangeRequest) ([]backupsource.Range, error) {
	return []backupsource.Range{
		{Kind: backupsource.RangeKindData, Offset: 0, Length: 8},
		{Kind: backupsource.RangeKindZero, Offset: 6, Length: 4},
		{Kind: backupsource.RangeKindData, Offset: 10, Length: 2},
	}, nil
}

func (p *overlapRangeProvider) ReadBlock(_ context.Context, req backupsource.ReadRequest) ([]byte, error) {
	out := make([]byte, req.Length)
	for i := range out {
		out[i] = byte((req.Offset + int64(i)) % 251)
	}
	return out, nil
}

func reconcileBackupToTerminal(ctx context.Context, reconciler *BackupReconciler, name string) {
	req := reconcile.Request{NamespacedName: types.NamespacedName{Name: name, Namespace: "default"}}
	for i := 0; i < 10; i++ {
		result, err := reconciler.Reconcile(ctx, req)
		Expect(err).NotTo(HaveOccurred())

		var refreshed snaplanev1alpha1.Backup
		Expect(k8sClient.Get(ctx, req.NamespacedName, &refreshed)).To(Succeed())
		if backupIsTerminal(&refreshed) {
			return
		}
		if result.RequeueAfter > 0 {
			time.Sleep(result.RequeueAfter)
		}
	}
	Fail("backup did not reach terminal state")
}

func expectBackupFailedReason(ctx context.Context, backupName string, reason string) {
	var refreshed snaplanev1alpha1.Backup
	Expect(k8sClient.Get(ctx, types.NamespacedName{Name: backupName, Namespace: "default"}, &refreshed)).To(Succeed())
	Expect(backupIsFailed(&refreshed)).To(BeTrue())
	succeeded := meta.FindStatusCondition(refreshed.Status.Conditions, snaplanev1alpha1.BackupConditionSucceeded)
	Expect(succeeded).NotTo(BeNil())
	Expect(succeeded.Status).To(Equal(metav1.ConditionFalse))
	Expect(succeeded.Reason).To(Equal(reason))
}

func seedBackupExecutionPrerequisites(snapshotName string, withLease bool) {
	createPolicyForBackup()
	if withLease {
		createLeaseForBackupNode("node-a")
	}
	createSnapshotForBackup(snapshotName)
}

var _ = Describe("Backup Controller", func() {
	var (
		ctx        context.Context
		reconciler *BackupReconciler
	)

	BeforeEach(func() {
		ctx = context.Background()
		reconciler = &BackupReconciler{
			Client:              k8sClient,
			Scheme:              k8sClient.Scheme(),
			WriterClientFactory: &fakeWriterFactory{},
			LeaseNamespace:      "default",
		}
	})

	It("marks backup failed when snapshot is missing", func() {
		name := fmt.Sprintf("backup-missing-%d", time.Now().UnixNano())
		backup := createBackupWithSnapshotRef(name, "missing-snapshot")
		Expect(k8sClient.Create(ctx, backup)).To(Succeed())

		_, err := reconciler.Reconcile(ctx, reconcile.Request{NamespacedName: types.NamespacedName{Name: name, Namespace: "default"}})
		Expect(err).NotTo(HaveOccurred())

		var refreshed snaplanev1alpha1.Backup
		Expect(k8sClient.Get(ctx, types.NamespacedName{Name: name, Namespace: "default"}, &refreshed)).To(Succeed())
		Expect(backupIsFailed(&refreshed)).To(BeTrue())
		Expect(refreshed.Status.LastError).To(ContainSubstring("not found"))
		Expect(refreshed.Status.CompletionTime).NotTo(BeNil())
	})

	It("marks backup succeeded with restore metadata and progress", func() {
		snapshotName := fmt.Sprintf("snapshot-%d", time.Now().UnixNano())
		backupName := fmt.Sprintf("backup-%d", time.Now().UnixNano())
		seedBackupExecutionPrerequisites(snapshotName, true)

		backup := createBackupWithSnapshotRef(backupName, snapshotName)
		Expect(k8sClient.Create(ctx, backup)).To(Succeed())

		reconcileBackupToTerminal(ctx, reconciler, backupName)

		var refreshed snaplanev1alpha1.Backup
		Expect(k8sClient.Get(ctx, types.NamespacedName{Name: backupName, Namespace: "default"}, &refreshed)).To(Succeed())
		Expect(backupIsSucceeded(&refreshed)).To(BeTrue())
		Expect(refreshed.Status.StartTime).NotTo(BeNil())
		Expect(refreshed.Status.CompletionTime).NotTo(BeNil())
		Expect(refreshed.Status.RestoreSource.NodeName).To(Equal("node-a"))
		Expect(refreshed.Status.RestoreSource.RepositoryPath).To(Equal("/var/backup/default/pvc-a/backup"))
		Expect(refreshed.Status.RestoreSource.ManifestID).To(Equal("mock.img"))
		Expect(refreshed.Status.RestoreSource.RepoUUID).NotTo(BeEmpty())
		Expect(refreshed.Status.RestoreSource.Format).To(Equal(snaplanev1alpha1.RestoreSourceFormatMockImageV1))
		Expect(refreshed.Status.Progress.TotalBytes).To(BeNumerically(">", 0))
		Expect(refreshed.Status.Progress.BytesDone).To(BeNumerically(">", 0))
		succeeded := meta.FindStatusCondition(refreshed.Status.Conditions, snaplanev1alpha1.BackupConditionSucceeded)
		Expect(succeeded).NotTo(BeNil())
		Expect(succeeded.Status).To(Equal(metav1.ConditionTrue))
		Expect(refreshed.Status.Progress.IncrementalBytes).To(BeNumerically(">", 0))
	})

	It("accepts structured CAS restore source metadata", func() {
		snapshotName := fmt.Sprintf("snapshot-cas-%d", time.Now().UnixNano())
		backupName := fmt.Sprintf("backup-cas-%d", time.Now().UnixNano())
		seedBackupExecutionPrerequisites(snapshotName, true)

		reconciler.WriterClientFactory = &scriptedWriterFactory{
			client: &scriptedWriterClient{
				commitResp: &writerv1.CommitWriteResponse{
					TargetPath:      "/var/backup/default/pvc-a/repo/manifest-cas",
					RepositoryPath:  "/var/backup/default/pvc-a/repo",
					ManifestId:      "manifest-cas",
					ManifestChain:   []string{"base-cas", "manifest-cas"},
					RepoUuid:        "repo-cas-1",
					VolumeSizeBytes: 8388608,
					ChunkSizeBytes:  4194304,
					RestoreFormat:   snaplanev1alpha1.RestoreSourceFormatCASV1,
				},
			},
		}

		backup := createBackupWithSnapshotRef(backupName, snapshotName)
		Expect(k8sClient.Create(ctx, backup)).To(Succeed())

		reconcileBackupToTerminal(ctx, reconciler, backupName)

		var refreshed snaplanev1alpha1.Backup
		Expect(k8sClient.Get(ctx, types.NamespacedName{Name: backupName, Namespace: "default"}, &refreshed)).To(Succeed())
		Expect(backupIsSucceeded(&refreshed)).To(BeTrue())
		Expect(refreshed.Status.RestoreSource.Format).To(Equal(snaplanev1alpha1.RestoreSourceFormatCASV1))
		Expect(refreshed.Status.RestoreSource.RepositoryPath).To(Equal("/var/backup/default/pvc-a/repo"))
		Expect(refreshed.Status.RestoreSource.ManifestID).To(Equal("manifest-cas"))
		Expect(refreshed.Status.RestoreSource.ManifestChain).To(Equal([]string{"base-cas", "manifest-cas"}))
		Expect(refreshed.Status.RestoreSource.ChunkSizeBytes).To(Equal(int64(4194304)))
	})

	It("keeps terminal backup idempotent on further reconcile", func() {
		snapshotName := fmt.Sprintf("snapshot-term-%d", time.Now().UnixNano())
		backupName := fmt.Sprintf("backup-term-%d", time.Now().UnixNano())
		seedBackupExecutionPrerequisites(snapshotName, true)

		backup := createBackupWithSnapshotRef(backupName, snapshotName)
		Expect(k8sClient.Create(ctx, backup)).To(Succeed())

		req := reconcile.Request{NamespacedName: types.NamespacedName{Name: backupName, Namespace: "default"}}
		reconcileBackupToTerminal(ctx, reconciler, backupName)

		var completed snaplanev1alpha1.Backup
		Expect(k8sClient.Get(ctx, req.NamespacedName, &completed)).To(Succeed())
		Expect(backupIsSucceeded(&completed)).To(BeTrue())
		completion := completed.Status.CompletionTime.DeepCopy()

		_, err := reconciler.Reconcile(ctx, req)
		Expect(err).NotTo(HaveOccurred())

		var after snaplanev1alpha1.Backup
		Expect(k8sClient.Get(ctx, req.NamespacedName, &after)).To(Succeed())
		Expect(backupIsSucceeded(&after)).To(BeTrue())
		Expect(after.Status.CompletionTime).NotTo(BeNil())
		Expect(after.Status.CompletionTime.Time).To(Equal(completion.Time))
	})

	It("fails when assigned node is unavailable", func() {
		snapshotName := fmt.Sprintf("snapshot-unavailable-%d", time.Now().UnixNano())
		backupName := fmt.Sprintf("backup-unavailable-%d", time.Now().UnixNano())
		seedBackupExecutionPrerequisites(snapshotName, false)

		backup := createBackupWithSnapshotRef(backupName, snapshotName)
		backup.Spec.Destination.NodeName = unavailableNodeName
		Expect(k8sClient.Create(ctx, backup)).To(Succeed())

		reconcileBackupToTerminal(ctx, reconciler, backupName)
		expectBackupFailedReason(ctx, backupName, snaplanev1alpha1.BackupFailureReasonAssignedNodeUnavailable)
	})

	It("fails with WriterEndpointNotReady when writer client factory cannot connect", func() {
		snapshotName := fmt.Sprintf("snapshot-endpoint-%d", time.Now().UnixNano())
		backupName := fmt.Sprintf("backup-endpoint-%d", time.Now().UnixNano())
		seedBackupExecutionPrerequisites(snapshotName, true)

		reconciler.WriterClientFactory = &scriptedWriterFactory{
			newErr: errors.New("dial failed"),
		}

		backup := createBackupWithSnapshotRef(backupName, snapshotName)
		backup.Spec.Transfer.BackoffLimit = ptr.To(int32(0))
		Expect(k8sClient.Create(ctx, backup)).To(Succeed())

		reconcileBackupToTerminal(ctx, reconciler, backupName)
		expectBackupFailedReason(ctx, backupName, snaplanev1alpha1.BackupFailureReasonWriterEndpointNotReady)
	})

	It("fails with WriteSessionStartFailed when StartWrite returns error", func() {
		snapshotName := fmt.Sprintf("snapshot-start-%d", time.Now().UnixNano())
		backupName := fmt.Sprintf("backup-start-%d", time.Now().UnixNano())
		seedBackupExecutionPrerequisites(snapshotName, true)

		reconciler.WriterClientFactory = &scriptedWriterFactory{
			client: &scriptedWriterClient{startErr: errors.New("start failed")},
		}

		backup := createBackupWithSnapshotRef(backupName, snapshotName)
		backup.Spec.Transfer.BackoffLimit = ptr.To(int32(0))
		Expect(k8sClient.Create(ctx, backup)).To(Succeed())

		reconcileBackupToTerminal(ctx, reconciler, backupName)
		expectBackupFailedReason(ctx, backupName, snaplanev1alpha1.BackupFailureReasonWriteSessionStartFailed)
	})

	It("fails with WriteStreamFailed when WriteFrames returns error", func() {
		snapshotName := fmt.Sprintf("snapshot-stream-%d", time.Now().UnixNano())
		backupName := fmt.Sprintf("backup-stream-%d", time.Now().UnixNano())
		seedBackupExecutionPrerequisites(snapshotName, true)

		reconciler.WriterClientFactory = &scriptedWriterFactory{
			client: &scriptedWriterClient{streamErr: errors.New("stream failed")},
		}

		backup := createBackupWithSnapshotRef(backupName, snapshotName)
		backup.Spec.Transfer.BackoffLimit = ptr.To(int32(0))
		Expect(k8sClient.Create(ctx, backup)).To(Succeed())

		reconcileBackupToTerminal(ctx, reconciler, backupName)
		expectBackupFailedReason(ctx, backupName, snaplanev1alpha1.BackupFailureReasonWriteStreamFailed)
	})

	It("fails with WriteCommitFailed when CommitWrite returns error", func() {
		snapshotName := fmt.Sprintf("snapshot-commit-%d", time.Now().UnixNano())
		backupName := fmt.Sprintf("backup-commit-%d", time.Now().UnixNano())
		seedBackupExecutionPrerequisites(snapshotName, true)

		reconciler.WriterClientFactory = &scriptedWriterFactory{
			client: &scriptedWriterClient{commitErr: errors.New("commit failed")},
		}

		backup := createBackupWithSnapshotRef(backupName, snapshotName)
		backup.Spec.Transfer.BackoffLimit = ptr.To(int32(0))
		Expect(k8sClient.Create(ctx, backup)).To(Succeed())

		reconcileBackupToTerminal(ctx, reconciler, backupName)
		expectBackupFailedReason(ctx, backupName, snaplanev1alpha1.BackupFailureReasonWriteCommitFailed)
	})

	It("fails with RestoreSourceInvalid when writer target path is empty", func() {
		snapshotName := fmt.Sprintf("snapshot-restore-source-%d", time.Now().UnixNano())
		backupName := fmt.Sprintf("backup-restore-source-%d", time.Now().UnixNano())
		seedBackupExecutionPrerequisites(snapshotName, true)

		empty := ""
		reconciler.WriterClientFactory = &scriptedWriterFactory{
			client: &scriptedWriterClient{
				startTargetPath:  &empty,
				commitTargetPath: &empty,
			},
		}

		backup := createBackupWithSnapshotRef(backupName, snapshotName)
		Expect(k8sClient.Create(ctx, backup)).To(Succeed())

		reconcileBackupToTerminal(ctx, reconciler, backupName)
		expectBackupFailedReason(ctx, backupName, snaplanev1alpha1.BackupFailureReasonRestoreSourceInvalid)
	})

	It("keeps backup running and schedules retry on retryable error", func() {
		snapshotName := fmt.Sprintf("snapshot-retry-%d", time.Now().UnixNano())
		backupName := fmt.Sprintf("backup-retry-%d", time.Now().UnixNano())
		seedBackupExecutionPrerequisites(snapshotName, true)

		reconciler.WriterClientFactory = &scriptedWriterFactory{
			client: &scriptedWriterClient{streamErr: errors.New("stream temporary error")},
		}

		backup := createBackupWithSnapshotRef(backupName, snapshotName)
		Expect(k8sClient.Create(ctx, backup)).To(Succeed())

		req := reconcile.Request{NamespacedName: types.NamespacedName{Name: backupName, Namespace: "default"}}
		_, err := reconciler.Reconcile(ctx, req)
		Expect(err).NotTo(HaveOccurred())
		_, err = reconciler.Reconcile(ctx, req)
		Expect(err).NotTo(HaveOccurred())
		_, err = reconciler.Reconcile(ctx, req)
		Expect(err).NotTo(HaveOccurred())

		var refreshed snaplanev1alpha1.Backup
		Expect(k8sClient.Get(ctx, req.NamespacedName, &refreshed)).To(Succeed())
		succeeded := meta.FindStatusCondition(refreshed.Status.Conditions, snaplanev1alpha1.BackupConditionSucceeded)
		Expect(succeeded).NotTo(BeNil())
		Expect(succeeded.Status).To(Equal(metav1.ConditionUnknown))
		Expect(succeeded.Reason).To(Equal("WaitingForRetry"))
		Expect(backupIsFailed(&refreshed)).To(BeFalse())
		Expect(refreshed.Status.Retry.Attempt).To(Equal(int32(1)))
		Expect(refreshed.Status.Retry.LastFailureTime).NotTo(BeNil())
		Expect(refreshed.Status.Retry.LastFailureReason).To(Equal(snaplanev1alpha1.BackupFailureReasonWriteStreamFailed))
		Expect(refreshed.Status.LastError).To(ContainSubstring("failed to stream write frames"))
	})

	It("fails after retry exhaustion for retryable error", func() {
		snapshotName := fmt.Sprintf("snapshot-retry-exhaust-%d", time.Now().UnixNano())
		backupName := fmt.Sprintf("backup-retry-exhaust-%d", time.Now().UnixNano())
		seedBackupExecutionPrerequisites(snapshotName, true)

		reconciler.WriterClientFactory = &scriptedWriterFactory{
			client: &scriptedWriterClient{streamErr: errors.New("stream temporary error")},
		}

		backup := createBackupWithSnapshotRef(backupName, snapshotName)
		backup.Spec.Transfer.BackoffLimit = ptr.To(int32(1))
		Expect(k8sClient.Create(ctx, backup)).To(Succeed())

		req := reconcile.Request{NamespacedName: types.NamespacedName{Name: backupName, Namespace: "default"}}
		for i := 0; i < 8; i++ {
			_, err := reconciler.Reconcile(ctx, req)
			Expect(err).NotTo(HaveOccurred())
			time.Sleep(1200 * time.Millisecond)
		}

		expectBackupFailedReason(ctx, backupName, snaplanev1alpha1.BackupFailureReasonWriteStreamFailed)
	})

	It("resumes from accepted offset and skips earlier frames", func() {
		snapshotName := fmt.Sprintf("snapshot-resume-%d", time.Now().UnixNano())
		backupName := fmt.Sprintf("backup-resume-%d", time.Now().UnixNano())
		seedBackupExecutionPrerequisites(snapshotName, true)

		client := &scriptedWriterClient{
			startAccepted: 5,
		}
		reconciler.WriterClientFactory = &scriptedWriterFactory{client: client}

		backup := createBackupWithSnapshotRef(backupName, snapshotName)
		Expect(k8sClient.Create(ctx, backup)).To(Succeed())

		reconcileBackupToTerminal(ctx, reconciler, backupName)

		Expect(client.seenFrameOffsets).NotTo(BeEmpty())
		minOffset := client.seenFrameOffsets[0]
		for _, off := range client.seenFrameOffsets {
			if off < minOffset {
				minOffset = off
			}
		}
		Expect(minOffset).To(BeNumerically(">=", 5))

		var refreshed snaplanev1alpha1.Backup
		Expect(k8sClient.Get(ctx, types.NamespacedName{Name: backupName, Namespace: "default"}, &refreshed)).To(Succeed())
		Expect(backupIsSucceeded(&refreshed)).To(BeTrue())
		Expect(refreshed.Status.Progress.BytesDone).To(BeNumerically(">=", 5))
		Expect(refreshed.Status.Retry.Attempt).To(Equal(int32(0)))
		Expect(refreshed.Status.Retry.LastFailureTime).To(BeNil())
	})

	It("fails with WriteTimeout when transfer exceeds timeoutSeconds", func() {
		snapshotName := fmt.Sprintf("snapshot-timeout-%d", time.Now().UnixNano())
		backupName := fmt.Sprintf("backup-timeout-%d", time.Now().UnixNano())
		seedBackupExecutionPrerequisites(snapshotName, true)

		reconciler.WriterClientFactory = &scriptedWriterFactory{
			client: &scriptedWriterClient{streamDelay: 2 * time.Second},
		}

		backup := createBackupWithSnapshotRef(backupName, snapshotName)
		backup.Spec.Transfer.TimeoutSeconds = func(v int64) *int64 { return &v }(1)
		backup.Spec.Transfer.BackoffLimit = ptr.To(int32(0))
		Expect(k8sClient.Create(ctx, backup)).To(Succeed())

		reconcileBackupToTerminal(ctx, reconciler, backupName)
		expectBackupFailedReason(ctx, backupName, snaplanev1alpha1.BackupFailureReasonWriteTimeout)
	})

	It("normalizes overlapping ranges and keeps frame offsets contiguous", func() {
		provider := &overlapRangeProvider{}
		producer, _, _, err := buildFrameProducer(context.Background(), provider, backupsource.RangeRequest{}, 0)
		Expect(err).NotTo(HaveOccurred())

		offsets := make([]int64, 0, 4)
		err = producer(func(frame *writerv1.WriteFrame) error {
			switch body := frame.Body.(type) {
			case *writerv1.WriteFrame_Data:
				offsets = append(offsets, body.Data.GetOffset())
			case *writerv1.WriteFrame_Zero:
				offsets = append(offsets, body.Zero.GetOffset())
			}
			return nil
		})
		Expect(err).NotTo(HaveOccurred())
		Expect(offsets).To(Equal([]int64{0, 8, 10}))
	})

	It("filters status-only updates in backup predicate", func() {
		pred := reconciler.backupPredicate()
		oldBackup := &snaplanev1alpha1.Backup{
			ObjectMeta: metav1.ObjectMeta{
				Name:       "b",
				Namespace:  "default",
				Generation: 1,
			},
		}
		newBackup := oldBackup.DeepCopy()
		newBackup.Status.LastError = "status-only"
		Expect(pred.Update(event.UpdateEvent{ObjectOld: oldBackup, ObjectNew: newBackup})).To(BeFalse())

		newBackupGeneration := oldBackup.DeepCopy()
		newBackupGeneration.Generation = 2
		Expect(pred.Update(event.UpdateEvent{ObjectOld: oldBackup, ObjectNew: newBackupGeneration})).To(BeTrue())
	})
})

package writer

import (
	"context"
	"encoding/json"
	"hash/crc32"
	"net"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"

	writerv1 "github.com/molpako/snaplane/api/proto/writer/v1"
	snaplanev1alpha1 "github.com/molpako/snaplane/api/v1alpha1"
)

const testBufSize = 1024 * 1024

func startTestGRPCServer(t *testing.T, rootDir string) (writerv1.LocalBackupWriterClient, func()) {
	t.Helper()
	return startTestGRPCServerWithMode(t, rootDir, WriteModeMock)
}

func startTestGRPCServerWithMode(
	t *testing.T,
	rootDir string,
	mode WriteMode,
) (writerv1.LocalBackupWriterClient, func()) {
	t.Helper()

	listener := bufconn.Listen(testBufSize)
	grpcServer := grpc.NewServer()
	writerv1.RegisterLocalBackupWriterServer(grpcServer, NewServerWithOptions(ServerOptions{
		RootDir:   rootDir,
		WriteMode: mode,
	}))

	go func() {
		_ = grpcServer.Serve(listener)
	}()

	dialer := func(context.Context, string) (net.Conn, error) {
		return listener.Dial()
	}

	conn, err := grpc.DialContext(context.Background(), "bufnet", grpc.WithContextDialer(dialer), grpc.WithInsecure()) //nolint:staticcheck
	if err != nil {
		t.Fatalf("failed to dial bufconn: %v", err)
	}

	cleanup := func() {
		_ = conn.Close()
		grpcServer.Stop()
		_ = listener.Close()
	}

	return writerv1.NewLocalBackupWriterClient(conn), cleanup
}

func TestWriterServerWriteAndCommit(t *testing.T) {
	rootDir := t.TempDir()
	client, cleanup := startTestGRPCServer(t, rootDir)
	defer cleanup()

	ctx := context.Background()
	startResp, err := client.StartWrite(ctx, &writerv1.StartWriteRequest{
		BackupUid:  "backup-uid-1",
		BackupName: "backup-a",
		Namespace:  "default",
		PvcName:    "pvc-a",
	})
	if err != nil {
		t.Fatalf("StartWrite failed: %v", err)
	}

	payload := []byte("abc")
	table := crc32.MakeTable(crc32.Castagnoli)
	crc := crc32.Checksum(payload, table)

	stream, err := client.WriteFrames(ctx)
	if err != nil {
		t.Fatalf("WriteFrames open failed: %v", err)
	}

	if err := stream.Send(&writerv1.WriteFrame{
		SessionId: startResp.GetSessionId(),
		Body: &writerv1.WriteFrame_Data{Data: &writerv1.DataFrame{
			Offset:  0,
			Payload: payload,
			Crc32C:  crc,
		}},
	}); err != nil {
		t.Fatalf("send DATA failed: %v", err)
	}

	if err := stream.Send(&writerv1.WriteFrame{
		SessionId: startResp.GetSessionId(),
		Body:      &writerv1.WriteFrame_Zero{Zero: &writerv1.ZeroFrame{Offset: 3, Length: 5}},
	}); err != nil {
		t.Fatalf("send ZERO failed: %v", err)
	}

	if err := stream.Send(&writerv1.WriteFrame{
		SessionId: startResp.GetSessionId(),
		Body:      &writerv1.WriteFrame_Eof{Eof: &writerv1.EofFrame{LogicalSize: 8}},
	}); err != nil {
		t.Fatalf("send EOF failed: %v", err)
	}

	summary, err := stream.CloseAndRecv()
	if err != nil {
		t.Fatalf("WriteFrames close failed: %v", err)
	}
	if summary.GetDataBytes() != 3 || summary.GetZeroBytes() != 5 || summary.GetLastAckedOffset() != 8 {
		t.Fatalf("unexpected summary: %+v", summary)
	}

	commitResp, err := client.CommitWrite(ctx, &writerv1.CommitWriteRequest{SessionId: startResp.GetSessionId()})
	if err != nil {
		t.Fatalf("CommitWrite failed: %v", err)
	}

	if _, err := os.Stat(commitResp.GetTargetPath()); err != nil {
		t.Fatalf("expected committed file to exist: %v", err)
	}
	if commitResp.GetRestoreFormat() != snaplanev1alpha1.RestoreSourceFormatMockImageV1 {
		t.Fatalf("expected mock restore format, got %q", commitResp.GetRestoreFormat())
	}
	metaPath := filepath.Join(filepath.Dir(commitResp.GetTargetPath()), "meta.json")
	if _, err := os.Stat(metaPath); err != nil {
		t.Fatalf("expected meta.json to exist: %v", err)
	}
}

func TestWriterServerCASCommitCreatesRepositoryAndMetadata(t *testing.T) {
	rootDir := t.TempDir()
	client, cleanup := startTestGRPCServerWithMode(t, rootDir, WriteModeCAS)
	defer cleanup()

	ctx := context.Background()
	startResp, err := client.StartWrite(ctx, &writerv1.StartWriteRequest{
		BackupUid:  "backup-uid-cas-1",
		BackupName: "backup-cas-a",
		Namespace:  "default",
		PvcName:    "pvc-a",
	})
	if err != nil {
		t.Fatalf("StartWrite failed: %v", err)
	}

	stream, err := client.WriteFrames(ctx)
	if err != nil {
		t.Fatalf("WriteFrames open failed: %v", err)
	}
	if err := stream.Send(&writerv1.WriteFrame{
		SessionId: startResp.GetSessionId(),
		Body:      &writerv1.WriteFrame_Data{Data: &writerv1.DataFrame{Offset: 0, Payload: []byte("abc")}},
	}); err != nil {
		t.Fatalf("send DATA failed: %v", err)
	}
	if err := stream.Send(&writerv1.WriteFrame{
		SessionId: startResp.GetSessionId(),
		Body:      &writerv1.WriteFrame_Zero{Zero: &writerv1.ZeroFrame{Offset: 3, Length: 4096}},
	}); err != nil {
		t.Fatalf("send ZERO failed: %v", err)
	}
	if err := stream.Send(&writerv1.WriteFrame{
		SessionId: startResp.GetSessionId(),
		Body:      &writerv1.WriteFrame_Eof{Eof: &writerv1.EofFrame{LogicalSize: 4099}},
	}); err != nil {
		t.Fatalf("send EOF failed: %v", err)
	}
	if _, err := stream.CloseAndRecv(); err != nil {
		t.Fatalf("WriteFrames close failed: %v", err)
	}

	commitResp, err := client.CommitWrite(ctx, &writerv1.CommitWriteRequest{SessionId: startResp.GetSessionId()})
	if err != nil {
		t.Fatalf("CommitWrite failed: %v", err)
	}

	if commitResp.GetRestoreFormat() != snaplanev1alpha1.RestoreSourceFormatCASV1 {
		t.Fatalf("expected cas restore format, got %q", commitResp.GetRestoreFormat())
	}
	if commitResp.GetRepositoryPath() == "" || commitResp.GetManifestId() == "" || commitResp.GetRepoUuid() == "" {
		t.Fatalf("expected structured CAS metadata, got %+v", commitResp)
	}
	if _, err := os.Stat(filepath.Join(commitResp.GetRepositoryPath(), "repo.json")); err != nil {
		t.Fatalf("expected repo.json: %v", err)
	}
	if _, err := os.Stat(filepath.Join(commitResp.GetRepositoryPath(), "snapshots", commitResp.GetManifestId(), "manifest.json")); err != nil {
		t.Fatalf("expected manifest.json: %v", err)
	}
}

func TestWriterServerCASCommitDedupsSecondGeneration(t *testing.T) {
	rootDir := t.TempDir()
	client, cleanup := startTestGRPCServerWithMode(t, rootDir, WriteModeCAS)
	defer cleanup()

	ctx := context.Background()
	commitA := writeAndCommit(t, ctx, client, "backup-cas-uid-a", "backup-cas-a", "default", "pvc-a")
	packPath := filepath.Join(commitA.GetRepositoryPath(), "packs", "pack-000001.pack")
	infoA, err := os.Stat(packPath)
	if err != nil {
		t.Fatalf("stat pack A: %v", err)
	}
	commitB := writeAndCommit(t, ctx, client, "backup-cas-uid-b", "backup-cas-b", "default", "pvc-a")
	infoB, err := os.Stat(packPath)
	if err != nil {
		t.Fatalf("stat pack B: %v", err)
	}
	if infoA.Size() != infoB.Size() {
		t.Fatalf("expected dedup without pack growth, before=%d after=%d", infoA.Size(), infoB.Size())
	}
	if commitB.GetManifestId() == "" {
		t.Fatalf("expected manifest id in second commit")
	}

	activeRaw, err := os.ReadFile(filepath.Join(commitA.GetRepositoryPath(), "indexes", "active.json"))
	if err != nil {
		t.Fatalf("read active index: %v", err)
	}
	var active struct {
		Segments []string `json:"segments"`
	}
	if err := json.Unmarshal(activeRaw, &active); err != nil {
		t.Fatalf("decode active index: %v", err)
	}
	if len(active.Segments) < 2 {
		t.Fatalf("expected at least two index segments after two commits, got %d", len(active.Segments))
	}
}

func TestWriterServerRejectsOffsetRollback(t *testing.T) {
	rootDir := t.TempDir()
	client, cleanup := startTestGRPCServer(t, rootDir)
	defer cleanup()

	ctx := context.Background()
	startResp, err := client.StartWrite(ctx, &writerv1.StartWriteRequest{
		BackupUid:  "backup-uid-2",
		BackupName: "backup-b",
		Namespace:  "default",
		PvcName:    "pvc-b",
	})
	if err != nil {
		t.Fatalf("StartWrite failed: %v", err)
	}

	stream, err := client.WriteFrames(ctx)
	if err != nil {
		t.Fatalf("WriteFrames open failed: %v", err)
	}

	if err := stream.Send(&writerv1.WriteFrame{
		SessionId: startResp.GetSessionId(),
		Body:      &writerv1.WriteFrame_Data{Data: &writerv1.DataFrame{Offset: 0, Payload: []byte("abc")}},
	}); err != nil {
		t.Fatalf("send DATA #1 failed: %v", err)
	}

	if err := stream.Send(&writerv1.WriteFrame{
		SessionId: startResp.GetSessionId(),
		Body:      &writerv1.WriteFrame_Data{Data: &writerv1.DataFrame{Offset: 1, Payload: []byte("x")}},
	}); err != nil {
		if status.Code(err) != codes.OutOfRange {
			t.Fatalf("expected OutOfRange from send, got: %v", err)
		}
		return
	}

	_, err = stream.CloseAndRecv()
	if status.Code(err) != codes.OutOfRange {
		t.Fatalf("expected OutOfRange from close, got: %v", err)
	}
}

func TestWriterServerStartWriteIdempotent(t *testing.T) {
	rootDir := t.TempDir()
	client, cleanup := startTestGRPCServer(t, rootDir)
	defer cleanup()

	ctx := context.Background()
	first, err := client.StartWrite(ctx, &writerv1.StartWriteRequest{
		BackupUid:  "backup-uid-3",
		BackupName: "backup-c",
		Namespace:  "default",
		PvcName:    "pvc-c",
	})
	if err != nil {
		t.Fatalf("first StartWrite failed: %v", err)
	}
	second, err := client.StartWrite(ctx, &writerv1.StartWriteRequest{
		BackupUid:  "backup-uid-3",
		BackupName: "backup-c",
		Namespace:  "default",
		PvcName:    "pvc-c",
	})
	if err != nil {
		t.Fatalf("second StartWrite failed: %v", err)
	}

	if first.GetSessionId() != second.GetSessionId() {
		t.Fatalf("expected idempotent session id, got %q and %q", first.GetSessionId(), second.GetSessionId())
	}
}

func TestWriterServerStartWriteReturnsAcceptedOffsetAfterInterruptedStream(t *testing.T) {
	rootDir := t.TempDir()
	client, cleanup := startTestGRPCServer(t, rootDir)
	defer cleanup()

	ctx := context.Background()
	startResp, err := client.StartWrite(ctx, &writerv1.StartWriteRequest{
		BackupUid:  "backup-uid-interrupted",
		BackupName: "backup-interrupted",
		Namespace:  "default",
		PvcName:    "pvc-interrupted",
	})
	if err != nil {
		t.Fatalf("StartWrite failed: %v", err)
	}

	stream, err := client.WriteFrames(ctx)
	if err != nil {
		t.Fatalf("WriteFrames open failed: %v", err)
	}
	if err := stream.Send(&writerv1.WriteFrame{
		SessionId: startResp.GetSessionId(),
		Body:      &writerv1.WriteFrame_Data{Data: &writerv1.DataFrame{Offset: 0, Payload: []byte("abc")}},
	}); err != nil {
		t.Fatalf("send DATA failed: %v", err)
	}

	// Close stream without EOF frame; session should remain resumable.
	summary, err := stream.CloseAndRecv()
	if err != nil {
		t.Fatalf("close stream without EOF frame failed: %v", err)
	}
	if summary.GetLastAckedOffset() != 3 {
		t.Fatalf("expected summary offset 3, got %d", summary.GetLastAckedOffset())
	}

	resumeResp, err := client.StartWrite(ctx, &writerv1.StartWriteRequest{
		BackupUid:  "backup-uid-interrupted",
		BackupName: "backup-interrupted",
		Namespace:  "default",
		PvcName:    "pvc-interrupted",
	})
	if err != nil {
		t.Fatalf("resume StartWrite failed: %v", err)
	}
	if resumeResp.GetAcceptedOffset() != 3 {
		t.Fatalf("expected accepted_offset=3 after interrupted stream, got %d", resumeResp.GetAcceptedOffset())
	}
}

func TestWriterServerResumeFromAcceptedOffsetAndCommit(t *testing.T) {
	rootDir := t.TempDir()
	client, cleanup := startTestGRPCServer(t, rootDir)
	defer cleanup()

	ctx := context.Background()
	startResp, err := client.StartWrite(ctx, &writerv1.StartWriteRequest{
		BackupUid:  "backup-uid-resume-commit",
		BackupName: "backup-resume-commit",
		Namespace:  "default",
		PvcName:    "pvc-resume-commit",
	})
	if err != nil {
		t.Fatalf("StartWrite failed: %v", err)
	}

	first, err := client.WriteFrames(ctx)
	if err != nil {
		t.Fatalf("WriteFrames open failed: %v", err)
	}
	if err := first.Send(&writerv1.WriteFrame{
		SessionId: startResp.GetSessionId(),
		Body:      &writerv1.WriteFrame_Data{Data: &writerv1.DataFrame{Offset: 0, Payload: []byte("abc")}},
	}); err != nil {
		t.Fatalf("send DATA failed: %v", err)
	}
	if _, err := first.CloseAndRecv(); err != nil {
		t.Fatalf("first close stream failed: %v", err)
	}

	resumeResp, err := client.StartWrite(ctx, &writerv1.StartWriteRequest{
		BackupUid:  "backup-uid-resume-commit",
		BackupName: "backup-resume-commit",
		Namespace:  "default",
		PvcName:    "pvc-resume-commit",
	})
	if err != nil {
		t.Fatalf("resume StartWrite failed: %v", err)
	}
	if resumeResp.GetAcceptedOffset() != 3 {
		t.Fatalf("expected accepted_offset=3 after first stream, got %d", resumeResp.GetAcceptedOffset())
	}

	second, err := client.WriteFrames(ctx)
	if err != nil {
		t.Fatalf("resume WriteFrames open failed: %v", err)
	}
	if err := second.Send(&writerv1.WriteFrame{
		SessionId: resumeResp.GetSessionId(),
		Body:      &writerv1.WriteFrame_Zero{Zero: &writerv1.ZeroFrame{Offset: 3, Length: 2}},
	}); err != nil {
		t.Fatalf("send resumed ZERO failed: %v", err)
	}
	if err := second.Send(&writerv1.WriteFrame{
		SessionId: resumeResp.GetSessionId(),
		Body:      &writerv1.WriteFrame_Eof{Eof: &writerv1.EofFrame{LogicalSize: 5}},
	}); err != nil {
		t.Fatalf("send resumed EOF failed: %v", err)
	}
	summary, err := second.CloseAndRecv()
	if err != nil {
		t.Fatalf("resume close stream failed: %v", err)
	}
	if summary.GetLastAckedOffset() != 5 {
		t.Fatalf("expected resumed summary offset 5, got %d", summary.GetLastAckedOffset())
	}

	commitResp, err := client.CommitWrite(ctx, &writerv1.CommitWriteRequest{SessionId: resumeResp.GetSessionId()})
	if err != nil {
		t.Fatalf("CommitWrite failed: %v", err)
	}
	if _, err := os.Stat(commitResp.GetTargetPath()); err != nil {
		t.Fatalf("expected committed file to exist: %v", err)
	}
}

func TestWriterServerRejectsOffsetGap(t *testing.T) {
	rootDir := t.TempDir()
	client, cleanup := startTestGRPCServer(t, rootDir)
	defer cleanup()

	ctx := context.Background()
	startResp, err := client.StartWrite(ctx, &writerv1.StartWriteRequest{
		BackupUid:  "backup-uid-gap",
		BackupName: "backup-gap",
		Namespace:  "default",
		PvcName:    "pvc-gap",
	})
	if err != nil {
		t.Fatalf("StartWrite failed: %v", err)
	}

	stream, err := client.WriteFrames(ctx)
	if err != nil {
		t.Fatalf("WriteFrames open failed: %v", err)
	}

	if err := stream.Send(&writerv1.WriteFrame{
		SessionId: startResp.GetSessionId(),
		Body:      &writerv1.WriteFrame_Data{Data: &writerv1.DataFrame{Offset: 0, Payload: []byte("abc")}},
	}); err != nil {
		t.Fatalf("send DATA #1 failed: %v", err)
	}
	if err := stream.Send(&writerv1.WriteFrame{
		SessionId: startResp.GetSessionId(),
		Body:      &writerv1.WriteFrame_Data{Data: &writerv1.DataFrame{Offset: 5, Payload: []byte("x")}},
	}); err != nil {
		if status.Code(err) != codes.InvalidArgument {
			t.Fatalf("expected InvalidArgument from send, got: %v", err)
		}
		return
	}

	_, err = stream.CloseAndRecv()
	if status.Code(err) != codes.InvalidArgument {
		t.Fatalf("expected InvalidArgument from close, got: %v", err)
	}
}

func TestWriterServerRejectsCRCMismatch(t *testing.T) {
	rootDir := t.TempDir()
	client, cleanup := startTestGRPCServer(t, rootDir)
	defer cleanup()

	ctx := context.Background()
	startResp, err := client.StartWrite(ctx, &writerv1.StartWriteRequest{
		BackupUid:  "backup-uid-crc",
		BackupName: "backup-crc",
		Namespace:  "default",
		PvcName:    "pvc-crc",
	})
	if err != nil {
		t.Fatalf("StartWrite failed: %v", err)
	}

	stream, err := client.WriteFrames(ctx)
	if err != nil {
		t.Fatalf("WriteFrames open failed: %v", err)
	}
	if err := stream.Send(&writerv1.WriteFrame{
		SessionId: startResp.GetSessionId(),
		Body: &writerv1.WriteFrame_Data{Data: &writerv1.DataFrame{
			Offset:  0,
			Payload: []byte("abc"),
			Crc32C:  1,
		}},
	}); err != nil {
		if status.Code(err) != codes.InvalidArgument {
			t.Fatalf("expected InvalidArgument from send, got: %v", err)
		}
		return
	}

	_, err = stream.CloseAndRecv()
	if status.Code(err) != codes.InvalidArgument {
		t.Fatalf("expected InvalidArgument from close, got: %v", err)
	}
}

func TestWriterServerSeparatesOutputPathByNamespace(t *testing.T) {
	rootDir := t.TempDir()
	client, cleanup := startTestGRPCServer(t, rootDir)
	defer cleanup()

	ctx := context.Background()
	commitA := writeAndCommit(t, ctx, client, "backup-uid-ns-a", "backup-a", "team-a", "pvc-same")
	commitB := writeAndCommit(t, ctx, client, "backup-uid-ns-b", "backup-b", "team-b", "pvc-same")

	if commitA.GetTargetPath() == commitB.GetTargetPath() {
		t.Fatalf("expected different output paths for different namespaces, both were %q", commitA.GetTargetPath())
	}
	if !strings.Contains(commitA.GetTargetPath(), "/team-a/") {
		t.Fatalf("expected namespace team-a in path %q", commitA.GetTargetPath())
	}
	if !strings.Contains(commitB.GetTargetPath(), "/team-b/") {
		t.Fatalf("expected namespace team-b in path %q", commitB.GetTargetPath())
	}
}

func writeAndCommit(
	t *testing.T,
	ctx context.Context,
	client writerv1.LocalBackupWriterClient,
	backupUID, backupName, namespace, pvcName string,
) *writerv1.CommitWriteResponse {
	t.Helper()

	startResp, err := client.StartWrite(ctx, &writerv1.StartWriteRequest{
		BackupUid:  backupUID,
		BackupName: backupName,
		Namespace:  namespace,
		PvcName:    pvcName,
	})
	if err != nil {
		t.Fatalf("StartWrite failed: %v", err)
	}

	stream, err := client.WriteFrames(ctx)
	if err != nil {
		t.Fatalf("WriteFrames open failed: %v", err)
	}
	if err := stream.Send(&writerv1.WriteFrame{
		SessionId: startResp.GetSessionId(),
		Body:      &writerv1.WriteFrame_Data{Data: &writerv1.DataFrame{Offset: 0, Payload: []byte("abc")}},
	}); err != nil {
		t.Fatalf("send DATA failed: %v", err)
	}
	if err := stream.Send(&writerv1.WriteFrame{
		SessionId: startResp.GetSessionId(),
		Body:      &writerv1.WriteFrame_Eof{Eof: &writerv1.EofFrame{LogicalSize: 3}},
	}); err != nil {
		t.Fatalf("send EOF failed: %v", err)
	}
	if _, err := stream.CloseAndRecv(); err != nil {
		t.Fatalf("WriteFrames close failed: %v", err)
	}

	commitResp, err := client.CommitWrite(ctx, &writerv1.CommitWriteRequest{SessionId: startResp.GetSessionId()})
	if err != nil {
		t.Fatalf("CommitWrite failed: %v", err)
	}
	if commitResp.GetRestoreFormat() == snaplanev1alpha1.RestoreSourceFormatCASV1 {
		if _, err := os.Stat(filepath.Join(commitResp.GetRepositoryPath(), "snapshots", commitResp.GetManifestId(), "manifest.json")); err != nil {
			t.Fatalf("expected CAS manifest to exist: %v", err)
		}
	} else {
		if _, err := os.Stat(commitResp.GetTargetPath()); err != nil {
			t.Fatalf("expected committed file to exist: %v", err)
		}
	}
	return commitResp
}

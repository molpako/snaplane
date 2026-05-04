package writer

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"sync"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	writerv1 "github.com/molpako/snaplane/api/proto/writer/v1"
	snaplanev1alpha1 "github.com/molpako/snaplane/api/v1alpha1"
	"github.com/molpako/snaplane/internal/casrepo"
)

const (
	defaultFilePerm = 0o640
	defaultDirPerm  = 0o750

	EnvBackupWriteMode = "SNAPLANE_BACKUP_WRITE_MODE"
)

type WriteMode string

const (
	WriteModeMock WriteMode = "mock"
	WriteModeCAS  WriteMode = "cas"
)

type ServerOptions struct {
	RootDir   string
	WriteMode WriteMode
}

type Server struct {
	writerv1.UnimplementedLocalBackupWriterServer

	rootDir  string
	mode     WriteMode
	sessions map[string]*writeSession
	mu       sync.Mutex
}

type writeSession struct {
	id         string
	backupUID  string
	backupName string
	namespace  string
	pvcName    string

	dirPath         string
	repositoryPath  string
	tmpPath         string
	finalPath       string
	metaPath        string
	plannedManifest string

	file *os.File

	lastAckedOffset int64
	dataBytes       int64
	zeroBytes       int64
	logicalSize     int64
	eofSeen         bool
	spans           []casrepo.FrameSpan
}

type metaFile struct {
	SessionID       string    `json:"sessionID"`
	BackupUID       string    `json:"backupUID"`
	BackupName      string    `json:"backupName"`
	Namespace       string    `json:"namespace"`
	PVCName         string    `json:"pvcName"`
	LastAckedOffset int64     `json:"lastAckedOffset"`
	DataBytes       int64     `json:"dataBytes"`
	ZeroBytes       int64     `json:"zeroBytes"`
	LogicalSize     int64     `json:"logicalSize"`
	CommittedAt     time.Time `json:"committedAt"`
}

func NewServer(rootDir string) *Server {
	return NewServerWithOptions(ServerOptions{RootDir: rootDir, WriteMode: modeFromEnv()})
}

func NewServerWithOptions(opts ServerOptions) *Server {
	mode := opts.WriteMode
	if mode == "" {
		mode = modeFromEnv()
	}
	if mode != WriteModeCAS {
		mode = WriteModeMock
	}
	return &Server{
		rootDir:  opts.RootDir,
		mode:     mode,
		sessions: map[string]*writeSession{},
	}
}

func modeFromEnv() WriteMode {
	if os.Getenv(EnvBackupWriteMode) == string(WriteModeCAS) {
		return WriteModeCAS
	}
	return WriteModeMock
}

func (s *Server) StartWrite(_ context.Context, req *writerv1.StartWriteRequest) (*writerv1.StartWriteResponse, error) {
	if req.GetBackupUid() == "" || req.GetBackupName() == "" || req.GetNamespace() == "" || req.GetPvcName() == "" {
		return nil, status.Error(codes.InvalidArgument, "backup_uid, backup_name, namespace and pvc_name are required")
	}

	sessionID := req.GetBackupUid()
	s.mu.Lock()
	defer s.mu.Unlock()

	if existing, ok := s.sessions[sessionID]; ok {
		return &writerv1.StartWriteResponse{
			SessionId:      existing.id,
			AcceptedOffset: existing.lastAckedOffset,
			TargetPath:     existing.finalPath,
		}, nil
	}

	dirPath := filepath.Join(s.rootDir, req.GetNamespace(), req.GetPvcName(), req.GetBackupName())
	if err := os.MkdirAll(dirPath, defaultDirPerm); err != nil {
		return nil, status.Errorf(codes.Internal, "failed to create destination directory: %v", err)
	}

	tmpPath := filepath.Join(dirPath, "mock.img.tmp")
	f, err := os.OpenFile(tmpPath, os.O_CREATE|os.O_RDWR, defaultFilePerm)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "failed to open temp file: %v", err)
	}

	repositoryPath := filepath.Join(s.rootDir, req.GetNamespace(), req.GetPvcName(), "repo")
	plannedManifest := req.GetBackupName()
	finalPath := filepath.Join(dirPath, "mock.img")
	if s.mode == WriteModeCAS {
		finalPath = filepath.Join(repositoryPath, plannedManifest)
	}

	session := &writeSession{
		id:              sessionID,
		backupUID:       req.GetBackupUid(),
		backupName:      req.GetBackupName(),
		namespace:       req.GetNamespace(),
		pvcName:         req.GetPvcName(),
		dirPath:         dirPath,
		repositoryPath:  repositoryPath,
		tmpPath:         tmpPath,
		finalPath:       finalPath,
		metaPath:        filepath.Join(dirPath, "meta.json"),
		file:            f,
		plannedManifest: plannedManifest,
	}

	s.sessions[sessionID] = session
	return &writerv1.StartWriteResponse{SessionId: sessionID, AcceptedOffset: 0, TargetPath: session.finalPath}, nil
}

func (s *Server) WriteFrames(stream writerv1.LocalBackupWriter_WriteFramesServer) error {
	var session *writeSession

	for {
		frame, err := stream.Recv()
		if errors.Is(err, io.EOF) {
			if session == nil {
				return status.Error(codes.InvalidArgument, "missing frames")
			}
			return stream.SendAndClose(&writerv1.WriteFramesSummary{
				LastAckedOffset: session.lastAckedOffset,
				DataBytes:       session.dataBytes,
				ZeroBytes:       session.zeroBytes,
			})
		}
		if err != nil {
			return status.Errorf(codes.Internal, "failed to receive stream frame: %v", err)
		}

		s.mu.Lock()
		candidate, ok := s.sessions[frame.GetSessionId()]
		s.mu.Unlock()
		if !ok {
			return status.Error(codes.NotFound, "session not found")
		}
		session = candidate

		switch body := frame.Body.(type) {
		case *writerv1.WriteFrame_Data:
			if err := applyDataFrame(session, body.Data); err != nil {
				return err
			}
		case *writerv1.WriteFrame_Zero:
			if err := applyZeroFrame(session, body.Zero); err != nil {
				return err
			}
		case *writerv1.WriteFrame_Eof:
			if body.Eof.GetLogicalSize() < 0 {
				return status.Error(codes.InvalidArgument, "logical_size must be >= 0")
			}
			session.eofSeen = true
			session.logicalSize = body.Eof.GetLogicalSize()
		default:
			return status.Error(codes.InvalidArgument, "frame body is required")
		}
	}
}

func applyDataFrame(session *writeSession, frame *writerv1.DataFrame) error {
	offset := frame.GetOffset()
	payload := frame.GetPayload()

	if err := validateWriteOffset(session.lastAckedOffset, offset); err != nil {
		return err
	}
	if len(payload) == 0 {
		return status.Error(codes.InvalidArgument, "payload must not be empty")
	}

	if frame.GetCrc32C() != 0 {
		table := crc32.MakeTable(crc32.Castagnoli)
		actual := crc32.Checksum(payload, table)
		if actual != frame.GetCrc32C() {
			return status.Error(codes.InvalidArgument, "payload crc32c mismatch")
		}
	}

	if _, err := session.file.WriteAt(payload, offset); err != nil {
		return status.Errorf(codes.Internal, "failed to write payload: %v", err)
	}

	session.lastAckedOffset = offset + int64(len(payload))
	session.dataBytes += int64(len(payload))
	session.spans = append(session.spans, casrepo.FrameSpan{Kind: casrepo.SpanKindData, Offset: offset, Length: int64(len(payload))})
	return nil
}

func applyZeroFrame(session *writeSession, frame *writerv1.ZeroFrame) error {
	offset := frame.GetOffset()
	length := frame.GetLength()

	if err := validateWriteOffset(session.lastAckedOffset, offset); err != nil {
		return err
	}
	if length <= 0 {
		return status.Error(codes.InvalidArgument, "zero length must be > 0")
	}

	const chunk = 1 << 20
	buf := make([]byte, chunk)
	var written int64
	for written < length {
		step := int64(len(buf))
		if remaining := length - written; remaining < step {
			step = remaining
		}
		if _, err := session.file.WriteAt(buf[:step], offset+written); err != nil {
			return status.Errorf(codes.Internal, "failed to zero-fill payload: %v", err)
		}
		written += step
	}

	session.lastAckedOffset = offset + length
	session.zeroBytes += length
	session.spans = append(session.spans, casrepo.FrameSpan{Kind: casrepo.SpanKindZero, Offset: offset, Length: length})
	return nil
}

func validateWriteOffset(lastAcked, offset int64) error {
	switch {
	case offset < 0:
		return status.Error(codes.InvalidArgument, "offset must be >= 0")
	case offset < lastAcked:
		return status.Error(codes.OutOfRange, "frame offset is behind accepted offset")
	case offset > lastAcked:
		return status.Error(codes.InvalidArgument, "frame offset must be contiguous")
	default:
		return nil
	}
}

func (s *Server) CommitWrite(_ context.Context, req *writerv1.CommitWriteRequest) (*writerv1.CommitWriteResponse, error) {
	s.mu.Lock()
	session, ok := s.sessions[req.GetSessionId()]
	s.mu.Unlock()
	if !ok {
		return nil, status.Error(codes.NotFound, "session not found")
	}
	if !session.eofSeen {
		return nil, status.Error(codes.FailedPrecondition, "EOF frame was not received")
	}
	if err := session.file.Sync(); err != nil {
		return nil, status.Errorf(codes.Internal, "failed to sync temp file: %v", err)
	}
	if err := session.file.Close(); err != nil {
		return nil, status.Errorf(codes.Internal, "failed to close temp file: %v", err)
	}

	if s.mode == WriteModeCAS {
		return s.commitCAS(session)
	}
	return s.commitMock(session)
}

func (s *Server) commitMock(session *writeSession) (*writerv1.CommitWriteResponse, error) {
	if err := os.Rename(session.tmpPath, session.finalPath); err != nil {
		return nil, status.Errorf(codes.Internal, "failed to rename temp file: %v", err)
	}

	meta := metaFile{
		SessionID:       session.id,
		BackupUID:       session.backupUID,
		BackupName:      session.backupName,
		Namespace:       session.namespace,
		PVCName:         session.pvcName,
		LastAckedOffset: session.lastAckedOffset,
		DataBytes:       session.dataBytes,
		ZeroBytes:       session.zeroBytes,
		LogicalSize:     session.logicalSize,
		CommittedAt:     time.Now().UTC(),
	}
	encoded, err := json.MarshalIndent(meta, "", "  ")
	if err != nil {
		return nil, status.Errorf(codes.Internal, "failed to encode metadata file: %v", err)
	}
	if err := os.WriteFile(session.metaPath, append(encoded, '\n'), defaultFilePerm); err != nil {
		return nil, status.Errorf(codes.Internal, "failed to write metadata file: %v", err)
	}

	s.mu.Lock()
	delete(s.sessions, session.id)
	s.mu.Unlock()

	return &writerv1.CommitWriteResponse{
		TargetPath:      session.finalPath,
		RepositoryPath:  filepath.Dir(session.finalPath),
		ManifestId:      filepath.Base(session.finalPath),
		RepoUuid:        session.backupUID,
		VolumeSizeBytes: session.logicalSize,
		ChunkSizeBytes:  4096,
		RestoreFormat:   snaplanev1alpha1.RestoreSourceFormatMockImageV1,
	}, nil
}

func (s *Server) commitCAS(session *writeSession) (*writerv1.CommitWriteResponse, error) {
	if err := os.MkdirAll(session.repositoryPath, defaultDirPerm); err != nil {
		return nil, status.Errorf(codes.Internal, "failed to create repository path: %v", err)
	}

	manifestID := session.plannedManifest
	for i := 1; ; i++ {
		candidate := manifestID
		if i > 1 {
			candidate = manifestID + "-" + strconv.Itoa(i)
		}
		_, statErr := os.Stat(filepath.Join(session.repositoryPath, "snapshots", candidate))
		if os.IsNotExist(statErr) {
			manifestID = candidate
			break
		}
		if statErr != nil {
			return nil, status.Errorf(codes.Internal, "failed to probe manifest path: %v", statErr)
		}
	}

	result, err := casrepo.Commit(casrepo.CommitRequest{
		RepositoryPath: session.repositoryPath,
		ManifestID:     manifestID,
		SpoolPath:      session.tmpPath,
		LogicalSize:    session.logicalSize,
		Spans:          session.spans,
	})
	if err != nil {
		return nil, status.Errorf(codes.Internal, "failed to commit CAS repository: %v", err)
	}

	s.mu.Lock()
	delete(s.sessions, session.id)
	s.mu.Unlock()
	_ = os.Remove(session.tmpPath)

	targetPath := filepath.Join(result.RepositoryPath, result.ManifestID)
	return &writerv1.CommitWriteResponse{
		TargetPath:      targetPath,
		RepositoryPath:  result.RepositoryPath,
		ManifestId:      result.ManifestID,
		ManifestChain:   result.ManifestChain,
		RepoUuid:        result.RepoUUID,
		VolumeSizeBytes: result.VolumeSize,
		ChunkSizeBytes:  result.ChunkSize,
		RestoreFormat:   snaplanev1alpha1.RestoreSourceFormatCASV1,
	}, nil
}

func (s *Server) AbortWrite(_ context.Context, req *writerv1.AbortWriteRequest) (*writerv1.AbortWriteResponse, error) {
	s.mu.Lock()
	session, ok := s.sessions[req.GetSessionId()]
	if ok {
		delete(s.sessions, req.GetSessionId())
	}
	s.mu.Unlock()
	if !ok {
		return &writerv1.AbortWriteResponse{}, nil
	}
	if session.file != nil {
		_ = session.file.Close()
	}
	if err := os.Remove(session.tmpPath); err != nil && !errors.Is(err, os.ErrNotExist) {
		return nil, status.Errorf(codes.Internal, "failed to remove temp file: %v", err)
	}
	return &writerv1.AbortWriteResponse{}, nil
}

func LeaseNameForNode(nodeName string) string {
	return fmt.Sprintf("backup-node-%s", nodeName)
}

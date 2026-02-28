package backupsource

import (
	"context"
	"fmt"
	"os"
	"sync"
)

const (
	EnvCBTProvider = "SNAPLANE_CBT_PROVIDER"
	ProviderFake   = "fake"
	// ProviderSnapshotMetadata uses Kubernetes SnapshotMetadataService APIs.
	ProviderSnapshotMetadata = "snapshot-metadata"
	// ProviderCephSMS is a deprecated alias kept for backward compatibility.
	ProviderCephSMS = "ceph-sms"
)

type RangeKind string

const (
	RangeKindData RangeKind = "data"
	RangeKindZero RangeKind = "zero"
)

type Range struct {
	Kind   RangeKind
	Offset int64
	Length int64
}

type RangeRequest struct {
	BackupName   string
	BackupUID    string
	Namespace    string
	PVCName      string
	SnapshotName string
}

type ReadRequest struct {
	RangeRequest
	Offset int64
	Length int64
}

type RangeProvider interface {
	GetChangedRanges(ctx context.Context, req RangeRequest) ([]Range, error)
}

type BlockReader interface {
	ReadBlock(ctx context.Context, req ReadRequest) ([]byte, error)
}

type Provider interface {
	RangeProvider
	BlockReader
}

type ReaderCleanup interface {
	Cleanup(ctx context.Context, req RangeRequest) error
}

func NewProviderFromEnv() Provider {
	switch os.Getenv(EnvCBTProvider) {
	case "", ProviderFake:
		return NewFakeProvider()
	case ProviderSnapshotMetadata, ProviderCephSMS:
		return NewSnapshotMetadataProvider()
	default:
		return NewFakeProvider()
	}
}

type FakeProvider struct{}

func NewFakeProvider() *FakeProvider {
	return &FakeProvider{}
}

func (p *FakeProvider) GetChangedRanges(_ context.Context, req RangeRequest) ([]Range, error) {
	payload := []byte("mock-data:" + req.BackupName)
	return []Range{
		{Kind: RangeKindData, Offset: 0, Length: int64(len(payload))},
		{Kind: RangeKindZero, Offset: int64(len(payload)), Length: 4096},
	}, nil
}

func (p *FakeProvider) ReadBlock(_ context.Context, req ReadRequest) ([]byte, error) {
	if req.Length < 0 {
		return nil, fmt.Errorf("length must be >= 0")
	}
	payload := []byte("mock-data:" + req.BackupName)
	if req.Offset < 0 {
		return nil, fmt.Errorf("offset must be >= 0")
	}
	if req.Offset > int64(len(payload)) {
		return []byte{}, nil
	}
	end := req.Offset + req.Length
	if end > int64(len(payload)) {
		end = int64(len(payload))
	}
	if end < req.Offset {
		return nil, fmt.Errorf("invalid read range")
	}
	out := make([]byte, end-req.Offset)
	copy(out, payload[req.Offset:end])
	return out, nil
}

type SnapshotMetadataProvider struct {
	mu      sync.Mutex
	clients *snapshotMetadataClients
	readers map[string]snapshotReaderRef
}

func NewSnapshotMetadataProvider() *SnapshotMetadataProvider {
	return &SnapshotMetadataProvider{}
}

// NewCephSMSProvider is kept as a deprecated constructor alias.
func NewCephSMSProvider() *SnapshotMetadataProvider {
	return NewSnapshotMetadataProvider()
}

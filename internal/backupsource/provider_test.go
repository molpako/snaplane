package backupsource

import (
	"context"
	"errors"
	"os"
	"testing"
	"time"

	smsiterator "github.com/kubernetes-csi/external-snapshot-metadata/pkg/iterator"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/kubernetes/fake"
	k8stesting "k8s.io/client-go/testing"
)

func TestFakeProviderProducesDeterministicRangesAndData(t *testing.T) {
	t.Parallel()

	provider := NewFakeProvider()
	req := RangeRequest{BackupName: "backup-a"}
	ranges, err := provider.GetChangedRanges(context.Background(), req)
	if err != nil {
		t.Fatalf("GetChangedRanges failed: %v", err)
	}
	if len(ranges) != 2 {
		t.Fatalf("expected 2 ranges, got %d", len(ranges))
	}
	if ranges[0].Kind != RangeKindData || ranges[0].Offset != 0 {
		t.Fatalf("unexpected first range: %+v", ranges[0])
	}
	if ranges[1].Kind != RangeKindZero || ranges[1].Offset != ranges[0].Length {
		t.Fatalf("unexpected second range: %+v", ranges[1])
	}

	data, err := provider.ReadBlock(context.Background(), ReadRequest{RangeRequest: req, Offset: 0, Length: ranges[0].Length})
	if err != nil {
		t.Fatalf("ReadBlock failed: %v", err)
	}
	if string(data) != "mock-data:backup-a" {
		t.Fatalf("unexpected data %q", string(data))
	}
}

func TestNewProviderFromEnvDefaultsToFake(t *testing.T) {
	t.Parallel()

	old := os.Getenv(EnvCBTProvider)
	t.Cleanup(func() {
		_ = os.Setenv(EnvCBTProvider, old)
	})
	_ = os.Unsetenv(EnvCBTProvider)

	provider := NewProviderFromEnv()
	if _, ok := provider.(*FakeProvider); !ok {
		t.Fatalf("expected fake provider, got %T", provider)
	}
}

func TestSnapshotMetadataProviderReadBlockReturnsDeterministicBytes(t *testing.T) {
	t.Parallel()

	provider := NewSnapshotMetadataProvider()
	if _, err := provider.GetChangedRanges(context.Background(), RangeRequest{}); err == nil {
		t.Fatalf("expected GetChangedRanges error for invalid request")
	}
	blockA, err := provider.ReadBlock(context.Background(), ReadRequest{
		RangeRequest: RangeRequest{BackupUID: "uid-1", PVCName: "pvc-a", SnapshotName: "snap-a"},
		Offset:       10,
		Length:       32,
	})
	if err != nil {
		t.Fatalf("ReadBlock failed: %v", err)
	}
	blockB, err := provider.ReadBlock(context.Background(), ReadRequest{
		RangeRequest: RangeRequest{BackupUID: "uid-1", PVCName: "pvc-a", SnapshotName: "snap-a"},
		Offset:       10,
		Length:       32,
	})
	if err != nil {
		t.Fatalf("ReadBlock failed: %v", err)
	}
	if len(blockA) != 32 {
		t.Fatalf("expected 32 bytes, got %d", len(blockA))
	}
	if string(blockA) != string(blockB) {
		t.Fatalf("expected deterministic payload")
	}
}

func TestNewProviderFromEnvAcceptsDeprecatedCephSMSAlias(t *testing.T) {
	t.Parallel()

	old := os.Getenv(EnvCBTProvider)
	t.Cleanup(func() {
		_ = os.Setenv(EnvCBTProvider, old)
	})
	_ = os.Setenv(EnvCBTProvider, ProviderCephSMS)

	provider := NewProviderFromEnv()
	if _, ok := provider.(*SnapshotMetadataProvider); !ok {
		t.Fatalf("expected snapshot metadata provider via deprecated alias, got %T", provider)
	}
}

func TestSnapshotMetadataProviderCleanupRemovesReaderResources(t *testing.T) {
	t.Parallel()

	const (
		ns       = "default"
		snapshot = "snap-a"
	)
	pvcName, podName := readerNames(ns, snapshot)
	kube := fake.NewSimpleClientset(
		&corev1.PersistentVolumeClaim{ObjectMeta: metav1.ObjectMeta{Name: pvcName, Namespace: ns}},
		&corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: podName, Namespace: ns}},
	)
	provider := NewSnapshotMetadataProvider()
	provider.clients = &snapshotMetadataClients{
		clients: smsiterator.Clients{KubeClient: kube},
	}
	provider.readers = map[string]snapshotReaderRef{
		ns + "/" + snapshot: {namespace: ns, podName: podName},
	}

	if err := provider.Cleanup(context.Background(), RangeRequest{Namespace: ns, SnapshotName: snapshot}); err != nil {
		t.Fatalf("Cleanup failed: %v", err)
	}

	if _, err := kube.CoreV1().Pods(ns).Get(context.Background(), podName, metav1.GetOptions{}); !apierrors.IsNotFound(err) {
		t.Fatalf("expected pod to be deleted, err=%v", err)
	}
	if _, err := kube.CoreV1().PersistentVolumeClaims(ns).Get(context.Background(), pvcName, metav1.GetOptions{}); !apierrors.IsNotFound(err) {
		t.Fatalf("expected pvc to be deleted, err=%v", err)
	}
	if _, ok := provider.readers[ns+"/"+snapshot]; ok {
		t.Fatalf("expected reader cache entry to be removed")
	}
}

func TestSnapshotMetadataProviderEnsureSnapshotReaderReturnsCreateErrors(t *testing.T) {
	t.Parallel()

	const (
		ns        = "default"
		sourcePVC = "source-pvc"
	)
	kube := fake.NewSimpleClientset(
		&corev1.PersistentVolumeClaim{ObjectMeta: metav1.ObjectMeta{Name: sourcePVC, Namespace: ns}},
	)
	kube.PrependReactor("create", "persistentvolumeclaims", func(_ k8stesting.Action) (bool, runtime.Object, error) {
		return true, nil, errors.New("boom")
	})

	provider := NewSnapshotMetadataProvider()
	clients := &snapshotMetadataClients{
		clients: smsiterator.Clients{KubeClient: kube},
	}
	_, err := provider.ensureSnapshotReader(context.Background(), clients, ReadRequest{
		RangeRequest: RangeRequest{
			Namespace:    ns,
			SnapshotName: "snap-a",
			PVCName:      sourcePVC,
		},
		Offset: 0,
		Length: 1,
	})
	if err == nil {
		t.Fatalf("expected create error")
	}
}

func TestSnapshotMetadataProviderEnsureSnapshotReaderCleansUpOnTimeout(t *testing.T) {
	t.Parallel()

	const (
		ns        = "default"
		sourcePVC = "source-pvc"
		snapshot  = "snap-timeout"
	)
	kube := fake.NewSimpleClientset(
		&corev1.PersistentVolumeClaim{ObjectMeta: metav1.ObjectMeta{Name: sourcePVC, Namespace: ns}},
	)
	provider := NewSnapshotMetadataProvider()
	clients := &snapshotMetadataClients{
		clients: smsiterator.Clients{KubeClient: kube},
	}

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	_, err := provider.ensureSnapshotReader(ctx, clients, ReadRequest{
		RangeRequest: RangeRequest{
			Namespace:    ns,
			SnapshotName: snapshot,
			PVCName:      sourcePVC,
		},
		Offset: 0,
		Length: 1,
	})
	if err == nil {
		t.Fatalf("expected timeout error")
	}

	pvcName, podName := readerNames(ns, snapshot)
	if _, err := kube.CoreV1().Pods(ns).Get(context.Background(), podName, metav1.GetOptions{}); !apierrors.IsNotFound(err) {
		t.Fatalf("expected pod cleanup after timeout, err=%v", err)
	}
	if _, err := kube.CoreV1().PersistentVolumeClaims(ns).Get(context.Background(), pvcName, metav1.GetOptions{}); !apierrors.IsNotFound(err) {
		t.Fatalf("expected pvc cleanup after timeout, err=%v", err)
	}
}

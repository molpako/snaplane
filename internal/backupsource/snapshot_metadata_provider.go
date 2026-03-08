package backupsource

import (
	"bytes"
	"context"
	"crypto/sha1"
	"encoding/hex"
	"fmt"
	"os"
	"sort"
	"strings"
	"time"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	resourceapi "k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/remotecommand"
	ctrlconfig "sigs.k8s.io/controller-runtime/pkg/client/config"

	smsiterator "github.com/kubernetes-csi/external-snapshot-metadata/pkg/iterator"
)

const (
	EnvSnapshotDataMode = "SNAPLANE_SNAPSHOT_DATA_MODE"
	SnapshotDataMock    = "mock"
	SnapshotDataLive    = "live"

	readerPodImage      = "busybox:1.36"
	readerReadyTimeout  = 2 * time.Minute
	defaultReadChunkMax = int64(1 << 20) // 1MiB
)

type snapshotMetadataClients struct {
	clients    smsiterator.Clients
	restConfig *rest.Config
}

type snapshotMetadataCollector struct {
	ranges         []Range
	volumeCapacity int64
}

type snapshotReaderRef struct {
	namespace string
	podName   string
}

func (p *SnapshotMetadataProvider) GetChangedRanges(ctx context.Context, req RangeRequest) ([]Range, error) {
	if req.Namespace == "" {
		return nil, fmt.Errorf("namespace is required")
	}
	if req.SnapshotName == "" {
		return nil, fmt.Errorf("snapshot name is required")
	}

	clients, err := p.getIteratorClients()
	if err != nil {
		return nil, err
	}

	collector := &snapshotMetadataCollector{}
	args := smsiterator.Args{
		Clients:        clients.clients,
		Emitter:        collector,
		Namespace:      req.Namespace,
		SnapshotName:   req.SnapshotName,
		StartingOffset: 0,
	}

	if err := smsiterator.GetSnapshotMetadata(ctx, args); err != nil {
		return nil, fmt.Errorf("get snapshot metadata for %s/%s: %w", req.Namespace, req.SnapshotName, err)
	}

	out := normalizeAndChunkRanges(collector.ranges, defaultReadChunkMax)
	maxEnd := int64(0)
	for i := range out {
		end := out[i].Offset + out[i].Length
		if end > maxEnd {
			maxEnd = end
		}
	}
	if collector.volumeCapacity > maxEnd {
		out = append(out, Range{
			Kind:   RangeKindZero,
			Offset: maxEnd,
			Length: collector.volumeCapacity - maxEnd,
		})
	}
	return out, nil
}

func (p *SnapshotMetadataProvider) ReadBlock(ctx context.Context, req ReadRequest) ([]byte, error) {
	if req.Offset < 0 {
		return nil, fmt.Errorf("offset must be >= 0")
	}
	if req.Length < 0 {
		return nil, fmt.Errorf("length must be >= 0")
	}
	if req.Length == 0 {
		return []byte{}, nil
	}
	if snapshotDataMode() != SnapshotDataLive {
		return deterministicPayload(req), nil
	}
	if req.Namespace == "" || req.SnapshotName == "" || req.PVCName == "" {
		return nil, fmt.Errorf("namespace/snapshotName/pvcName are required in live mode")
	}

	clients, err := p.getIteratorClients()
	if err != nil {
		return nil, err
	}
	reader, err := p.ensureSnapshotReader(ctx, clients, req)
	if err != nil {
		return nil, err
	}
	return execReadBlock(ctx, clients.restConfig, reader.namespace, reader.podName, req.Offset, req.Length)
}

func (p *SnapshotMetadataProvider) Cleanup(ctx context.Context, req RangeRequest) error {
	if req.Namespace == "" || req.SnapshotName == "" {
		return nil
	}
	clients, err := p.getIteratorClients()
	if err != nil {
		return err
	}
	return p.cleanupSnapshotReader(ctx, clients.clients.KubeClient, req.Namespace, req.SnapshotName)
}

func (p *SnapshotMetadataProvider) getIteratorClients() (*snapshotMetadataClients, error) {
	p.mu.Lock()
	defer p.mu.Unlock()

	if p.clients != nil {
		return p.clients, nil
	}

	cfg, err := loadKubeConfig()
	if err != nil {
		return nil, fmt.Errorf("build kube config for snapshot-metadata provider: %w", err)
	}
	clients, err := smsiterator.BuildClients(cfg)
	if err != nil {
		return nil, fmt.Errorf("build snapshot-metadata clients: %w", err)
	}
	p.clients = &snapshotMetadataClients{
		clients:    clients,
		restConfig: cfg,
	}
	if p.readers == nil {
		p.readers = map[string]snapshotReaderRef{}
	}
	return p.clients, nil
}

func (p *SnapshotMetadataProvider) ensureSnapshotReader(
	ctx context.Context,
	clients *snapshotMetadataClients,
	req ReadRequest,
) (snapshotReaderRef, error) {
	key := req.Namespace + "/" + req.SnapshotName

	p.mu.Lock()
	if ref, ok := p.readers[key]; ok {
		p.mu.Unlock()
		if err := waitReaderPodRunning(ctx, clients.clients.KubeClient, ref.namespace, ref.podName); err == nil {
			return ref, nil
		}
		if err := p.cleanupSnapshotReader(ctx, clients.clients.KubeClient, req.Namespace, req.SnapshotName); err != nil {
			return snapshotReaderRef{}, err
		}
	} else {
		p.mu.Unlock()
	}

	pvcName, podName := readerNames(req.Namespace, req.SnapshotName)
	sourcePVC, err := clients.clients.KubeClient.CoreV1().PersistentVolumeClaims(req.Namespace).Get(ctx, req.PVCName, metav1.GetOptions{})
	if err != nil {
		return snapshotReaderRef{}, fmt.Errorf("get source pvc %s/%s: %w", req.Namespace, req.PVCName, err)
	}
	requestSize := resourceapi.MustParse("1Gi")
	if sourcePVC.Spec.Resources.Requests != nil {
		if size, ok := sourcePVC.Spec.Resources.Requests[corev1.ResourceStorage]; ok {
			requestSize = size
		}
	}

	pvc := &corev1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{
			Name:      pvcName,
			Namespace: req.Namespace,
			Labels: map[string]string{
				"snaplane.molpako.github.io/component": "snapshot-reader",
				"snaplane.molpako.github.io/snapshot":  req.SnapshotName,
			},
		},
		Spec: corev1.PersistentVolumeClaimSpec{
			AccessModes: []corev1.PersistentVolumeAccessMode{corev1.ReadWriteOnce},
			Resources: corev1.VolumeResourceRequirements{
				Requests: corev1.ResourceList{
					corev1.ResourceStorage: requestSize,
				},
			},
			DataSource: &corev1.TypedLocalObjectReference{
				APIGroup: strPtr("snapshot.storage.k8s.io"),
				Kind:     "VolumeSnapshot",
				Name:     req.SnapshotName,
			},
			StorageClassName: sourcePVC.Spec.StorageClassName,
			VolumeMode:       volumeModePtr(corev1.PersistentVolumeBlock),
		},
	}
	if _, err := clients.clients.KubeClient.CoreV1().PersistentVolumeClaims(req.Namespace).Create(ctx, pvc, metav1.CreateOptions{}); err != nil && !apierrors.IsAlreadyExists(err) {
		return snapshotReaderRef{}, fmt.Errorf("create snapshot-reader pvc %s/%s: %w", req.Namespace, pvcName, err)
	}

	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      podName,
			Namespace: req.Namespace,
			Labels: map[string]string{
				"snaplane.molpako.github.io/component": "snapshot-reader",
				"snaplane.molpako.github.io/snapshot":  req.SnapshotName,
			},
		},
		Spec: corev1.PodSpec{
			RestartPolicy: corev1.RestartPolicyAlways,
			Containers: []corev1.Container{
				{
					Name:    "reader",
					Image:   readerPodImage,
					Command: []string{"sh", "-c", "sleep 36000"},
					VolumeDevices: []corev1.VolumeDevice{
						{Name: "source", DevicePath: "/dev/snapshot"},
					},
				},
			},
			Volumes: []corev1.Volume{
				{
					Name: "source",
					VolumeSource: corev1.VolumeSource{
						PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{
							ClaimName: pvcName,
						},
					},
				},
			},
		},
	}
	if _, err := clients.clients.KubeClient.CoreV1().Pods(req.Namespace).Create(ctx, pod, metav1.CreateOptions{}); err != nil && !apierrors.IsAlreadyExists(err) {
		return snapshotReaderRef{}, fmt.Errorf("create snapshot-reader pod %s/%s: %w", req.Namespace, podName, err)
	}

	if err := waitReaderPodRunning(ctx, clients.clients.KubeClient, req.Namespace, podName); err != nil {
		if cleanupErr := p.cleanupSnapshotReader(ctx, clients.clients.KubeClient, req.Namespace, req.SnapshotName); cleanupErr != nil {
			return snapshotReaderRef{}, fmt.Errorf("reader pod not running and cleanup failed: %w", cleanupErr)
		}
		if _, err := clients.clients.KubeClient.CoreV1().PersistentVolumeClaims(req.Namespace).Create(ctx, pvc, metav1.CreateOptions{}); err != nil && !apierrors.IsAlreadyExists(err) {
			return snapshotReaderRef{}, fmt.Errorf("recreate snapshot-reader pvc %s/%s: %w", req.Namespace, pvcName, err)
		}
		if _, err := clients.clients.KubeClient.CoreV1().Pods(req.Namespace).Create(ctx, pod, metav1.CreateOptions{}); err != nil && !apierrors.IsAlreadyExists(err) {
			return snapshotReaderRef{}, fmt.Errorf("recreate snapshot-reader pod %s/%s: %w", req.Namespace, podName, err)
		}
		if err := waitReaderPodRunning(ctx, clients.clients.KubeClient, req.Namespace, podName); err != nil {
			if cleanupErr := p.cleanupSnapshotReader(ctx, clients.clients.KubeClient, req.Namespace, req.SnapshotName); cleanupErr != nil {
				return snapshotReaderRef{}, fmt.Errorf("wait reader pod running failed and cleanup failed: %w", cleanupErr)
			}
			return snapshotReaderRef{}, err
		}
	}
	ref := snapshotReaderRef{namespace: req.Namespace, podName: podName}

	p.mu.Lock()
	p.readers[key] = ref
	p.mu.Unlock()
	return ref, nil
}

func (p *SnapshotMetadataProvider) cleanupSnapshotReader(ctx context.Context, kube kubernetes.Interface, namespace, snapshotName string) error {
	pvcName, podName := readerNames(namespace, snapshotName)
	if err := kube.CoreV1().Pods(namespace).Delete(ctx, podName, metav1.DeleteOptions{}); err != nil && !apierrors.IsNotFound(err) {
		return fmt.Errorf("delete snapshot-reader pod %s/%s: %w", namespace, podName, err)
	}
	if err := kube.CoreV1().PersistentVolumeClaims(namespace).Delete(ctx, pvcName, metav1.DeleteOptions{}); err != nil && !apierrors.IsNotFound(err) {
		return fmt.Errorf("delete snapshot-reader pvc %s/%s: %w", namespace, pvcName, err)
	}

	p.mu.Lock()
	if p.readers != nil {
		delete(p.readers, namespace+"/"+snapshotName)
	}
	p.mu.Unlock()
	return nil
}

func execReadBlock(ctx context.Context, cfg *rest.Config, namespace, podName string, offset, length int64) ([]byte, error) {
	cmd := []string{
		"sh", "-c",
		fmt.Sprintf("dd if=/dev/snapshot bs=1 skip=%d count=%d status=none", offset, length),
	}
	req := kubernetes.NewForConfigOrDie(cfg).CoreV1().RESTClient().
		Post().
		Namespace(namespace).
		Resource("pods").
		Name(podName).
		SubResource("exec").
		VersionedParams(&corev1.PodExecOptions{
			Container: "reader",
			Command:   cmd,
			Stdout:    true,
			Stderr:    true,
		}, scheme.ParameterCodec)

	exec, err := remotecommand.NewSPDYExecutor(cfg, "POST", req.URL())
	if err != nil {
		return nil, fmt.Errorf("build pod exec for %s/%s: %w", namespace, podName, err)
	}
	var stdout, stderr bytes.Buffer
	if err := exec.StreamWithContext(ctx, remotecommand.StreamOptions{
		Stdout: &stdout,
		Stderr: &stderr,
	}); err != nil {
		return nil, fmt.Errorf("exec dd on %s/%s: %w (stderr=%s)", namespace, podName, err, strings.TrimSpace(stderr.String()))
	}
	out := stdout.Bytes()
	if int64(len(out)) != length {
		return nil, fmt.Errorf("reader payload length mismatch: expected=%d got=%d", length, len(out))
	}
	return out, nil
}

func waitReaderPodRunning(ctx context.Context, kube kubernetes.Interface, namespace, podName string) error {
	ticker := time.NewTicker(2 * time.Second)
	defer ticker.Stop()
	timeout := time.NewTimer(readerReadyTimeout)
	defer timeout.Stop()

	for {
		pod, err := kube.CoreV1().Pods(namespace).Get(ctx, podName, metav1.GetOptions{})
		if err == nil && pod.Status.Phase == corev1.PodRunning {
			return nil
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-timeout.C:
			return fmt.Errorf("wait reader pod running timed out for %s/%s", namespace, podName)
		case <-ticker.C:
		}
	}
}

func readerNames(namespace, snapshot string) (pvcName, podName string) {
	h := sha1.Sum([]byte(namespace + "/" + snapshot))
	suffix := hex.EncodeToString(h[:])[:10]
	return "snaplane-sr-" + suffix + "-pvc", "snaplane-sr-" + suffix + "-pod"
}

func loadKubeConfig() (*rest.Config, error) {
	if cfg, err := rest.InClusterConfig(); err == nil {
		return cfg, nil
	}
	return ctrlconfig.GetConfig()
}

func (c *snapshotMetadataCollector) SnapshotMetadataIteratorRecord(_ int, metadata smsiterator.IteratorMetadata) error {
	if metadata.VolumeCapacityBytes > c.volumeCapacity {
		c.volumeCapacity = metadata.VolumeCapacityBytes
	}
	for _, block := range metadata.BlockMetadata {
		if block == nil {
			continue
		}
		offset := block.GetByteOffset()
		length := block.GetSizeBytes()
		if offset < 0 || length <= 0 {
			continue
		}
		c.ranges = append(c.ranges, Range{
			Kind:   RangeKindData,
			Offset: offset,
			Length: length,
		})
	}
	return nil
}

func (c *snapshotMetadataCollector) SnapshotMetadataIteratorDone(_ int) error {
	return nil
}

func normalizeAndChunkRanges(in []Range, chunkSize int64) []Range {
	if len(in) == 0 {
		return nil
	}
	out := make([]Range, 0, len(in))
	for i := range in {
		if in[i].Length <= 0 {
			continue
		}
		out = append(out, in[i])
	}
	if len(out) == 0 {
		return nil
	}

	sort.Slice(out, func(i, j int) bool {
		if out[i].Offset == out[j].Offset {
			return out[i].Length < out[j].Length
		}
		return out[i].Offset < out[j].Offset
	})

	merged := make([]Range, 0, len(out))
	current := out[0]
	for i := 1; i < len(out); i++ {
		next := out[i]
		currentEnd := current.Offset + current.Length
		nextEnd := next.Offset + next.Length
		if next.Offset <= currentEnd {
			if nextEnd > currentEnd {
				current.Length = nextEnd - current.Offset
			}
			continue
		}
		merged = append(merged, current)
		current = next
	}
	merged = append(merged, current)

	if chunkSize <= 0 {
		return merged
	}
	chunked := make([]Range, 0, len(merged))
	for _, rg := range merged {
		remaining := rg.Length
		offset := rg.Offset
		for remaining > 0 {
			size := chunkSize
			if remaining < size {
				size = remaining
			}
			chunked = append(chunked, Range{
				Kind:   rg.Kind,
				Offset: offset,
				Length: size,
			})
			offset += size
			remaining -= size
		}
	}
	return chunked
}

func snapshotDataMode() string {
	mode := strings.TrimSpace(strings.ToLower(os.Getenv(EnvSnapshotDataMode)))
	if mode == SnapshotDataLive {
		return SnapshotDataLive
	}
	return SnapshotDataMock
}

func deterministicPayload(req ReadRequest) []byte {
	out := make([]byte, req.Length)
	seed := int64(len(req.BackupUID) + len(req.PVCName) + len(req.SnapshotName))
	for i := range out {
		out[i] = byte((req.Offset + int64(i) + seed) % 251)
	}
	return out
}

func strPtr(v string) *string { return &v }

func volumeModePtr(v corev1.PersistentVolumeMode) *corev1.PersistentVolumeMode { return &v }

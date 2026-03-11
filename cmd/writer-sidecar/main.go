package main

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"flag"
	"fmt"
	"net"
	"os"
	"os/signal"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"time"

	coordinationv1 "k8s.io/api/coordination/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/klog/v2"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"

	writerv1 "github.com/molpako/snaplane/api/proto/writer/v1"
	snaplanev1alpha1 "github.com/molpako/snaplane/api/v1alpha1"
	"github.com/molpako/snaplane/internal/writer"
)

const (
	defaultListenAddr       = ":9443"
	defaultRootDir          = "/var/backup"
	defaultHeartbeat        = 30 * time.Second
	defaultLeaseDurationSec = int32(120)
)

func main() {
	var (
		listenAddr             string
		rootDir                string
		heartbeat              time.Duration
		casMaintenanceInterval time.Duration
		tlsCertFile            string
		tlsKeyFile             string
		clientCAFile           string
		leaseNamespace         string
		nodeName               string
		podIP                  string
	)

	flag.StringVar(&listenAddr, "listen-address", defaultListenAddr, "gRPC listen address")
	flag.StringVar(&rootDir, "backup-root", defaultRootDir, "Host path root where backup data is written")
	flag.DurationVar(&heartbeat, "heartbeat-interval", defaultHeartbeat, "Lease heartbeat interval")
	flag.DurationVar(&casMaintenanceInterval, "cas-maintenance-interval", writer.DefaultCASMaintenanceInterval, "Interval for CAS repository GC/compaction scans; set to 0 to disable")
	flag.StringVar(&tlsCertFile, "tls-cert-file", "", "server TLS cert file")
	flag.StringVar(&tlsKeyFile, "tls-key-file", "", "server TLS key file")
	flag.StringVar(&clientCAFile, "client-ca-file", "", "client CA bundle file for mTLS")
	flag.StringVar(&leaseNamespace, "lease-namespace", envOrDefault("POD_NAMESPACE", "default"), "namespace for node writer leases")
	flag.StringVar(&nodeName, "node-name", envOrDefault("NODE_NAME", ""), "node name for heartbeat")
	flag.StringVar(&podIP, "pod-ip", envOrDefault("POD_IP", ""), "pod IP used in writer endpoint annotation")
	klog.InitFlags(nil)
	flag.Parse()

	if nodeName == "" {
		klog.Fatalf("node-name (or NODE_NAME) is required")
	}
	if podIP == "" {
		klog.Fatalf("pod-ip (or POD_IP) is required")
	}
	if tlsCertFile == "" || tlsKeyFile == "" || clientCAFile == "" {
		klog.Fatalf("tls-cert-file, tls-key-file and client-ca-file are required for mTLS")
	}

	serverTLS, err := buildServerTLSConfig(tlsCertFile, tlsKeyFile, clientCAFile)
	if err != nil {
		klog.Fatalf("failed to build server TLS config: %v", err)
	}

	lis, err := net.Listen("tcp", listenAddr)
	if err != nil {
		klog.Fatalf("failed to listen on %s: %v", listenAddr, err)
	}

	grpcServer := grpc.NewServer(grpc.Creds(credentials.NewTLS(serverTLS)))
	writerServer := writer.NewServer(rootDir)
	writerv1.RegisterLocalBackupWriterServer(grpcServer, writerServer)

	cfg, err := rest.InClusterConfig()
	if err != nil {
		klog.Fatalf("failed to build in-cluster config: %v", err)
	}
	kubeClient, err := kubernetes.NewForConfig(cfg)
	if err != nil {
		klog.Fatalf("failed to create kube client: %v", err)
	}

	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()

	go func() {
		if err := runHeartbeat(ctx, kubeClient, leaseNamespace, nodeName, podIP, listenAddr, rootDir, heartbeat); err != nil {
			klog.Errorf("heartbeat loop terminated: %v", err)
		}
	}()
	if os.Getenv(writer.EnvBackupWriteMode) == string(writer.WriteModeCAS) {
		go writer.StartCASMaintenanceLoop(ctx, rootDir, casMaintenanceInterval)
	}

	go func() {
		<-ctx.Done()
		grpcServer.GracefulStop()
	}()

	klog.Infof("writer sidecar listening on %s", listenAddr)
	if err := grpcServer.Serve(lis); err != nil && !errors.Is(err, grpc.ErrServerStopped) {
		klog.Fatalf("gRPC server stopped with error: %v", err)
	}
}

func envOrDefault(key, fallback string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return fallback
}

func buildServerTLSConfig(certFile, keyFile, caFile string) (*tls.Config, error) {
	serverCert, err := tls.LoadX509KeyPair(certFile, keyFile)
	if err != nil {
		return nil, fmt.Errorf("load key pair: %w", err)
	}
	caPEM, err := os.ReadFile(filepath.Clean(caFile))
	if err != nil {
		return nil, fmt.Errorf("read client ca: %w", err)
	}
	pool := x509.NewCertPool()
	if ok := pool.AppendCertsFromPEM(caPEM); !ok {
		return nil, fmt.Errorf("failed to parse client ca PEM")
	}

	return &tls.Config{
		MinVersion:   tls.VersionTLS12,
		Certificates: []tls.Certificate{serverCert},
		ClientAuth:   tls.RequireAndVerifyClientCert,
		ClientCAs:    pool,
	}, nil
}

func runHeartbeat(
	ctx context.Context,
	kubeClient kubernetes.Interface,
	leaseNamespace, nodeName, podIP, listenAddr, backupRoot string,
	interval time.Duration,
) error {
	if interval <= 0 {
		interval = defaultHeartbeat
	}

	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		if err := upsertNodeLease(ctx, kubeClient, leaseNamespace, nodeName, podIP, listenAddr, backupRoot); err != nil {
			klog.Errorf("failed to upsert lease heartbeat: %v", err)
		}

		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
		}
	}
}

func upsertNodeLease(
	ctx context.Context,
	kubeClient kubernetes.Interface,
	leaseNamespace, nodeName, podIP, listenAddr, backupRoot string,
) error {
	used, available, err := fsUsage(backupRoot)
	if err != nil {
		return err
	}

	endpoint := fmt.Sprintf("%s:%s", podIP, normalizePort(listenAddr))
	leaseName := writer.LeaseNameForNode(nodeName)
	now := metav1.MicroTime{Time: time.Now().UTC()}

	leases := kubeClient.CoordinationV1().Leases(leaseNamespace)
	lease, err := leases.Get(ctx, leaseName, metav1.GetOptions{})
	if err != nil {
		newLease := &coordinationv1.Lease{
			ObjectMeta: metav1.ObjectMeta{
				Name:      leaseName,
				Namespace: leaseNamespace,
				Annotations: map[string]string{
					snaplanev1alpha1.WriterEndpointAnnotationKey:      endpoint,
					snaplanev1alpha1.LeaseUsedBytesAnnotationKey:      strconv.FormatInt(used, 10),
					snaplanev1alpha1.LeaseAvailableBytesAnnotationKey: strconv.FormatInt(available, 10),
				},
			},
			Spec: coordinationv1.LeaseSpec{
				HolderIdentity:       ptr(nodeName),
				RenewTime:            &now,
				LeaseDurationSeconds: ptr(defaultLeaseDurationSec),
			},
		}
		if _, createErr := leases.Create(ctx, newLease, metav1.CreateOptions{}); createErr != nil {
			return fmt.Errorf("create lease: %w", createErr)
		}
		return nil
	}

	if lease.Annotations == nil {
		lease.Annotations = map[string]string{}
	}
	lease.Annotations[snaplanev1alpha1.WriterEndpointAnnotationKey] = endpoint
	lease.Annotations[snaplanev1alpha1.LeaseUsedBytesAnnotationKey] = strconv.FormatInt(used, 10)
	lease.Annotations[snaplanev1alpha1.LeaseAvailableBytesAnnotationKey] = strconv.FormatInt(available, 10)
	lease.Spec.HolderIdentity = ptr(nodeName)
	lease.Spec.RenewTime = &now
	lease.Spec.LeaseDurationSeconds = ptr(defaultLeaseDurationSec)
	if _, err := leases.Update(ctx, lease, metav1.UpdateOptions{}); err != nil {
		return fmt.Errorf("update lease: %w", err)
	}
	return nil
}

func fsUsage(root string) (used int64, available int64, err error) {
	var stat syscall.Statfs_t
	if err := syscall.Statfs(root, &stat); err != nil {
		return 0, 0, fmt.Errorf("statfs for %s: %w", root, err)
	}
	blockSize := int64(stat.Bsize)
	total := int64(stat.Blocks) * blockSize
	avail := int64(stat.Bavail) * blockSize
	used = total - avail
	return used, avail, nil
}

func normalizePort(listenAddr string) string {
	if strings.HasPrefix(listenAddr, ":") {
		return strings.TrimPrefix(listenAddr, ":")
	}
	_, port, err := net.SplitHostPort(listenAddr)
	if err != nil {
		return "9443"
	}
	return port
}

func ptr[T any](value T) *T {
	return &value
}

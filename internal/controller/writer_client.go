package controller

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"os"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"

	writerv1 "github.com/molpako/snaplane/api/proto/writer/v1"
)

const (
	envWriterClientCertFile = "WRITER_CLIENT_CERT_FILE"
	envWriterClientKeyFile  = "WRITER_CLIENT_KEY_FILE"
	envWriterClientCAFile   = "WRITER_CLIENT_CA_FILE"
)

type FrameProducer func(send func(frame *writerv1.WriteFrame) error) error

type WriterClient interface {
	StartWrite(ctx context.Context, req *writerv1.StartWriteRequest) (*writerv1.StartWriteResponse, error)
	WriteFrames(ctx context.Context, sessionID string, producer FrameProducer) (*writerv1.WriteFramesSummary, error)
	CommitWrite(ctx context.Context, req *writerv1.CommitWriteRequest) (*writerv1.CommitWriteResponse, error)
	AbortWrite(ctx context.Context, req *writerv1.AbortWriteRequest) error
	Close() error
}

type WriterClientFactory interface {
	New(ctx context.Context, endpoint string) (WriterClient, error)
}

type GRPCWriterClientFactory struct {
	ClientCertFile string
	ClientKeyFile  string
	ClientCAFile   string
	DialTimeout    time.Duration
}

func NewGRPCWriterClientFactoryFromEnv() *GRPCWriterClientFactory {
	return &GRPCWriterClientFactory{
		ClientCertFile: os.Getenv(envWriterClientCertFile),
		ClientKeyFile:  os.Getenv(envWriterClientKeyFile),
		ClientCAFile:   os.Getenv(envWriterClientCAFile),
		DialTimeout:    10 * time.Second,
	}
}

func (f *GRPCWriterClientFactory) New(ctx context.Context, endpoint string) (WriterClient, error) {
	tlsConfig, err := f.buildClientTLSConfig()
	if err != nil {
		return nil, err
	}
	if f.DialTimeout > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, f.DialTimeout)
		defer cancel()
	}

	conn, err := grpc.DialContext(ctx, endpoint,
		grpc.WithTransportCredentials(credentials.NewTLS(tlsConfig)),
		grpc.WithBlock(),
	)
	if err != nil {
		return nil, err
	}
	return &grpcWriterClient{conn: conn, client: writerv1.NewLocalBackupWriterClient(conn)}, nil
}

func (f *GRPCWriterClientFactory) buildClientTLSConfig() (*tls.Config, error) {
	if f.ClientCertFile == "" || f.ClientKeyFile == "" || f.ClientCAFile == "" {
		return nil, fmt.Errorf("%s, %s and %s must be set", envWriterClientCertFile, envWriterClientKeyFile, envWriterClientCAFile)
	}

	clientCert, err := tls.LoadX509KeyPair(f.ClientCertFile, f.ClientKeyFile)
	if err != nil {
		return nil, fmt.Errorf("load client key pair: %w", err)
	}
	caPEM, err := os.ReadFile(f.ClientCAFile)
	if err != nil {
		return nil, fmt.Errorf("read client ca bundle: %w", err)
	}
	pool := x509.NewCertPool()
	if ok := pool.AppendCertsFromPEM(caPEM); !ok {
		return nil, fmt.Errorf("parse client ca bundle")
	}

	return &tls.Config{
		MinVersion:   tls.VersionTLS12,
		Certificates: []tls.Certificate{clientCert},
		RootCAs:      pool,
		// Sidecars are reached by PodIP endpoint advertised in Lease.
		// Verify the issuing CA chain while skipping hostname enforcement.
		InsecureSkipVerify: true, //nolint:gosec
		VerifyPeerCertificate: func(rawCerts [][]byte, _ [][]*x509.Certificate) error {
			if len(rawCerts) == 0 {
				return fmt.Errorf("missing peer certificate")
			}
			leaf, err := x509.ParseCertificate(rawCerts[0])
			if err != nil {
				return fmt.Errorf("parse peer leaf certificate: %w", err)
			}

			intermediates := x509.NewCertPool()
			for i := 1; i < len(rawCerts); i++ {
				intermediate, parseErr := x509.ParseCertificate(rawCerts[i])
				if parseErr != nil {
					return fmt.Errorf("parse peer intermediate certificate: %w", parseErr)
				}
				intermediates.AddCert(intermediate)
			}

			_, err = leaf.Verify(x509.VerifyOptions{
				Roots:         pool,
				Intermediates: intermediates,
				KeyUsages:     []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
			})
			if err != nil {
				return fmt.Errorf("verify peer certificate chain: %w", err)
			}
			return nil
		},
	}, nil
}

type grpcWriterClient struct {
	conn   *grpc.ClientConn
	client writerv1.LocalBackupWriterClient
}

func (c *grpcWriterClient) StartWrite(ctx context.Context, req *writerv1.StartWriteRequest) (*writerv1.StartWriteResponse, error) {
	return c.client.StartWrite(ctx, req)
}

func (c *grpcWriterClient) WriteFrames(ctx context.Context, sessionID string, producer FrameProducer) (*writerv1.WriteFramesSummary, error) {
	if producer == nil {
		return nil, fmt.Errorf("frame producer is required")
	}
	stream, err := c.client.WriteFrames(ctx)
	if err != nil {
		return nil, err
	}

	send := func(frame *writerv1.WriteFrame) error {
		if frame == nil {
			return fmt.Errorf("frame is nil")
		}
		if frame.SessionId == "" {
			frame.SessionId = sessionID
		}
		return stream.Send(frame)
	}

	if err := producer(send); err != nil {
		return nil, err
	}
	return stream.CloseAndRecv()
}

func (c *grpcWriterClient) CommitWrite(ctx context.Context, req *writerv1.CommitWriteRequest) (*writerv1.CommitWriteResponse, error) {
	return c.client.CommitWrite(ctx, req)
}

func (c *grpcWriterClient) AbortWrite(ctx context.Context, req *writerv1.AbortWriteRequest) error {
	_, err := c.client.AbortWrite(ctx, req)
	return err
}

func (c *grpcWriterClient) Close() error {
	return c.conn.Close()
}

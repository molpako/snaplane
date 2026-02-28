package controller

import (
	"context"
	"crypto/rand"
	"crypto/rsa"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"math/big"
	"net"
	"os"
	"path/filepath"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"

	writerv1 "github.com/molpako/snaplane/api/proto/writer/v1"
	"github.com/molpako/snaplane/internal/writer"
)

type mTLSFixture struct {
	clientCertFile string
	clientKeyFile  string
	caCertFile     string
	wrongCACert    string
	serverCertFile string
	serverKeyFile  string
}

func TestGRPCWriterClientFactoryConnectsWithValidMTLSCredentials(t *testing.T) {
	fixture := createMTLSFixture(t)
	endpoint, stop := startMTLSWriterServer(t, fixture.serverCertFile, fixture.serverKeyFile, fixture.caCertFile)
	defer stop()

	factory := &GRPCWriterClientFactory{
		ClientCertFile: fixture.clientCertFile,
		ClientKeyFile:  fixture.clientKeyFile,
		ClientCAFile:   fixture.caCertFile,
		DialTimeout:    3 * time.Second,
	}
	client, err := factory.New(context.Background(), endpoint)
	if err != nil {
		t.Fatalf("factory.New failed: %v", err)
	}
	defer func() {
		_ = client.Close()
	}()

	resp, err := client.StartWrite(context.Background(), &writerv1.StartWriteRequest{
		BackupUid:  "backup-uid-mtls",
		BackupName: "backup-mtls",
		Namespace:  "default",
		PvcName:    "pvc-a",
	})
	if err != nil {
		t.Fatalf("StartWrite failed over mTLS: %v", err)
	}
	if resp.GetSessionId() != "backup-uid-mtls" {
		t.Fatalf("unexpected session id: %q", resp.GetSessionId())
	}
}

func TestGRPCWriterClientFactoryRejectsWrongCA(t *testing.T) {
	fixture := createMTLSFixture(t)
	endpoint, stop := startMTLSWriterServer(t, fixture.serverCertFile, fixture.serverKeyFile, fixture.caCertFile)
	defer stop()

	factory := &GRPCWriterClientFactory{
		ClientCertFile: fixture.clientCertFile,
		ClientKeyFile:  fixture.clientKeyFile,
		ClientCAFile:   fixture.wrongCACert,
		DialTimeout:    3 * time.Second,
	}

	client, err := factory.New(context.Background(), endpoint)
	if err == nil {
		_ = client.Close()
		t.Fatalf("expected connection failure with wrong CA")
	}
}

func TestMTLSServerRejectsConnectionWithoutClientCertificate(t *testing.T) {
	fixture := createMTLSFixture(t)
	endpoint, stop := startMTLSWriterServer(t, fixture.serverCertFile, fixture.serverKeyFile, fixture.caCertFile)
	defer stop()

	caPEM, err := os.ReadFile(filepath.Clean(fixture.caCertFile))
	if err != nil {
		t.Fatalf("read ca cert: %v", err)
	}
	roots := x509.NewCertPool()
	if ok := roots.AppendCertsFromPEM(caPEM); !ok {
		t.Fatalf("append ca cert failed")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	conn, err := grpc.DialContext(
		ctx,
		endpoint,
		grpc.WithTransportCredentials(credentials.NewTLS(&tls.Config{
			MinVersion: tls.VersionTLS12,
			RootCAs:    roots,
			ServerName: "localhost",
		})),
		grpc.WithBlock(),
	)
	if err == nil {
		_ = conn.Close()
		t.Fatalf("expected mTLS handshake failure without client certificate")
	}
}

func createMTLSFixture(t *testing.T) mTLSFixture {
	t.Helper()

	dir := t.TempDir()
	caCertPEM, _, caCert, caKey := createCA(t, "writer-ca")
	serverCertPEM, serverKeyPEM := createLeafCert(t, caCert, caKey, certProfile{
		commonName: "writer-sidecar",
		dnsNames:   []string{"localhost"},
		ipAddrs:    []net.IP{net.ParseIP("127.0.0.1")},
		serverAuth: true,
	})
	clientCertPEM, clientKeyPEM := createLeafCert(t, caCert, caKey, certProfile{
		commonName: "writer-client",
		clientAuth: true,
	})
	wrongCAPEM, _, _, _ := createCA(t, "wrong-ca")

	return mTLSFixture{
		clientCertFile: writePEMFile(t, dir, "client.crt", clientCertPEM),
		clientKeyFile:  writePEMFile(t, dir, "client.key", clientKeyPEM),
		caCertFile:     writePEMFile(t, dir, "ca.crt", caCertPEM),
		wrongCACert:    writePEMFile(t, dir, "wrong-ca.crt", wrongCAPEM),
		serverCertFile: writePEMFile(t, dir, "server.crt", serverCertPEM),
		serverKeyFile:  writePEMFile(t, dir, "server.key", serverKeyPEM),
	}
}

func startMTLSWriterServer(t *testing.T, serverCertFile, serverKeyFile, clientCAFile string) (string, func()) {
	t.Helper()

	serverCert, err := tls.LoadX509KeyPair(serverCertFile, serverKeyFile)
	if err != nil {
		t.Fatalf("load server key pair: %v", err)
	}
	clientCAPEM, err := os.ReadFile(filepath.Clean(clientCAFile))
	if err != nil {
		t.Fatalf("read client ca: %v", err)
	}
	clientCAs := x509.NewCertPool()
	if ok := clientCAs.AppendCertsFromPEM(clientCAPEM); !ok {
		t.Fatalf("append client ca failed")
	}

	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	server := grpc.NewServer(grpc.Creds(credentials.NewTLS(&tls.Config{
		MinVersion:   tls.VersionTLS12,
		Certificates: []tls.Certificate{serverCert},
		ClientAuth:   tls.RequireAndVerifyClientCert,
		ClientCAs:    clientCAs,
	})))
	writerv1.RegisterLocalBackupWriterServer(server, writer.NewServer(t.TempDir()))

	go func() {
		_ = server.Serve(lis)
	}()

	return lis.Addr().String(), func() {
		server.Stop()
		_ = lis.Close()
	}
}

type certProfile struct {
	commonName string
	dnsNames   []string
	ipAddrs    []net.IP
	serverAuth bool
	clientAuth bool
}

func createCA(t *testing.T, commonName string) ([]byte, []byte, *x509.Certificate, *rsa.PrivateKey) {
	t.Helper()

	key, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatalf("generate ca key: %v", err)
	}
	serial, err := rand.Int(rand.Reader, big.NewInt(1<<62))
	if err != nil {
		t.Fatalf("generate ca serial: %v", err)
	}
	template := &x509.Certificate{
		SerialNumber: serial,
		Subject: pkix.Name{
			CommonName: commonName,
		},
		NotBefore:             time.Now().Add(-time.Hour),
		NotAfter:              time.Now().Add(24 * time.Hour),
		KeyUsage:              x509.KeyUsageCertSign | x509.KeyUsageCRLSign,
		BasicConstraintsValid: true,
		IsCA:                  true,
	}
	der, err := x509.CreateCertificate(rand.Reader, template, template, &key.PublicKey, key)
	if err != nil {
		t.Fatalf("create ca cert: %v", err)
	}
	cert, err := x509.ParseCertificate(der)
	if err != nil {
		t.Fatalf("parse ca cert: %v", err)
	}

	return pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der}),
		pem.EncodeToMemory(&pem.Block{Type: "RSA PRIVATE KEY", Bytes: x509.MarshalPKCS1PrivateKey(key)}),
		cert, key
}

func createLeafCert(t *testing.T, caCert *x509.Certificate, caKey *rsa.PrivateKey, profile certProfile) ([]byte, []byte) {
	t.Helper()

	key, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatalf("generate leaf key: %v", err)
	}
	serial, err := rand.Int(rand.Reader, big.NewInt(1<<62))
	if err != nil {
		t.Fatalf("generate leaf serial: %v", err)
	}

	keyUsages := []x509.ExtKeyUsage{}
	if profile.serverAuth {
		keyUsages = append(keyUsages, x509.ExtKeyUsageServerAuth)
	}
	if profile.clientAuth {
		keyUsages = append(keyUsages, x509.ExtKeyUsageClientAuth)
	}

	template := &x509.Certificate{
		SerialNumber: serial,
		Subject: pkix.Name{
			CommonName: profile.commonName,
		},
		NotBefore:    time.Now().Add(-time.Hour),
		NotAfter:     time.Now().Add(24 * time.Hour),
		KeyUsage:     x509.KeyUsageDigitalSignature | x509.KeyUsageKeyEncipherment,
		ExtKeyUsage:  keyUsages,
		DNSNames:     profile.dnsNames,
		IPAddresses:  profile.ipAddrs,
		IsCA:         false,
		SubjectKeyId: []byte{1, 2, 3, 4},
	}

	der, err := x509.CreateCertificate(rand.Reader, template, caCert, &key.PublicKey, caKey)
	if err != nil {
		t.Fatalf("create leaf cert: %v", err)
	}
	return pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der}),
		pem.EncodeToMemory(&pem.Block{Type: "RSA PRIVATE KEY", Bytes: x509.MarshalPKCS1PrivateKey(key)})
}

func writePEMFile(t *testing.T, dir, name string, data []byte) string {
	t.Helper()
	path := filepath.Join(dir, name)
	if err := os.WriteFile(path, data, 0o600); err != nil {
		t.Fatalf("write %s: %v", name, err)
	}
	return path
}

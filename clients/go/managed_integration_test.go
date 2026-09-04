package kuttidb

import (
	"net"
	"os"
	"testing"
	"time"
)

// This is opt-in because it launches a real local server. The project-level
// lifecycle harness enables it with KUTTIDB_MANAGED_INTEGRATION=1.
func TestManagedLifecycleIntegration(t *testing.T) {
	if os.Getenv("KUTTIDB_MANAGED_INTEGRATION") != "1" {
		t.Skip("managed integration is opt-in")
	}
	executable := os.Getenv("KUTTIDB_SERVER")
	if executable == "" {
		t.Fatal("KUTTIDB_SERVER is required for managed integration")
	}
	dataDir := t.TempDir()
	if err := os.Chmod(dataDir, 0700); err != nil {
		t.Fatalf("secure temp directory: %v", err)
	}
	client, err := NewManaged(ManagedOptions{
		DataDir:        dataDir,
		Executable:     executable,
		IdleTimeout:    250 * time.Millisecond,
		StartupTimeout: 5 * time.Second,
	})
	if err != nil {
		t.Fatalf("NewManaged: %v", err)
	}
	if err := client.Put("managed-go", []byte("value")); err != nil {
		t.Fatalf("Put: %v", err)
	}
	got, err := client.Get("managed-go")
	if err != nil || string(got) != "value" {
		t.Fatalf("Get = %q, %v", got, err)
	}
	client.Close()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("reserve loopback port: %v", err)
	}
	port := listener.Addr().(*net.TCPAddr).Port
	listener.Close()
	tcpDir := t.TempDir()
	if err := os.Chmod(tcpDir, 0700); err != nil {
		t.Fatalf("secure TCP temp directory: %v", err)
	}
	tcpClient, err := NewManaged(ManagedOptions{
		DataDir:        tcpDir,
		Executable:     executable,
		Transport:      "tcp",
		Host:           "127.0.0.1",
		Port:           port,
		IdleTimeout:    250 * time.Millisecond,
		StartupTimeout: 5 * time.Second,
	})
	if err != nil {
		t.Fatalf("NewManaged TCP: %v", err)
	}
	defer tcpClient.Close()
	if err := tcpClient.Put("managed-go-tcp", []byte("value")); err != nil {
		t.Fatalf("TCP Put: %v", err)
	}
	got, err = tcpClient.Get("managed-go-tcp")
	if err != nil || string(got) != "value" {
		t.Fatalf("TCP Get = %q, %v", got, err)
	}
}

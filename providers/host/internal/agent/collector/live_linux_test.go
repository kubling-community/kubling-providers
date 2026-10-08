//go:build linux

package collector

import (
	"context"
	"os"
	"testing"
)

func TestLiveLinuxMemoryCollection(t *testing.T) {
	if os.Getenv("KUBLING_HOST_LIVE_TEST") != "1" {
		t.Skip("set KUBLING_HOST_LIVE_TEST=1 to inspect the local Linux host")
	}

	memory, err := New(Config{}).Memory(context.Background())
	if err != nil {
		t.Fatalf("Memory() error = %v", err)
	}
	if len(memory.Rows) != 1 || memory.Rows[0].TotalBytes == 0 {
		t.Fatalf("Memory() rows = %#v", memory.Rows)
	}
	t.Logf(
		"total_bytes=%d available_bytes=%d warnings=%d",
		memory.Rows[0].TotalBytes,
		memory.Rows[0].AvailableBytes,
		len(memory.Warnings),
	)
}

func TestLiveLinuxCollection(t *testing.T) {
	if os.Getenv("KUBLING_HOST_LIVE_TEST") != "1" {
		t.Skip("set KUBLING_HOST_LIVE_TEST=1 to inspect the local Linux host")
	}

	ctx := context.Background()
	collector := New(Config{})

	host, err := collector.Host(ctx)
	if err != nil {
		t.Fatalf("Host() error = %v", err)
	}
	if len(host.Rows) != 1 {
		t.Fatalf("Host() rows = %d, want 1", len(host.Rows))
	}

	cpuInfo, err := collector.CPUInfo(ctx)
	if err != nil {
		t.Fatalf("CPUInfo() error = %v", err)
	}
	cpuTimes, err := collector.CPUTimes(ctx)
	if err != nil {
		t.Fatalf("CPUTimes() error = %v", err)
	}
	memory, err := collector.Memory(ctx)
	if err != nil {
		t.Fatalf("Memory() error = %v", err)
	}
	filesystems, err := collector.Filesystems(ctx)
	if err != nil {
		t.Fatalf("Filesystems() error = %v", err)
	}
	interfaces, err := collector.NetworkInterfaces(ctx)
	if err != nil {
		t.Fatalf("NetworkInterfaces() error = %v", err)
	}
	networkIO, err := collector.NetworkIO(ctx)
	if err != nil {
		t.Fatalf("NetworkIO() error = %v", err)
	}
	processes, err := collector.Processes(ctx)
	if err != nil {
		t.Fatalf("Processes() error = %v", err)
	}

	t.Logf(
		"host=%s cpu_info=%d cpu_times=%d memory=%d filesystems=%d interfaces=%d network_io=%d processes=%d warnings=%d",
		host.Rows[0].Hostname,
		len(cpuInfo.Rows),
		len(cpuTimes.Rows),
		len(memory.Rows),
		len(filesystems.Rows),
		len(interfaces.Rows),
		len(networkIO.Rows),
		len(processes.Rows),
		len(memory.Warnings)+len(filesystems.Warnings)+len(processes.Warnings),
	)
}

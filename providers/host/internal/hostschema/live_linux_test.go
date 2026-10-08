//go:build linux

package hostschema

import (
	"context"
	"os"
	"testing"

	"github.com/kubling-community/kubling-providers/providers/host/internal/agent/collector"
	providerv1 "github.com/kubling-community/kubling-providers/sdk-go/kubling/provider/v1"
)

func TestLiveLinuxSchemaEncoding(t *testing.T) {
	if os.Getenv("KUBLING_HOST_LIVE_TEST") != "1" {
		t.Skip("set KUBLING_HOST_LIVE_TEST=1 to inspect the local Linux host")
	}

	ctx := context.Background()
	source := collector.New(collector.Config{})
	encoded := make(map[string][]*providerv1.Tuple)

	host, err := source.Host(ctx)
	if err != nil {
		t.Fatalf("Host() error = %v", err)
	}
	encoded[HostFactsTable], err = EncodeHostFactsRows(host, mustLocalColumns(t, HostFactsTable))
	if err != nil {
		t.Fatalf("EncodeHostFactsRows() error = %v", err)
	}

	cpuInfo, err := source.CPUInfo(ctx)
	if err != nil {
		t.Fatalf("CPUInfo() error = %v", err)
	}
	encoded[CPUInfoTable], err = EncodeCPUInfoRows(cpuInfo, mustLocalColumns(t, CPUInfoTable))
	if err != nil {
		t.Fatalf("EncodeCPUInfoRows() error = %v", err)
	}

	cpuTimes, err := source.CPUTimes(ctx)
	if err != nil {
		t.Fatalf("CPUTimes() error = %v", err)
	}
	encoded[CPUTimesTable], err = EncodeCPUTimesRows(cpuTimes, mustLocalColumns(t, CPUTimesTable))
	if err != nil {
		t.Fatalf("EncodeCPUTimesRows() error = %v", err)
	}

	memory, err := source.Memory(ctx)
	if err != nil {
		t.Fatalf("Memory() error = %v", err)
	}
	encoded[MemoryTable], err = EncodeMemoryRows(memory, mustLocalColumns(t, MemoryTable))
	if err != nil {
		t.Fatalf("EncodeMemoryRows() error = %v", err)
	}

	filesystems, err := source.Filesystems(ctx)
	if err != nil {
		t.Fatalf("Filesystems() error = %v", err)
	}
	encoded[FilesystemsTable], err = EncodeFilesystemRows(filesystems, mustLocalColumns(t, FilesystemsTable))
	if err != nil {
		t.Fatalf("EncodeFilesystemRows() error = %v", err)
	}

	interfaces, err := source.NetworkInterfaces(ctx)
	if err != nil {
		t.Fatalf("NetworkInterfaces() error = %v", err)
	}
	encoded[NetworkInterfacesTable], err = EncodeNetworkInterfaceRows(interfaces, mustLocalColumns(t, NetworkInterfacesTable))
	if err != nil {
		t.Fatalf("EncodeNetworkInterfaceRows() error = %v", err)
	}

	networkIO, err := source.NetworkIO(ctx)
	if err != nil {
		t.Fatalf("NetworkIO() error = %v", err)
	}
	encoded[NetworkIOTable], err = EncodeNetworkIORows(networkIO, mustLocalColumns(t, NetworkIOTable))
	if err != nil {
		t.Fatalf("EncodeNetworkIORows() error = %v", err)
	}

	processes, err := source.Processes(ctx)
	if err != nil {
		t.Fatalf("Processes() error = %v", err)
	}
	encoded[ProcessesTable], err = EncodeProcessRows(processes, mustLocalColumns(t, ProcessesTable))
	if err != nil {
		t.Fatalf("EncodeProcessRows() error = %v", err)
	}

	for table, rows := range encoded {
		t.Logf("table=%s rows=%d", table, len(rows))
	}
}

func mustLocalColumns(t *testing.T, table string) []string {
	t.Helper()
	columns, ok := LocalColumns(table)
	if !ok {
		t.Fatalf("LocalColumns(%q) not found", table)
	}
	return columns
}

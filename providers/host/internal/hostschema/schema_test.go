package hostschema

import (
	"math"
	"reflect"
	"testing"
	"time"

	kublingv1 "github.com/kubling-community/kubling-grpc/sdk-go/kubling/v1"
	"github.com/kubling-community/kubling-providers/providers/host/internal/agent/collector"
	gatewaypb "github.com/kubling-community/kubling-providers/providers/host/internal/gatewaypb"
	providerv1 "github.com/kubling-community/kubling-providers/sdk-go/kubling/provider/v1"
)

func TestMetadataDeclaresCompleteSchemaAndStableKeys(t *testing.T) {
	expectedKeys := map[string][]string{
		HostTable:              {NamespaceColumn, HostIDColumn},
		HostFactsTable:         {NamespaceColumn, HostIDColumn},
		CPUInfoTable:           {NamespaceColumn, HostIDColumn, "logical_id"},
		CPUTimesTable:          {NamespaceColumn, HostIDColumn, "cpu"},
		MemoryTable:            {NamespaceColumn, HostIDColumn},
		FilesystemsTable:       {NamespaceColumn, HostIDColumn, "mountpoint", "device"},
		NetworkInterfacesTable: {NamespaceColumn, HostIDColumn, "name"},
		NetworkIOTable:         {NamespaceColumn, HostIDColumn, "name"},
		ProcessesTable:         {NamespaceColumn, HostIDColumn, "pid", "created_at"},
	}
	metadata := Metadata()
	if len(metadata.GetTables()) != len(expectedKeys) {
		t.Fatalf("tables = %d, want %d", len(metadata.GetTables()), len(expectedKeys))
	}
	for _, table := range metadata.GetTables() {
		expected, exists := expectedKeys[table.GetName()]
		if !exists {
			t.Fatalf("unexpected table %q", table.GetName())
		}
		identifier := table.GetColumns()[len(table.GetColumns())-1]
		if identifier.GetName() != IdentifierColumn ||
			identifier.GetStableKey().GetFormat() != providerv1.StableKeyFormat_STABLE_KEY_FORMAT_VAL_PK_V1 ||
			!reflect.DeepEqual(identifier.GetStableKey().GetColumns(), expected) {
			t.Fatalf("table %s stable identifier = %v, want %v", table.GetName(), identifier, expected)
		}
		for index := 0; index < 3; index++ {
			if table.GetColumns()[index].GetSearchability() != providerv1.ColumnSearchability_COLUMN_SEARCHABILITY_EQUALITY {
				t.Fatalf("table %s access field %d is not equality searchable", table.GetName(), index)
			}
		}
		if table.GetName() == HostTable {
			if len(table.GetAccessPatterns()) != 0 {
				t.Fatalf("HOST access patterns = %v, want none", table.GetAccessPatterns())
			}
			continue
		}
		wantPatternColumns := []string{NamespaceColumn, HostIDColumn, HostnameColumn}
		if len(table.GetAccessPatterns()) != len(wantPatternColumns) {
			t.Fatalf(
				"table %s access patterns = %v, want alternatives for %v",
				table.GetName(),
				table.GetAccessPatterns(),
				wantPatternColumns,
			)
		}
		for index, pattern := range table.GetAccessPatterns() {
			if !reflect.DeepEqual(pattern.GetColumns(), []string{wantPatternColumns[index]}) {
				t.Fatalf(
					"table %s access pattern %d columns = %v, want %v",
					table.GetName(),
					index,
					pattern.GetColumns(),
					[]string{wantPatternColumns[index]},
				)
			}
		}
	}
}

func TestMetadataOmitsRoutingRequirementsWhenUnboundedFanoutIsAllowed(t *testing.T) {
	metadata := MetadataWithOptions(MetadataOptions{AllowUnboundedFanout: true})
	for _, table := range metadata.GetTables() {
		if len(table.GetAccessPatterns()) != 0 {
			t.Fatalf("table %s access patterns = %v, want none", table.GetName(), table.GetAccessPatterns())
		}
	}
}

func TestMetadataDeclaresStringArrayDimensions(t *testing.T) {
	metadata := Metadata()
	for _, table := range metadata.GetTables() {
		for _, column := range table.GetColumns() {
			if column.GetType() != kublingv1.ValueType_VALUE_TYPE_ARRAY {
				continue
			}
			descriptor := column.GetTypeDescriptor()
			if descriptor.GetType() != kublingv1.ValueType_VALUE_TYPE_ARRAY ||
				descriptor.GetElementType().GetType() != kublingv1.ValueType_VALUE_TYPE_STRING {
				t.Fatalf("%s.%s descriptor = %v, want ARRAY<STRING>", table.GetName(), column.GetName(), descriptor)
			}
		}
	}
}

func TestAgentTableMappingsAndLocalColumns(t *testing.T) {
	tests := map[string]gatewaypb.HostTable{
		HostFactsTable:         gatewaypb.HostTable_HOST_TABLE_FACTS,
		CPUInfoTable:           gatewaypb.HostTable_HOST_TABLE_CPU_INFO,
		CPUTimesTable:          gatewaypb.HostTable_HOST_TABLE_CPU_TIMES,
		MemoryTable:            gatewaypb.HostTable_HOST_TABLE_MEMORY,
		FilesystemsTable:       gatewaypb.HostTable_HOST_TABLE_FILESYSTEMS,
		NetworkInterfacesTable: gatewaypb.HostTable_HOST_TABLE_NETWORK_INTERFACES,
		NetworkIOTable:         gatewaypb.HostTable_HOST_TABLE_NETWORK_IO,
		ProcessesTable:         gatewaypb.HostTable_HOST_TABLE_PROCESSES,
	}
	for name, gatewayTable := range tests {
		t.Run(name, func(t *testing.T) {
			mapped, ok := GatewayTable(name)
			if !ok || mapped != gatewayTable {
				t.Fatalf("GatewayTable() = %v, %v; want %v, true", mapped, ok, gatewayTable)
			}
			mappedName, ok := TableName(gatewayTable)
			if !ok || mappedName != name {
				t.Fatalf("TableName() = %q, %v; want %q, true", mappedName, ok, name)
			}
			columns, ok := LocalColumns(name)
			if !ok || len(columns) == 0 {
				t.Fatalf("LocalColumns() = %v, %v", columns, ok)
			}
			for _, column := range columns {
				if column == NamespaceColumn || column == HostIDColumn || column == HostnameColumn {
					t.Fatalf("LocalColumns() contains provider-owned field %q", column)
				}
			}
		})
	}
	if _, ok := GatewayTable(HostTable); ok {
		t.Fatal("HOST unexpectedly maps to an agent table")
	}
}

func TestEncodersCoverEveryAgentTable(t *testing.T) {
	at := time.Date(2026, time.October, 8, 10, 11, 12, 13, time.UTC)
	u64 := uint64(25)
	i32 := int32(7)
	f64 := 2.5
	text := "value"

	hostFacts, err := EncodeHostFactsRows(
		collector.Observation[collector.HostRow]{ObservedAt: at, Rows: []collector.HostRow{{
			Hostname: "node-a", OS: "linux", UptimeSeconds: 10, ProcessCount: 3,
		}}},
		[]string{"observed_at", "reported_hostname", "booted_at", "uptime_seconds"},
	)
	assertTupleCount(t, HostFactsTable, hostFacts, err, 1)

	cpuInfo, err := EncodeCPUInfoRows(
		collector.Observation[collector.CPUInfoRow]{ObservedAt: at, Rows: []collector.CPUInfoRow{{
			LogicalID: 2, MHz: 3200, Flags: []string{"sse", "avx"},
		}}},
		[]string{"logical_id", "mhz", "flags"},
	)
	cpuInfo = assertTupleCount(t, CPUInfoTable, cpuInfo, err, 1)
	if got := cpuInfo[0].GetValues()[2].GetArrayValue().GetElements(); len(got) != 2 || got[0].GetStringValue() != "sse" || got[1].GetStringValue() != "avx" {
		t.Fatalf("CPU_INFO flags = %v", got)
	}

	cpuTimes, err := EncodeCPUTimesRows(
		collector.Observation[collector.CPUTimesRow]{ObservedAt: at, Rows: []collector.CPUTimesRow{{
			CPU: "cpu0", User: 1.5, System: 0.5,
		}}},
		[]string{"cpu", "user_seconds", "system_seconds"},
	)
	assertTupleCount(t, CPUTimesTable, cpuTimes, err, 1)

	memory, err := EncodeMemoryRows(
		collector.Observation[collector.MemoryRow]{ObservedAt: at, Rows: []collector.MemoryRow{{
			TotalBytes: ^uint64(0), SwapTotalBytes: &u64,
		}}},
		[]string{"observed_at", "total_bytes", "swap_total_bytes", "swap_used_bytes"},
	)
	memory = assertTupleCount(t, MemoryTable, memory, err, 1)
	if values := memory[0].GetValues(); values[0].GetTimestampValue() != "2026-10-08T10:11:12.000000013" ||
		values[1].GetBigintegerValue() != "18446744073709551615" ||
		values[2].GetBigintegerValue() != "25" || values[3].GetNullValue() == nil {
		t.Fatalf("MEMORY values = %v", values)
	}

	filesystems, err := EncodeFilesystemRows(
		collector.Observation[collector.FilesystemRow]{ObservedAt: at, Rows: []collector.FilesystemRow{{
			Device: "/dev/sda1", Mountpoint: "/", Options: []string{"rw"}, TotalBytes: &u64,
		}}},
		[]string{"device", "mountpoint", "options", "total_bytes", "free_bytes"},
	)
	filesystems = assertTupleCount(t, FilesystemsTable, filesystems, err, 1)
	if filesystems[0].GetValues()[4].GetNullValue() == nil {
		t.Fatalf("FILESYSTEMS nullable usage = %v", filesystems[0].GetValues()[4])
	}

	interfaces, err := EncodeNetworkInterfaceRows(
		collector.Observation[collector.NetworkInterfaceRow]{ObservedAt: at, Rows: []collector.NetworkInterfaceRow{{
			Index: 2, Name: "eth0", MTU: 1500, Addresses: []string{"192.0.2.1/24"},
		}}},
		[]string{"interface_index", "name", "mtu", "addresses"},
	)
	interfaces = assertTupleCount(t, NetworkInterfacesTable, interfaces, err, 1)
	if interfaces[0].GetValues()[0].GetLongValue() != 2 ||
		interfaces[0].GetValues()[3].GetArrayValue().GetElements()[0].GetStringValue() != "192.0.2.1/24" {
		t.Fatalf("NETWORK_INTERFACES values = %v", interfaces[0].GetValues())
	}

	networkIO, err := EncodeNetworkIORows(
		collector.Observation[collector.NetworkIORow]{ObservedAt: at, Rows: []collector.NetworkIORow{{
			Name: "eth0", BytesReceived: 42,
		}}},
		[]string{"name", "bytes_received"},
	)
	assertTupleCount(t, NetworkIOTable, networkIO, err, 1)

	processes, err := EncodeProcessRows(
		collector.Observation[collector.ProcessRow]{ObservedAt: at, Rows: []collector.ProcessRow{{
			PID: 12, CreatedAt: at, ParentPID: &i32, Name: &text, Statuses: []string{"running"},
			ResidentMemoryBytes: &u64, CPUUserSeconds: &f64,
		}}},
		[]string{"pid", "created_at", "parent_pid", "name", "statuses", "resident_memory_bytes", "cpu_user_seconds", "cpu_system_seconds"},
	)
	processes = assertTupleCount(t, ProcessesTable, processes, err, 1)
	if processes[0].GetValues()[7].GetNullValue() == nil {
		t.Fatalf("PROCESSES nullable CPU value = %v", processes[0].GetValues()[7])
	}
}

func TestValidateValuesRejectsMalformedAgentRows(t *testing.T) {
	tests := map[string]struct {
		table   string
		columns []string
		values  []*kublingv1.Value
	}{
		"wrong type": {
			table: MemoryTable, columns: []string{"total_bytes"},
			values: []*kublingv1.Value{{Kind: &kublingv1.Value_StringValue{StringValue: "1024"}}},
		},
		"negative unsigned metric": {
			table: MemoryTable, columns: []string{"total_bytes"},
			values: []*kublingv1.Value{{Kind: &kublingv1.Value_BigintegerValue{BigintegerValue: "-1"}}},
		},
		"non-finite percentage": {
			table: MemoryTable, columns: []string{"used_percent"},
			values: []*kublingv1.Value{{Kind: &kublingv1.Value_DoubleValue{DoubleValue: math.NaN()}}},
		},
		"null required value": {
			table: MemoryTable, columns: []string{"observed_at"},
			values: []*kublingv1.Value{{Kind: &kublingv1.Value_NullValue{NullValue: &kublingv1.NullValue{}}}},
		},
		"provider-owned column": {
			table: MemoryTable, columns: []string{NamespaceColumn},
			values: []*kublingv1.Value{{Kind: &kublingv1.Value_StringValue{StringValue: "fleet-a"}}},
		},
		"array element type": {
			table: CPUInfoTable, columns: []string{"flags"},
			values: []*kublingv1.Value{{Kind: &kublingv1.Value_ArrayValue{ArrayValue: &kublingv1.ArrayValue{
				ElementType: &kublingv1.TypeDescriptor{Type: kublingv1.ValueType_VALUE_TYPE_INTEGER},
			}}}},
		},
	}
	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			if err := ValidateValues(test.table, test.columns, test.values); err == nil {
				t.Fatal("ValidateValues() error = nil")
			}
		})
	}
}

func assertTupleCount(
	t *testing.T,
	table string,
	tuples []*providerv1.Tuple,
	err error,
	want int,
) []*providerv1.Tuple {
	t.Helper()
	if err != nil {
		t.Fatalf("encode %s error = %v", table, err)
	}
	if len(tuples) != want {
		t.Fatalf("encode %s tuples = %d, want %d", table, len(tuples), want)
	}
	return tuples
}

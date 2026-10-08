package hostschema

import (
	"errors"
	"fmt"
	"math"
	"math/big"
	"strings"
	"time"

	kublingv1 "github.com/kubling-community/kubling-grpc/sdk-go/kubling/v1"
	gatewaypb "github.com/kubling-community/kubling-providers/providers/host/internal/gatewaypb"
	providerv1 "github.com/kubling-community/kubling-providers/sdk-go/kubling/provider/v1"
	providersdk "github.com/kubling-community/kubling-providers/sdk-go/provider"
	"google.golang.org/protobuf/proto"
)

const (
	Version = "host-v1"

	HostTable              = "HOST"
	HostFactsTable         = "HOST_FACTS"
	CPUInfoTable           = "CPU_INFO"
	CPUTimesTable          = "CPU_TIMES"
	MemoryTable            = "MEMORY"
	FilesystemsTable       = "FILESYSTEMS"
	NetworkInterfacesTable = "NETWORK_INTERFACES"
	NetworkIOTable         = "NETWORK_IO"
	ProcessesTable         = "PROCESSES"

	NamespaceColumn  = "namespace"
	HostIDColumn     = "host_id"
	HostnameColumn   = "hostname"
	IdentifierColumn = "identifier"
)

var stringArrayType = &kublingv1.TypeDescriptor{
	Type: kublingv1.ValueType_VALUE_TYPE_ARRAY,
	ElementType: &kublingv1.TypeDescriptor{
		Type: kublingv1.ValueType_VALUE_TYPE_STRING,
	},
}

// Column describes one provider-returned column. Engine-generated columns are
// represented only in metadata and never enter Agent Gateway scan requests.
type Column struct {
	Name           string
	Type           kublingv1.ValueType
	TypeDescriptor *kublingv1.TypeDescriptor
	NativeType     string
	Nullable       bool
	Access         bool
	NonNegative    bool
	Annotation     string
}

type tableDefinition struct {
	name          string
	sourceName    string
	annotation    string
	columns       []Column
	keyColumns    []string
	gatewayTable  gatewaypb.HostTable
	singletonHost bool
}

var tableDefinitions = []tableDefinition{
	{
		name:       HostTable,
		sourceName: "hosts",
		annotation: "Hosts registered with the provider",
		columns: withAccessColumns(
			Column{Name: "state", Type: kublingv1.ValueType_VALUE_TYPE_STRING, NativeType: "string", Annotation: "Lease-derived host availability state"},
			Column{Name: "enrolled_at", Type: kublingv1.ValueType_VALUE_TYPE_TIMESTAMP, NativeType: "timestamp", Annotation: "Time at which the host identity was enrolled"},
			Column{Name: "last_seen_at", Type: kublingv1.ValueType_VALUE_TYPE_TIMESTAMP, NativeType: "timestamp", Nullable: true, Annotation: "Time of the most recent authenticated agent check-in"},
		),
		keyColumns: []string{NamespaceColumn, HostIDColumn},
	},
	{
		name:          HostFactsTable,
		sourceName:    "host_facts",
		annotation:    "Point-in-time operating system facts reported by active hosts",
		gatewayTable:  gatewaypb.HostTable_HOST_TABLE_FACTS,
		singletonHost: true,
		columns: withAccessColumns(
			observedAtColumn(),
			Column{Name: "reported_hostname", Type: kublingv1.ValueType_VALUE_TYPE_STRING, NativeType: "string", Annotation: "Hostname observed when host facts were collected"},
			Column{Name: "os", Type: kublingv1.ValueType_VALUE_TYPE_STRING, NativeType: "string", Annotation: "Operating system name"},
			Column{Name: "platform", Type: kublingv1.ValueType_VALUE_TYPE_STRING, NativeType: "string", Annotation: "Operating system platform"},
			Column{Name: "platform_family", Type: kublingv1.ValueType_VALUE_TYPE_STRING, NativeType: "string", Annotation: "Operating system platform family"},
			Column{Name: "platform_version", Type: kublingv1.ValueType_VALUE_TYPE_STRING, NativeType: "string", Annotation: "Operating system platform version"},
			Column{Name: "kernel_version", Type: kublingv1.ValueType_VALUE_TYPE_STRING, NativeType: "string", Annotation: "Kernel version"},
			Column{Name: "kernel_architecture", Type: kublingv1.ValueType_VALUE_TYPE_STRING, NativeType: "string", Annotation: "Kernel architecture"},
			Column{Name: "virtualization_system", Type: kublingv1.ValueType_VALUE_TYPE_STRING, NativeType: "string", Annotation: "Detected virtualization system"},
			Column{Name: "virtualization_role", Type: kublingv1.ValueType_VALUE_TYPE_STRING, NativeType: "string", Annotation: "Detected virtualization role"},
			Column{Name: "booted_at", Type: kublingv1.ValueType_VALUE_TYPE_TIMESTAMP, NativeType: "timestamp", Nullable: true, Annotation: "Host boot time when available"},
			unsignedColumn("uptime_seconds", "Host uptime in seconds"),
			unsignedColumn("process_count", "Number of processes observed by the operating system"),
		),
		keyColumns: []string{NamespaceColumn, HostIDColumn},
	},
	{
		name:         CPUInfoTable,
		sourceName:   "cpu_info",
		annotation:   "Logical processor facts reported by active hosts",
		gatewayTable: gatewaypb.HostTable_HOST_TABLE_CPU_INFO,
		columns: withAccessColumns(
			observedAtColumn(),
			integerColumn("logical_id", "Logical processor identifier", true),
			Column{Name: "vendor_id", Type: kublingv1.ValueType_VALUE_TYPE_STRING, NativeType: "string", Annotation: "Processor vendor identifier"},
			Column{Name: "family", Type: kublingv1.ValueType_VALUE_TYPE_STRING, NativeType: "string", Annotation: "Processor family"},
			Column{Name: "model", Type: kublingv1.ValueType_VALUE_TYPE_STRING, NativeType: "string", Annotation: "Processor model identifier"},
			integerColumn("stepping", "Processor stepping", true),
			Column{Name: "physical_id", Type: kublingv1.ValueType_VALUE_TYPE_STRING, NativeType: "string", Annotation: "Physical package identifier"},
			Column{Name: "core_id", Type: kublingv1.ValueType_VALUE_TYPE_STRING, NativeType: "string", Annotation: "Physical core identifier"},
			integerColumn("core_count", "Logical processor-reported core count", true),
			Column{Name: "model_name", Type: kublingv1.ValueType_VALUE_TYPE_STRING, NativeType: "string", Annotation: "Human-readable processor model"},
			Column{Name: "mhz", Type: kublingv1.ValueType_VALUE_TYPE_DOUBLE, NativeType: "float64", NonNegative: true, Annotation: "Reported processor frequency in MHz"},
			stringArrayColumn("flags", "Processor feature flags", false),
			Column{Name: "microcode", Type: kublingv1.ValueType_VALUE_TYPE_STRING, NativeType: "string", Annotation: "Processor microcode revision"},
		),
		keyColumns: []string{NamespaceColumn, HostIDColumn, "logical_id"},
	},
	{
		name:         CPUTimesTable,
		sourceName:   "cpu_times",
		annotation:   "Cumulative per-processor CPU counters reported by active hosts",
		gatewayTable: gatewaypb.HostTable_HOST_TABLE_CPU_TIMES,
		columns: withAccessColumns(
			observedAtColumn(),
			Column{Name: "cpu", Type: kublingv1.ValueType_VALUE_TYPE_STRING, NativeType: "string", Annotation: "Logical processor counter name"},
			nonNegativeDoubleColumn("user_seconds", "Cumulative user CPU time in seconds"),
			nonNegativeDoubleColumn("system_seconds", "Cumulative system CPU time in seconds"),
			nonNegativeDoubleColumn("idle_seconds", "Cumulative idle CPU time in seconds"),
			nonNegativeDoubleColumn("nice_seconds", "Cumulative nice CPU time in seconds"),
			nonNegativeDoubleColumn("iowait_seconds", "Cumulative I/O wait CPU time in seconds"),
			nonNegativeDoubleColumn("irq_seconds", "Cumulative interrupt CPU time in seconds"),
			nonNegativeDoubleColumn("softirq_seconds", "Cumulative software-interrupt CPU time in seconds"),
			nonNegativeDoubleColumn("steal_seconds", "Cumulative stolen CPU time in seconds"),
			nonNegativeDoubleColumn("guest_seconds", "Cumulative guest CPU time in seconds"),
			nonNegativeDoubleColumn("guest_nice_seconds", "Cumulative niced guest CPU time in seconds"),
		),
		keyColumns: []string{NamespaceColumn, HostIDColumn, "cpu"},
	},
	{
		name:          MemoryTable,
		sourceName:    "memory",
		annotation:    "Point-in-time memory state reported by active hosts",
		gatewayTable:  gatewaypb.HostTable_HOST_TABLE_MEMORY,
		singletonHost: true,
		columns: withAccessColumns(
			observedAtColumn(),
			unsignedColumn("total_bytes", "Total physical memory in bytes"),
			unsignedColumn("available_bytes", "Memory available without swapping in bytes"),
			unsignedColumn("used_bytes", "Used physical memory in bytes"),
			nonNegativeDoubleColumn("used_percent", "Percentage of physical memory in use"),
			unsignedColumn("free_bytes", "Free physical memory in bytes"),
			nullableUnsignedColumn("swap_total_bytes", "Total swap capacity in bytes"),
			nullableUnsignedColumn("swap_used_bytes", "Used swap capacity in bytes"),
			nullableUnsignedColumn("swap_free_bytes", "Free swap capacity in bytes"),
			nullableDoubleColumn("swap_used_percent", "Percentage of swap capacity in use", true),
			nullableUnsignedColumn("swap_in_bytes", "Cumulative bytes swapped into memory"),
			nullableUnsignedColumn("swap_out_bytes", "Cumulative bytes swapped out of memory"),
		),
		keyColumns: []string{NamespaceColumn, HostIDColumn},
	},
	{
		name:         FilesystemsTable,
		sourceName:   "filesystems",
		annotation:   "Mounted filesystems and point-in-time usage reported by active hosts",
		gatewayTable: gatewaypb.HostTable_HOST_TABLE_FILESYSTEMS,
		columns: withAccessColumns(
			observedAtColumn(),
			Column{Name: "device", Type: kublingv1.ValueType_VALUE_TYPE_STRING, NativeType: "string", Annotation: "Mounted device identifier"},
			Column{Name: "mountpoint", Type: kublingv1.ValueType_VALUE_TYPE_STRING, NativeType: "string", Annotation: "Filesystem mount point"},
			Column{Name: "filesystem_type", Type: kublingv1.ValueType_VALUE_TYPE_STRING, NativeType: "string", Annotation: "Filesystem type"},
			stringArrayColumn("options", "Mount options", false),
			nullableUnsignedColumn("total_bytes", "Filesystem capacity in bytes"),
			nullableUnsignedColumn("free_bytes", "Filesystem free space in bytes"),
			nullableUnsignedColumn("used_bytes", "Filesystem used space in bytes"),
			nullableDoubleColumn("used_percent", "Percentage of filesystem capacity in use", true),
			nullableUnsignedColumn("inodes_total", "Total filesystem inode count"),
			nullableUnsignedColumn("inodes_used", "Used filesystem inode count"),
			nullableUnsignedColumn("inodes_free", "Free filesystem inode count"),
			nullableDoubleColumn("inodes_used_percent", "Percentage of filesystem inodes in use", true),
		),
		keyColumns: []string{NamespaceColumn, HostIDColumn, "mountpoint", "device"},
	},
	{
		name:         NetworkInterfacesTable,
		sourceName:   "network_interfaces",
		annotation:   "Network interface inventory reported by active hosts",
		gatewayTable: gatewaypb.HostTable_HOST_TABLE_NETWORK_INTERFACES,
		columns: withAccessColumns(
			observedAtColumn(),
			longColumn("interface_index", "Operating system interface index", true),
			Column{Name: "name", Type: kublingv1.ValueType_VALUE_TYPE_STRING, NativeType: "string", Annotation: "Network interface name"},
			longColumn("mtu", "Network interface maximum transmission unit", true),
			Column{Name: "hardware_address", Type: kublingv1.ValueType_VALUE_TYPE_STRING, NativeType: "string", Annotation: "Network interface hardware address"},
			stringArrayColumn("flags", "Network interface flags", false),
			stringArrayColumn("addresses", "Assigned network addresses with prefixes", false),
		),
		keyColumns: []string{NamespaceColumn, HostIDColumn, "name"},
	},
	{
		name:         NetworkIOTable,
		sourceName:   "network_io",
		annotation:   "Cumulative per-interface network counters reported by active hosts",
		gatewayTable: gatewaypb.HostTable_HOST_TABLE_NETWORK_IO,
		columns: withAccessColumns(
			observedAtColumn(),
			Column{Name: "name", Type: kublingv1.ValueType_VALUE_TYPE_STRING, NativeType: "string", Annotation: "Network interface name"},
			unsignedColumn("bytes_sent", "Cumulative bytes sent"),
			unsignedColumn("bytes_received", "Cumulative bytes received"),
			unsignedColumn("packets_sent", "Cumulative packets sent"),
			unsignedColumn("packets_received", "Cumulative packets received"),
			unsignedColumn("errors_in", "Cumulative receive errors"),
			unsignedColumn("errors_out", "Cumulative transmit errors"),
			unsignedColumn("drops_in", "Cumulative receive drops"),
			unsignedColumn("drops_out", "Cumulative transmit drops"),
			unsignedColumn("fifo_errors_in", "Cumulative receive FIFO errors"),
			unsignedColumn("fifo_errors_out", "Cumulative transmit FIFO errors"),
		),
		keyColumns: []string{NamespaceColumn, HostIDColumn, "name"},
	},
	{
		name:         ProcessesTable,
		sourceName:   "processes",
		annotation:   "Point-in-time process inventory reported by active hosts",
		gatewayTable: gatewaypb.HostTable_HOST_TABLE_PROCESSES,
		columns: withAccessColumns(
			observedAtColumn(),
			integerColumn("pid", "Operating system process identifier", true),
			Column{Name: "created_at", Type: kublingv1.ValueType_VALUE_TYPE_TIMESTAMP, NativeType: "timestamp", Annotation: "Process creation time"},
			nullableIntegerColumn("parent_pid", "Parent process identifier", true),
			nullableStringColumn("name", "Process name"),
			nullableStringColumn("executable", "Process executable path"),
			nullableStringColumn("command_line", "Process command line"),
			stringArrayColumn("statuses", "Operating system process statuses", true),
			nullableStringColumn("username", "Process owner username"),
			nullableStringColumn("working_directory", "Process working directory"),
			nullableUnsignedColumn("resident_memory_bytes", "Resident process memory in bytes"),
			nullableUnsignedColumn("virtual_memory_bytes", "Virtual process memory in bytes"),
			nullableIntegerColumn("thread_count", "Process thread count", true),
			nullableDoubleColumn("cpu_user_seconds", "Cumulative process user CPU time in seconds", true),
			nullableDoubleColumn("cpu_system_seconds", "Cumulative process system CPU time in seconds", true),
		),
		keyColumns: []string{NamespaceColumn, HostIDColumn, "pid", "created_at"},
	},
}

func accessColumns() []Column {
	return []Column{
		{Name: NamespaceColumn, Type: kublingv1.ValueType_VALUE_TYPE_STRING, NativeType: "string", Access: true, Annotation: "Logical fleet namespace assigned during enrollment"},
		{Name: HostIDColumn, Type: kublingv1.ValueType_VALUE_TYPE_STRING, NativeType: "string", Access: true, Annotation: "Stable host identity within the namespace"},
		{Name: HostnameColumn, Type: kublingv1.ValueType_VALUE_TYPE_STRING, NativeType: "string", Nullable: true, Access: true, Annotation: "Mutable hostname routing alias reported by the agent"},
	}
}

func withAccessColumns(columns ...Column) []Column {
	return append(accessColumns(), columns...)
}

func observedAtColumn() Column {
	return Column{Name: "observed_at", Type: kublingv1.ValueType_VALUE_TYPE_TIMESTAMP, NativeType: "timestamp", Annotation: "Time at which the agent observed the local table"}
}

func unsignedColumn(name, annotation string) Column {
	return Column{Name: name, Type: kublingv1.ValueType_VALUE_TYPE_BIGINTEGER, NativeType: "uint64", NonNegative: true, Annotation: annotation}
}

func nullableUnsignedColumn(name, annotation string) Column {
	column := unsignedColumn(name, annotation)
	column.Nullable = true
	return column
}

func integerColumn(name, annotation string, nonNegative bool) Column {
	return Column{Name: name, Type: kublingv1.ValueType_VALUE_TYPE_INTEGER, NativeType: "int32", NonNegative: nonNegative, Annotation: annotation}
}

func nullableIntegerColumn(name, annotation string, nonNegative bool) Column {
	column := integerColumn(name, annotation, nonNegative)
	column.Nullable = true
	return column
}

func longColumn(name, annotation string, nonNegative bool) Column {
	return Column{Name: name, Type: kublingv1.ValueType_VALUE_TYPE_LONG, NativeType: "int", NonNegative: nonNegative, Annotation: annotation}
}

func nonNegativeDoubleColumn(name, annotation string) Column {
	return Column{Name: name, Type: kublingv1.ValueType_VALUE_TYPE_DOUBLE, NativeType: "float64", NonNegative: true, Annotation: annotation}
}

func nullableDoubleColumn(name, annotation string, nonNegative bool) Column {
	return Column{Name: name, Type: kublingv1.ValueType_VALUE_TYPE_DOUBLE, NativeType: "float64", Nullable: true, NonNegative: nonNegative, Annotation: annotation}
}

func nullableStringColumn(name, annotation string) Column {
	return Column{Name: name, Type: kublingv1.ValueType_VALUE_TYPE_STRING, NativeType: "string", Nullable: true, Annotation: annotation}
}

func stringArrayColumn(name, annotation string, nullable bool) Column {
	return Column{
		Name:           name,
		Type:           kublingv1.ValueType_VALUE_TYPE_ARRAY,
		TypeDescriptor: cloneTypeDescriptor(stringArrayType),
		NativeType:     "[]string",
		Nullable:       nullable,
		Annotation:     annotation,
	}
}

func findTable(name string) (*tableDefinition, bool) {
	name = strings.ToUpper(strings.TrimSpace(name))
	for index := range tableDefinitions {
		if tableDefinitions[index].name == name {
			return &tableDefinitions[index], true
		}
	}
	return nil, false
}

// Columns returns a detached ordered description of one exposed table.
func Columns(table string) ([]Column, bool) {
	definition, exists := findTable(table)
	if !exists {
		return nil, false
	}
	columns := make([]Column, 0, len(definition.columns))
	for _, column := range definition.columns {
		column.TypeDescriptor = cloneTypeDescriptor(column.TypeDescriptor)
		columns = append(columns, column)
	}
	return columns, true
}

// LocalColumns returns the ordered columns supplied by an agent. Provider-owned
// access fields are deliberately excluded.
func LocalColumns(table string) ([]string, bool) {
	definition, exists := findTable(table)
	if !exists || definition.gatewayTable == gatewaypb.HostTable_HOST_TABLE_UNSPECIFIED {
		return nil, false
	}
	columns := make([]string, 0, len(definition.columns))
	for _, column := range definition.columns {
		if !column.Access {
			columns = append(columns, column.Name)
		}
	}
	return columns, true
}

// GatewayTable resolves a northbound table to the private agent protocol.
func GatewayTable(table string) (gatewaypb.HostTable, bool) {
	definition, exists := findTable(table)
	if !exists || definition.gatewayTable == gatewaypb.HostTable_HOST_TABLE_UNSPECIFIED {
		return gatewaypb.HostTable_HOST_TABLE_UNSPECIFIED, false
	}
	return definition.gatewayTable, true
}

// TableName resolves a private Agent Gateway table to its northbound name.
func TableName(table gatewaypb.HostTable) (string, bool) {
	for _, definition := range tableDefinitions {
		if definition.gatewayTable == table && table != gatewaypb.HostTable_HOST_TABLE_UNSPECIFIED {
			return definition.name, true
		}
	}
	return "", false
}

// SingletonPerHost reports whether a successful target scan must return one
// and only one row.
func SingletonPerHost(table string) bool {
	definition, exists := findTable(table)
	return exists && definition.singletonHost
}

// RequiresArrayFeature reports whether the selected columns use ARRAY values.
func RequiresArrayFeature(table string, columns []string) bool {
	definition, exists := findTable(table)
	if !exists {
		return false
	}
	requested := make(map[string]struct{}, len(columns))
	for _, column := range columns {
		requested[strings.ToLower(column)] = struct{}{}
	}
	for _, column := range definition.columns {
		if column.Type != kublingv1.ValueType_VALUE_TYPE_ARRAY {
			continue
		}
		if _, selected := requested[strings.ToLower(column.Name)]; selected {
			return true
		}
	}
	return false
}

// ValidateValues rejects malformed or type-confused values received from a
// remote agent before they enter the provider result stream.
func ValidateValues(table string, columns []string, values []*kublingv1.Value) error {
	definition, exists := findTable(table)
	if !exists || definition.gatewayTable == gatewaypb.HostTable_HOST_TABLE_UNSPECIFIED {
		return fmt.Errorf("unknown agent host table %q", table)
	}
	if len(values) != len(columns) {
		return fmt.Errorf("%s row has %d values, want %d", definition.name, len(values), len(columns))
	}
	definitions := make(map[string]Column, len(definition.columns))
	for _, column := range definition.columns {
		definitions[column.Name] = column
	}
	for index, name := range columns {
		column, exists := definitions[name]
		if !exists || column.Access || name == IdentifierColumn {
			return fmt.Errorf("unknown agent %s column %q", definition.name, name)
		}
		if err := validateValue(column, values[index]); err != nil {
			return fmt.Errorf("%s row column %q: %w", definition.name, name, err)
		}
	}
	return nil
}

func validateValue(column Column, value *kublingv1.Value) error {
	if value == nil || value.GetKind() == nil {
		return errors.New("value is required")
	}
	if _, isNull := value.GetKind().(*kublingv1.Value_NullValue); isNull {
		if !column.Nullable {
			return errors.New("null is not allowed")
		}
		return nil
	}
	switch column.Type {
	case kublingv1.ValueType_VALUE_TYPE_STRING:
		if _, ok := value.GetKind().(*kublingv1.Value_StringValue); !ok {
			return errors.New("expected STRING")
		}
	case kublingv1.ValueType_VALUE_TYPE_INTEGER:
		typed, ok := value.GetKind().(*kublingv1.Value_IntegerValue)
		if !ok {
			return errors.New("expected INTEGER")
		}
		if column.NonNegative && typed.IntegerValue < 0 {
			return errors.New("expected a non-negative integer")
		}
	case kublingv1.ValueType_VALUE_TYPE_LONG:
		typed, ok := value.GetKind().(*kublingv1.Value_LongValue)
		if !ok {
			return errors.New("expected LONG")
		}
		if column.NonNegative && typed.LongValue < 0 {
			return errors.New("expected a non-negative integer")
		}
	case kublingv1.ValueType_VALUE_TYPE_BIGINTEGER:
		typed, ok := value.GetKind().(*kublingv1.Value_BigintegerValue)
		if !ok {
			return errors.New("expected BIGINTEGER")
		}
		integer, valid := new(big.Int).SetString(typed.BigintegerValue, 10)
		if !valid || column.NonNegative && integer.Sign() < 0 {
			return errors.New("expected a non-negative decimal integer")
		}
	case kublingv1.ValueType_VALUE_TYPE_DOUBLE:
		typed, ok := value.GetKind().(*kublingv1.Value_DoubleValue)
		if !ok {
			return errors.New("expected DOUBLE")
		}
		if math.IsNaN(typed.DoubleValue) || math.IsInf(typed.DoubleValue, 0) {
			return errors.New("expected a finite number")
		}
		if column.NonNegative && typed.DoubleValue < 0 {
			return errors.New("expected a non-negative number")
		}
	case kublingv1.ValueType_VALUE_TYPE_TIMESTAMP:
		typed, ok := value.GetKind().(*kublingv1.Value_TimestampValue)
		if !ok {
			return errors.New("expected TIMESTAMP")
		}
		if _, err := time.Parse("2006-01-02T15:04:05.999999999", typed.TimestampValue); err != nil {
			return errors.New("expected an ISO local timestamp")
		}
	case kublingv1.ValueType_VALUE_TYPE_ARRAY:
		if err := validateStringArray(column, value); err != nil {
			return err
		}
	default:
		return fmt.Errorf("unsupported host value type %s", column.Type)
	}
	return nil
}

func validateStringArray(column Column, value *kublingv1.Value) error {
	typed, ok := value.GetKind().(*kublingv1.Value_ArrayValue)
	if !ok || typed.ArrayValue == nil {
		return errors.New("expected ARRAY")
	}
	expected := column.TypeDescriptor.GetElementType()
	if !proto.Equal(typed.ArrayValue.GetElementType(), expected) {
		return errors.New("expected ARRAY<STRING> element type")
	}
	for index, element := range typed.ArrayValue.GetElements() {
		if element == nil {
			return fmt.Errorf("array element %d is required", index)
		}
		if _, ok := element.GetKind().(*kublingv1.Value_StringValue); !ok {
			return fmt.Errorf("array element %d must be STRING", index)
		}
	}
	return nil
}

// Metadata returns the complete stable schema implemented end to end by the
// Host Provider.
func Metadata() *providerv1.SchemaMetadata {
	metadata := &providerv1.SchemaMetadata{
		Tables: make([]*providerv1.TableMetadata, 0, len(tableDefinitions)),
	}
	for _, definition := range tableDefinitions {
		table := metadataTable(definition)
		providersdk.MustAddStablePrimaryKey(
			table,
			IdentifierColumn,
			definition.keyColumns...,
		)
		metadata.Tables = append(metadata.Tables, table)
	}
	return metadata
}

func metadataTable(definition tableDefinition) *providerv1.TableMetadata {
	updatable := false
	table := &providerv1.TableMetadata{
		Name:       definition.name,
		SourceName: definition.sourceName,
		Kind:       providerv1.TableKind_TABLE_KIND_TABLE,
		Updatable:  &updatable,
		Annotation: definition.annotation,
		Columns:    make([]*providerv1.ColumnMetadata, 0, len(definition.columns)+1),
	}
	for _, column := range definition.columns {
		nullable := column.Nullable
		searchability := providerv1.ColumnSearchability_COLUMN_SEARCHABILITY_UNSEARCHABLE
		if column.Access {
			searchability = providerv1.ColumnSearchability_COLUMN_SEARCHABILITY_EQUALITY
		}
		table.Columns = append(table.Columns, &providerv1.ColumnMetadata{
			Name:           column.Name,
			SourceName:     column.Name,
			Type:           column.Type,
			TypeDescriptor: cloneTypeDescriptor(column.TypeDescriptor),
			NativeType:     column.NativeType,
			Nullable:       &nullable,
			Updatable:      &updatable,
			Searchability:  searchability,
			Annotation:     column.Annotation,
		})
	}
	return table
}

func cloneTypeDescriptor(descriptor *kublingv1.TypeDescriptor) *kublingv1.TypeDescriptor {
	if descriptor == nil {
		return nil
	}
	return proto.Clone(descriptor).(*kublingv1.TypeDescriptor)
}

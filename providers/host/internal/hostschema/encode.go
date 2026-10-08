package hostschema

import (
	"fmt"
	"strconv"
	"time"

	kublingv1 "github.com/kubling-community/kubling-grpc/sdk-go/kubling/v1"
	"github.com/kubling-community/kubling-providers/providers/host/internal/agent/collector"
	providerv1 "github.com/kubling-community/kubling-providers/sdk-go/kubling/provider/v1"
)

// EncodeHostFactsRows maps host facts to the exact local column order requested
// through the Agent Gateway.
func EncodeHostFactsRows(
	observation collector.Observation[collector.HostRow],
	columns []string,
) ([]*providerv1.Tuple, error) {
	return encodeRows(HostFactsTable, observation, columns, func(row collector.HostRow, column string) (*kublingv1.Value, error) {
		switch column {
		case "reported_hostname":
			return stringValue(row.Hostname), nil
		case "os":
			return stringValue(row.OS), nil
		case "platform":
			return stringValue(row.Platform), nil
		case "platform_family":
			return stringValue(row.PlatformFamily), nil
		case "platform_version":
			return stringValue(row.PlatformVersion), nil
		case "kernel_version":
			return stringValue(row.KernelVersion), nil
		case "kernel_architecture":
			return stringValue(row.KernelArchitecture), nil
		case "virtualization_system":
			return stringValue(row.VirtualizationSystem), nil
		case "virtualization_role":
			return stringValue(row.VirtualizationRole), nil
		case "booted_at":
			return optionalTimestampValue(row.BootedAt), nil
		case "uptime_seconds":
			return unsignedValue(row.UptimeSeconds), nil
		case "process_count":
			return unsignedValue(row.ProcessCount), nil
		default:
			return nil, unknownColumn(HostFactsTable, column)
		}
	})
}

// EncodeCPUInfoRows maps logical processor facts to gateway tuples.
func EncodeCPUInfoRows(
	observation collector.Observation[collector.CPUInfoRow],
	columns []string,
) ([]*providerv1.Tuple, error) {
	return encodeRows(CPUInfoTable, observation, columns, func(row collector.CPUInfoRow, column string) (*kublingv1.Value, error) {
		switch column {
		case "logical_id":
			return integerValue(row.LogicalID), nil
		case "vendor_id":
			return stringValue(row.VendorID), nil
		case "family":
			return stringValue(row.Family), nil
		case "model":
			return stringValue(row.Model), nil
		case "stepping":
			return integerValue(row.Stepping), nil
		case "physical_id":
			return stringValue(row.PhysicalID), nil
		case "core_id":
			return stringValue(row.CoreID), nil
		case "core_count":
			return integerValue(row.CoreCount), nil
		case "model_name":
			return stringValue(row.ModelName), nil
		case "mhz":
			return doubleValue(row.MHz), nil
		case "flags":
			return stringArrayValue(row.Flags), nil
		case "microcode":
			return stringValue(row.Microcode), nil
		default:
			return nil, unknownColumn(CPUInfoTable, column)
		}
	})
}

// EncodeCPUTimesRows maps cumulative CPU counters to gateway tuples.
func EncodeCPUTimesRows(
	observation collector.Observation[collector.CPUTimesRow],
	columns []string,
) ([]*providerv1.Tuple, error) {
	return encodeRows(CPUTimesTable, observation, columns, func(row collector.CPUTimesRow, column string) (*kublingv1.Value, error) {
		switch column {
		case "cpu":
			return stringValue(row.CPU), nil
		case "user_seconds":
			return doubleValue(row.User), nil
		case "system_seconds":
			return doubleValue(row.System), nil
		case "idle_seconds":
			return doubleValue(row.Idle), nil
		case "nice_seconds":
			return doubleValue(row.Nice), nil
		case "iowait_seconds":
			return doubleValue(row.IOWait), nil
		case "irq_seconds":
			return doubleValue(row.IRQ), nil
		case "softirq_seconds":
			return doubleValue(row.SoftIRQ), nil
		case "steal_seconds":
			return doubleValue(row.Steal), nil
		case "guest_seconds":
			return doubleValue(row.Guest), nil
		case "guest_nice_seconds":
			return doubleValue(row.GuestNice), nil
		default:
			return nil, unknownColumn(CPUTimesTable, column)
		}
	})
}

// EncodeMemoryRows maps memory and swap observations to gateway tuples.
func EncodeMemoryRows(
	observation collector.Observation[collector.MemoryRow],
	columns []string,
) ([]*providerv1.Tuple, error) {
	return encodeRows(MemoryTable, observation, columns, func(row collector.MemoryRow, column string) (*kublingv1.Value, error) {
		switch column {
		case "total_bytes":
			return unsignedValue(row.TotalBytes), nil
		case "available_bytes":
			return unsignedValue(row.AvailableBytes), nil
		case "used_bytes":
			return unsignedValue(row.UsedBytes), nil
		case "used_percent":
			return doubleValue(row.UsedPercent), nil
		case "free_bytes":
			return unsignedValue(row.FreeBytes), nil
		case "swap_total_bytes":
			return optionalUnsignedValue(row.SwapTotalBytes), nil
		case "swap_used_bytes":
			return optionalUnsignedValue(row.SwapUsedBytes), nil
		case "swap_free_bytes":
			return optionalUnsignedValue(row.SwapFreeBytes), nil
		case "swap_used_percent":
			return optionalDoubleValue(row.SwapUsedPercent), nil
		case "swap_in_bytes":
			return optionalUnsignedValue(row.SwapInBytes), nil
		case "swap_out_bytes":
			return optionalUnsignedValue(row.SwapOutBytes), nil
		default:
			return nil, unknownColumn(MemoryTable, column)
		}
	})
}

// EncodeFilesystemRows maps mounted filesystems and optional usage to tuples.
func EncodeFilesystemRows(
	observation collector.Observation[collector.FilesystemRow],
	columns []string,
) ([]*providerv1.Tuple, error) {
	return encodeRows(FilesystemsTable, observation, columns, func(row collector.FilesystemRow, column string) (*kublingv1.Value, error) {
		switch column {
		case "device":
			return stringValue(row.Device), nil
		case "mountpoint":
			return stringValue(row.Mountpoint), nil
		case "filesystem_type":
			return stringValue(row.FilesystemType), nil
		case "options":
			return stringArrayValue(row.Options), nil
		case "total_bytes":
			return optionalUnsignedValue(row.TotalBytes), nil
		case "free_bytes":
			return optionalUnsignedValue(row.FreeBytes), nil
		case "used_bytes":
			return optionalUnsignedValue(row.UsedBytes), nil
		case "used_percent":
			return optionalDoubleValue(row.UsedPercent), nil
		case "inodes_total":
			return optionalUnsignedValue(row.InodesTotal), nil
		case "inodes_used":
			return optionalUnsignedValue(row.InodesUsed), nil
		case "inodes_free":
			return optionalUnsignedValue(row.InodesFree), nil
		case "inodes_used_percent":
			return optionalDoubleValue(row.InodesUsedPercent), nil
		default:
			return nil, unknownColumn(FilesystemsTable, column)
		}
	})
}

// EncodeNetworkInterfaceRows maps interface inventory to gateway tuples.
func EncodeNetworkInterfaceRows(
	observation collector.Observation[collector.NetworkInterfaceRow],
	columns []string,
) ([]*providerv1.Tuple, error) {
	return encodeRows(NetworkInterfacesTable, observation, columns, func(row collector.NetworkInterfaceRow, column string) (*kublingv1.Value, error) {
		switch column {
		case "interface_index":
			return longValue(int64(row.Index)), nil
		case "name":
			return stringValue(row.Name), nil
		case "mtu":
			return longValue(int64(row.MTU)), nil
		case "hardware_address":
			return stringValue(row.HardwareAddress), nil
		case "flags":
			return stringArrayValue(row.Flags), nil
		case "addresses":
			return stringArrayValue(row.Addresses), nil
		default:
			return nil, unknownColumn(NetworkInterfacesTable, column)
		}
	})
}

// EncodeNetworkIORows maps per-interface counters to gateway tuples.
func EncodeNetworkIORows(
	observation collector.Observation[collector.NetworkIORow],
	columns []string,
) ([]*providerv1.Tuple, error) {
	return encodeRows(NetworkIOTable, observation, columns, func(row collector.NetworkIORow, column string) (*kublingv1.Value, error) {
		switch column {
		case "name":
			return stringValue(row.Name), nil
		case "bytes_sent":
			return unsignedValue(row.BytesSent), nil
		case "bytes_received":
			return unsignedValue(row.BytesReceived), nil
		case "packets_sent":
			return unsignedValue(row.PacketsSent), nil
		case "packets_received":
			return unsignedValue(row.PacketsReceived), nil
		case "errors_in":
			return unsignedValue(row.ErrorsIn), nil
		case "errors_out":
			return unsignedValue(row.ErrorsOut), nil
		case "drops_in":
			return unsignedValue(row.DropsIn), nil
		case "drops_out":
			return unsignedValue(row.DropsOut), nil
		case "fifo_errors_in":
			return unsignedValue(row.FIFOErrorsIn), nil
		case "fifo_errors_out":
			return unsignedValue(row.FIFOErrorsOut), nil
		default:
			return nil, unknownColumn(NetworkIOTable, column)
		}
	})
}

// EncodeProcessRows maps process observations, including unavailable fields,
// to gateway tuples.
func EncodeProcessRows(
	observation collector.Observation[collector.ProcessRow],
	columns []string,
) ([]*providerv1.Tuple, error) {
	return encodeRows(ProcessesTable, observation, columns, func(row collector.ProcessRow, column string) (*kublingv1.Value, error) {
		switch column {
		case "pid":
			return integerValue(row.PID), nil
		case "created_at":
			return timestampValue(row.CreatedAt), nil
		case "parent_pid":
			return optionalIntegerValue(row.ParentPID), nil
		case "name":
			return optionalStringValue(row.Name), nil
		case "executable":
			return optionalStringValue(row.Executable), nil
		case "command_line":
			return optionalStringValue(row.CommandLine), nil
		case "statuses":
			return optionalStringArrayValue(row.Statuses), nil
		case "username":
			return optionalStringValue(row.Username), nil
		case "working_directory":
			return optionalStringValue(row.WorkingDirectory), nil
		case "resident_memory_bytes":
			return optionalUnsignedValue(row.ResidentMemoryBytes), nil
		case "virtual_memory_bytes":
			return optionalUnsignedValue(row.VirtualMemoryBytes), nil
		case "thread_count":
			return optionalIntegerValue(row.ThreadCount), nil
		case "cpu_user_seconds":
			return optionalDoubleValue(row.CPUUserSeconds), nil
		case "cpu_system_seconds":
			return optionalDoubleValue(row.CPUSystemSeconds), nil
		default:
			return nil, unknownColumn(ProcessesTable, column)
		}
	})
}

func encodeRows[T any](
	table string,
	observation collector.Observation[T],
	columns []string,
	encode func(T, string) (*kublingv1.Value, error),
) ([]*providerv1.Tuple, error) {
	if SingletonPerHost(table) && len(observation.Rows) != 1 {
		return nil, fmt.Errorf("%s observation contains %d rows, want 1", table, len(observation.Rows))
	}
	tuples := make([]*providerv1.Tuple, 0, len(observation.Rows))
	for rowIndex, row := range observation.Rows {
		values := make([]*kublingv1.Value, 0, len(columns))
		for _, column := range columns {
			var value *kublingv1.Value
			var err error
			if column == "observed_at" {
				value = timestampValue(observation.ObservedAt)
			} else {
				value, err = encode(row, column)
			}
			if err != nil {
				return nil, err
			}
			values = append(values, value)
		}
		if err := ValidateValues(table, columns, values); err != nil {
			return nil, fmt.Errorf("encode %s row %d: %w", table, rowIndex, err)
		}
		tuples = append(tuples, &providerv1.Tuple{Values: values})
	}
	return tuples, nil
}

func unknownColumn(table, column string) error {
	if column == NamespaceColumn || column == HostIDColumn || column == HostnameColumn || column == IdentifierColumn {
		return fmt.Errorf("agent cannot supply provider-owned column %q", column)
	}
	return fmt.Errorf("unknown %s scan column %q", table, column)
}

func stringValue(value string) *kublingv1.Value {
	return &kublingv1.Value{Kind: &kublingv1.Value_StringValue{StringValue: value}}
}

func optionalStringValue(value *string) *kublingv1.Value {
	if value == nil {
		return nullValue()
	}
	return stringValue(*value)
}

func integerValue(value int32) *kublingv1.Value {
	return &kublingv1.Value{Kind: &kublingv1.Value_IntegerValue{IntegerValue: value}}
}

func optionalIntegerValue(value *int32) *kublingv1.Value {
	if value == nil {
		return nullValue()
	}
	return integerValue(*value)
}

func longValue(value int64) *kublingv1.Value {
	return &kublingv1.Value{Kind: &kublingv1.Value_LongValue{LongValue: value}}
}

func timestampValue(value time.Time) *kublingv1.Value {
	return &kublingv1.Value{Kind: &kublingv1.Value_TimestampValue{
		TimestampValue: value.UTC().Format("2006-01-02T15:04:05.999999999"),
	}}
}

func optionalTimestampValue(value time.Time) *kublingv1.Value {
	if value.IsZero() {
		return nullValue()
	}
	return timestampValue(value)
}

func unsignedValue(value uint64) *kublingv1.Value {
	return &kublingv1.Value{Kind: &kublingv1.Value_BigintegerValue{
		BigintegerValue: strconv.FormatUint(value, 10),
	}}
}

func doubleValue(value float64) *kublingv1.Value {
	return &kublingv1.Value{Kind: &kublingv1.Value_DoubleValue{DoubleValue: value}}
}

func optionalUnsignedValue(value *uint64) *kublingv1.Value {
	if value == nil {
		return nullValue()
	}
	return unsignedValue(*value)
}

func optionalDoubleValue(value *float64) *kublingv1.Value {
	if value == nil {
		return nullValue()
	}
	return doubleValue(*value)
}

func stringArrayValue(values []string) *kublingv1.Value {
	elements := make([]*kublingv1.Value, 0, len(values))
	for _, value := range values {
		elements = append(elements, stringValue(value))
	}
	return &kublingv1.Value{Kind: &kublingv1.Value_ArrayValue{ArrayValue: &kublingv1.ArrayValue{
		ElementType: cloneTypeDescriptor(stringArrayType.GetElementType()),
		Elements:    elements,
	}}}
}

func optionalStringArrayValue(values []string) *kublingv1.Value {
	if values == nil {
		return nullValue()
	}
	return stringArrayValue(values)
}

func nullValue() *kublingv1.Value {
	return &kublingv1.Value{Kind: &kublingv1.Value_NullValue{
		NullValue: &kublingv1.NullValue{},
	}}
}

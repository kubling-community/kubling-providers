package collector

import "time"

// Warning describes a non-fatal collection problem. A caller may still use
// the rows returned with the same observation.
type Warning struct {
	Code      string
	Message   string
	Target    string
	Retryable bool
}

// Observation contains rows captured as part of one table read.
type Observation[T any] struct {
	ObservedAt time.Time
	Rows       []T
	Warnings   []Warning
}

// HostRow contains portable facts about the operating system instance. Agent
// identity and routing fields are deliberately added by the Host Provider.
type HostRow struct {
	Hostname             string
	OS                   string
	Platform             string
	PlatformFamily       string
	PlatformVersion      string
	KernelVersion        string
	KernelArchitecture   string
	VirtualizationSystem string
	VirtualizationRole   string
	BootedAt             time.Time
	UptimeSeconds        uint64
	ProcessCount         uint64
}

// CPUInfoRow describes one logical CPU as reported by the host.
type CPUInfoRow struct {
	LogicalID  int32
	VendorID   string
	Family     string
	Model      string
	Stepping   int32
	PhysicalID string
	CoreID     string
	CoreCount  int32
	ModelName  string
	MHz        float64
	Flags      []string
	Microcode  string
}

// CPUTimesRow contains cumulative CPU counters in seconds.
type CPUTimesRow struct {
	CPU       string
	User      float64
	System    float64
	Idle      float64
	Nice      float64
	IOWait    float64
	IRQ       float64
	SoftIRQ   float64
	Steal     float64
	Guest     float64
	GuestNice float64
}

// MemoryRow contains portable memory and swap measurements. Swap fields are
// nil when the platform cannot report them.
type MemoryRow struct {
	TotalBytes      uint64
	AvailableBytes  uint64
	UsedBytes       uint64
	UsedPercent     float64
	FreeBytes       uint64
	SwapTotalBytes  *uint64
	SwapUsedBytes   *uint64
	SwapFreeBytes   *uint64
	SwapUsedPercent *float64
	SwapInBytes     *uint64
	SwapOutBytes    *uint64
}

// FilesystemRow combines mount identity with usage. Usage fields are nil when
// a mount remains visible but cannot be inspected.
type FilesystemRow struct {
	Device            string
	Mountpoint        string
	FilesystemType    string
	Options           []string
	TotalBytes        *uint64
	FreeBytes         *uint64
	UsedBytes         *uint64
	UsedPercent       *float64
	InodesTotal       *uint64
	InodesUsed        *uint64
	InodesFree        *uint64
	InodesUsedPercent *float64
}

// NetworkInterfaceRow describes one local network interface. Addresses retain
// the prefix supplied by the operating system.
type NetworkInterfaceRow struct {
	Index           int
	Name            string
	MTU             int
	HardwareAddress string
	Flags           []string
	Addresses       []string
}

// NetworkIORow contains cumulative counters for one network interface.
type NetworkIORow struct {
	Name            string
	BytesSent       uint64
	BytesReceived   uint64
	PacketsSent     uint64
	PacketsReceived uint64
	ErrorsIn        uint64
	ErrorsOut       uint64
	DropsIn         uint64
	DropsOut        uint64
	FIFOErrorsIn    uint64
	FIFOErrorsOut   uint64
}

// ProcessRow describes one process incarnation. PID plus CreatedAt provides a
// stable native identity across PID reuse. Other fields are nullable because
// permissions and normal process churn may make them unavailable.
type ProcessRow struct {
	PID                 int32
	CreatedAt           time.Time
	ParentPID           *int32
	Name                *string
	Executable          *string
	CommandLine         *string
	Statuses            []string
	Username            *string
	WorkingDirectory    *string
	ResidentMemoryBytes *uint64
	VirtualMemoryBytes  *uint64
	ThreadCount         *int32
	CPUUserSeconds      *float64
	CPUSystemSeconds    *float64
}

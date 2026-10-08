package collector

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"time"

	gocpu "github.com/shirou/gopsutil/v4/cpu"
	godisk "github.com/shirou/gopsutil/v4/disk"
	gohost "github.com/shirou/gopsutil/v4/host"
	gomem "github.com/shirou/gopsutil/v4/mem"
	gonet "github.com/shirou/gopsutil/v4/net"
	goprocess "github.com/shirou/gopsutil/v4/process"
)

const (
	warningSwapUnavailable          = "SWAP_UNAVAILABLE"
	warningFilesystemUnavailable    = "FILESYSTEM_USAGE_UNAVAILABLE"
	warningProcessIdentity          = "PROCESS_IDENTITY_UNAVAILABLE"
	warningProcessFieldsUnavailable = "PROCESS_FIELDS_UNAVAILABLE"
)

// Config controls collection choices that alter which rows are visible.
type Config struct {
	IncludePseudoFilesystems bool
}

// Collector maps gopsutil observations into the stable host-agent model.
type Collector struct {
	api                      systemAPI
	now                      func() time.Time
	includePseudoFilesystems bool
}

// New creates a collector backed by gopsutil.
func New(config Config) *Collector {
	return newCollector(gopsutilAPI{}, time.Now, config)
}

func newCollector(api systemAPI, now func() time.Time, config Config) *Collector {
	return &Collector{
		api:                      api,
		now:                      now,
		includePseudoFilesystems: config.IncludePseudoFilesystems,
	}
}

// Host collects one row of portable host facts.
func (c *Collector) Host(ctx context.Context) (Observation[HostRow], error) {
	observedAt := c.observedAt()
	info, err := c.api.HostInfo(ctx)
	if err != nil {
		return Observation[HostRow]{}, collectionError(ctx, "host", err)
	}
	if info == nil {
		return Observation[HostRow]{}, errors.New("collect host: gopsutil returned nil host info")
	}

	bootedAt := time.Time{}
	if info.BootTime != 0 {
		bootedAt = time.Unix(int64(info.BootTime), 0).UTC()
	}
	return Observation[HostRow]{
		ObservedAt: observedAt,
		Rows: []HostRow{{
			Hostname:             info.Hostname,
			OS:                   info.OS,
			Platform:             info.Platform,
			PlatformFamily:       info.PlatformFamily,
			PlatformVersion:      info.PlatformVersion,
			KernelVersion:        info.KernelVersion,
			KernelArchitecture:   info.KernelArch,
			VirtualizationSystem: info.VirtualizationSystem,
			VirtualizationRole:   info.VirtualizationRole,
			BootedAt:             bootedAt,
			UptimeSeconds:        info.Uptime,
			ProcessCount:         info.Procs,
		}},
	}, nil
}

// CPUInfo collects relatively stable processor facts.
func (c *Collector) CPUInfo(ctx context.Context) (Observation[CPUInfoRow], error) {
	observedAt := c.observedAt()
	info, err := c.api.CPUInfo(ctx)
	if err != nil {
		return Observation[CPUInfoRow]{}, collectionError(ctx, "cpu info", err)
	}
	rows := make([]CPUInfoRow, 0, len(info))
	for _, item := range info {
		rows = append(rows, CPUInfoRow{
			LogicalID:  item.CPU,
			VendorID:   item.VendorID,
			Family:     item.Family,
			Model:      item.Model,
			Stepping:   item.Stepping,
			PhysicalID: item.PhysicalID,
			CoreID:     item.CoreID,
			CoreCount:  item.Cores,
			ModelName:  item.ModelName,
			MHz:        item.Mhz,
			Flags:      append([]string(nil), item.Flags...),
			Microcode:  item.Microcode,
		})
	}
	sort.Slice(rows, func(i, j int) bool { return rows[i].LogicalID < rows[j].LogicalID })
	return Observation[CPUInfoRow]{ObservedAt: observedAt, Rows: rows}, nil
}

// CPUTimes collects cumulative counters for every logical CPU.
func (c *Collector) CPUTimes(ctx context.Context) (Observation[CPUTimesRow], error) {
	observedAt := c.observedAt()
	times, err := c.api.CPUTimes(ctx, true)
	if err != nil {
		return Observation[CPUTimesRow]{}, collectionError(ctx, "cpu times", err)
	}
	rows := make([]CPUTimesRow, 0, len(times))
	for _, item := range times {
		rows = append(rows, CPUTimesRow{
			CPU:       item.CPU,
			User:      item.User,
			System:    item.System,
			Idle:      item.Idle,
			Nice:      item.Nice,
			IOWait:    item.Iowait,
			IRQ:       item.Irq,
			SoftIRQ:   item.Softirq,
			Steal:     item.Steal,
			Guest:     item.Guest,
			GuestNice: item.GuestNice,
		})
	}
	sort.Slice(rows, func(i, j int) bool { return rows[i].CPU < rows[j].CPU })
	return Observation[CPUTimesRow]{ObservedAt: observedAt, Rows: rows}, nil
}

// Memory collects portable virtual-memory fields and best-effort swap fields.
func (c *Collector) Memory(ctx context.Context) (Observation[MemoryRow], error) {
	observedAt := c.observedAt()
	virtual, err := c.api.VirtualMemory(ctx)
	if err != nil {
		return Observation[MemoryRow]{}, collectionError(ctx, "memory", err)
	}
	if virtual == nil {
		return Observation[MemoryRow]{}, errors.New("collect memory: gopsutil returned nil memory info")
	}

	row := MemoryRow{
		TotalBytes:     virtual.Total,
		AvailableBytes: virtual.Available,
		UsedBytes:      virtual.Used,
		UsedPercent:    virtual.UsedPercent,
		FreeBytes:      virtual.Free,
	}
	observation := Observation[MemoryRow]{ObservedAt: observedAt}
	swap, swapErr := c.api.SwapMemory(ctx)
	if swapErr != nil {
		if err := ctx.Err(); err != nil {
			return Observation[MemoryRow]{}, err
		}
		observation.Warnings = append(observation.Warnings, Warning{
			Code:      warningSwapUnavailable,
			Message:   fmt.Sprintf("swap metrics are unavailable: %v", swapErr),
			Target:    "memory",
			Retryable: true,
		})
	} else if swap == nil {
		observation.Warnings = append(observation.Warnings, Warning{
			Code:      warningSwapUnavailable,
			Message:   "swap metrics are unavailable: gopsutil returned nil swap info",
			Target:    "memory",
			Retryable: true,
		})
	} else {
		row.SwapTotalBytes = pointer(swap.Total)
		row.SwapUsedBytes = pointer(swap.Used)
		row.SwapFreeBytes = pointer(swap.Free)
		row.SwapUsedPercent = pointer(swap.UsedPercent)
		row.SwapInBytes = pointer(swap.Sin)
		row.SwapOutBytes = pointer(swap.Sout)
	}
	observation.Rows = []MemoryRow{row}
	return observation, nil
}

// Filesystems collects mounted filesystems. A failed usage read retains the
// mount row and reports nullable usage with a warning.
func (c *Collector) Filesystems(ctx context.Context) (Observation[FilesystemRow], error) {
	observedAt := c.observedAt()
	partitions, err := c.api.Partitions(ctx, c.includePseudoFilesystems)
	if err != nil {
		return Observation[FilesystemRow]{}, collectionError(ctx, "filesystems", err)
	}
	observation := Observation[FilesystemRow]{
		ObservedAt: observedAt,
		Rows:       make([]FilesystemRow, 0, len(partitions)),
	}
	for _, partition := range partitions {
		if err := ctx.Err(); err != nil {
			return Observation[FilesystemRow]{}, err
		}
		row := FilesystemRow{
			Device:         partition.Device,
			Mountpoint:     partition.Mountpoint,
			FilesystemType: partition.Fstype,
			Options:        append([]string(nil), partition.Opts...),
		}
		usage, usageErr := c.api.DiskUsage(ctx, partition.Mountpoint)
		if usageErr != nil {
			if err := ctx.Err(); err != nil {
				return Observation[FilesystemRow]{}, err
			}
			observation.Warnings = append(observation.Warnings, Warning{
				Code:      warningFilesystemUnavailable,
				Message:   fmt.Sprintf("filesystem usage is unavailable: %v", usageErr),
				Target:    partition.Mountpoint,
				Retryable: true,
			})
		} else if usage == nil {
			observation.Warnings = append(observation.Warnings, Warning{
				Code:      warningFilesystemUnavailable,
				Message:   "filesystem usage is unavailable: gopsutil returned nil usage",
				Target:    partition.Mountpoint,
				Retryable: true,
			})
		} else {
			row.TotalBytes = pointer(usage.Total)
			row.FreeBytes = pointer(usage.Free)
			row.UsedBytes = pointer(usage.Used)
			row.UsedPercent = pointer(usage.UsedPercent)
			row.InodesTotal = pointer(usage.InodesTotal)
			row.InodesUsed = pointer(usage.InodesUsed)
			row.InodesFree = pointer(usage.InodesFree)
			row.InodesUsedPercent = pointer(usage.InodesUsedPercent)
		}
		observation.Rows = append(observation.Rows, row)
	}
	sort.Slice(observation.Rows, func(i, j int) bool {
		if observation.Rows[i].Mountpoint == observation.Rows[j].Mountpoint {
			return observation.Rows[i].Device < observation.Rows[j].Device
		}
		return observation.Rows[i].Mountpoint < observation.Rows[j].Mountpoint
	})
	return observation, nil
}

// NetworkInterfaces collects interface inventory and assigned addresses.
func (c *Collector) NetworkInterfaces(ctx context.Context) (Observation[NetworkInterfaceRow], error) {
	observedAt := c.observedAt()
	interfaces, err := c.api.Interfaces(ctx)
	if err != nil {
		return Observation[NetworkInterfaceRow]{}, collectionError(ctx, "network interfaces", err)
	}
	rows := make([]NetworkInterfaceRow, 0, len(interfaces))
	for _, item := range interfaces {
		addresses := make([]string, 0, len(item.Addrs))
		for _, address := range item.Addrs {
			addresses = append(addresses, address.Addr)
		}
		rows = append(rows, NetworkInterfaceRow{
			Index:           item.Index,
			Name:            item.Name,
			MTU:             item.MTU,
			HardwareAddress: item.HardwareAddr,
			Flags:           append([]string(nil), item.Flags...),
			Addresses:       addresses,
		})
	}
	sort.Slice(rows, func(i, j int) bool {
		if rows[i].Index == rows[j].Index {
			return rows[i].Name < rows[j].Name
		}
		return rows[i].Index < rows[j].Index
	})
	return Observation[NetworkInterfaceRow]{ObservedAt: observedAt, Rows: rows}, nil
}

// NetworkIO collects cumulative per-interface network counters.
func (c *Collector) NetworkIO(ctx context.Context) (Observation[NetworkIORow], error) {
	observedAt := c.observedAt()
	counters, err := c.api.NetworkIO(ctx, true)
	if err != nil {
		return Observation[NetworkIORow]{}, collectionError(ctx, "network I/O", err)
	}
	rows := make([]NetworkIORow, 0, len(counters))
	for _, item := range counters {
		rows = append(rows, NetworkIORow{
			Name:            item.Name,
			BytesSent:       item.BytesSent,
			BytesReceived:   item.BytesRecv,
			PacketsSent:     item.PacketsSent,
			PacketsReceived: item.PacketsRecv,
			ErrorsIn:        item.Errin,
			ErrorsOut:       item.Errout,
			DropsIn:         item.Dropin,
			DropsOut:        item.Dropout,
			FIFOErrorsIn:    item.Fifoin,
			FIFOErrorsOut:   item.Fifoout,
		})
	}
	sort.Slice(rows, func(i, j int) bool { return rows[i].Name < rows[j].Name })
	return Observation[NetworkIORow]{ObservedAt: observedAt, Rows: rows}, nil
}

// Processes collects a point-in-time process roster. Processes that disappear
// before their creation time can be read are omitted because a stable native
// identity cannot be constructed. Optional field failures retain the row.
func (c *Collector) Processes(ctx context.Context) (Observation[ProcessRow], error) {
	observedAt := c.observedAt()
	processes, err := c.api.Processes(ctx)
	if err != nil {
		return Observation[ProcessRow]{}, collectionError(ctx, "processes", err)
	}
	observation := Observation[ProcessRow]{
		ObservedAt: observedAt,
		Rows:       make([]ProcessRow, 0, len(processes)),
	}
	skipped := 0
	partial := 0
	for _, process := range processes {
		if err := ctx.Err(); err != nil {
			return Observation[ProcessRow]{}, err
		}
		createdMillis, createdErr := process.CreateTime(ctx)
		if createdErr != nil || createdMillis <= 0 {
			skipped++
			continue
		}

		row := ProcessRow{
			PID:       process.PID(),
			CreatedAt: time.UnixMilli(createdMillis).UTC(),
		}
		rowPartial := false
		parentPID, parentPIDErr := process.ParentPID(ctx)
		row.ParentPID = optionalValue(parentPID, parentPIDErr, &rowPartial)
		name, nameErr := process.Name(ctx)
		row.Name = optionalValue(name, nameErr, &rowPartial)
		executable, executableErr := process.Executable(ctx)
		row.Executable = optionalValue(executable, executableErr, &rowPartial)
		commandLine, commandLineErr := process.CommandLine(ctx)
		row.CommandLine = optionalValue(commandLine, commandLineErr, &rowPartial)
		statuses, statusesErr := process.Statuses(ctx)
		row.Statuses = optionalSlice(statuses, statusesErr, &rowPartial)
		username, usernameErr := process.Username(ctx)
		row.Username = optionalValue(username, usernameErr, &rowPartial)
		workingDirectory, workingDirectoryErr := process.WorkingDirectory(ctx)
		row.WorkingDirectory = optionalValue(workingDirectory, workingDirectoryErr, &rowPartial)
		threadCount, threadCountErr := process.ThreadCount(ctx)
		row.ThreadCount = optionalValue(threadCount, threadCountErr, &rowPartial)
		if memory, memoryErr := process.Memory(ctx); memoryErr != nil || memory == nil {
			rowPartial = true
		} else {
			row.ResidentMemoryBytes = pointer(memory.RSS)
			row.VirtualMemoryBytes = pointer(memory.VMS)
		}
		if times, timesErr := process.Times(ctx); timesErr != nil || times == nil {
			rowPartial = true
		} else {
			row.CPUUserSeconds = pointer(times.User)
			row.CPUSystemSeconds = pointer(times.System)
		}
		if err := ctx.Err(); err != nil {
			return Observation[ProcessRow]{}, err
		}
		if rowPartial {
			partial++
		}
		observation.Rows = append(observation.Rows, row)
	}
	sort.Slice(observation.Rows, func(i, j int) bool {
		if observation.Rows[i].PID == observation.Rows[j].PID {
			return observation.Rows[i].CreatedAt.Before(observation.Rows[j].CreatedAt)
		}
		return observation.Rows[i].PID < observation.Rows[j].PID
	})
	if skipped != 0 {
		observation.Warnings = append(observation.Warnings, Warning{
			Code:      warningProcessIdentity,
			Message:   fmt.Sprintf("%d processes were omitted because their creation time was unavailable", skipped),
			Target:    "processes",
			Retryable: true,
		})
	}
	if partial != 0 {
		observation.Warnings = append(observation.Warnings, Warning{
			Code:      warningProcessFieldsUnavailable,
			Message:   fmt.Sprintf("%d processes have fields unavailable because of permissions or process churn", partial),
			Target:    "processes",
			Retryable: true,
		})
	}
	return observation, nil
}

func (c *Collector) observedAt() time.Time {
	return c.now().UTC()
}

func collectionError(ctx context.Context, target string, err error) error {
	if contextErr := ctx.Err(); contextErr != nil {
		return contextErr
	}
	return fmt.Errorf("collect %s: %w", target, err)
}

func optionalValue[T any](value T, err error, partial *bool) *T {
	if err != nil {
		*partial = true
		return nil
	}
	return pointer(value)
}

func optionalSlice[T any](value []T, err error, partial *bool) []T {
	if err != nil {
		*partial = true
		return nil
	}
	return append([]T(nil), value...)
}

func pointer[T any](value T) *T {
	return &value
}

type processHandle interface {
	PID() int32
	CreateTime(context.Context) (int64, error)
	ParentPID(context.Context) (int32, error)
	Name(context.Context) (string, error)
	Executable(context.Context) (string, error)
	CommandLine(context.Context) (string, error)
	Statuses(context.Context) ([]string, error)
	Username(context.Context) (string, error)
	WorkingDirectory(context.Context) (string, error)
	ThreadCount(context.Context) (int32, error)
	Memory(context.Context) (*goprocess.MemoryInfoStat, error)
	Times(context.Context) (*gocpu.TimesStat, error)
}

type systemAPI interface {
	HostInfo(context.Context) (*gohost.InfoStat, error)
	CPUInfo(context.Context) ([]gocpu.InfoStat, error)
	CPUTimes(context.Context, bool) ([]gocpu.TimesStat, error)
	VirtualMemory(context.Context) (*gomem.VirtualMemoryStat, error)
	SwapMemory(context.Context) (*gomem.SwapMemoryStat, error)
	Partitions(context.Context, bool) ([]godisk.PartitionStat, error)
	DiskUsage(context.Context, string) (*godisk.UsageStat, error)
	Interfaces(context.Context) (gonet.InterfaceStatList, error)
	NetworkIO(context.Context, bool) ([]gonet.IOCountersStat, error)
	Processes(context.Context) ([]processHandle, error)
}

package collector

import (
	"context"

	gocpu "github.com/shirou/gopsutil/v4/cpu"
	godisk "github.com/shirou/gopsutil/v4/disk"
	gohost "github.com/shirou/gopsutil/v4/host"
	gomem "github.com/shirou/gopsutil/v4/mem"
	gonet "github.com/shirou/gopsutil/v4/net"
	goprocess "github.com/shirou/gopsutil/v4/process"
)

type gopsutilAPI struct{}

func (gopsutilAPI) HostInfo(ctx context.Context) (*gohost.InfoStat, error) {
	return gohost.InfoWithContext(ctx)
}

func (gopsutilAPI) CPUInfo(ctx context.Context) ([]gocpu.InfoStat, error) {
	return gocpu.InfoWithContext(ctx)
}

func (gopsutilAPI) CPUTimes(ctx context.Context, perCPU bool) ([]gocpu.TimesStat, error) {
	return gocpu.TimesWithContext(ctx, perCPU)
}

func (gopsutilAPI) VirtualMemory(ctx context.Context) (*gomem.VirtualMemoryStat, error) {
	return gomem.VirtualMemoryWithContext(ctx)
}

func (gopsutilAPI) SwapMemory(ctx context.Context) (*gomem.SwapMemoryStat, error) {
	return gomem.SwapMemoryWithContext(ctx)
}

func (gopsutilAPI) Partitions(ctx context.Context, all bool) ([]godisk.PartitionStat, error) {
	return godisk.PartitionsWithContext(ctx, all)
}

func (gopsutilAPI) DiskUsage(ctx context.Context, path string) (*godisk.UsageStat, error) {
	return godisk.UsageWithContext(ctx, path)
}

func (gopsutilAPI) Interfaces(ctx context.Context) (gonet.InterfaceStatList, error) {
	return gonet.InterfacesWithContext(ctx)
}

func (gopsutilAPI) NetworkIO(ctx context.Context, perNIC bool) ([]gonet.IOCountersStat, error) {
	return gonet.IOCountersWithContext(ctx, perNIC)
}

func (gopsutilAPI) Processes(ctx context.Context) ([]processHandle, error) {
	processes, err := goprocess.ProcessesWithContext(ctx)
	if err != nil {
		return nil, err
	}
	result := make([]processHandle, 0, len(processes))
	for _, process := range processes {
		result = append(result, gopsutilProcess{process: process})
	}
	return result, nil
}

type gopsutilProcess struct {
	process *goprocess.Process
}

func (p gopsutilProcess) PID() int32 { return p.process.Pid }

func (p gopsutilProcess) CreateTime(ctx context.Context) (int64, error) {
	return p.process.CreateTimeWithContext(ctx)
}

func (p gopsutilProcess) ParentPID(ctx context.Context) (int32, error) {
	return p.process.PpidWithContext(ctx)
}

func (p gopsutilProcess) Name(ctx context.Context) (string, error) {
	return p.process.NameWithContext(ctx)
}

func (p gopsutilProcess) Executable(ctx context.Context) (string, error) {
	return p.process.ExeWithContext(ctx)
}

func (p gopsutilProcess) CommandLine(ctx context.Context) (string, error) {
	return p.process.CmdlineWithContext(ctx)
}

func (p gopsutilProcess) Statuses(ctx context.Context) ([]string, error) {
	return p.process.StatusWithContext(ctx)
}

func (p gopsutilProcess) Username(ctx context.Context) (string, error) {
	return p.process.UsernameWithContext(ctx)
}

func (p gopsutilProcess) WorkingDirectory(ctx context.Context) (string, error) {
	return p.process.CwdWithContext(ctx)
}

func (p gopsutilProcess) ThreadCount(ctx context.Context) (int32, error) {
	return p.process.NumThreadsWithContext(ctx)
}

func (p gopsutilProcess) Memory(ctx context.Context) (*goprocess.MemoryInfoStat, error) {
	return p.process.MemoryInfoWithContext(ctx)
}

func (p gopsutilProcess) Times(ctx context.Context) (*gocpu.TimesStat, error) {
	return p.process.TimesWithContext(ctx)
}

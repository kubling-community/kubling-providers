package collector

import (
	"context"
	"errors"
	"testing"
	"time"

	gocpu "github.com/shirou/gopsutil/v4/cpu"
	godisk "github.com/shirou/gopsutil/v4/disk"
	gohost "github.com/shirou/gopsutil/v4/host"
	gomem "github.com/shirou/gopsutil/v4/mem"
	gonet "github.com/shirou/gopsutil/v4/net"
	goprocess "github.com/shirou/gopsutil/v4/process"
)

func TestCollectorMapsCoreObservations(t *testing.T) {
	observedAt := time.Date(2026, time.October, 7, 12, 30, 0, 0, time.FixedZone("test", 2*60*60))
	api := &fakeSystemAPI{
		hostInfo: &gohost.InfoStat{
			Hostname:             "node-a",
			Uptime:               300,
			BootTime:             1_700_000_000,
			Procs:                42,
			OS:                   "linux",
			Platform:             "ubuntu",
			PlatformFamily:       "debian",
			PlatformVersion:      "test",
			KernelVersion:        "test-kernel",
			KernelArch:           "x86_64",
			VirtualizationSystem: "kvm",
			VirtualizationRole:   "guest",
		},
		cpuInfo: []gocpu.InfoStat{
			{CPU: 1, VendorID: "vendor", Flags: []string{"flag-b"}},
			{CPU: 0, VendorID: "vendor", Flags: []string{"flag-a"}},
		},
		cpuTimes: []gocpu.TimesStat{
			{CPU: "cpu1", User: 2, System: 3},
			{CPU: "cpu0", User: 1, System: 4},
		},
		virtualMemory: &gomem.VirtualMemoryStat{
			Total: 1000, Available: 600, Used: 400, UsedPercent: 40, Free: 250,
		},
		swapMemory: &gomem.SwapMemoryStat{
			Total: 200, Used: 50, Free: 150, UsedPercent: 25, Sin: 10, Sout: 20,
		},
		partitions: []godisk.PartitionStat{
			{Device: "/dev/b", Mountpoint: "/var", Fstype: "ext4", Opts: []string{"rw"}},
			{Device: "/dev/a", Mountpoint: "/", Fstype: "ext4", Opts: []string{"rw"}},
		},
		diskUsage: map[string]*godisk.UsageStat{
			"/": {Total: 100, Free: 40, Used: 60, UsedPercent: 60},
		},
		diskUsageErrors: map[string]error{
			"/var": errors.New("permission denied"),
		},
		interfaces: gonet.InterfaceStatList{
			{Index: 2, Name: "eth0", MTU: 1500, HardwareAddr: "00:11", Flags: []string{"up"}, Addrs: []gonet.InterfaceAddr{{Addr: "10.0.0.1/24"}}},
			{Index: 1, Name: "lo", MTU: 65536, Flags: []string{"up", "loopback"}, Addrs: []gonet.InterfaceAddr{{Addr: "127.0.0.1/8"}}},
		},
		networkIO: []gonet.IOCountersStat{
			{Name: "eth0", BytesSent: 20, BytesRecv: 30},
			{Name: "lo", BytesSent: 10, BytesRecv: 10},
		},
	}
	collector := newCollector(api, func() time.Time { return observedAt }, Config{})

	hostObservation, err := collector.Host(context.Background())
	if err != nil {
		t.Fatalf("Host() error = %v", err)
	}
	if got := hostObservation.ObservedAt.Location(); got != time.UTC {
		t.Fatalf("Host() location = %v, want UTC", got)
	}
	if got := hostObservation.Rows[0].Hostname; got != "node-a" {
		t.Fatalf("Host() hostname = %q, want node-a", got)
	}
	if got := hostObservation.Rows[0].BootedAt; !got.Equal(time.Unix(1_700_000_000, 0)) {
		t.Fatalf("Host() booted at = %v", got)
	}

	cpuInfo, err := collector.CPUInfo(context.Background())
	if err != nil {
		t.Fatalf("CPUInfo() error = %v", err)
	}
	if got := cpuInfo.Rows[0].LogicalID; got != 0 {
		t.Fatalf("CPUInfo() first logical ID = %d, want 0", got)
	}
	api.cpuInfo[1].Flags[0] = "mutated"
	if got := cpuInfo.Rows[0].Flags[0]; got != "flag-a" {
		t.Fatalf("CPUInfo() retained mutable source flags: %q", got)
	}

	cpuTimes, err := collector.CPUTimes(context.Background())
	if err != nil {
		t.Fatalf("CPUTimes() error = %v", err)
	}
	if got := cpuTimes.Rows[0].CPU; got != "cpu0" {
		t.Fatalf("CPUTimes() first CPU = %q, want cpu0", got)
	}

	memory, err := collector.Memory(context.Background())
	if err != nil {
		t.Fatalf("Memory() error = %v", err)
	}
	if got := *memory.Rows[0].SwapUsedBytes; got != 50 {
		t.Fatalf("Memory() swap used = %d, want 50", got)
	}

	filesystems, err := collector.Filesystems(context.Background())
	if err != nil {
		t.Fatalf("Filesystems() error = %v", err)
	}
	if got := filesystems.Rows[0].Mountpoint; got != "/" {
		t.Fatalf("Filesystems() first mountpoint = %q, want /", got)
	}
	if filesystems.Rows[1].TotalBytes != nil {
		t.Fatal("Filesystems() unavailable usage should remain nil")
	}
	assertWarning(t, filesystems.Warnings, warningFilesystemUnavailable, "/var")
	if api.partitionAll {
		t.Fatal("Filesystems() requested pseudo filesystems by default")
	}

	interfaces, err := collector.NetworkInterfaces(context.Background())
	if err != nil {
		t.Fatalf("NetworkInterfaces() error = %v", err)
	}
	if got := interfaces.Rows[0].Name; got != "lo" {
		t.Fatalf("NetworkInterfaces() first name = %q, want lo", got)
	}
	if got := interfaces.Rows[1].Addresses[0]; got != "10.0.0.1/24" {
		t.Fatalf("NetworkInterfaces() address = %q", got)
	}

	networkIO, err := collector.NetworkIO(context.Background())
	if err != nil {
		t.Fatalf("NetworkIO() error = %v", err)
	}
	if got := networkIO.Rows[0].Name; got != "eth0" {
		t.Fatalf("NetworkIO() first name = %q, want eth0", got)
	}
}

func TestCollectorMemoryRetainsVirtualMetricsWhenSwapFails(t *testing.T) {
	collector := newCollector(&fakeSystemAPI{
		virtualMemory: &gomem.VirtualMemoryStat{Total: 1024},
		swapMemoryErr: errors.New("not supported"),
	}, time.Now, Config{})

	observation, err := collector.Memory(context.Background())
	if err != nil {
		t.Fatalf("Memory() error = %v", err)
	}
	if got := observation.Rows[0].TotalBytes; got != 1024 {
		t.Fatalf("Memory() total = %d, want 1024", got)
	}
	if observation.Rows[0].SwapTotalBytes != nil {
		t.Fatal("Memory() swap total should be nil")
	}
	assertWarning(t, observation.Warnings, warningSwapUnavailable, "memory")
}

func TestCollectorProcessesRequireStableIdentityAndKeepPartialRows(t *testing.T) {
	complete := &fakeProcess{
		pid:              20,
		createdMillis:    1_700_000_000_000,
		parentPID:        1,
		name:             "worker",
		executable:       "/usr/bin/worker",
		commandLine:      "worker --serve",
		statuses:         []string{"sleep"},
		username:         "service",
		workingDirectory: "/srv",
		threadCount:      4,
		memory:           &goprocess.MemoryInfoStat{RSS: 100, VMS: 200},
		times:            &gocpu.TimesStat{User: 1.5, System: 0.5},
	}
	partial := &fakeProcess{
		pid:           10,
		createdMillis: 1_600_000_000_000,
		nameErr:       errors.New("permission denied"),
		memoryErr:     errors.New("process exited"),
		times:         &gocpu.TimesStat{},
	}
	unstable := &fakeProcess{pid: 30, createdErr: errors.New("process exited")}
	collector := newCollector(&fakeSystemAPI{
		processes: []processHandle{complete, unstable, partial},
	}, time.Now, Config{})

	observation, err := collector.Processes(context.Background())
	if err != nil {
		t.Fatalf("Processes() error = %v", err)
	}
	if got := len(observation.Rows); got != 2 {
		t.Fatalf("Processes() rows = %d, want 2", got)
	}
	if got := observation.Rows[0].PID; got != 10 {
		t.Fatalf("Processes() first PID = %d, want 10", got)
	}
	if observation.Rows[0].Name != nil || observation.Rows[0].ResidentMemoryBytes != nil {
		t.Fatal("Processes() failed optional fields should remain nil")
	}
	if got := *observation.Rows[1].Name; got != "worker" {
		t.Fatalf("Processes() complete name = %q, want worker", got)
	}
	assertWarning(t, observation.Warnings, warningProcessIdentity, "processes")
	assertWarning(t, observation.Warnings, warningProcessFieldsUnavailable, "processes")
}

func TestCollectorPropagatesCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	collector := newCollector(&fakeSystemAPI{hostInfoErr: context.Canceled}, time.Now, Config{})

	_, err := collector.Host(ctx)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("Host() error = %v, want context.Canceled", err)
	}
}

func assertWarning(t *testing.T, warnings []Warning, code string, target string) {
	t.Helper()
	for _, warning := range warnings {
		if warning.Code == code && warning.Target == target {
			return
		}
	}
	t.Fatalf("warning %s for %s not found in %#v", code, target, warnings)
}

type fakeSystemAPI struct {
	hostInfo         *gohost.InfoStat
	hostInfoErr      error
	cpuInfo          []gocpu.InfoStat
	cpuInfoErr       error
	cpuTimes         []gocpu.TimesStat
	cpuTimesErr      error
	virtualMemory    *gomem.VirtualMemoryStat
	virtualMemoryErr error
	swapMemory       *gomem.SwapMemoryStat
	swapMemoryErr    error
	partitions       []godisk.PartitionStat
	partitionsErr    error
	partitionAll     bool
	diskUsage        map[string]*godisk.UsageStat
	diskUsageErrors  map[string]error
	interfaces       gonet.InterfaceStatList
	interfacesErr    error
	networkIO        []gonet.IOCountersStat
	networkIOErr     error
	processes        []processHandle
	processesErr     error
}

func (f *fakeSystemAPI) HostInfo(context.Context) (*gohost.InfoStat, error) {
	return f.hostInfo, f.hostInfoErr
}

func (f *fakeSystemAPI) CPUInfo(context.Context) ([]gocpu.InfoStat, error) {
	return f.cpuInfo, f.cpuInfoErr
}

func (f *fakeSystemAPI) CPUTimes(context.Context, bool) ([]gocpu.TimesStat, error) {
	return f.cpuTimes, f.cpuTimesErr
}

func (f *fakeSystemAPI) VirtualMemory(context.Context) (*gomem.VirtualMemoryStat, error) {
	return f.virtualMemory, f.virtualMemoryErr
}

func (f *fakeSystemAPI) SwapMemory(context.Context) (*gomem.SwapMemoryStat, error) {
	return f.swapMemory, f.swapMemoryErr
}

func (f *fakeSystemAPI) Partitions(_ context.Context, all bool) ([]godisk.PartitionStat, error) {
	f.partitionAll = all
	return f.partitions, f.partitionsErr
}

func (f *fakeSystemAPI) DiskUsage(_ context.Context, path string) (*godisk.UsageStat, error) {
	return f.diskUsage[path], f.diskUsageErrors[path]
}

func (f *fakeSystemAPI) Interfaces(context.Context) (gonet.InterfaceStatList, error) {
	return f.interfaces, f.interfacesErr
}

func (f *fakeSystemAPI) NetworkIO(context.Context, bool) ([]gonet.IOCountersStat, error) {
	return f.networkIO, f.networkIOErr
}

func (f *fakeSystemAPI) Processes(context.Context) ([]processHandle, error) {
	return f.processes, f.processesErr
}

type fakeProcess struct {
	pid                 int32
	createdMillis       int64
	createdErr          error
	parentPID           int32
	parentPIDErr        error
	name                string
	nameErr             error
	executable          string
	executableErr       error
	commandLine         string
	commandLineErr      error
	statuses            []string
	statusesErr         error
	username            string
	usernameErr         error
	workingDirectory    string
	workingDirectoryErr error
	threadCount         int32
	threadCountErr      error
	memory              *goprocess.MemoryInfoStat
	memoryErr           error
	times               *gocpu.TimesStat
	timesErr            error
}

func (f *fakeProcess) PID() int32 { return f.pid }

func (f *fakeProcess) CreateTime(context.Context) (int64, error) {
	return f.createdMillis, f.createdErr
}

func (f *fakeProcess) ParentPID(context.Context) (int32, error) {
	return f.parentPID, f.parentPIDErr
}

func (f *fakeProcess) Name(context.Context) (string, error) { return f.name, f.nameErr }

func (f *fakeProcess) Executable(context.Context) (string, error) {
	return f.executable, f.executableErr
}

func (f *fakeProcess) CommandLine(context.Context) (string, error) {
	return f.commandLine, f.commandLineErr
}

func (f *fakeProcess) Statuses(context.Context) ([]string, error) {
	return f.statuses, f.statusesErr
}

func (f *fakeProcess) Username(context.Context) (string, error) {
	return f.username, f.usernameErr
}

func (f *fakeProcess) WorkingDirectory(context.Context) (string, error) {
	return f.workingDirectory, f.workingDirectoryErr
}

func (f *fakeProcess) ThreadCount(context.Context) (int32, error) {
	return f.threadCount, f.threadCountErr
}

func (f *fakeProcess) Memory(context.Context) (*goprocess.MemoryInfoStat, error) {
	return f.memory, f.memoryErr
}

func (f *fakeProcess) Times(context.Context) (*gocpu.TimesStat, error) {
	return f.times, f.timesErr
}

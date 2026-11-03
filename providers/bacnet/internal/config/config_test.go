package config

import (
	"fmt"
	"log/slog"
	"net/netip"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"
)

func TestLoadNormalizesProcessConfiguration(t *testing.T) {
	path := writeConfig(t, `
server:
  listen: "127.0.0.1:55051"
  shutdownGracePeriod: 25s
logging:
  level: debug
`)

	configuration, err := Load(path)
	if err != nil {
		t.Fatalf("Load() error = %v", err)
	}
	if configuration.ListenAddress() != "127.0.0.1:55051" {
		t.Fatalf("ListenAddress() = %q", configuration.ListenAddress())
	}
	if configuration.ShutdownGracePeriod() != 25*time.Second {
		t.Fatalf("ShutdownGracePeriod() = %v", configuration.ShutdownGracePeriod())
	}
	if configuration.LogLevel() != slog.LevelDebug {
		t.Fatalf("LogLevel() = %v", configuration.LogLevel())
	}
	if len(configuration.ProfileFiles()) != 0 {
		t.Fatalf("ProfileFiles() = %v", configuration.ProfileFiles())
	}
	formatted := fmt.Sprintf("%v %#v", configuration, configuration)
	if !strings.Contains(formatted, "127.0.0.1:55051") || !strings.Contains(formatted, "DEBUG") {
		t.Fatalf("formatted config = %q", formatted)
	}
}

func TestLoadResolvesAndDetachesProfileFiles(t *testing.T) {
	directory := t.TempDir()
	path := filepath.Join(directory, "provider.yaml")
	if err := os.WriteFile(path, []byte(`
profiles:
  files:
    - profiles/vendor.yaml
    - /etc/kubling/device.yaml
`), 0o600); err != nil {
		t.Fatalf("os.WriteFile() error = %v", err)
	}
	configuration, err := Load(path)
	if err != nil {
		t.Fatalf("Load() error = %v", err)
	}
	want := []string{
		filepath.Join(directory, "profiles/vendor.yaml"),
		"/etc/kubling/device.yaml",
	}
	files := configuration.ProfileFiles()
	if !reflect.DeepEqual(files, want) {
		t.Fatalf("ProfileFiles() = %v, want %v", files, want)
	}
	files[0] = "mutated"
	if reflect.DeepEqual(configuration.ProfileFiles(), files) {
		t.Fatal("ProfileFiles() exposes mutable configuration state")
	}
}

func TestLoadNormalizesBACnetTopology(t *testing.T) {
	path := writeConfig(t, `
attachments:
  - id: building-lan
    interface: eth0
    bind: 0.0.0.0:47808
    transaction:
      apduTimeout: 4s
      retries: 2
      segmentTimeout: 1500ms
    registry:
      observationTTL: 20m
      maxObservations: 1000
      maxPathsPerInstance: 4
scopes:
  - namespace: madrid.dc1
    id: cooling
    attachment: building-lan
    mutations:
      enabled: true
      priority: 16
    discovery:
      broadcast: true
      interval: 45s
      window: 2s
      lowDeviceInstance: 1000
      highDeviceInstance: 1999
      targets:
        - endpoint: 192.0.2.10
          remoteNetwork: 20
    staticDevices:
      - deviceInstance: 1201
        endpoint: 192.0.2.11:47808
`)
	configuration, err := Load(path)
	if err != nil {
		t.Fatalf("Load() error = %v", err)
	}
	attachments := configuration.Attachments()
	if len(attachments) != 1 ||
		attachments[0].ID != "building-lan" ||
		attachments[0].Bind != netip.MustParseAddrPort("0.0.0.0:47808") ||
		attachments[0].APDUTimeout != 4*time.Second ||
		attachments[0].Retries != 2 ||
		attachments[0].MaxObservations != 1000 {
		t.Fatalf("Attachments() = %#v", attachments)
	}
	scopes := configuration.Scopes()
	if len(scopes) != 1 ||
		scopes[0].Key() != "madrid.dc1/cooling" ||
		!scopes[0].Discovery.Broadcast ||
		!scopes[0].Mutations.Enabled ||
		scopes[0].Mutations.Priority != 16 ||
		scopes[0].Discovery.Interval != 45*time.Second ||
		len(scopes[0].Discovery.Targets) != 1 ||
		len(scopes[0].StaticDevices) != 1 ||
		scopes[0].StaticDevices[0].DeviceInstance != 1201 {
		t.Fatalf("Scopes() = %#v", scopes)
	}
}

func TestLoadDetachesNestedTopology(t *testing.T) {
	path := writeConfig(t, `
attachments:
  - id: routed
    bind: 127.0.0.1:0
    foreignDevice:
      bbmd: 192.0.2.10:47808
      ttl: 60s
scopes:
  - namespace: test
    id: remote
    attachment: routed
    discovery:
      interval: 10s
      window: 1s
      targets:
        - endpoint: 192.0.2.10:47808
          remoteNetwork: 42
    staticDevices:
      - deviceInstance: 7
        endpoint: 192.0.2.10:47808
        remoteNetwork: 42
        mac: "0a:00:00:07"
`)
	configuration, err := Load(path)
	if err != nil {
		t.Fatalf("Load() error = %v", err)
	}
	attachments := configuration.Attachments()
	attachments[0].ForeignDevice.TTL = 2 * time.Second
	if configuration.Attachments()[0].ForeignDevice.TTL != 60*time.Second {
		t.Fatal("Attachments() exposes mutable foreign-device state")
	}
	scopes := configuration.Scopes()
	*scopes[0].Discovery.Targets[0].RemoteNetwork = 7
	scopes[0].StaticDevices[0].MAC[0] = 0xff
	fresh := configuration.Scopes()
	if *fresh[0].Discovery.Targets[0].RemoteNetwork != 42 || fresh[0].StaticDevices[0].MAC[0] != 0x0a {
		t.Fatal("Scopes() exposes mutable nested state")
	}
}

func TestLoadAppliesSafeDefaults(t *testing.T) {
	configuration, err := Load(writeConfig(t, "{}\n"))
	if err != nil {
		t.Fatalf("Load() error = %v", err)
	}
	if configuration.ListenAddress() != defaultListenAddress ||
		configuration.ShutdownGracePeriod() != defaultShutdownGracePeriod ||
		configuration.LogLevel() != slog.LevelInfo {
		t.Fatalf("default configuration = %#v", configuration)
	}
}

func TestLoadRejectsUnknownAndMultipleDocuments(t *testing.T) {
	for name, contents := range map[string]string{
		"unknown":  "server:\n  unknown: true\n",
		"multiple": "{}\n---\n{}\n",
	} {
		t.Run(name, func(t *testing.T) {
			_, err := Load(writeConfig(t, contents))
			if err == nil {
				t.Fatal("Load() error = nil")
			}
		})
	}
}

func TestLoadRejectsInvalidValues(t *testing.T) {
	tests := map[string]string{
		"listen":            "server:\n  listen: missing-port\n",
		"port":              "server:\n  listen: '127.0.0.1:0'\n",
		"duration":          "server:\n  shutdownGracePeriod: forever\n",
		"zero":              "server:\n  shutdownGracePeriod: 0s\n",
		"too-long":          "server:\n  shutdownGracePeriod: 6m\n",
		"log-level":         "logging:\n  level: verbose\n",
		"empty-profile":     "profiles:\n  files: ['']\n",
		"duplicate-profile": "profiles:\n  files: [vendor.yaml, ./vendor.yaml]\n",
	}
	for name, contents := range tests {
		t.Run(name, func(t *testing.T) {
			_, err := Load(writeConfig(t, contents))
			if err == nil {
				t.Fatal("Load() error = nil")
			}
		})
	}
}

func TestLoadRejectsInvalidBACnetTopology(t *testing.T) {
	tests := map[string]string{
		"duplicate attachment": `
attachments:
  - id: local
  - id: local
`,
		"conflicting binds": `
attachments:
  - id: first
    bind: 0.0.0.0:47808
  - id: second
    bind: 127.0.0.1:47808
`,
		"missing attachment": `
scopes:
  - namespace: test
    id: local
    attachment: missing
    discovery:
      broadcast: true
`,
		"unsafe local broadcast bind": `
attachments:
  - id: local
    bind: 127.0.0.1:47808
scopes:
  - namespace: test
    id: local
    attachment: local
    discovery:
      broadcast: true
`,
		"invalid bounds": `
attachments:
  - id: local
scopes:
  - namespace: test
    id: local
    attachment: local
    discovery:
      broadcast: true
      lowDeviceInstance: 20
      highDeviceInstance: 10
`,
		"duplicate static device": `
attachments:
  - id: local
scopes:
  - namespace: test
    id: local
    attachment: local
    staticDevices:
      - deviceInstance: 7
        endpoint: 192.0.2.7
      - deviceInstance: 7
        endpoint: 192.0.2.8
`,
		"routed device without mac": `
attachments:
  - id: local
scopes:
  - namespace: test
    id: local
    attachment: local
    staticDevices:
      - deviceInstance: 7
        endpoint: 192.0.2.1
        remoteNetwork: 20
`,
		"empty scope": `
attachments:
  - id: local
scopes:
  - namespace: test
    id: local
    attachment: local
`,
		"fractional foreign ttl": `
attachments:
  - id: local
    foreignDevice:
      bbmd: 192.0.2.1
      ttl: 2500ms
`,
		"ipv6 endpoint": `
attachments:
  - id: local
scopes:
  - namespace: test
    id: local
    attachment: local
    staticDevices:
      - deviceInstance: 7
        endpoint: "[2001:db8::1]:47808"
`,
		"mutation priority without enable": `
attachments:
  - id: local
scopes:
  - namespace: test
    id: local
    attachment: local
    mutations:
      priority: 16
    staticDevices:
      - deviceInstance: 7
        endpoint: 192.0.2.7
`,
		"enabled mutations without priority": `
attachments:
  - id: local
scopes:
  - namespace: test
    id: local
    attachment: local
    mutations:
      enabled: true
    staticDevices:
      - deviceInstance: 7
        endpoint: 192.0.2.7
`,
		"invalid mutation priority": `
attachments:
  - id: local
scopes:
  - namespace: test
    id: local
    attachment: local
    mutations:
      enabled: true
      priority: 17
    staticDevices:
      - deviceInstance: 7
        endpoint: 192.0.2.7
`,
	}
	for name, contents := range tests {
		t.Run(name, func(t *testing.T) {
			if _, err := Load(writeConfig(t, contents)); err == nil {
				t.Fatal("Load() error = nil")
			}
		})
	}
}

func writeConfig(t *testing.T, contents string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "provider.yaml")
	if err := os.WriteFile(path, []byte(contents), 0o600); err != nil {
		t.Fatalf("os.WriteFile() error = %v", err)
	}
	return path
}

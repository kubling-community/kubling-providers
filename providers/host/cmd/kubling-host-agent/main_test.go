package main

import (
	"os"
	"path/filepath"
	"testing"
)

func TestResolveConfigUsesFlagEnvironmentFilePrecedence(t *testing.T) {
	path := filepath.Join(t.TempDir(), "agent.yaml")
	if err := os.WriteFile(path, []byte(`
gatewayAddress: file-gateway:50052
namespace: file-namespace
stateDirectory: /file/state
insecure: true
attributes:
  zone: file
maxConcurrentScans: 2
collector:
  includePseudoFilesystems: true
`), 0o600); err != nil {
		t.Fatalf("os.WriteFile() error = %v", err)
	}
	environment := map[string]string{
		"KUBLING_HOST_AGENT_CONFIG":                     path,
		"KUBLING_HOST_AGENT_GATEWAY":                    "env-gateway:50052",
		"KUBLING_HOST_AGENT_MAX_CONCURRENT_SCANS":       "3",
		"KUBLING_HOST_AGENT_INCLUDE_PSEUDO_FILESYSTEMS": "false",
		"KUBLING_HOST_AGENT_ATTRIBUTES":                 "rack=env",
	}
	config, err := resolveConfig(
		[]string{
			"-namespace", "cli-namespace",
			"-max-concurrent-scans", "4",
			"-attribute", "zone=cli",
		},
		func(name string) string { return environment[name] },
	)
	if err != nil {
		t.Fatalf("resolveConfig() error = %v", err)
	}
	if config.GatewayAddress != "env-gateway:50052" ||
		config.Namespace != "cli-namespace" || config.StateDirectory != "/file/state" ||
		!config.Insecure || config.MaxConcurrentScans != 4 || config.Collector.IncludePseudoFilesystems ||
		config.Attributes["rack"] != "env" || config.Attributes["zone"] != "cli" {
		t.Fatalf("resolved config = %#v", config)
	}
}

func TestResolveConfigRejectsInvalidEnvironment(t *testing.T) {
	environment := map[string]string{
		"KUBLING_HOST_AGENT_GATEWAY":                    "gateway:50052",
		"KUBLING_HOST_AGENT_NAMESPACE":                  "fleet-a",
		"KUBLING_HOST_AGENT_INCLUDE_PSEUDO_FILESYSTEMS": "sometimes",
	}
	if _, err := resolveConfig(nil, func(name string) string { return environment[name] }); err == nil {
		t.Fatal("resolveConfig() error = nil")
	}
}

func TestResolveConfigLoadsSecureTransportEnvironment(t *testing.T) {
	environment := map[string]string{
		"KUBLING_HOST_AGENT_GATEWAY":         "gateway:50052",
		"KUBLING_HOST_AGENT_NAMESPACE":       "fleet-a",
		"KUBLING_HOST_AGENT_CREDENTIAL_FILE": "/run/secrets/token",
		"KUBLING_HOST_AGENT_TLS_CA":          "/run/secrets/ca.pem",
		"KUBLING_HOST_AGENT_TLS_SERVER_NAME": "provider.internal",
	}
	config, err := resolveConfig(nil, func(name string) string { return environment[name] })
	if err != nil {
		t.Fatalf("resolveConfig() error = %v", err)
	}
	if config.Insecure || config.CredentialFile != "/run/secrets/token" ||
		config.TLS.CAFile != "/run/secrets/ca.pem" ||
		config.TLS.ServerName != "provider.internal" {
		t.Fatalf("secure config = %#v", config)
	}
}

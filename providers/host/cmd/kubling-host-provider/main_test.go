package main

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestNormalizeConfig(t *testing.T) {
	configuration, err := normalizeConfig(config{
		providerListen: " 127.0.0.1:50051 ",
		agentListen:    " 127.0.0.1:50052 ",
		stateDirectory: "/state",
		namespaces:     []string{" rack-a ", "rack-a", "rack-b"},
		agentInsecure:  true,
		scanTimeout:    time.Second,
	})
	if err != nil {
		t.Fatalf("normalizeConfig() error = %v", err)
	}
	if configuration.providerListen != "127.0.0.1:50051" ||
		configuration.agentListen != "127.0.0.1:50052" ||
		len(configuration.namespaces) != 2 ||
		configuration.maxConcurrentTargets <= 0 ||
		configuration.maxRowsPerTarget <= 0 ||
		configuration.maxBytesPerTarget <= 0 {
		t.Fatalf("normalized config = %#v", configuration)
	}
	if _, err := normalizeConfig(config{
		providerListen: "same",
		agentListen:    "same",
		namespaces:     []string{"rack-a"},
		scanTimeout:    time.Second,
	}); err == nil {
		t.Fatal("normalizeConfig(equal listeners) error = nil")
	}
}

func TestNormalizeConfigRequiresSecureAgentCredentialsByDefault(t *testing.T) {
	configuration, err := normalizeConfig(config{
		providerListen:      "127.0.0.1:50051",
		agentListen:         "127.0.0.1:50052",
		stateDirectory:      "/state",
		agentTLSCertificate: "/tls/server.pem",
		agentTLSPrivateKey:  "/tls/server-key.pem",
		namespaceTokenFiles: map[string]string{"fleet-a": "/tokens/fleet-a"},
		scanTimeout:         time.Second,
	})
	if err != nil {
		t.Fatalf("normalizeConfig(secure) error = %v", err)
	}
	if configuration.agentInsecure || len(configuration.namespaces) != 1 ||
		configuration.namespaces[0] != "fleet-a" {
		t.Fatalf("secure config = %#v", configuration)
	}
	if _, err := normalizeConfig(config{
		providerListen: "127.0.0.1:50051",
		agentListen:    "127.0.0.1:50052",
		stateDirectory: "/state",
		scanTimeout:    time.Second,
	}); err == nil {
		t.Fatal("normalizeConfig(without secure credentials) error = nil")
	}
}

func TestLoadNamespaceTokensReadsSecretFiles(t *testing.T) {
	path := filepath.Join(t.TempDir(), "fleet-a.token")
	if err := os.WriteFile(path, []byte("0123456789abcdef0123456789abcdef\n"), 0o600); err != nil {
		t.Fatalf("WriteFile() error = %v", err)
	}
	tokens, err := loadNamespaceTokens(map[string]string{"fleet-a": path})
	if err != nil {
		t.Fatalf("loadNamespaceTokens() error = %v", err)
	}
	if tokens["fleet-a"] != "0123456789abcdef0123456789abcdef" {
		t.Fatalf("loaded token = %q", tokens["fleet-a"])
	}
}

func TestNamespaceTokenFileMapRejectsDuplicateNamespace(t *testing.T) {
	files := make(namespaceTokenFileMap)
	if err := files.Set("fleet-a=/tokens/first"); err != nil {
		t.Fatalf("Set(first) error = %v", err)
	}
	if err := files.Set("fleet-a=/tokens/second"); err == nil {
		t.Fatal("Set(duplicate) error = nil")
	}
}

func TestNamespaceAuthorizerUsesAllowlist(t *testing.T) {
	authorizer := namespaceAllowlistAuthorizer{"rack-a": {}}
	if namespace, err := authorizer.Authorize(context.Background(), "rack-a"); err != nil || namespace != "rack-a" {
		t.Fatalf("Authorize(allowed) = %q, %v", namespace, err)
	}
	if _, err := authorizer.Authorize(context.Background(), "rack-b"); status.Code(err) != codes.PermissionDenied {
		t.Fatalf("Authorize(denied) code = %v", status.Code(err))
	}
}

func TestEnvironmentParsingRejectsInvalidValues(t *testing.T) {
	t.Setenv("KUBLING_HOST_TEST_BOOL", "sometimes")
	if _, err := environmentBool("KUBLING_HOST_TEST_BOOL", false); err == nil {
		t.Fatal("environmentBool() error = nil")
	}
	t.Setenv("KUBLING_HOST_TEST_DURATION", "later")
	if _, err := environmentDuration("KUBLING_HOST_TEST_DURATION", time.Second); err == nil {
		t.Fatal("environmentDuration() error = nil")
	}
	t.Setenv("KUBLING_HOST_TEST_INT", "many")
	if _, err := environmentInt("KUBLING_HOST_TEST_INT", 1); err == nil {
		t.Fatal("environmentInt() error = nil")
	}
}

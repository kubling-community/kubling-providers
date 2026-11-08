package config

import (
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestLoadNormalizesMultipleBuckets(t *testing.T) {
	path := writeConfig(t, `
logLevel: debug
shutdownGracePeriod: 15s
lobs:
  maxBytes: 1073741824
  maxChunkBytes: 131072
  retention: 2m
backends:
  primary:
    type: s3
    region: eu-west-1
  lab:
    type: s3
    region: us-east-1
    endpoint: http://minio:9000/
    forcePathStyle: true
    uploadPartSizeBytes: 8388608
    multipartThresholdBytes: 16777216
    transferConcurrency: 3
buckets:
  media:
    backend: primary
    name: physical-media
    prefix: tenants/acme
  archive:
    backend: lab
    name: physical-archive
    allowUnboundedList: true
`)

	config, err := Load(path)
	if err != nil {
		t.Fatalf("Load() error = %v", err)
	}
	if config.ListenAddress != ":50051" {
		t.Fatalf("listen = %q", config.ListenAddress)
	}
	if config.LogLevel != slog.LevelDebug {
		t.Fatalf("log level = %s", config.LogLevel)
	}
	if config.ShutdownGracePeriod != 15*time.Second {
		t.Fatalf("shutdown grace period = %s", config.ShutdownGracePeriod)
	}
	if config.Lobs.MaxBytes != 1024*1024*1024 ||
		config.Lobs.MaxChunkBytes != 128*1024 ||
		config.Lobs.Retention != 2*time.Minute {
		t.Fatalf("LOB policy = %+v", config.Lobs)
	}
	if got := config.Backends["lab"].Endpoint; got != "http://minio:9000" {
		t.Fatalf("endpoint = %q", got)
	}
	if got := config.Backends["lab"].TransferConcurrency; got != 3 {
		t.Fatalf("transfer concurrency = %d", got)
	}
	if got := config.Buckets["media"].Prefix; got != "tenants/acme/" {
		t.Fatalf("prefix = %q", got)
	}
	if !config.Buckets["archive"].AllowUnboundedList {
		t.Fatal("archive must permit unbounded listing")
	}
}

func TestLoadNormalizesMixedS3AndFilesystemBackends(t *testing.T) {
	root := filepath.Join(t.TempDir(), "objects")
	path := writeConfig(t, `
backends:
  cloud:
    type: s3
    region: eu-west-1
  local:
    type: filesystem
    root: `+root+`
    createDirectories: true
buckets:
  media:
    backend: cloud
    name: company-media
  scratch:
    backend: local
    name: scratch
    prefix: tests/current
`)

	configuration, err := Load(path)
	if err != nil {
		t.Fatalf("Load() error = %v", err)
	}
	local := configuration.Backends["local"]
	if local.Type != "filesystem" || local.Root != root || !local.CreateDirectories {
		t.Fatalf("filesystem backend = %+v", local)
	}
	if got := configuration.Buckets["scratch"].Prefix; got != "tests/current/" {
		t.Fatalf("filesystem bucket prefix = %q", got)
	}
}

func TestLoadNormalizesAzureBlobBackends(t *testing.T) {
	path := writeConfig(t, `
backends:
  cloud:
    type: azure-blob
    accountUrl: https://example.blob.core.windows.net/
    uploadBlockSizeBytes: 8388608
    transferConcurrency: 3
  emulator:
    type: azure-blob
    accountUrl: http://azurite:10000/devstoreaccount1/
    authentication: shared-key
    accountName: devstoreaccount1
    accountKeyEnvironment: AZURE_STORAGE_ACCOUNT_KEY
buckets:
  media: {backend: cloud, name: media}
  scratch: {backend: emulator, name: scratch, prefix: tests/current}
`)

	configuration, err := Load(path)
	if err != nil {
		t.Fatalf("Load() error = %v", err)
	}
	cloud := configuration.Backends["cloud"]
	if cloud.Type != "azure-blob" || cloud.Authentication != "default" ||
		cloud.AccountURL != "https://example.blob.core.windows.net" ||
		cloud.UploadBlockSizeBytes != 8*1024*1024 || cloud.TransferConcurrency != 3 {
		t.Fatalf("Azure cloud backend = %+v", cloud)
	}
	emulator := configuration.Backends["emulator"]
	if emulator.Authentication != "shared-key" || emulator.AccountName != "devstoreaccount1" ||
		emulator.AccountKeyEnvironment != "AZURE_STORAGE_ACCOUNT_KEY" ||
		emulator.AccountURL != "http://azurite:10000/devstoreaccount1" {
		t.Fatalf("Azure emulator backend = %+v", emulator)
	}
}

func TestLoadNormalizesGCSBackends(t *testing.T) {
	path := writeConfig(t, `
backends:
  cloud:
    type: gcs
    uploadChunkSizeBytes: 8388608
  emulator:
    type: gcs
    endpoint: http://fake-gcs:4443/
    authentication: emulator
buckets:
  media: {backend: cloud, name: media}
  scratch: {backend: emulator, name: scratch, prefix: tests/current}
`)

	configuration, err := Load(path)
	if err != nil {
		t.Fatalf("Load() error = %v", err)
	}
	cloud := configuration.Backends["cloud"]
	if cloud.Type != "gcs" || cloud.Authentication != "default" ||
		cloud.Endpoint != "" || cloud.UploadChunkSizeBytes != 8*1024*1024 {
		t.Fatalf("GCS cloud backend = %+v", cloud)
	}
	emulator := configuration.Backends["emulator"]
	if emulator.Authentication != "emulator" || emulator.Endpoint != "http://fake-gcs:4443" {
		t.Fatalf("GCS emulator backend = %+v", emulator)
	}
}

func TestLoadRejectsUnknownFields(t *testing.T) {
	path := writeConfig(t, `
backends:
  primary:
    type: s3
    region: eu-west-1
    secretAccessKey: leaked
buckets:
  media:
    backend: primary
    name: media
`)

	_, err := Load(path)
	if err == nil || !strings.Contains(err.Error(), "field secretAccessKey not found") {
		t.Fatalf("Load() error = %v", err)
	}
}

func TestLoadRejectsInvalidTopology(t *testing.T) {
	tests := map[string]string{
		"missing backend": `
backends:
  primary: {type: s3, region: eu-west-1}
buckets:
  media: {backend: absent, name: media}
`,
		"case collision": `
backends:
  primary: {type: s3, region: eu-west-1}
buckets:
  media: {backend: primary, name: one}
  MEDIA: {backend: primary, name: two}
`,
		"unsafe endpoint": `
backends:
  primary:
    type: s3
    region: eu-west-1
    endpoint: https://user:pass@example.invalid/storage
buckets:
  media: {backend: primary, name: media}
`,
		"unsafe prefix": `
backends:
  primary: {type: s3, region: eu-west-1}
buckets:
  media: {backend: primary, name: media, prefix: tenant/../other}
`,
		"small multipart part": `
backends:
  primary: {type: s3, region: eu-west-1, uploadPartSizeBytes: 1024}
buckets:
  media: {backend: primary, name: media}
`,
		"short LOB retention": `
lobs: {retention: 500ms}
backends:
  primary: {type: s3, region: eu-west-1}
buckets:
  media: {backend: primary, name: media}
`,
		"large LOB chunk": `
lobs: {maxChunkBytes: 8388608}
backends:
  primary: {type: s3, region: eu-west-1}
buckets:
  media: {backend: primary, name: media}
`,
		"relative filesystem root": `
backends:
  local: {type: filesystem, root: relative/path}
buckets:
  media: {backend: local, name: media}
`,
		"filesystem root directory": `
backends:
  local: {type: filesystem, root: /}
buckets:
  media: {backend: local, name: media}
`,
		"filesystem with S3 settings": `
backends:
  local: {type: filesystem, root: /var/lib/kubling/objects, region: eu-west-1}
buckets:
  media: {backend: local, name: media}
`,
		"unsafe filesystem bucket": `
backends:
  local: {type: filesystem, root: /var/lib/kubling/objects}
buckets:
  media: {backend: local, name: ../media}
`,
		"Azure default auth over HTTP": `
backends:
  cloud: {type: azure-blob, accountUrl: http://example.invalid}
buckets:
  media: {backend: cloud, name: media}
`,
		"Azure URL with credentials": `
backends:
  cloud: {type: azure-blob, accountUrl: https://user:pass@example.invalid}
buckets:
  media: {backend: cloud, name: media}
`,
		"Azure shared key without environment": `
backends:
  cloud:
    type: azure-blob
    accountUrl: https://example.blob.core.windows.net
    authentication: shared-key
    accountName: example
buckets:
  media: {backend: cloud, name: media}
`,
		"Azure small upload block": `
backends:
  cloud:
    type: azure-blob
    accountUrl: https://example.blob.core.windows.net
    uploadBlockSizeBytes: 1024
buckets:
  media: {backend: cloud, name: media}
`,
		"Azure with S3 settings": `
backends:
  cloud:
    type: azure-blob
    accountUrl: https://example.blob.core.windows.net
    region: eu-west-1
buckets:
  media: {backend: cloud, name: media}
`,
		"GCS default auth with endpoint": `
backends:
  cloud: {type: gcs, endpoint: https://example.invalid}
buckets:
  media: {backend: cloud, name: media}
`,
		"GCS emulator without endpoint": `
backends:
  cloud: {type: gcs, authentication: emulator}
buckets:
  media: {backend: cloud, name: media}
`,
		"GCS unsafe endpoint": `
backends:
  cloud: {type: gcs, authentication: emulator, endpoint: https://user:pass@example.invalid}
buckets:
  media: {backend: cloud, name: media}
`,
		"GCS small upload chunk": `
backends:
  cloud: {type: gcs, uploadChunkSizeBytes: 1024}
buckets:
  media: {backend: cloud, name: media}
`,
		"GCS with Azure settings": `
backends:
  cloud:
    type: gcs
    accountUrl: https://example.blob.core.windows.net
buckets:
  media: {backend: cloud, name: media}
`,
	}

	for name, document := range tests {
		t.Run(name, func(t *testing.T) {
			if _, err := Load(writeConfig(t, document)); err == nil {
				t.Fatal("Load() error = nil")
			}
		})
	}
}

func TestLoadAppliesLobDefaults(t *testing.T) {
	configuration, err := Load(writeConfig(t, `
backends:
  primary: {type: s3, region: eu-west-1}
buckets:
  media: {backend: primary, name: media}
`))
	if err != nil {
		t.Fatalf("Load() error = %v", err)
	}
	if configuration.Lobs.MaxBytes != defaultMaxLobBytes ||
		configuration.Lobs.MaxChunkBytes != defaultMaxLobChunkBytes ||
		configuration.Lobs.Retention != defaultLobRetention {
		t.Fatalf("LOB defaults = %+v", configuration.Lobs)
	}
}

func writeConfig(t *testing.T, document string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "provider.yaml")
	if err := os.WriteFile(path, []byte(strings.TrimSpace(document)+"\n"), 0o600); err != nil {
		t.Fatalf("write config: %v", err)
	}
	return path
}

package main

import (
	"context"
	"path/filepath"
	"testing"

	"github.com/kubling-community/kubling-providers/providers/object-storage/internal/backend"
	"github.com/kubling-community/kubling-providers/providers/object-storage/internal/config"
)

func TestBuildStoresInitializesFilesystemBackend(t *testing.T) {
	stores, err := buildStores(context.Background(), config.Config{
		Backends: map[string]config.Backend{
			"local": {
				Type:              "filesystem",
				Root:              filepath.Join(t.TempDir(), "objects"),
				CreateDirectories: true,
			},
		},
	})
	if err != nil {
		t.Fatalf("buildStores() error = %v", err)
	}
	t.Cleanup(func() { _ = closeStores(stores) })
	store := stores["local"]
	if store == nil {
		t.Fatal("filesystem store is nil")
	}
	if err := store.Probe(context.Background(), backend.Bucket{Name: "media"}); err != nil {
		t.Fatalf("Probe() error = %v", err)
	}
}

func TestBuildStoresInitializesAzureBlobBackend(t *testing.T) {
	t.Setenv("KUBLING_TEST_AZURE_KEY", "a3VibGluZy10ZXN0LWtleQ==")
	stores, err := buildStores(context.Background(), config.Config{
		Backends: map[string]config.Backend{
			"azure": {
				Type:                  "azure-blob",
				AccountURL:            "http://127.0.0.1:10000/kublinglocal",
				Authentication:        "shared-key",
				AccountName:           "kublinglocal",
				AccountKeyEnvironment: "KUBLING_TEST_AZURE_KEY",
			},
		},
	})
	if err != nil {
		t.Fatalf("buildStores() error = %v", err)
	}
	t.Cleanup(func() { _ = closeStores(stores) })
	if stores["azure"] == nil {
		t.Fatal("Azure Blob store is nil")
	}
}

func TestBuildStoresInitializesGCSBackend(t *testing.T) {
	stores, err := buildStores(context.Background(), config.Config{
		Backends: map[string]config.Backend{
			"gcs": {
				Type:           "gcs",
				Endpoint:       "http://127.0.0.1:4443",
				Authentication: "emulator",
			},
		},
	})
	if err != nil {
		t.Fatalf("buildStores() error = %v", err)
	}
	t.Cleanup(func() { _ = closeStores(stores) })
	if stores["gcs"] == nil {
		t.Fatal("GCS store is nil")
	}
}

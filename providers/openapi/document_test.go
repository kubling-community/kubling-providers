package openapi

import (
	"context"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestNewLoadsOpenAPISpecificationFromURL(t *testing.T) {
	client := testHTTPClient(func(request *http.Request) (*http.Response, error) {
		if request.Method != http.MethodGet || request.URL.Path != "/openapi.yaml" {
			t.Errorf("specification request = %s %s", request.Method, request.URL.Path)
		}
		if request.URL.Query().Get("revision") != "main" {
			t.Errorf("specification query = %q", request.URL.RawQuery)
		}
		if request.Header.Get("X-Spec-Token") != "spec-secret" {
			t.Errorf("specification header = %q", request.Header.Get("X-Spec-Token"))
		}
		if request.Header.Get("X-API-Key") != "" || request.Header.Get("Authorization") != "" {
			t.Errorf("datasource credentials leaked into specification request: %v", request.Header)
		}
		return &http.Response{
			StatusCode: http.StatusOK,
			Header:     http.Header{"Content-Type": []string{"application/yaml"}},
			Body:       io.NopCloser(strings.NewReader(testOpenAPISpec)),
		}, nil
	})

	provider, err := New(Config{
		SpecFile:    "https://spec.example.test/openapi.yaml?revision=main",
		SpecHeaders: map[string]string{"X-Spec-Token": "spec-secret"},
		BaseURL:     "https://billing.example.test",
		HTTPClient:  client,
		Headers:     map[string]string{"X-API-Key": "datasource-secret"},
		Authentication: &AuthenticationConfig{
			SecurityScheme: "BearerAuth",
			Credential:     "datasource-token",
		},
		Entities: []EntityConfig{{
			Name:          "INVOICE",
			ListOperation: "listInvoices",
			ResponsePath:  "/data/items",
		}},
	})
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	if provider.catalog.entities["INVOICE"] == nil {
		t.Fatal("remote specification did not produce INVOICE entity")
	}
}

func TestNewUsesFirstOpenAPIServerWhenBaseURLIsOmitted(t *testing.T) {
	specPath := writeSpecificationWithServer(t, "https://api.example.test/v1")
	provider, err := New(Config{
		SpecFile:  specPath,
		Discovery: &DiscoveryConfig{Enabled: true},
	})
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	if provider.config.BaseURL != "https://api.example.test/v1" {
		t.Fatalf("provider base URL = %q", provider.config.BaseURL)
	}
}

func TestNewRequiresBaseURLForParameterizedOpenAPIServer(t *testing.T) {
	specPath := writeSpecificationWithServer(t, "https://{tenant}.example.test/v1")
	_, err := New(Config{
		SpecFile:  specPath,
		Discovery: &DiscoveryConfig{Enabled: true},
	})
	if err == nil || !strings.Contains(err.Error(), "server contains variables") {
		t.Fatalf("New() error = %v, want explicit baseUrl error", err)
	}
}

func writeSpecificationWithServer(t *testing.T, serverURL string) string {
	t.Helper()
	specification := strings.Replace(
		testOpenAPISpec,
		"paths:",
		"servers:\n  - url: "+serverURL+"\npaths:",
		1,
	)
	path := filepath.Join(t.TempDir(), "api.yaml")
	writeTestFile(t, path, specification)
	return path
}

func TestNewReportsRemoteSpecificationHTTPError(t *testing.T) {
	client := testHTTPClient(func(*http.Request) (*http.Response, error) {
		return &http.Response{
			StatusCode: http.StatusNotFound,
			Header:     make(http.Header),
			Body:       io.NopCloser(strings.NewReader("missing")),
		}, nil
	})
	config := remoteTestConfig("https://spec.example.test/missing.yaml")
	config.HTTPClient = client

	_, err := New(config)
	if err == nil || !strings.Contains(err.Error(), "HTTP status 404") {
		t.Fatalf("New() error = %v, want HTTP status error", err)
	}
}

func TestNewRejectsOversizedRemoteSpecification(t *testing.T) {
	client := testHTTPClient(func(*http.Request) (*http.Response, error) {
		return &http.Response{
			StatusCode:    http.StatusOK,
			ContentLength: maximumSpecificationSize + 1,
			Body:          io.NopCloser(strings.NewReader("openapi: 3.1.0")),
			Header:        make(http.Header),
		}, nil
	})
	config := remoteTestConfig("https://spec.example.test/openapi.yaml")
	config.HTTPClient = client

	_, err := New(config)
	if err == nil || !strings.Contains(err.Error(), "document exceeds") {
		t.Fatalf("New() error = %v, want size limit error", err)
	}
}

func TestNewRejectsOversizedLocalSpecification(t *testing.T) {
	specPath := filepath.Join(t.TempDir(), "openapi.yaml")
	file, err := os.Create(specPath)
	if err != nil {
		t.Fatalf("os.Create() error = %v", err)
	}
	if err := file.Truncate(maximumSpecificationSize + 1); err != nil {
		file.Close()
		t.Fatalf("Truncate() error = %v", err)
	}
	if err := file.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}

	config := remoteTestConfig(specPath)
	_, err = New(config)
	if err == nil || !strings.Contains(err.Error(), "document exceeds") {
		t.Fatalf("New() error = %v, want local size limit error", err)
	}
}

func TestNewTimesOutRemoteSpecificationDownload(t *testing.T) {
	client := testHTTPClient(func(request *http.Request) (*http.Response, error) {
		<-request.Context().Done()
		return nil, request.Context().Err()
	})
	config := remoteTestConfig("https://spec.example.test/openapi.yaml")
	config.HTTPClient = client
	config.RequestTimeout = time.Millisecond

	_, err := New(config)
	if err == nil || !strings.Contains(err.Error(), context.DeadlineExceeded.Error()) {
		t.Fatalf("New() error = %v, want deadline exceeded", err)
	}
}

func remoteTestConfig(specURL string) Config {
	return Config{
		SpecFile: specURL,
		BaseURL:  "https://billing.example.test",
		Entities: []EntityConfig{{
			Name:          "INVOICE",
			ListOperation: "listInvoices",
			ResponsePath:  "/data/items",
		}},
	}
}

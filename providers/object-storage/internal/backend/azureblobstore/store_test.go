package azureblobstore

import (
	"bytes"
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob"
	"github.com/kubling-community/kubling-providers/providers/object-storage/internal/backend"
	providerconfig "github.com/kubling-community/kubling-providers/providers/object-storage/internal/config"
)

func TestListScopesRequestAndReturnsLogicalKeys(t *testing.T) {
	store := testStore(t, func(writer http.ResponseWriter, request *http.Request) {
		if request.Method != http.MethodGet || request.URL.Path != "/physical" {
			t.Fatalf("request = %s %s", request.Method, request.URL.String())
		}
		query := request.URL.Query()
		if query.Get("restype") != "container" || query.Get("comp") != "list" ||
			query.Get("prefix") != "tenant/acme/images/" ||
			query.Get("marker") != "previous" || query.Get("maxresults") != "100" ||
			query.Get("include") != "metadata" {
			t.Fatalf("query = %v", query)
		}
		writer.Header().Set("Content-Type", "application/xml")
		_, _ = io.WriteString(writer, `<?xml version="1.0" encoding="utf-8"?>
<EnumerationResults ServiceEndpoint="http://127.0.0.1" ContainerName="physical">
  <Prefix>tenant/acme/images/</Prefix>
  <Marker>previous</Marker>
  <MaxResults>100</MaxResults>
  <Blobs>
    <Blob>
      <Name>tenant/acme/images/logo.png</Name>
      <Properties>
        <Last-Modified>Fri, 06 Nov 2026 12:00:00 GMT</Last-Modified>
        <Etag>&quot;etag-1&quot;</Etag>
        <Content-Length>42</Content-Length>
        <Content-Type>image/png</Content-Type>
        <BlobType>BlockBlob</BlobType>
      </Properties>
      <Metadata><owner>docs</owner></Metadata>
      <VersionId>version-1</VersionId>
    </Blob>
    <Blob>
      <Name>another/root/ignored</Name>
      <Properties>
        <Last-Modified>Fri, 06 Nov 2026 12:00:00 GMT</Last-Modified>
        <Etag>&quot;etag-2&quot;</Etag>
        <Content-Length>1</Content-Length>
        <BlobType>BlockBlob</BlobType>
      </Properties>
    </Blob>
  </Blobs>
  <NextMarker>next</NextMarker>
</EnumerationResults>`)
	})

	page, err := store.List(
		context.Background(),
		backend.Bucket{Name: "physical", Prefix: "tenant/acme/"},
		backend.ListRequest{Prefix: "images/", ContinuationToken: "previous", Limit: 100},
	)
	if err != nil {
		t.Fatalf("List() error = %v", err)
	}
	if page.ContinuationToken != "next" || len(page.Objects) != 1 {
		t.Fatalf("page = %+v", page)
	}
	object := page.Objects[0]
	if object.Key != "images/logo.png" || object.Size != 42 || object.ETag != "etag-1" ||
		object.Revision != "etag-1" || object.ContentType != "image/png" ||
		object.VersionID != "version-1" || object.Metadata["owner"] != "docs" {
		t.Fatalf("object = %+v", object)
	}
	if page.Fields != backend.AllObjectFields {
		t.Fatalf("list fields = %v, want %v", page.Fields, backend.AllObjectFields)
	}
}

func TestStatAndRangedOpenPreserveBlobIdentity(t *testing.T) {
	store := testStore(t, func(writer http.ResponseWriter, request *http.Request) {
		if request.URL.Path != "/physical/root/video.bin" {
			t.Fatalf("path = %q", request.URL.Path)
		}
		setObjectHeaders(writer.Header())
		switch request.Method {
		case http.MethodHead:
			writer.Header().Set("Content-Length", "10")
			writer.WriteHeader(http.StatusOK)
		case http.MethodGet:
			if request.Header.Get("x-ms-range") != "bytes=5-7" ||
				request.Header.Get("If-Match") != `"etag-1"` ||
				request.URL.Query().Get("versionid") != "version-1" {
				t.Fatalf("GET headers/query = %v / %v", request.Header, request.URL.Query())
			}
			writer.Header().Set("Content-Length", "3")
			writer.Header().Set("Content-Range", "bytes 5-7/10")
			writer.WriteHeader(http.StatusPartialContent)
			_, _ = io.WriteString(writer, "567")
		default:
			t.Fatalf("method = %s", request.Method)
		}
	})
	bucket := backend.Bucket{Name: "physical", Prefix: "root/"}

	object, err := store.Stat(context.Background(), bucket, "video.bin")
	if err != nil {
		t.Fatalf("Stat() error = %v", err)
	}
	if object.Size != 10 || object.Revision != "etag-1" || object.Metadata["owner"] != "media" {
		t.Fatalf("object = %+v", object)
	}

	length := int64(3)
	reader, err := store.Open(context.Background(), bucket, backend.GetRequest{
		Key: "video.bin", Offset: 5, Length: &length,
		VersionID: "version-1", Revision: "etag-1",
	})
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	content, readErr := io.ReadAll(reader.Body)
	closeErr := reader.Body.Close()
	if readErr != nil || closeErr != nil || string(content) != "567" || reader.Object.Size != 10 {
		t.Fatalf("content/object = %q / %+v, read=%v close=%v", content, reader.Object, readErr, closeErr)
	}
}

func TestPutAndDeleteApplyOptimisticConditions(t *testing.T) {
	var requests int
	store := testStore(t, func(writer http.ResponseWriter, request *http.Request) {
		requests++
		if request.URL.Path != "/physical/root/document.txt" {
			t.Fatalf("path = %q", request.URL.Path)
		}
		switch request.Method {
		case http.MethodPut:
			if request.Header.Get("If-None-Match") != "*" ||
				request.Header.Get("x-ms-blob-content-type") != "text/plain" ||
				request.Header.Get("x-ms-meta-owner") != "docs" {
				t.Fatalf("PUT headers = %v", request.Header)
			}
			body, err := io.ReadAll(request.Body)
			if err != nil || string(body) != "hello" {
				t.Fatalf("PUT body = %q, %v", body, err)
			}
			writer.Header().Set("ETag", `"etag-2"`)
			writer.Header().Set("Last-Modified", "Fri, 06 Nov 2026 12:00:00 GMT")
			writer.Header().Set("x-ms-version-id", "version-2")
			writer.WriteHeader(http.StatusCreated)
		case http.MethodDelete:
			if request.Header.Get("If-Match") != `"etag-2"` ||
				request.URL.Query().Get("versionid") != "version-2" {
				t.Fatalf("DELETE headers/query = %v / %v", request.Header, request.URL.Query())
			}
			writer.WriteHeader(http.StatusAccepted)
		default:
			t.Fatalf("method = %s", request.Method)
		}
	})
	bucket := backend.Bucket{Name: "physical", Prefix: "root/"}
	size := int64(5)
	created, err := store.Put(context.Background(), bucket, backend.PutRequest{
		Key: "document.txt", Body: strings.NewReader("hello"), Size: &size,
		ContentType: "text/plain", Metadata: map[string]string{"owner": "docs"},
		IfNoneMatch: true,
	})
	if err != nil {
		t.Fatalf("Put() error = %v", err)
	}
	if created.ETag != "etag-2" || created.Revision != "etag-2" || created.VersionID != "version-2" {
		t.Fatalf("created = %+v", created)
	}
	if err := store.Delete(context.Background(), bucket, backend.DeleteRequest{
		Key: "document.txt", VersionID: created.VersionID, IfRevisionMatch: created.Revision,
	}); err != nil {
		t.Fatalf("Delete() error = %v", err)
	}
	if requests != 2 {
		t.Fatalf("requests = %d", requests)
	}
}

func TestUpdateAttributesUsesNativeAndAtomicAzureOperations(t *testing.T) {
	t.Run("metadata", func(t *testing.T) {
		store := testStore(t, func(writer http.ResponseWriter, request *http.Request) {
			if request.Method != http.MethodPut || request.URL.Path != "/physical/root/document.txt" ||
				request.URL.Query().Get("comp") != "metadata" ||
				request.Header.Get("If-Match") != `"etag-1"` ||
				request.Header.Get("x-ms-meta-owner") != "docs" {
				t.Fatalf("request = %s %s headers=%v", request.Method, request.URL.String(), request.Header)
			}
			writer.Header().Set("ETag", `"etag-2"`)
			writer.WriteHeader(http.StatusOK)
		})
		err := store.UpdateAttributes(context.Background(), backend.Bucket{
			Name: "physical", Prefix: "root/",
		}, backend.UpdateAttributesRequest{
			Key: "document.txt", Size: 7,
			Metadata: map[string]string{"owner": "docs"},
			Fields:   backend.AttributeFields{Metadata: true}, IfRevisionMatch: "etag-1",
		})
		if err != nil {
			t.Fatalf("UpdateAttributes() error = %v", err)
		}
	})

	t.Run("HTTP properties", func(t *testing.T) {
		store := testStore(t, func(writer http.ResponseWriter, request *http.Request) {
			if request.Method != http.MethodPut || request.URL.Path != "/physical/root/document.txt" ||
				request.URL.Query().Get("comp") != "properties" ||
				request.Header.Get("If-Match") != `"etag-1"` ||
				request.Header.Get("x-ms-blob-content-type") != "text/plain" {
				t.Fatalf("request = %s %s headers=%v", request.Method, request.URL.String(), request.Header)
			}
			writer.Header().Set("ETag", `"etag-2"`)
			writer.WriteHeader(http.StatusOK)
		})
		err := store.UpdateAttributes(context.Background(), backend.Bucket{
			Name: "physical", Prefix: "root/",
		}, backend.UpdateAttributesRequest{
			Key: "document.txt", Size: 7, ContentType: "text/plain",
			Fields: backend.AttributeFields{ContentType: true}, IfRevisionMatch: "etag-1",
		})
		if err != nil {
			t.Fatalf("UpdateAttributes() error = %v", err)
		}
	})

	t.Run("combined", func(t *testing.T) {
		store := testStore(t, func(writer http.ResponseWriter, request *http.Request) {
			if request.Method != http.MethodPut || request.URL.Path != "/physical/root/document.txt" ||
				request.URL.RawQuery != "" ||
				request.Header.Get("If-Match") != `"etag-1"` ||
				request.Header.Get("x-ms-source-if-match") != `"etag-1"` ||
				request.Header.Get("x-ms-copy-source") == "" ||
				request.Header.Get("x-ms-copy-source-blob-properties") != "false" ||
				request.Header.Get("x-ms-blob-content-type") != "text/plain" ||
				request.Header.Get("x-ms-meta-owner") != "docs" {
				t.Fatalf("request = %s %s headers=%v", request.Method, request.URL.String(), request.Header)
			}
			writer.Header().Set("ETag", `"etag-2"`)
			writer.WriteHeader(http.StatusCreated)
		})
		err := store.UpdateAttributes(context.Background(), backend.Bucket{
			Name: "physical", Prefix: "root/",
		}, backend.UpdateAttributesRequest{
			Key: "document.txt", Size: 7, ContentType: "text/plain",
			Metadata:        map[string]string{"owner": "docs"},
			Fields:          backend.AttributeFields{ContentType: true, Metadata: true},
			IfRevisionMatch: "etag-1",
		})
		if err != nil {
			t.Fatalf("UpdateAttributes() error = %v", err)
		}
	})
}

func TestConflictAndNotFoundErrorsArePortable(t *testing.T) {
	store := testStore(t, func(writer http.ResponseWriter, request *http.Request) {
		switch request.Method {
		case http.MethodPut:
			writer.WriteHeader(http.StatusPreconditionFailed)
			_, _ = io.WriteString(writer, azureError("ConditionNotMet"))
		case http.MethodHead:
			writer.WriteHeader(http.StatusNotFound)
			_, _ = io.WriteString(writer, azureError("BlobNotFound"))
		default:
			t.Fatalf("method = %s", request.Method)
		}
	})
	bucket := backend.Bucket{Name: "physical"}
	_, err := store.Put(context.Background(), bucket, backend.PutRequest{
		Key: "conflict", Body: bytes.NewReader([]byte("x")), IfNoneMatch: true,
	})
	if !errors.Is(err, backend.ErrConflict) {
		t.Fatalf("Put() error = %v", err)
	}
	_, err = store.Stat(context.Background(), bucket, "missing")
	if !errors.Is(err, backend.ErrNotFound) {
		t.Fatalf("Stat() error = %v", err)
	}
}

func TestNewRequiresSharedKeyEnvironment(t *testing.T) {
	t.Setenv("KUBLING_TEST_AZURE_KEY", "")
	_, err := New(providerconfig.Backend{
		Type: "azure-blob", AccountURL: "http://127.0.0.1:10000/devstoreaccount1",
		Authentication: "shared-key", AccountName: "devstoreaccount1",
		AccountKeyEnvironment: "KUBLING_TEST_AZURE_KEY",
	})
	if err == nil || !strings.Contains(err.Error(), "KUBLING_TEST_AZURE_KEY") {
		t.Fatalf("New() error = %v", err)
	}
}

func testStore(t *testing.T, handler http.HandlerFunc) *Store {
	t.Helper()
	server := httptest.NewServer(handler)
	t.Cleanup(server.Close)
	client, err := azblob.NewClientWithNoCredential(server.URL, nil)
	if err != nil {
		t.Fatalf("azblob.NewClientWithNoCredential() error = %v", err)
	}
	return newWithClient(client, 1024*1024, 1)
}

func setObjectHeaders(header http.Header) {
	header.Set("ETag", `"etag-1"`)
	header.Set("Last-Modified", "Fri, 06 Nov 2026 12:00:00 GMT")
	header.Set("Content-Type", "application/octet-stream")
	header.Set("x-ms-version-id", "version-1")
	header.Set("x-ms-meta-owner", "media")
}

func azureError(code string) string {
	return `<?xml version="1.0" encoding="utf-8"?><Error><Code>` + code +
		`</Code><Message>test error</Message></Error>`
}

package gcsstore

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"cloud.google.com/go/storage"
	"github.com/kubling-community/kubling-providers/providers/object-storage/internal/backend"
	"google.golang.org/api/googleapi"
	"google.golang.org/api/option"
)

func TestListScopesRequestAndReturnsLogicalKeys(t *testing.T) {
	store := testStore(t, func(writer http.ResponseWriter, request *http.Request) {
		if request.Method != http.MethodGet || request.URL.Path != "/storage/v1/b/physical/o" {
			t.Fatalf("request = %s %s", request.Method, request.URL.String())
		}
		query := request.URL.Query()
		if query.Get("prefix") != "tenant/acme/images/" ||
			query.Get("pageToken") != "previous" || query.Get("maxResults") != "2" {
			t.Fatalf("query = %v", query)
		}
		writer.Header().Set("Content-Type", "application/json")
		_, _ = io.WriteString(writer, `{
  "kind": "storage#objects",
  "nextPageToken": "next",
  "items": [
    {
      "kind": "storage#object",
      "bucket": "physical",
      "name": "tenant/acme/images/logo.png",
      "size": "42",
      "generation": "1700000000000001",
      "metageneration": "1",
      "etag": "etag-1",
      "updated": "2026-11-06T12:00:00Z",
      "contentType": "image/png",
      "md5Hash": "aGVsbG8=",
      "metadata": {"owner": "docs"}
    },
    {
      "kind": "storage#object",
      "bucket": "physical",
      "name": "another/root/ignored",
      "size": "1",
      "generation": "1700000000000002"
    }
  ]
}`)
	})

	page, err := store.List(
		context.Background(),
		backend.Bucket{Name: "physical", Prefix: "tenant/acme/"},
		backend.ListRequest{Prefix: "images/", ContinuationToken: "previous", Limit: 2},
	)
	if err != nil {
		t.Fatalf("List() error = %v", err)
	}
	if page.ContinuationToken != "next" || len(page.Objects) != 1 {
		t.Fatalf("page = %+v", page)
	}
	object := page.Objects[0]
	if object.Key != "images/logo.png" || object.Size != 42 || object.ETag != "etag-1" ||
		object.Revision != "1700000000000001:1" || object.VersionID != "1700000000000001" ||
		object.ContentType != "image/png" || object.Metadata["owner"] != "docs" ||
		object.Checksum != "MD5:aGVsbG8=" {
		t.Fatalf("object = %+v", object)
	}
	if page.Fields != backend.AllObjectFields {
		t.Fatalf("list fields = %v, want %v", page.Fields, backend.AllObjectFields)
	}
}

func TestGenerationAndChecksumMapping(t *testing.T) {
	modified := time.Date(2026, 11, 6, 12, 0, 0, 0, time.UTC)
	object := objectFromAttrs("video.bin", &storage.ObjectAttrs{
		Size:           10,
		Generation:     1234,
		Metageneration: 7,
		Etag:           `"etag-1"`,
		Updated:        modified,
		CRC32C:         0x12345678,
		ContentType:    "video/mp4",
		Metadata:       map[string]string{"owner": "media"},
	})
	if object.Revision != "1234:7" || object.VersionID != "1234" ||
		object.ETag != "etag-1" || object.Checksum != "CRC32C:EjRWeA==" ||
		object.LastModified != modified || object.Metadata["owner"] != "media" {
		t.Fatalf("object = %+v", object)
	}
}

func TestRangedOpenUsesImmutableRawGeneration(t *testing.T) {
	store := testStore(t, func(writer http.ResponseWriter, request *http.Request) {
		if request.Method != http.MethodGet ||
			request.URL.Path != "/physical/root/video.bin" {
			t.Fatalf("request = %s %s", request.Method, request.URL.String())
		}
		query := request.URL.Query()
		if query.Get("generation") != "1234" ||
			request.Header.Get("x-goog-if-generation-match") != "1234" ||
			request.Header.Get("x-goog-if-metageneration-match") != "1" ||
			request.Header.Get("Range") != "bytes=5-7" ||
			request.Header.Get("Accept-Encoding") != "gzip" {
			t.Fatalf("query/headers = %v / %v", query, request.Header)
		}
		writer.Header().Set("Content-Type", "application/octet-stream")
		writer.Header().Set("Content-Encoding", "gzip")
		writer.Header().Set("Content-Length", "3")
		writer.Header().Set("Content-Range", "bytes 5-7/10")
		writer.Header().Set("Last-Modified", "Fri, 06 Nov 2026 12:00:00 GMT")
		writer.Header().Set("x-goog-generation", "1234")
		writer.Header().Set("x-goog-metageneration", "1")
		writer.WriteHeader(http.StatusPartialContent)
		_, _ = io.WriteString(writer, "567")
	})

	length := int64(3)
	reader, err := store.Open(context.Background(), backend.Bucket{
		Name: "physical", Prefix: "root/",
	}, backend.GetRequest{
		Key: "video.bin", Offset: 5, Length: &length,
		VersionID: "1234", Revision: "1234:1",
	})
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	content, readErr := io.ReadAll(reader.Body)
	closeErr := reader.Body.Close()
	if readErr != nil || closeErr != nil || string(content) != "567" ||
		reader.Object.Size != 10 || reader.Object.Revision != "1234:1" ||
		reader.Object.ContentEncoding != "gzip" {
		t.Fatalf(
			"content/object = %q / %+v, read=%v close=%v",
			content,
			reader.Object,
			readErr,
			closeErr,
		)
	}
}

func TestUpdateAttributesUsesGenerationAndMetageneration(t *testing.T) {
	store := testStore(t, func(writer http.ResponseWriter, request *http.Request) {
		if request.Method != http.MethodPatch ||
			!strings.HasSuffix(request.URL.Path, "/root/document.txt") ||
			request.URL.Query().Get("ifGenerationMatch") != "1234" ||
			request.URL.Query().Get("ifMetagenerationMatch") != "7" {
			t.Fatalf("request = %s %s", request.Method, request.URL.String())
		}
		var body map[string]any
		if err := json.NewDecoder(request.Body).Decode(&body); err != nil {
			t.Fatalf("decode body: %v", err)
		}
		metadata, found := body["metadata"]
		if body["cacheControl"] != "public, max-age=60" || !found || metadata != nil {
			t.Fatalf("body = %v", body)
		}
		writer.Header().Set("Content-Type", "application/json")
		_, _ = io.WriteString(writer, `{
  "kind": "storage#object",
  "bucket": "physical",
  "name": "root/document.txt",
  "size": "7",
  "generation": "1234",
  "metageneration": "8"
}`)
	})
	err := store.UpdateAttributes(context.Background(), backend.Bucket{
		Name: "physical", Prefix: "root/",
	}, backend.UpdateAttributesRequest{
		Key: "document.txt", Size: 7, CacheControl: "public, max-age=60",
		Metadata:        map[string]string{},
		Fields:          backend.AttributeFields{CacheControl: true, Metadata: true},
		IfRevisionMatch: "1234:7",
	})
	if err != nil {
		t.Fatalf("UpdateAttributes() error = %v", err)
	}
}

func TestGenerationValidationAndEmulatorEndpoint(t *testing.T) {
	if value, err := parseGeneration("revision", " 1234 "); err != nil || value != 1234 {
		t.Fatalf("parseGeneration() = %d, %v", value, err)
	}
	for _, value := range []string{"", "0", "-1", "not-a-number"} {
		if _, err := parseGeneration("revision", value); err == nil {
			t.Fatalf("parseGeneration(%q) error = nil", value)
		}
	}
	conditions, err := parseRevision("1234:7")
	if err != nil || conditions.GenerationMatch != 1234 || conditions.MetagenerationMatch != 7 {
		t.Fatalf("parseRevision() = %+v, %v", conditions, err)
	}
	for _, value := range []string{"1234:0", "1234:not-a-number", "1:2:3"} {
		if _, err := parseRevision(value); err == nil {
			t.Fatalf("parseRevision(%q) error = nil", value)
		}
	}
	if got := emulatorServiceEndpoint("http://127.0.0.1:4443/proxy/"); got != "http://127.0.0.1:4443/proxy/storage/v1/" {
		t.Fatalf("emulator endpoint = %q", got)
	}
}

func TestPortableErrors(t *testing.T) {
	if err := normalizeError(storage.ErrObjectNotExist); !errors.Is(err, backend.ErrNotFound) {
		t.Fatalf("not-found error = %v", err)
	}
	conflict := normalizeError(&googleapi.Error{Code: http.StatusPreconditionFailed})
	if !errors.Is(conflict, backend.ErrConflict) {
		t.Fatalf("conflict error = %v", conflict)
	}
}

func testStore(t *testing.T, handler http.HandlerFunc) *Store {
	t.Helper()
	server := httptest.NewServer(handler)
	t.Cleanup(server.Close)
	client, err := storage.NewClient(
		context.Background(),
		option.WithEndpoint(strings.TrimRight(server.URL, "/")+"/storage/v1/"),
		option.WithoutAuthentication(),
		storage.WithXMLReads(),
		storage.WithDisabledClientMetrics(),
	)
	if err != nil {
		t.Fatalf("storage.NewClient() error = %v", err)
	}
	t.Cleanup(func() { _ = client.Close() })
	return newWithClient(client, 1024*1024)
}

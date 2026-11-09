package fsstore

import (
	"bytes"
	"context"
	"errors"
	"io"
	"os"
	"path/filepath"
	"testing"

	"github.com/kubling-community/kubling-providers/providers/object-storage/internal/backend"
	"github.com/kubling-community/kubling-providers/providers/object-storage/internal/config"
)

func TestFilesystemLifecycleMetadataAndRanges(t *testing.T) {
	root := filepath.Join(t.TempDir(), "storage")
	store := newTestStore(t, root, true)
	bucket := backend.Bucket{Name: "media", Prefix: "tenant/current/"}
	if err := store.Probe(context.Background(), bucket); err != nil {
		t.Fatalf("Probe() error = %v", err)
	}

	body := []byte("hello filesystem")
	size := int64(len(body))
	created, err := store.Put(context.Background(), bucket, backend.PutRequest{
		Key:                "docs/greeting.txt",
		Body:               bytes.NewReader(body),
		Size:               &size,
		ContentType:        "text/plain; charset=utf-8",
		ContentDisposition: "inline",
		CacheControl:       "max-age=60",
		Metadata:           map[string]string{"owner": "docs"},
		IfNoneMatch:        true,
	})
	if err != nil {
		t.Fatalf("Put(create) error = %v", err)
	}
	if created.ETag == "" || created.Checksum == "" || created.Size != size {
		t.Fatalf("created object = %+v", created)
	}
	physical := filepath.Join(root, "media", "tenant", "current", "docs", "greeting.txt")
	if content, err := os.ReadFile(physical); err != nil || !bytes.Equal(content, body) {
		t.Fatalf("physical object = %q, %v", content, err)
	}

	stat, err := store.Stat(context.Background(), bucket, "docs/greeting.txt")
	if err != nil {
		t.Fatalf("Stat() error = %v", err)
	}
	if stat.ETag != created.ETag || stat.ContentType != created.ContentType ||
		stat.Metadata["owner"] != "docs" {
		t.Fatalf("stat object = %+v", stat)
	}

	length := int64(5)
	opened, err := store.Open(context.Background(), bucket, backend.GetRequest{
		Key: "docs/greeting.txt", Offset: 6, Length: &length, Revision: stat.Revision,
	})
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	content, err := io.ReadAll(opened.Body)
	closeErr := opened.Body.Close()
	if err != nil || closeErr != nil || string(content) != "files" {
		t.Fatalf("ranged content = %q, read=%v close=%v", content, err, closeErr)
	}
	if err := store.UpdateAttributes(context.Background(), bucket, backend.UpdateAttributesRequest{
		Key:          "docs/greeting.txt",
		Size:         stat.Size,
		ContentType:  stat.ContentType,
		CacheControl: "max-age=120",
		Metadata:     map[string]string{"owner": "reviewed"},
		Fields: backend.AttributeFields{
			CacheControl: true,
			Metadata:     true,
		},
		IfRevisionMatch: stat.Revision,
	}); err != nil {
		t.Fatalf("UpdateAttributes() error = %v", err)
	}
	attributeUpdated, err := store.Stat(context.Background(), bucket, "docs/greeting.txt")
	if err != nil {
		t.Fatalf("Stat(attribute updated) error = %v", err)
	}
	if attributeUpdated.ETag != stat.ETag || attributeUpdated.Revision == stat.Revision ||
		attributeUpdated.CacheControl != "max-age=120" ||
		attributeUpdated.Metadata["owner"] != "reviewed" {
		t.Fatalf("attribute-updated object = %+v", attributeUpdated)
	}
	if content, err := os.ReadFile(physical); err != nil || !bytes.Equal(content, body) {
		t.Fatalf("attribute update changed physical object = %q, %v", content, err)
	}
	if err := store.UpdateAttributes(context.Background(), bucket, backend.UpdateAttributesRequest{
		Key: "docs/greeting.txt", Fields: backend.AttributeFields{Metadata: true},
		IfRevisionMatch: stat.Revision,
	}); !errors.Is(err, backend.ErrConflict) {
		t.Fatalf("stale UpdateAttributes() error = %v", err)
	}

	if _, err := store.Put(context.Background(), bucket, backend.PutRequest{
		Key: "docs/greeting.txt", Body: bytes.NewReader([]byte("duplicate")), IfNoneMatch: true,
	}); !errors.Is(err, backend.ErrConflict) {
		t.Fatalf("duplicate Put() error = %v", err)
	}
	if _, err := store.Put(context.Background(), bucket, backend.PutRequest{
		Key: "docs/greeting.txt", Body: bytes.NewReader([]byte("replacement")), IfRevisionMatch: "wrong",
	}); !errors.Is(err, backend.ErrConflict) {
		t.Fatalf("stale Put() error = %v", err)
	}

	replacement := []byte("replacement")
	replacementSize := int64(len(replacement))
	updated, err := store.Put(context.Background(), bucket, backend.PutRequest{
		Key: "docs/greeting.txt", Body: bytes.NewReader(replacement), Size: &replacementSize,
		ContentType: "text/plain", Metadata: map[string]string{"owner": "new"},
		IfRevisionMatch: attributeUpdated.Revision,
	})
	if err != nil {
		t.Fatalf("Put(update) error = %v", err)
	}
	if updated.ETag == stat.ETag {
		t.Fatal("replacement retained the previous ETag")
	}

	if err := store.Delete(context.Background(), bucket, backend.DeleteRequest{
		Key: "docs/greeting.txt", IfRevisionMatch: "stale",
	}); !errors.Is(err, backend.ErrConflict) {
		t.Fatalf("stale Delete() error = %v", err)
	}
	if err := store.Delete(context.Background(), bucket, backend.DeleteRequest{
		Key: "docs/greeting.txt", IfRevisionMatch: updated.Revision,
	}); err != nil {
		t.Fatalf("Delete() error = %v", err)
	}
	if _, err := store.Stat(context.Background(), bucket, "docs/greeting.txt"); !errors.Is(err, backend.ErrNotFound) {
		t.Fatalf("Stat(deleted) error = %v", err)
	}
}

func TestFilesystemListPaginationAndRestart(t *testing.T) {
	root := filepath.Join(t.TempDir(), "storage")
	bucket := backend.Bucket{Name: "archive"}
	store := newTestStore(t, root, true)
	if err := store.Probe(context.Background(), bucket); err != nil {
		t.Fatalf("Probe() error = %v", err)
	}
	for _, key := range []string{"docs/c.txt", "docs/a.txt", "docs/b.txt", "other.txt"} {
		if _, err := store.Put(context.Background(), bucket, backend.PutRequest{
			Key: key, Body: bytes.NewReader([]byte(key)), IfNoneMatch: true,
		}); err != nil {
			t.Fatalf("Put(%q) error = %v", key, err)
		}
	}

	first, err := store.List(context.Background(), bucket, backend.ListRequest{Prefix: "docs/", Limit: 2})
	if err != nil {
		t.Fatalf("List(first) error = %v", err)
	}
	if len(first.Objects) != 2 || first.Objects[0].Key != "docs/a.txt" ||
		first.Objects[1].Key != "docs/b.txt" || first.ContinuationToken != "docs/b.txt" {
		t.Fatalf("first page = %+v", first)
	}
	wantFields := backend.ObjectFieldKey |
		backend.ObjectFieldContentType |
		backend.ObjectFieldContentEncoding |
		backend.ObjectFieldContentDisposition |
		backend.ObjectFieldCacheControl |
		backend.ObjectFieldContentLength |
		backend.ObjectFieldLastModified |
		backend.ObjectFieldVersionID |
		backend.ObjectFieldMetadata
	if first.Fields != wantFields {
		t.Fatalf("list fields = %v, want %v", first.Fields, wantFields)
	}
	second, err := store.List(context.Background(), bucket, backend.ListRequest{
		Prefix: "docs/", ContinuationToken: first.ContinuationToken, Limit: 2,
	})
	if err != nil {
		t.Fatalf("List(second) error = %v", err)
	}
	if len(second.Objects) != 1 || second.Objects[0].Key != "docs/c.txt" || second.ContinuationToken != "" {
		t.Fatalf("second page = %+v", second)
	}

	before, err := store.Stat(context.Background(), bucket, "docs/a.txt")
	if err != nil {
		t.Fatalf("Stat(before restart) error = %v", err)
	}
	if err := store.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}
	reopened := newTestStore(t, root, false)
	after, err := reopened.Stat(context.Background(), bucket, "docs/a.txt")
	if err != nil {
		t.Fatalf("Stat(after restart) error = %v", err)
	}
	if before.ETag != after.ETag || before.ContentType != after.ContentType {
		t.Fatalf("metadata before=%+v after=%+v", before, after)
	}
}

func TestFilesystemReadsExistingFilesAndRejectsEscapes(t *testing.T) {
	root := t.TempDir()
	bucketDirectory := filepath.Join(root, "media")
	if err := os.MkdirAll(filepath.Join(bucketDirectory, "docs"), 0o750); err != nil {
		t.Fatalf("MkdirAll() error = %v", err)
	}
	if err := os.WriteFile(filepath.Join(bucketDirectory, "docs", "manual.json"), []byte(`{"ok":true}`), 0o640); err != nil {
		t.Fatalf("WriteFile() error = %v", err)
	}
	outside := filepath.Join(t.TempDir(), "secret.txt")
	if err := os.WriteFile(outside, []byte("secret"), 0o600); err != nil {
		t.Fatalf("WriteFile(outside) error = %v", err)
	}
	if err := os.Symlink(outside, filepath.Join(bucketDirectory, "escape")); err != nil {
		t.Fatalf("Symlink() error = %v", err)
	}

	store := newTestStore(t, root, false)
	bucket := backend.Bucket{Name: "media"}
	object, err := store.Stat(context.Background(), bucket, "docs/manual.json")
	if err != nil {
		t.Fatalf("Stat(manual) error = %v", err)
	}
	if object.ETag == "" || object.Checksum == "" || object.ContentType != "application/json" {
		t.Fatalf("manual object = %+v", object)
	}
	if _, err := store.Stat(context.Background(), bucket, "escape"); !errors.Is(err, backend.ErrNotFound) {
		t.Fatalf("Stat(symlink) error = %v", err)
	}
	if _, err := store.Stat(context.Background(), bucket, "../secret.txt"); err == nil {
		t.Fatal("Stat(traversal) error = nil")
	}
}

func TestFilesystemRequiresConfiguredDirectoriesWhenCreationIsDisabled(t *testing.T) {
	root := t.TempDir()
	store := newTestStore(t, root, false)
	if err := store.Probe(context.Background(), backend.Bucket{Name: "missing"}); err == nil {
		t.Fatal("Probe() error = nil")
	}
}

func TestFilesystemRejectsOversizedMetadataBeforePublishing(t *testing.T) {
	root := filepath.Join(t.TempDir(), "storage")
	store := newTestStore(t, root, true)
	bucket := backend.Bucket{Name: "media"}
	if err := store.Probe(context.Background(), bucket); err != nil {
		t.Fatalf("Probe() error = %v", err)
	}
	_, err := store.Put(context.Background(), bucket, backend.PutRequest{
		Key:  "large-metadata.bin",
		Body: bytes.NewReader([]byte("content")),
		Metadata: map[string]string{
			"value": string(bytes.Repeat([]byte("x"), maxMetadataBytes)),
		},
		IfNoneMatch: true,
	})
	if err == nil {
		t.Fatal("Put() error = nil")
	}
	if _, err := store.Stat(context.Background(), bucket, "large-metadata.bin"); !errors.Is(err, backend.ErrNotFound) {
		t.Fatalf("Stat(unpublished) error = %v", err)
	}
}

func newTestStore(t *testing.T, root string, createDirectories bool) *Store {
	t.Helper()
	store, err := New(config.Backend{
		Type: "filesystem", Root: root, CreateDirectories: createDirectories,
	})
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	t.Cleanup(func() { _ = store.Close() })
	return store
}

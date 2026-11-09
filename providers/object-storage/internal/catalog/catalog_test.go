package catalog

import (
	"strings"
	"testing"

	kublingv1 "github.com/kubling-community/kubling-grpc/sdk-go/kubling/v1"
	"github.com/kubling-community/kubling-providers/providers/object-storage/internal/config"
	providerv1 "github.com/kubling-community/kubling-providers/sdk-go/kubling/provider/v1"
	"google.golang.org/protobuf/encoding/protojson"
)

func TestCompileExposesOneObjectsTablePerLogicalBucket(t *testing.T) {
	compiled, err := Compile(config.Config{
		Backends: map[string]config.Backend{
			"primary": {Type: "s3", Region: "eu-west-1"},
		},
		Buckets: map[string]config.Bucket{
			"media": {
				Backend: "primary",
				Name:    "company-private-media",
				Prefix:  "tenants/acme/",
			},
			"archive": {
				Backend:            "primary",
				Name:               "company-private-archive",
				AllowUnboundedList: true,
			},
		},
	})
	if err != nil {
		t.Fatalf("Compile() error = %v", err)
	}

	metadata := compiled.Metadata()
	if len(metadata.GetNamespaces()) != 2 || len(metadata.GetTables()) != 2 {
		t.Fatalf(
			"metadata namespaces/tables = %d/%d",
			len(metadata.GetNamespaces()),
			len(metadata.GetTables()),
		)
	}
	if metadata.GetNamespaces()[0].GetName() != "archive" ||
		metadata.GetNamespaces()[1].GetName() != "media" {
		t.Fatalf("namespace order = %v", metadata.GetNamespaces())
	}

	media := table(t, metadata, "media")
	if !media.GetUpdatable() {
		t.Fatal("media.OBJECTS must be updatable")
	}
	if got := columnByName(t, media, ContentColumn).GetType(); got != kublingv1.ValueType_VALUE_TYPE_BLOB {
		t.Fatalf("content type = %s", got)
	}
	if columnByName(t, media, KeyColumn).GetUpdatable() {
		t.Fatal("object key must be immutable")
	}
	if got := columnByName(t, media, BucketColumn).GetProperties()["val_constant"]; got != "media" {
		t.Fatalf("bucket val_constant = %q", got)
	}
	if got := media.GetKeys()[0].GetColumns(); len(got) != 2 ||
		got[0] != BucketColumn || got[1] != KeyColumn {
		t.Fatalf("primary key columns = %v", got)
	}
	if got := media.GetAccessPatterns(); len(got) != 1 ||
		len(got[0].GetColumns()) != 1 || got[0].GetColumns()[0] != KeyColumn {
		t.Fatalf("media access patterns = %v", got)
	}
	if got := table(t, metadata, "archive").GetAccessPatterns(); len(got) != 0 {
		t.Fatalf("archive access patterns = %v", got)
	}
}

func TestCompileDoesNotExposePhysicalBucketNames(t *testing.T) {
	compiled, err := Compile(config.Config{
		Backends: map[string]config.Backend{
			"primary": {Type: "s3", Region: "eu-west-1"},
		},
		Buckets: map[string]config.Bucket{
			"media": {
				Backend: "primary",
				Name:    "account-123-production-media",
			},
		},
	})
	if err != nil {
		t.Fatalf("Compile() error = %v", err)
	}
	encoded, err := protojson.Marshal(compiled.Metadata())
	if err != nil {
		t.Fatalf("protojson.Marshal() error = %v", err)
	}
	if strings.Contains(string(encoded), "account-123-production-media") {
		t.Fatalf("metadata leaks physical bucket name: %s", encoded)
	}
}

func TestResolveKeepsPhysicalRoutingOutOfMetadata(t *testing.T) {
	compiled, err := Compile(config.Config{
		Backends: map[string]config.Backend{
			"primary": {Type: "s3", Region: "eu-west-1"},
		},
		Buckets: map[string]config.Bucket{
			"media": {
				Backend: "primary",
				Name:    "physical-media",
				Prefix:  "root/",
			},
		},
	})
	if err != nil {
		t.Fatalf("Compile() error = %v", err)
	}

	resolved, ok := compiled.Resolve("media", "objects")
	if !ok {
		t.Fatal("Resolve() did not find media.OBJECTS")
	}
	if resolved.Target.Name != "physical-media" || resolved.Target.Prefix != "root/" {
		t.Fatalf("resolved target = %+v", resolved.Target)
	}
	if _, ok := compiled.Resolve("missing", ObjectsTable); ok {
		t.Fatal("Resolve() found an unknown namespace")
	}
}

func table(
	t *testing.T,
	metadata *providerv1.SchemaMetadata,
	namespace string,
) *providerv1.TableMetadata {
	t.Helper()
	for _, candidate := range metadata.GetTables() {
		if candidate.GetNamespace() == namespace && candidate.GetName() == ObjectsTable {
			return candidate
		}
	}
	t.Fatalf("table %s.%s not found", namespace, ObjectsTable)
	return nil
}

func columnByName(
	t *testing.T,
	table *providerv1.TableMetadata,
	name string,
) *providerv1.ColumnMetadata {
	t.Helper()
	for _, candidate := range table.GetColumns() {
		if candidate.GetName() == name {
			return candidate
		}
	}
	t.Fatalf("column %s.%s not found", table.GetName(), name)
	return nil
}

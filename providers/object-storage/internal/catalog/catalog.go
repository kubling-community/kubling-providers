// Package catalog compiles configured buckets into Kubling metadata.
package catalog

import (
	"fmt"
	"slices"
	"strconv"
	"strings"

	kublingv1 "github.com/kubling-community/kubling-grpc/sdk-go/kubling/v1"
	"github.com/kubling-community/kubling-providers/providers/object-storage/internal/backend"
	"github.com/kubling-community/kubling-providers/providers/object-storage/internal/config"
	providerv1 "github.com/kubling-community/kubling-providers/sdk-go/kubling/provider/v1"
	providersdk "github.com/kubling-community/kubling-providers/sdk-go/provider"
	"google.golang.org/protobuf/proto"
)

const (
	ObjectsTable             = "OBJECTS"
	BucketColumn             = "bucket"
	KeyColumn                = "key"
	ContentColumn            = "content"
	ContentTypeColumn        = "content_type"
	ContentEncodingColumn    = "content_encoding"
	ContentDispositionColumn = "content_disposition"
	CacheControlColumn       = "cache_control"
	ContentLengthColumn      = "content_length"
	ETagColumn               = "etag"
	LastModifiedColumn       = "last_modified"
	VersionIDColumn          = "version_id"
	ChecksumColumn           = "checksum"
	MetadataColumn           = "metadata"
)

// Bucket is one immutable logical-to-physical routing entry.
type Bucket struct {
	Namespace          string
	BackendName        string
	BackendType        string
	Target             backend.Bucket
	AllowUnboundedList bool
}

// Catalog owns the immutable schema and bucket routing table.
type Catalog struct {
	metadata *providerv1.SchemaMetadata
	buckets  map[string]Bucket
}

// Compile builds metadata and routing from validated configuration.
func Compile(configuration config.Config) (*Catalog, error) {
	if len(configuration.Buckets) == 0 {
		return nil, fmt.Errorf("object-storage catalog requires at least one bucket")
	}

	aliases := make([]string, 0, len(configuration.Buckets))
	for alias := range configuration.Buckets {
		aliases = append(aliases, alias)
	}
	slices.SortFunc(aliases, func(left, right string) int {
		return strings.Compare(strings.ToUpper(left), strings.ToUpper(right))
	})

	metadata := &providerv1.SchemaMetadata{
		Properties: map[string]string{
			"object_storage.schema_model": "bucket-namespaces",
		},
	}
	buckets := make(map[string]Bucket, len(aliases))
	for _, alias := range aliases {
		bucketConfig := configuration.Buckets[alias]
		backendConfig, exists := configuration.Backends[bucketConfig.Backend]
		if !exists {
			return nil, fmt.Errorf(
				"bucket %q references unknown backend %q",
				alias,
				bucketConfig.Backend,
			)
		}
		routing := Bucket{
			Namespace:   alias,
			BackendName: bucketConfig.Backend,
			BackendType: backendConfig.Type,
			Target: backend.Bucket{
				Name:   bucketConfig.Name,
				Prefix: bucketConfig.Prefix,
			},
			AllowUnboundedList: bucketConfig.AllowUnboundedList,
		}
		buckets[strings.ToUpper(alias)] = routing
		metadata.Namespaces = append(metadata.Namespaces, namespaceMetadata(routing))
		metadata.Tables = append(metadata.Tables, objectsMetadata(routing))
	}

	return &Catalog{metadata: metadata, buckets: buckets}, nil
}

// Metadata returns an isolated copy of the compiled schema.
func (c *Catalog) Metadata() *providersdk.Metadata {
	if c == nil || c.metadata == nil {
		return nil
	}
	return proto.Clone(c.metadata).(*providerv1.SchemaMetadata)
}

// Resolve returns routing for one advertised namespace and entity.
func (c *Catalog) Resolve(namespace string, entity string) (Bucket, bool) {
	if c == nil || !strings.EqualFold(strings.TrimSpace(entity), ObjectsTable) {
		return Bucket{}, false
	}
	bucket, found := c.buckets[strings.ToUpper(strings.TrimSpace(namespace))]
	return bucket, found
}

// Buckets returns an isolated, deterministic snapshot of configured routes.
func (c *Catalog) Buckets() []Bucket {
	if c == nil {
		return nil
	}
	buckets := make([]Bucket, 0, len(c.buckets))
	for _, bucket := range c.buckets {
		buckets = append(buckets, bucket)
	}
	slices.SortFunc(buckets, func(left, right Bucket) int {
		return strings.Compare(strings.ToUpper(left.Namespace), strings.ToUpper(right.Namespace))
	})
	return buckets
}

func namespaceMetadata(bucket Bucket) *providerv1.NamespaceMetadata {
	return &providerv1.NamespaceMetadata{
		Name:       bucket.Namespace,
		Annotation: "Configured logical object-storage bucket.",
		Properties: map[string]string{
			"object_storage.backend_type": bucket.BackendType,
		},
	}
}

func objectsMetadata(bucket Bucket) *providerv1.TableMetadata {
	updatable := true
	table := &providerv1.TableMetadata{
		Name:       ObjectsTable,
		Kind:       providerv1.TableKind_TABLE_KIND_TABLE,
		Namespace:  bucket.Namespace,
		Updatable:  &updatable,
		Annotation: "Objects stored in the configured logical bucket.",
		Properties: map[string]string{
			"object_storage.allow_unbounded_list": strconv.FormatBool(bucket.AllowUnboundedList),
		},
		Columns: []*providerv1.ColumnMetadata{
			column(BucketColumn, kublingv1.ValueType_VALUE_TYPE_STRING, false, false,
				providerv1.ColumnSearchability_COLUMN_SEARCHABILITY_EQUALITY,
				"Logical bucket namespace.", map[string]string{"val_constant": bucket.Namespace}),
			column(KeyColumn, kublingv1.ValueType_VALUE_TYPE_STRING, false, false,
				providerv1.ColumnSearchability_COLUMN_SEARCHABILITY_EQUALITY,
				"Portable object key relative to the configured bucket root.", nil),
			column(ContentColumn, kublingv1.ValueType_VALUE_TYPE_BLOB, false, true,
				providerv1.ColumnSearchability_COLUMN_SEARCHABILITY_UNSEARCHABLE,
				"Object content.", nil),
			column(ContentTypeColumn, kublingv1.ValueType_VALUE_TYPE_STRING, true, true,
				providerv1.ColumnSearchability_COLUMN_SEARCHABILITY_UNSEARCHABLE,
				"Media type supplied by the object store.", nil),
			column(ContentEncodingColumn, kublingv1.ValueType_VALUE_TYPE_STRING, true, true,
				providerv1.ColumnSearchability_COLUMN_SEARCHABILITY_UNSEARCHABLE,
				"Content encoding supplied by the object store.", nil),
			column(ContentDispositionColumn, kublingv1.ValueType_VALUE_TYPE_STRING, true, true,
				providerv1.ColumnSearchability_COLUMN_SEARCHABILITY_UNSEARCHABLE,
				"Content disposition supplied by the object store.", nil),
			column(CacheControlColumn, kublingv1.ValueType_VALUE_TYPE_STRING, true, true,
				providerv1.ColumnSearchability_COLUMN_SEARCHABILITY_UNSEARCHABLE,
				"Cache control policy supplied by the object store.", nil),
			column(ContentLengthColumn, kublingv1.ValueType_VALUE_TYPE_LONG, false, false,
				providerv1.ColumnSearchability_COLUMN_SEARCHABILITY_UNSEARCHABLE,
				"Object size in bytes.", nil),
			column(ETagColumn, kublingv1.ValueType_VALUE_TYPE_STRING, true, false,
				providerv1.ColumnSearchability_COLUMN_SEARCHABILITY_UNSEARCHABLE,
				"Backend entity tag without interpreting its checksum semantics.", nil),
			column(LastModifiedColumn, kublingv1.ValueType_VALUE_TYPE_TIMESTAMP, true, false,
				providerv1.ColumnSearchability_COLUMN_SEARCHABILITY_UNSEARCHABLE,
				"Backend last-modified timestamp.", nil),
			column(VersionIDColumn, kublingv1.ValueType_VALUE_TYPE_STRING, true, false,
				providerv1.ColumnSearchability_COLUMN_SEARCHABILITY_UNSEARCHABLE,
				"Backend object version identifier when versioning is enabled.", nil),
			column(ChecksumColumn, kublingv1.ValueType_VALUE_TYPE_STRING, true, false,
				providerv1.ColumnSearchability_COLUMN_SEARCHABILITY_UNSEARCHABLE,
				"Backend checksum when one is available.", nil),
			column(MetadataColumn, kublingv1.ValueType_VALUE_TYPE_JSON, true, true,
				providerv1.ColumnSearchability_COLUMN_SEARCHABILITY_UNSEARCHABLE,
				"User-defined object metadata represented as JSON.", nil),
		},
		Keys: []*providerv1.KeyMetadata{{
			Name:    "PK_OBJECTS",
			Kind:    providerv1.KeyKind_KEY_KIND_PRIMARY,
			Columns: []string{BucketColumn, KeyColumn},
		}},
	}
	if !bucket.AllowUnboundedList {
		table.AccessPatterns = []*providerv1.AccessPatternMetadata{{
			Name:    "AP_OBJECTS_KEY",
			Columns: []string{KeyColumn},
		}}
	}
	return table
}

func column(
	name string,
	valueType kublingv1.ValueType,
	nullable bool,
	updatable bool,
	searchability providerv1.ColumnSearchability,
	annotation string,
	properties map[string]string,
) *providerv1.ColumnMetadata {
	return &providerv1.ColumnMetadata{
		Name:          name,
		Type:          valueType,
		Nullable:      proto.Bool(nullable),
		Updatable:     proto.Bool(updatable),
		Searchability: searchability,
		Annotation:    annotation,
		Properties:    properties,
	}
}

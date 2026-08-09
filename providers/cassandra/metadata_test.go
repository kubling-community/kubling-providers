package cassandra

import (
	"testing"

	"github.com/apache/cassandra-gocql-driver/v2"
	kublingv1 "github.com/kubling-community/kubling-grpc/sdk-go/kubling/v1"
	providerv1 "github.com/kubling-community/kubling-providers/sdk-go/kubling/provider/v1"
)

func TestSchemaMetadataBuildsDeterministicRelationalModel(t *testing.T) {
	tenant := testColumn("tenant_id", gocql.TypeUUID, gocql.ColumnPartitionKey)
	occurredAt := testColumn("occurred_at", gocql.TypeTimestamp, gocql.ColumnClusteringKey)
	occurredAt.Order = gocql.DESC
	payload := testColumn("payload", gocql.TypeBlob, gocql.ColumnRegular)
	statusColumn := testColumn("status", gocql.TypeVarchar, gocql.ColumnRegular)
	statusColumn.Index = gocql.ColumnIndexMetadata{
		Name: "events_status_idx",
		Type: "COMPOSITES",
	}

	metadata := schemaMetadata("inventory", &gocql.KeyspaceMetadata{
		Name:          "events",
		StrategyClass: "NetworkTopologyStrategy",
		Tables: map[string]*gocql.TableMetadata{
			"z_archive": {
				Name:    "z_archive",
				Columns: map[string]*gocql.ColumnMetadata{},
			},
			"event": {
				Name:              "event",
				PartitionKey:      []*gocql.ColumnMetadata{tenant},
				ClusteringColumns: []*gocql.ColumnMetadata{occurredAt},
				OrderedColumns:    []string{"tenant_id", "occurred_at", "payload"},
				Columns: map[string]*gocql.ColumnMetadata{
					"tenant_id":   tenant,
					"occurred_at": occurredAt,
					"payload":     payload,
					"status":      statusColumn,
				},
			},
		},
	})

	if len(metadata.GetTables()) != 2 {
		t.Fatalf("schema tables = %d, want 2", len(metadata.GetTables()))
	}
	if metadata.GetTables()[0].GetName() != "event" ||
		metadata.GetTables()[1].GetName() != "z_archive" {
		t.Fatalf("schema table order = %v", metadata.GetTables())
	}
	if metadata.GetProperties()["cassandra.replication_strategy"] != "NetworkTopologyStrategy" {
		t.Fatalf("schema properties = %v", metadata.GetProperties())
	}
	if len(metadata.GetNamespaces()) != 1 ||
		metadata.GetNamespaces()[0].GetName() != "inventory" {
		t.Fatalf("schema namespaces = %v, want [inventory]", metadata.GetNamespaces())
	}

	table := metadata.GetTables()[0]
	if table.GetNamespace() != "inventory" {
		t.Fatalf("table namespace = %q, want inventory", table.GetNamespace())
	}
	if table.GetKind() != providerv1.TableKind_TABLE_KIND_TABLE || !table.GetUpdatable() {
		t.Fatalf("table metadata = %v", table)
	}
	wantColumnOrder := []string{"tenant_id", "occurred_at", "payload", "status"}
	for index, columnName := range wantColumnOrder {
		if got := table.GetColumns()[index].GetName(); got != columnName {
			t.Fatalf("column %d = %q, want %q", index, got, columnName)
		}
	}

	primaryKey := table.GetKeys()[0]
	if primaryKey.GetKind() != providerv1.KeyKind_KEY_KIND_PRIMARY {
		t.Fatalf("primary key kind = %v", primaryKey.GetKind())
	}
	if len(primaryKey.GetColumns()) != 2 ||
		primaryKey.GetColumns()[0] != "tenant_id" ||
		primaryKey.GetColumns()[1] != "occurred_at" {
		t.Fatalf("primary key columns = %v", primaryKey.GetColumns())
	}

	if table.GetColumns()[0].GetType() != kublingv1.ValueType_VALUE_TYPE_STRING {
		t.Fatalf("UUID logical type = %v, want STRING", table.GetColumns()[0].GetType())
	}
	if table.GetColumns()[0].GetNullable() {
		t.Fatal("partition key nullable = true, want false")
	}
	if table.GetColumns()[1].GetSearchability() !=
		providerv1.ColumnSearchability_COLUMN_SEARCHABILITY_ORDERED {
		t.Fatalf("clustering searchability = %v", table.GetColumns()[1].GetSearchability())
	}
	if table.GetColumns()[1].GetProperties()["cassandra.clustering_order"] != "DESC" {
		t.Fatalf("clustering properties = %v", table.GetColumns()[1].GetProperties())
	}
	if table.GetColumns()[2].GetType() != kublingv1.ValueType_VALUE_TYPE_BLOB {
		t.Fatalf("blob logical type = %v, want BLOB", table.GetColumns()[2].GetType())
	}
	if table.GetColumns()[3].GetSearchability() !=
		providerv1.ColumnSearchability_COLUMN_SEARCHABILITY_EQUALITY {
		t.Fatalf("indexed searchability = %v", table.GetColumns()[3].GetSearchability())
	}
}

func TestLogicalValueTypeCoversCassandraTypes(t *testing.T) {
	tests := map[gocql.Type]kublingv1.ValueType{
		gocql.TypeAscii:     kublingv1.ValueType_VALUE_TYPE_STRING,
		gocql.TypeBigInt:    kublingv1.ValueType_VALUE_TYPE_LONG,
		gocql.TypeBlob:      kublingv1.ValueType_VALUE_TYPE_BLOB,
		gocql.TypeBoolean:   kublingv1.ValueType_VALUE_TYPE_BOOLEAN,
		gocql.TypeCounter:   kublingv1.ValueType_VALUE_TYPE_LONG,
		gocql.TypeDecimal:   kublingv1.ValueType_VALUE_TYPE_BIGDECIMAL,
		gocql.TypeDouble:    kublingv1.ValueType_VALUE_TYPE_DOUBLE,
		gocql.TypeFloat:     kublingv1.ValueType_VALUE_TYPE_FLOAT,
		gocql.TypeInt:       kublingv1.ValueType_VALUE_TYPE_INTEGER,
		gocql.TypeTimestamp: kublingv1.ValueType_VALUE_TYPE_TIMESTAMP,
		gocql.TypeVarint:    kublingv1.ValueType_VALUE_TYPE_BIGINTEGER,
		gocql.TypeDate:      kublingv1.ValueType_VALUE_TYPE_DATE,
		gocql.TypeTime:      kublingv1.ValueType_VALUE_TYPE_TIME,
		gocql.TypeSmallInt:  kublingv1.ValueType_VALUE_TYPE_SHORT,
		gocql.TypeTinyInt:   kublingv1.ValueType_VALUE_TYPE_BYTE,
		gocql.TypeTuple:     kublingv1.ValueType_VALUE_TYPE_JSON,
	}

	for cassandraType, expected := range tests {
		t.Run(simpleNativeTypeName(cassandraType), func(t *testing.T) {
			var typeInfo gocql.TypeInfo
			if cassandraType == gocql.TypeTuple {
				typeInfo = gocql.TupleTypeInfo{}
			} else {
				typeInfo = gocql.NewNativeType(4, cassandraType, "")
			}
			if got := logicalValueType(typeInfo); got != expected {
				t.Fatalf("logicalValueType() = %v, want %v", got, expected)
			}
		})
	}
}

func testColumn(
	name string,
	cassandraType gocql.Type,
	kind gocql.ColumnKind,
) *gocql.ColumnMetadata {
	return &gocql.ColumnMetadata{
		Name: name,
		Kind: kind,
		Type: gocql.NewNativeType(4, cassandraType, ""),
	}
}

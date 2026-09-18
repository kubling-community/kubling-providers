package provider

import (
	"testing"

	kublingv1 "github.com/kubling-community/kubling-grpc/sdk-go/kubling/v1"
	providerv1 "github.com/kubling-community/kubling-providers/sdk-go/kubling/provider/v1"
	"google.golang.org/protobuf/proto"
)

func TestExtendedValuesRoundTripThroughProviderMessages(t *testing.T) {
	srid := int32(4326)
	crs := "EPSG:4326"
	sizeBytes := uint64(4096)
	expiresAtUnixMs := int64(1_800_000_000_000)
	integerType := &kublingv1.TypeDescriptor{Type: kublingv1.ValueType_VALUE_TYPE_INTEGER}
	arrayType := &kublingv1.TypeDescriptor{
		Type:        kublingv1.ValueType_VALUE_TYPE_ARRAY,
		ElementType: integerType,
	}

	values := []*kublingv1.Value{
		{
			Kind: &kublingv1.Value_ArrayValue{ArrayValue: &kublingv1.ArrayValue{
				ElementType: integerType,
				Elements: []*kublingv1.Value{
					{Kind: &kublingv1.Value_IntegerValue{IntegerValue: 7}},
					{Kind: &kublingv1.Value_NullValue{NullValue: &kublingv1.NullValue{}}},
				},
			}},
		},
		{
			Kind: &kublingv1.Value_GeometryWithCrs{GeometryWithCrs: &kublingv1.SpatialValue{
				Wkb:  []byte{1, 1, 0, 0, 0},
				Srid: &srid,
				Crs:  &crs,
			}},
		},
		{
			Kind: &kublingv1.Value_GeographyWithCrs{GeographyWithCrs: &kublingv1.SpatialValue{
				Wkb:  []byte{1, 2, 0, 0, 0},
				Srid: &srid,
				Crs:  &crs,
			}},
		},
		{
			Kind: &kublingv1.Value_LobReference{LobReference: &kublingv1.LobReference{
				LobId:           "lob-1",
				Type:            kublingv1.ValueType_VALUE_TYPE_BLOB,
				SizeBytes:       &sizeBytes,
				SessionId:       "session-1",
				ExpiresAtUnixMs: &expiresAtUnixMs,
			}},
		},
	}

	batch := &providerv1.TupleBatch{
		Fields: []*providerv1.Field{
			{
				Name:           "numbers",
				Type:           kublingv1.ValueType_VALUE_TYPE_ARRAY,
				TypeDescriptor: arrayType,
			},
			{Name: "shape", Type: kublingv1.ValueType_VALUE_TYPE_GEOMETRY},
			{Name: "area", Type: kublingv1.ValueType_VALUE_TYPE_GEOGRAPHY},
			{Name: "payload", Type: kublingv1.ValueType_VALUE_TYPE_BLOB},
		},
		Tuples: []*providerv1.Tuple{{Values: values}},
	}
	encodedBatch, err := proto.Marshal(batch)
	if err != nil {
		t.Fatalf("proto.Marshal(TupleBatch) error = %v", err)
	}
	decodedBatch := &providerv1.TupleBatch{}
	if err := proto.Unmarshal(encodedBatch, decodedBatch); err != nil {
		t.Fatalf("proto.Unmarshal(TupleBatch) error = %v", err)
	}
	if !proto.Equal(decodedBatch, batch) {
		t.Fatalf("TupleBatch round trip = %v, want %v", decodedBatch, batch)
	}

	expression := &providerv1.Expression{Kind: &providerv1.Expression_Literal{
		Literal: &providerv1.Literal{
			Value:        values[0],
			DeclaredType: arrayType,
		},
	}}
	encodedExpression, err := proto.Marshal(expression)
	if err != nil {
		t.Fatalf("proto.Marshal(Expression) error = %v", err)
	}
	decodedExpression := &providerv1.Expression{}
	if err := proto.Unmarshal(encodedExpression, decodedExpression); err != nil {
		t.Fatalf("proto.Unmarshal(Expression) error = %v", err)
	}
	if !proto.Equal(decodedExpression, expression) {
		t.Fatalf("Expression round trip = %v, want %v", decodedExpression, expression)
	}

	metadata := &providerv1.SchemaMetadata{Tables: []*providerv1.TableMetadata{{
		Name: "samples",
		Columns: []*providerv1.ColumnMetadata{{
			Name:           "numbers",
			Type:           kublingv1.ValueType_VALUE_TYPE_ARRAY,
			TypeDescriptor: arrayType,
		}},
	}}}
	encodedMetadata, err := proto.Marshal(metadata)
	if err != nil {
		t.Fatalf("proto.Marshal(SchemaMetadata) error = %v", err)
	}
	decodedMetadata := &providerv1.SchemaMetadata{}
	if err := proto.Unmarshal(encodedMetadata, decodedMetadata); err != nil {
		t.Fatalf("proto.Unmarshal(SchemaMetadata) error = %v", err)
	}
	if !proto.Equal(decodedMetadata, metadata) {
		t.Fatalf("SchemaMetadata round trip = %v, want %v", decodedMetadata, metadata)
	}
}

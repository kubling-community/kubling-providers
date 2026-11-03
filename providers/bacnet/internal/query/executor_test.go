package query

import (
	"context"
	"errors"
	"io"
	"net/netip"
	"testing"
	"time"

	grpcfeatures "github.com/kubling-community/kubling-grpc/sdk-go/features"
	kublingv1 "github.com/kubling-community/kubling-grpc/sdk-go/kubling/v1"
	"github.com/kubling-community/kubling-providers/providers/bacnet/internal/catalog"
	"github.com/kubling-community/kubling-providers/providers/bacnet/internal/discovery"
	"github.com/kubling-community/kubling-providers/providers/bacnet/internal/profile"
	providerv1 "github.com/kubling-community/kubling-providers/sdk-go/kubling/provider/v1"
	bacnet "github.com/otfabric/go-bacnet"
	"github.com/otfabric/go-bacnet/service"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestExecuteReadsOneDeviceAndConvertsPointRows(t *testing.T) {
	compiled := testCatalog(t)
	analog := bacnet.ObjectIdentifier{Type: bacnet.ObjectTypeAnalogInput, Instance: 1}
	source := testDeviceSource(analog)
	source.values[analog] = map[bacnet.PropertyIdentifier]bacnet.ApplicationValue{
		bacnet.PropertyObjectIdentifier: bacnet.ObjectIDValue(analog),
		bacnet.PropertyObjectName:       textApplicationValue("Supply temperature"),
		bacnet.PropertyPresentValue:     bacnet.RealValue(21.5),
	}
	source.errors[analog] = map[bacnet.PropertyIdentifier]error{
		bacnet.PropertyDescription: errors.New("unknown property"),
	}

	stream, err := Execute(context.Background(), compiled, source, &providerv1.QueryRequest{
		Entity: &providerv1.EntityReference{Name: catalog.AnalogPointsTable},
		Projections: []*providerv1.Projection{
			fieldProjection("namespace"),
			fieldProjection("object_identifier"),
			fieldProjection("object_name"),
			fieldProjection("present_value"),
			fieldProjection("description"),
		},
		Filter: deviceFilter("madrid.dc1", "cooling", 1201),
	})
	if err != nil {
		t.Fatalf("Execute() error = %v", err)
	}
	batch := nextBatch(t, stream)
	if len(batch.GetTuples()) != 1 {
		t.Fatalf("tuple count = %d, want 1", len(batch.GetTuples()))
	}
	values := batch.GetTuples()[0].GetValues()
	if values[0].GetStringValue() != "madrid.dc1" ||
		values[1].GetStringValue() != "analog-input:1" ||
		values[2].GetStringValue() != "Supply temperature" ||
		values[3].GetDoubleValue() != 21.5 ||
		values[4].GetNullValue() == nil {
		t.Fatalf("values = %v", values)
	}
	if source.objectListCalls != 1 || source.propertyCalls != 1 {
		t.Fatalf("read calls = object-list %d, properties %d", source.objectListCalls, source.propertyCalls)
	}
	if _, err := stream.Next(context.Background()); !errors.Is(err, io.EOF) {
		t.Fatalf("stream.Next() error = %v, want EOF", err)
	}
}

func TestExecuteRejectsUnboundedDeviceQuery(t *testing.T) {
	_, err := Execute(context.Background(), testCatalog(t), testDeviceSource(), &providerv1.QueryRequest{
		Entity: &providerv1.EntityReference{Name: catalog.ObjectsTable},
	})
	if status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("Execute() code = %v, want FailedPrecondition (%v)", status.Code(err), err)
	}
}

func TestExecuteReturnsEmptyForUnknownDeviceWithoutPhysicalRead(t *testing.T) {
	source := testDeviceSource()
	stream, err := Execute(context.Background(), testCatalog(t), source, &providerv1.QueryRequest{
		Entity:      &providerv1.EntityReference{Name: catalog.ObjectsTable},
		Projections: []*providerv1.Projection{fieldProjection("object_name")},
		Filter:      deviceFilter("madrid.dc1", "cooling", 9999),
	})
	if err != nil {
		t.Fatalf("Execute() error = %v", err)
	}
	if _, err := stream.Next(context.Background()); !errors.Is(err, io.EOF) {
		t.Fatalf("stream.Next() error = %v, want EOF", err)
	}
	if source.objectListCalls != 0 || source.propertyCalls != 0 {
		t.Fatalf("unexpected physical reads: %#v", source)
	}
}

func TestExecuteServesScopesFromRegistryOnly(t *testing.T) {
	source := testDeviceSource()
	stream, err := Execute(context.Background(), testCatalog(t), source, &providerv1.QueryRequest{
		Entity: &providerv1.EntityReference{Name: catalog.ScopesTable},
		Projections: []*providerv1.Projection{
			fieldProjection("namespace"),
			fieldProjection("scope_id"),
			fieldProjection("device_count"),
		},
		Filter: stringEquality("namespace", "madrid.dc1"),
	})
	if err != nil {
		t.Fatalf("Execute() error = %v", err)
	}
	batch := nextBatch(t, stream)
	values := batch.GetTuples()[0].GetValues()
	if values[0].GetStringValue() != "madrid.dc1" ||
		values[1].GetStringValue() != "cooling" ||
		values[2].GetIntegerValue() != 1 {
		t.Fatalf("scope values = %v", values)
	}
	if source.objectListCalls != 0 || source.propertyCalls != 0 {
		t.Fatal("SCOPES query contacted a BACnet device")
	}
}

func TestExecuteServesDevicesFromRegistryWithoutBindingsOrPhysicalReads(t *testing.T) {
	source := testDeviceSource()
	source.snapshot.Scopes[0].Devices[0].Identity = discovery.Identity{
		VendorIdentifier:           77,
		VendorName:                 "Acme",
		ModelName:                  "Controller",
		FirmwareRevision:           "1.2.3",
		ApplicationSoftwareVersion: "4.5.6",
		ProtocolVersion:            1,
		ProtocolRevision:           24,
	}
	stream, err := Execute(context.Background(), testCatalog(t), source, &providerv1.QueryRequest{
		Entity: &providerv1.EntityReference{Name: catalog.DevicesTable},
		Projections: []*providerv1.Projection{
			fieldProjection("namespace"),
			fieldProjection("scope_id"),
			fieldProjection("device_instance"),
			fieldProjection("vendor_name"),
			fieldProjection("model_name"),
		},
	})
	if err != nil {
		t.Fatalf("Execute() error = %v", err)
	}
	values := nextBatch(t, stream).GetTuples()[0].GetValues()
	if values[0].GetStringValue() != "madrid.dc1" ||
		values[1].GetStringValue() != "cooling" ||
		values[2].GetIntegerValue() != 1201 ||
		values[3].GetStringValue() != "Acme" ||
		values[4].GetStringValue() != "Controller" {
		t.Fatalf("device values = %v", values)
	}
	if source.objectListCalls != 0 || source.propertyCalls != 0 {
		t.Fatal("DEVICES query contacted a BACnet device")
	}
}

func TestExecuteRequiresArrayFeatureForObjectList(t *testing.T) {
	request := &providerv1.QueryRequest{
		Entity:      &providerv1.EntityReference{Name: catalog.DeviceObjectsTable},
		Projections: []*providerv1.Projection{fieldProjection("object_list")},
		Filter:      deviceFilter("madrid.dc1", "cooling", 1201),
	}
	_, err := Execute(context.Background(), testCatalog(t), testDeviceSource(), request)
	if status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("Execute() code = %v, want FailedPrecondition (%v)", status.Code(err), err)
	}

	request.AcceptedFeatures = []string{grpcfeatures.ArrayValuesV1}
	stream, err := Execute(context.Background(), testCatalog(t), testDeviceSource(), request)
	if err != nil {
		t.Fatalf("Execute() with array feature error = %v", err)
	}
	array := nextBatch(t, stream).GetTuples()[0].GetValues()[0].GetArrayValue()
	if array == nil || len(array.GetElements()) != 1 ||
		array.GetElements()[0].GetStringValue() != "device:1201" {
		t.Fatalf("object_list = %v", array)
	}
}

func TestExecuteFailsWhenRequiredPropertyIsUnavailable(t *testing.T) {
	analog := bacnet.ObjectIdentifier{Type: bacnet.ObjectTypeAnalogInput, Instance: 1}
	source := testDeviceSource(analog)
	source.errors[analog] = map[bacnet.PropertyIdentifier]error{
		bacnet.PropertyObjectName: errors.New("unknown property"),
	}
	_, err := Execute(context.Background(), testCatalog(t), source, &providerv1.QueryRequest{
		Entity:      &providerv1.EntityReference{Name: catalog.AnalogPointsTable},
		Projections: []*providerv1.Projection{fieldProjection("object_name")},
		Filter:      deviceFilter("madrid.dc1", "cooling", 1201),
	})
	if status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("Execute() code = %v, want FailedPrecondition (%v)", status.Code(err), err)
	}
}

func TestExecuteRejectsDuplicateObjectIdentifiers(t *testing.T) {
	analog := bacnet.ObjectIdentifier{Type: bacnet.ObjectTypeAnalogInput, Instance: 1}
	_, err := Execute(context.Background(), testCatalog(t), testDeviceSource(analog, analog), &providerv1.QueryRequest{
		Entity:      &providerv1.EntityReference{Name: catalog.AnalogPointsTable},
		Projections: []*providerv1.Projection{fieldProjection("object_name")},
		Filter:      deviceFilter("madrid.dc1", "cooling", 1201),
	})
	if status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("Execute() code = %v, want FailedPrecondition (%v)", status.Code(err), err)
	}
}

func TestExecuteUsesObjectEqualityBeforePropertyReads(t *testing.T) {
	first := bacnet.ObjectIdentifier{Type: bacnet.ObjectTypeAnalogInput, Instance: 1}
	second := bacnet.ObjectIdentifier{Type: bacnet.ObjectTypeAnalogInput, Instance: 2}
	source := testDeviceSource(first, second)
	source.values[second] = map[bacnet.PropertyIdentifier]bacnet.ApplicationValue{
		bacnet.PropertyObjectName: textApplicationValue("Selected point"),
	}
	stream, err := Execute(context.Background(), testCatalog(t), source, &providerv1.QueryRequest{
		Entity:      &providerv1.EntityReference{Name: catalog.AnalogPointsTable},
		Projections: []*providerv1.Projection{fieldProjection("object_name")},
		Filter: logicalAnd(
			deviceFilter("madrid.dc1", "cooling", 1201),
			integerEquality("object_instance", 2),
		),
	})
	if err != nil {
		t.Fatalf("Execute() error = %v", err)
	}
	if got := nextBatch(t, stream).GetTuples()[0].GetValues()[0].GetStringValue(); got != "Selected point" {
		t.Fatalf("object_name = %q", got)
	}
	if len(source.lastSpecifications) != 1 || source.lastSpecifications[0].Object != second {
		t.Fatalf("property specifications = %#v", source.lastSpecifications)
	}
}

type fakeDeviceSource struct {
	snapshot           discovery.Snapshot
	objects            []bacnet.ObjectIdentifier
	values             map[bacnet.ObjectIdentifier]map[bacnet.PropertyIdentifier]bacnet.ApplicationValue
	errors             map[bacnet.ObjectIdentifier]map[bacnet.PropertyIdentifier]error
	objectListCalls    int
	propertyCalls      int
	lastSpecifications []service.ReadAccessSpecification
}

func testDeviceSource(objects ...bacnet.ObjectIdentifier) *fakeDeviceSource {
	if len(objects) == 0 {
		objects = []bacnet.ObjectIdentifier{{Type: bacnet.ObjectTypeDevice, Instance: 1201}}
	}
	return &fakeDeviceSource{
		snapshot: discovery.Snapshot{
			CapturedAt: time.Date(2026, time.November, 2, 12, 0, 0, 0, time.UTC),
			Scopes: []discovery.ScopeSnapshot{{
				Namespace:    "madrid.dc1",
				ScopeID:      "cooling",
				AttachmentID: "building-lan",
				LastAttempt:  time.Date(2026, time.November, 2, 11, 59, 0, 0, time.UTC),
				LastSuccess:  time.Date(2026, time.November, 2, 11, 59, 1, 0, time.UTC),
				Devices: []discovery.Device{{
					Namespace:    "madrid.dc1",
					ScopeID:      "cooling",
					AttachmentID: "building-lan",
					Instance:     1201,
					Route: discovery.Route{
						Endpoint: netip.MustParseAddrPort("192.0.2.21:47808"),
					},
					ProfileID: "bacnet-standard",
					LastSeen:  time.Date(2026, time.November, 2, 11, 58, 0, 0, time.UTC),
				}},
			}},
		},
		objects: objects,
		values:  make(map[bacnet.ObjectIdentifier]map[bacnet.PropertyIdentifier]bacnet.ApplicationValue),
		errors:  make(map[bacnet.ObjectIdentifier]map[bacnet.PropertyIdentifier]error),
	}
}

func (f *fakeDeviceSource) Snapshot() discovery.Snapshot { return f.snapshot }

func (f *fakeDeviceSource) ReadObjectList(
	context.Context,
	discovery.Device,
	int,
) ([]bacnet.ObjectIdentifier, error) {
	f.objectListCalls++
	return append([]bacnet.ObjectIdentifier(nil), f.objects...), nil
}

func (f *fakeDeviceSource) ReadProperties(
	_ context.Context,
	_ discovery.Device,
	specifications []service.ReadAccessSpecification,
) ([]service.ReadAccessResult, error) {
	f.propertyCalls++
	f.lastSpecifications = append([]service.ReadAccessSpecification(nil), specifications...)
	results := make([]service.ReadAccessResult, 0, len(specifications))
	for _, specification := range specifications {
		result := service.ReadAccessResult{Object: specification.Object}
		for _, property := range specification.Properties {
			value := f.values[specification.Object][property.Identifier]
			readErr := f.errors[specification.Object][property.Identifier]
			result.Properties = append(result.Properties, service.PropertyResult{
				Property: property,
				Value:    value,
				Err:      readErr,
			})
		}
		results = append(results, result)
	}
	return results, nil
}

func testCatalog(t *testing.T) *catalog.Catalog {
	t.Helper()
	profiles, err := profile.Load(nil)
	if err != nil {
		t.Fatalf("profile.Load() error = %v", err)
	}
	compiled, err := catalog.Compile(profiles)
	if err != nil {
		t.Fatalf("catalog.Compile() error = %v", err)
	}
	return compiled
}

func textApplicationValue(value string) bacnet.ApplicationValue {
	return bacnet.ApplicationValue{
		Kind:      bacnet.ValueCharacterString,
		Character: bacnet.CharacterString{Value: value},
	}
}

func nextBatch(t *testing.T, stream interface {
	Next(context.Context) (*providerv1.TupleBatch, error)
}) *providerv1.TupleBatch {
	t.Helper()
	batch, err := stream.Next(context.Background())
	if err != nil {
		t.Fatalf("stream.Next() error = %v", err)
	}
	return batch
}

func fieldProjection(name string) *providerv1.Projection {
	return &providerv1.Projection{Expression: &providerv1.Expression{
		Kind: &providerv1.Expression_Field{Field: &providerv1.FieldReference{Name: name}},
	}}
}

func deviceFilter(namespace string, scope string, instance int32) *providerv1.Expression {
	return logicalAnd(
		stringEquality("namespace", namespace),
		stringEquality("scope_id", scope),
		integerEquality("device_instance", instance),
	)
}

func stringEquality(field string, value string) *providerv1.Expression {
	return equality(field, &kublingv1.Value{Kind: &kublingv1.Value_StringValue{StringValue: value}})
}

func integerEquality(field string, value int32) *providerv1.Expression {
	return equality(field, &kublingv1.Value{Kind: &kublingv1.Value_IntegerValue{IntegerValue: value}})
}

func equality(field string, value *kublingv1.Value) *providerv1.Expression {
	return &providerv1.Expression{Kind: &providerv1.Expression_Comparison{
		Comparison: &providerv1.ComparisonExpression{
			Operator: providerv1.ComparisonOperator_COMPARISON_OPERATOR_EQUAL,
			Left: &providerv1.Expression{Kind: &providerv1.Expression_Field{
				Field: &providerv1.FieldReference{Name: field},
			}},
			Right: &providerv1.Expression{Kind: &providerv1.Expression_Literal{
				Literal: &providerv1.Literal{Value: value},
			}},
		},
	}}
}

func logicalAnd(expressions ...*providerv1.Expression) *providerv1.Expression {
	return &providerv1.Expression{Kind: &providerv1.Expression_Logical{
		Logical: &providerv1.LogicalExpression{
			Operator: providerv1.LogicalOperator_LOGICAL_OPERATOR_AND,
			Operands: expressions,
		},
	}}
}

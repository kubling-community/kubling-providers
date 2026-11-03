package mutation

import (
	"context"
	"errors"
	"net/netip"
	"testing"

	kublingv1 "github.com/kubling-community/kubling-grpc/sdk-go/kubling/v1"
	"github.com/kubling-community/kubling-providers/providers/bacnet/internal/catalog"
	"github.com/kubling-community/kubling-providers/providers/bacnet/internal/config"
	"github.com/kubling-community/kubling-providers/providers/bacnet/internal/discovery"
	"github.com/kubling-community/kubling-providers/providers/bacnet/internal/profile"
	providerv1 "github.com/kubling-community/kubling-providers/sdk-go/kubling/provider/v1"
	bacnet "github.com/otfabric/go-bacnet"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestExecuteWritesAnalogPresentValue(t *testing.T) {
	source := standardSource()
	response, err := Execute(
		context.Background(),
		standardCatalog(t),
		source,
		updateRequest(
			catalog.AnalogPointsTable,
			"analog-value",
			1,
			"present_value",
			doubleValue(22.5),
		),
	)
	if err != nil {
		t.Fatalf("Execute() error = %v", err)
	}
	if response.GetAffectedRows() != 1 || source.writes != 1 {
		t.Fatalf("Execute() response = %v, writes = %d", response, source.writes)
	}
	if source.object.Type != bacnet.ObjectTypeAnalogValue || source.object.Instance != 1 ||
		source.property.Identifier != bacnet.PropertyPresentValue ||
		source.value.Kind != bacnet.ValueReal || source.value.Real != 22.5 ||
		source.priority == nil || *source.priority != 16 {
		t.Fatalf(
			"write = object %#v property %#v value %#v priority %v",
			source.object,
			source.property,
			source.value,
			source.priority,
		)
	}
}

func TestExecuteRelinquishesCommandableValue(t *testing.T) {
	source := standardSource()
	response, err := Execute(
		context.Background(),
		standardCatalog(t),
		source,
		updateRequest(
			catalog.AnalogPointsTable,
			"analog-output",
			2,
			"present_value",
			nullValue(),
		),
	)
	if err != nil || response.GetAffectedRows() != 1 {
		t.Fatalf("Execute() = %v, %v", response, err)
	}
	if source.value.Kind != bacnet.ValueNull || source.priority == nil || *source.priority != 16 {
		t.Fatalf("relinquish value = %#v priority = %v", source.value, source.priority)
	}
}

func TestExecuteConvertsStandardEnumeratedAndUnsignedValues(t *testing.T) {
	tests := []struct {
		name       string
		table      string
		objectType string
		value      *kublingv1.Value
		kind       bacnet.ValueKind
		number     uint64
	}{
		{
			name:       "binary enumeration",
			table:      catalog.BinaryPointsTable,
			objectType: "binary-value",
			value:      stringValue("active"),
			kind:       bacnet.ValueEnumerated,
			number:     1,
		},
		{
			name:       "multi-state unsigned",
			table:      catalog.MultistatePointsTable,
			objectType: "multi-state-value",
			value:      integerValueLiteral(3),
			kind:       bacnet.ValueUnsigned,
			number:     3,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			source := standardSource()
			_, err := Execute(
				context.Background(),
				standardCatalog(t),
				source,
				updateRequest(test.table, test.objectType, 7, "present_value", test.value),
			)
			if err != nil {
				t.Fatalf("Execute() error = %v", err)
			}
			if source.value.Kind != test.kind {
				t.Fatalf("value kind = %v, want %v", source.value.Kind, test.kind)
			}
			if test.kind == bacnet.ValueEnumerated && uint64(source.value.Enumerated) != test.number {
				t.Fatalf("enumerated = %d, want %d", source.value.Enumerated, test.number)
			}
			if test.kind == bacnet.ValueUnsigned && source.value.Unsigned != test.number {
				t.Fatalf("unsigned = %d, want %d", source.value.Unsigned, test.number)
			}
		})
	}
}

func TestExecuteReturnsZeroWhenRegisteredDeviceIsAbsent(t *testing.T) {
	source := standardSource()
	source.snapshot.Scopes[0].Devices = nil
	response, err := Execute(
		context.Background(),
		standardCatalog(t),
		source,
		updateRequest(
			catalog.AnalogPointsTable,
			"analog-value",
			1,
			"present_value",
			doubleValue(22.5),
		),
	)
	if err != nil || response.GetAffectedRows() != 0 || source.writes != 0 {
		t.Fatalf("Execute() = %v, %v; writes = %d", response, err, source.writes)
	}
}

func TestExecuteRejectsUnsafeUpdates(t *testing.T) {
	tests := []struct {
		name     string
		mutate   func(*fakeSource, *providerv1.UpdateRequest)
		wantCode codes.Code
	}{
		{
			name: "disabled scope",
			mutate: func(source *fakeSource, _ *providerv1.UpdateRequest) {
				source.policy.Enabled = false
			},
			wantCode: codes.FailedPrecondition,
		},
		{
			name: "missing object identity",
			mutate: func(_ *fakeSource, request *providerv1.UpdateRequest) {
				request.Filter = equality(catalog.NamespaceColumn, stringValue("lab"))
			},
			wantCode: codes.InvalidArgument,
		},
		{
			name: "input object is not writable",
			mutate: func(_ *fakeSource, request *providerv1.UpdateRequest) {
				request.Filter = objectFilter("analog-input", 1)
			},
			wantCode: codes.InvalidArgument,
		},
		{
			name: "unsupported extra predicate",
			mutate: func(_ *fakeSource, request *providerv1.UpdateRequest) {
				request.Filter = logicalAnd(
					request.Filter,
					equality("present_value", doubleValue(10)),
				)
			},
			wantCode: codes.InvalidArgument,
		},
		{
			name: "multiple assignments",
			mutate: func(_ *fakeSource, request *providerv1.UpdateRequest) {
				request.Assignments = append(request.Assignments, request.Assignments[0])
			},
			wantCode: codes.InvalidArgument,
		},
		{
			name: "wrong assignment type",
			mutate: func(_ *fakeSource, request *providerv1.UpdateRequest) {
				request.Assignments[0].Value = literal(stringValue("warm"))
			},
			wantCode: codes.InvalidArgument,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			source := standardSource()
			request := updateRequest(
				catalog.AnalogPointsTable,
				"analog-value",
				1,
				"present_value",
				doubleValue(22.5),
			)
			test.mutate(source, request)
			_, err := Execute(context.Background(), standardCatalog(t), source, request)
			if status.Code(err) != test.wantCode || source.writes != 0 {
				t.Fatalf("Execute() error = %v (%v), writes = %d", err, status.Code(err), source.writes)
			}
		})
	}
}

func TestExecuteMapsWriteOutcomesWithoutRetry(t *testing.T) {
	tests := []struct {
		name     string
		err      error
		wantCode codes.Code
	}{
		{
			name:     "outcome unknown",
			err:      &bacnet.OutcomeUnknownError{Operation: "WriteProperty", Cause: bacnet.ErrTimeout},
			wantCode: codes.Unknown,
		},
		{
			name:     "unknown object",
			err:      &bacnet.ErrorResponse{Code: bacnet.ErrorCodeUnknownObject},
			wantCode: codes.NotFound,
		},
		{
			name:     "write denied",
			err:      &bacnet.ErrorResponse{Code: bacnet.ErrorCodeWriteAccessDenied},
			wantCode: codes.FailedPrecondition,
		},
		{
			name:     "transport unavailable",
			err:      errors.New("network down"),
			wantCode: codes.Unavailable,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			source := standardSource()
			source.writeErr = test.err
			_, err := Execute(
				context.Background(),
				standardCatalog(t),
				source,
				updateRequest(
					catalog.AnalogPointsTable,
					"analog-value",
					1,
					"present_value",
					doubleValue(22.5),
				),
			)
			if status.Code(err) != test.wantCode || source.writes != 1 {
				t.Fatalf("Execute() error = %v (%v), writes = %d", err, status.Code(err), source.writes)
			}
		})
	}
}

type fakeSource struct {
	snapshot discovery.Snapshot
	policy   config.MutationPolicy
	found    bool
	writeErr error
	writes   int
	device   discovery.Device
	object   bacnet.ObjectIdentifier
	property bacnet.PropertyReference
	value    bacnet.ApplicationValue
	priority *uint8
}

func standardSource() *fakeSource {
	device := discovery.Device{
		Namespace:    "lab",
		ScopeID:      "simulated",
		AttachmentID: "loopback",
		Instance:     1234,
		ProfileID:    profile.StandardProfileID,
		Route:        discovery.Route{Endpoint: netip.MustParseAddrPort("127.0.0.1:47808")},
	}
	return &fakeSource{
		snapshot: discovery.Snapshot{Scopes: []discovery.ScopeSnapshot{{
			Namespace: "lab",
			ScopeID:   "simulated",
			Devices:   []discovery.Device{device},
		}}},
		policy: config.MutationPolicy{Enabled: true, Priority: 16},
		found:  true,
	}
}

func (f *fakeSource) Snapshot() discovery.Snapshot {
	return f.snapshot
}

func (f *fakeSource) MutationPolicy(string, string) (config.MutationPolicy, bool) {
	return f.policy, f.found
}

func (f *fakeSource) WriteProperty(
	_ context.Context,
	device discovery.Device,
	object bacnet.ObjectIdentifier,
	property bacnet.PropertyReference,
	value bacnet.ApplicationValue,
	priority *uint8,
) error {
	f.writes++
	f.device = device
	f.object = object
	f.property = property
	f.value = value.Clone()
	if priority != nil {
		copied := *priority
		f.priority = &copied
	}
	return f.writeErr
}

func standardCatalog(t *testing.T) *catalog.Catalog {
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

func updateRequest(
	table string,
	objectType string,
	objectInstance int32,
	field string,
	value *kublingv1.Value,
) *providerv1.UpdateRequest {
	return &providerv1.UpdateRequest{
		Entity: &providerv1.EntityReference{Name: table},
		Assignments: []*providerv1.Assignment{{
			Field: field,
			Value: literal(value),
		}},
		Filter: objectFilter(objectType, objectInstance),
	}
}

func objectFilter(objectType string, objectInstance int32) *providerv1.Expression {
	return logicalAnd(
		equality(catalog.NamespaceColumn, stringValue("lab")),
		equality(catalog.ScopeIDColumn, stringValue("simulated")),
		equality(catalog.DeviceInstanceColumn, integerValueLiteral(1234)),
		equality(catalog.ObjectTypeColumn, stringValue(objectType)),
		equality(catalog.ObjectInstanceColumn, integerValueLiteral(objectInstance)),
	)
}

func equality(field string, value *kublingv1.Value) *providerv1.Expression {
	return &providerv1.Expression{Kind: &providerv1.Expression_Comparison{
		Comparison: &providerv1.ComparisonExpression{
			Operator: providerv1.ComparisonOperator_COMPARISON_OPERATOR_EQUAL,
			Left: &providerv1.Expression{Kind: &providerv1.Expression_Field{
				Field: &providerv1.FieldReference{Name: field},
			}},
			Right: literal(value),
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

func literal(value *kublingv1.Value) *providerv1.Expression {
	return &providerv1.Expression{Kind: &providerv1.Expression_Literal{
		Literal: &providerv1.Literal{Value: value},
	}}
}

func stringValue(value string) *kublingv1.Value {
	return &kublingv1.Value{Kind: &kublingv1.Value_StringValue{StringValue: value}}
}

func integerValueLiteral(value int32) *kublingv1.Value {
	return &kublingv1.Value{Kind: &kublingv1.Value_IntegerValue{IntegerValue: value}}
}

func doubleValue(value float64) *kublingv1.Value {
	return &kublingv1.Value{Kind: &kublingv1.Value_DoubleValue{DoubleValue: value}}
}

func nullValue() *kublingv1.Value {
	return &kublingv1.Value{Kind: &kublingv1.Value_NullValue{NullValue: &kublingv1.NullValue{}}}
}

package provider

import (
	"context"
	"strings"
	"testing"

	kublingv1 "github.com/kubling-community/kubling-grpc/sdk-go/kubling/v1"
	providerv1 "github.com/kubling-community/kubling-providers/sdk-go/kubling/provider/v1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestValidateTypeDescriptor(t *testing.T) {
	precision := int32(12)
	negativeScale := int32(-2)
	valid := []*kublingv1.TypeDescriptor{
		{Type: kublingv1.ValueType_VALUE_TYPE_STRING},
		{
			Type:      kublingv1.ValueType_VALUE_TYPE_BIGINTEGER,
			Precision: &precision,
		},
		{
			Type:      kublingv1.ValueType_VALUE_TYPE_BIGDECIMAL,
			Precision: &precision,
			Scale:     &negativeScale,
		},
		{
			Type: kublingv1.ValueType_VALUE_TYPE_ARRAY,
			ElementType: &kublingv1.TypeDescriptor{
				Type: kublingv1.ValueType_VALUE_TYPE_ARRAY,
				ElementType: &kublingv1.TypeDescriptor{
					Type: kublingv1.ValueType_VALUE_TYPE_INTEGER,
				},
			},
		},
	}
	for index, descriptor := range valid {
		if err := validateTypeDescriptor(descriptor); err != nil {
			t.Fatalf("valid descriptor %d: %v", index, err)
		}
	}

	zero := int32(0)
	tooLargeScale := int32(13)
	cycle := &kublingv1.TypeDescriptor{Type: kublingv1.ValueType_VALUE_TYPE_ARRAY}
	cycle.ElementType = cycle
	tests := []struct {
		name       string
		descriptor *kublingv1.TypeDescriptor
		wantText   string
	}{
		{name: "nil", wantText: "required"},
		{
			name:       "unknown type",
			descriptor: &kublingv1.TypeDescriptor{},
			wantText:   "VALUE_TYPE_UNKNOWN",
		},
		{
			name: "array missing element",
			descriptor: &kublingv1.TypeDescriptor{
				Type: kublingv1.ValueType_VALUE_TYPE_ARRAY,
			},
			wantText: "requires element_type",
		},
		{
			name: "scalar with element",
			descriptor: &kublingv1.TypeDescriptor{
				Type: kublingv1.ValueType_VALUE_TYPE_STRING,
				ElementType: &kublingv1.TypeDescriptor{
					Type: kublingv1.ValueType_VALUE_TYPE_INTEGER,
				},
			},
			wantText: "must not set element_type",
		},
		{
			name: "zero precision",
			descriptor: &kublingv1.TypeDescriptor{
				Type:      kublingv1.ValueType_VALUE_TYPE_BIGDECIMAL,
				Precision: &zero,
			},
			wantText: "precision must be positive",
		},
		{
			name: "precision on string",
			descriptor: &kublingv1.TypeDescriptor{
				Type:      kublingv1.ValueType_VALUE_TYPE_STRING,
				Precision: &precision,
			},
			wantText: "must not set precision",
		},
		{
			name: "scale without precision",
			descriptor: &kublingv1.TypeDescriptor{
				Type:  kublingv1.ValueType_VALUE_TYPE_BIGDECIMAL,
				Scale: &negativeScale,
			},
			wantText: "scale requires precision",
		},
		{
			name: "scale exceeds precision",
			descriptor: &kublingv1.TypeDescriptor{
				Type:      kublingv1.ValueType_VALUE_TYPE_BIGDECIMAL,
				Precision: &precision,
				Scale:     &tooLargeScale,
			},
			wantText: "scale must not exceed precision",
		},
		{
			name:       "cycle",
			descriptor: cycle,
			wantText:   "cycle",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			err := validateTypeDescriptor(test.descriptor)
			if err == nil || !strings.Contains(err.Error(), test.wantText) {
				t.Fatalf("validateTypeDescriptor() error = %v, want text %q", err, test.wantText)
			}
		})
	}
}

func TestValidateOutputBatchStructure(t *testing.T) {
	integerType := &kublingv1.TypeDescriptor{
		Type: kublingv1.ValueType_VALUE_TYPE_INTEGER,
	}
	arrayType := &kublingv1.TypeDescriptor{
		Type:        kublingv1.ValueType_VALUE_TYPE_ARRAY,
		ElementType: integerType,
	}
	validArray := &kublingv1.Value{Kind: &kublingv1.Value_ArrayValue{
		ArrayValue: &kublingv1.ArrayValue{
			ElementType: integerType,
			Elements: []*kublingv1.Value{
				{Kind: &kublingv1.Value_IntegerValue{IntegerValue: 1}},
				nullTestValue(),
			},
		},
	}}
	valid := &providerv1.TupleBatch{
		Fields: []*providerv1.Field{
			{Name: "name", Type: kublingv1.ValueType_VALUE_TYPE_STRING},
			{
				Name:           "numbers",
				Type:           kublingv1.ValueType_VALUE_TYPE_ARRAY,
				TypeDescriptor: arrayType,
			},
		},
		Tuples: []*providerv1.Tuple{{Values: []*kublingv1.Value{
			{Kind: &kublingv1.Value_StringValue{StringValue: "sample"}},
			validArray,
		}}},
	}
	if err := validateOutputBatchStructure(valid); err != nil {
		t.Fatalf("validateOutputBatchStructure(valid) error = %v", err)
	}

	tests := []struct {
		name     string
		batch    *providerv1.TupleBatch
		wantText string
	}{
		{
			name: "field and descriptor disagree",
			batch: &providerv1.TupleBatch{Fields: []*providerv1.Field{{
				Type: kublingv1.ValueType_VALUE_TYPE_STRING,
				TypeDescriptor: &kublingv1.TypeDescriptor{
					Type: kublingv1.ValueType_VALUE_TYPE_INTEGER,
				},
			}}},
			wantText: "does not match type_descriptor",
		},
		{
			name: "array field missing descriptor",
			batch: &providerv1.TupleBatch{Fields: []*providerv1.Field{{
				Type: kublingv1.ValueType_VALUE_TYPE_ARRAY,
			}}},
			wantText: "ARRAY requires type_descriptor",
		},
		{
			name: "tuple width",
			batch: &providerv1.TupleBatch{
				Fields: []*providerv1.Field{{
					Type: kublingv1.ValueType_VALUE_TYPE_STRING,
				}},
				Tuples: []*providerv1.Tuple{{}},
			},
			wantText: "0 values for 1 fields",
		},
		{
			name: "scalar value mismatch",
			batch: &providerv1.TupleBatch{
				Fields: []*providerv1.Field{{
					Type: kublingv1.ValueType_VALUE_TYPE_STRING,
				}},
				Tuples: []*providerv1.Tuple{{Values: []*kublingv1.Value{{
					Kind: &kublingv1.Value_IntegerValue{IntegerValue: 1},
				}}}},
			},
			wantText: "does not match declared type",
		},
		{
			name: "array descriptor mismatch",
			batch: &providerv1.TupleBatch{
				Fields: []*providerv1.Field{{
					Type:           kublingv1.ValueType_VALUE_TYPE_ARRAY,
					TypeDescriptor: arrayType,
				}},
				Tuples: []*providerv1.Tuple{{Values: []*kublingv1.Value{{
					Kind: &kublingv1.Value_ArrayValue{ArrayValue: &kublingv1.ArrayValue{
						ElementType: &kublingv1.TypeDescriptor{
							Type: kublingv1.ValueType_VALUE_TYPE_STRING,
						},
					}},
				}}}},
			},
			wantText: "does not match declared element_type",
		},
		{
			name: "array element mismatch",
			batch: &providerv1.TupleBatch{
				Fields: []*providerv1.Field{{
					Type:           kublingv1.ValueType_VALUE_TYPE_ARRAY,
					TypeDescriptor: arrayType,
				}},
				Tuples: []*providerv1.Tuple{{Values: []*kublingv1.Value{{
					Kind: &kublingv1.Value_ArrayValue{ArrayValue: &kublingv1.ArrayValue{
						ElementType: integerType,
						Elements: []*kublingv1.Value{{
							Kind: &kublingv1.Value_StringValue{StringValue: "wrong"},
						}},
					}},
				}}}},
			},
			wantText: "array element 0",
		},
		{
			name: "nil value",
			batch: &providerv1.TupleBatch{
				Fields: []*providerv1.Field{{
					Type: kublingv1.ValueType_VALUE_TYPE_STRING,
				}},
				Tuples: []*providerv1.Tuple{{Values: []*kublingv1.Value{nil}}},
			},
			wantText: "use null_value",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			err := validateOutputBatchStructure(test.batch)
			if err == nil || !strings.Contains(err.Error(), test.wantText) {
				t.Fatalf(
					"validateOutputBatchStructure() error = %v, want text %q",
					err,
					test.wantText,
				)
			}
		})
	}
}

func TestValidateInputBatchStructurePreservesLegacyFieldTyping(t *testing.T) {
	batch := &providerv1.TupleBatch{
		Fields: []*providerv1.Field{{Name: "title"}},
		Tuples: []*providerv1.Tuple{{Values: []*kublingv1.Value{{
			Kind: &kublingv1.Value_StringValue{StringValue: "legacy"},
		}}}},
	}
	if err := validateInputBatchStructure(batch); err != nil {
		t.Fatalf("validateInputBatchStructure() error = %v", err)
	}
}

func TestValidateQueryRequestValuesAcceptsAggregateExpressions(t *testing.T) {
	request := newQueryTestRequest("connection")
	request.Projections = []*providerv1.Projection{{
		OutputName: "average_amount",
		Expression: &providerv1.Expression{Kind: &providerv1.Expression_Aggregate{
			Aggregate: &providerv1.AggregateCall{
				Function: providerv1.AggregateFunction_AGGREGATE_FUNCTION_AVG,
				Arguments: []*providerv1.Expression{{Kind: &providerv1.Expression_Field{
					Field: &providerv1.FieldReference{Name: "amount"},
				}}},
				ResultType: &kublingv1.TypeDescriptor{
					Type: kublingv1.ValueType_VALUE_TYPE_DOUBLE,
				},
			},
		}},
	}}
	request.GroupBy = []*providerv1.Expression{{Kind: &providerv1.Expression_Field{
		Field: &providerv1.FieldReference{Name: "category"},
	}}}
	request.Having = &providerv1.Expression{Kind: &providerv1.Expression_Literal{
		Literal: &providerv1.Literal{Value: &kublingv1.Value{
			Kind: &kublingv1.Value_BooleanValue{BooleanValue: true},
		}},
	}}

	if err := validateQueryRequestValues(request); err != nil {
		t.Fatalf("validateQueryRequestValues() error = %v", err)
	}
}

func TestValidateExpressionValuesRejectsInvalidAggregates(t *testing.T) {
	stringResult := &kublingv1.TypeDescriptor{
		Type: kublingv1.ValueType_VALUE_TYPE_STRING,
	}
	fieldArgument := []*providerv1.Expression{{Kind: &providerv1.Expression_Field{
		Field: &providerv1.FieldReference{Name: "value"},
	}}}
	tests := []struct {
		name      string
		aggregate *providerv1.AggregateCall
		wantText  string
	}{
		{
			name:      "unspecified function",
			aggregate: &providerv1.AggregateCall{ResultType: stringResult},
			wantText:  "AGGREGATE_FUNCTION_UNSPECIFIED",
		},
		{
			name: "unknown function",
			aggregate: &providerv1.AggregateCall{
				Function:   99,
				ResultType: stringResult,
			},
			wantText: "function 99 is unknown",
		},
		{
			name: "count star argument",
			aggregate: &providerv1.AggregateCall{
				Function:   providerv1.AggregateFunction_AGGREGATE_FUNCTION_COUNT_STAR,
				Arguments:  fieldArgument,
				ResultType: stringResult,
			},
			wantText: "COUNT_STAR must not contain arguments",
		},
		{
			name: "count star distinct",
			aggregate: &providerv1.AggregateCall{
				Function:   providerv1.AggregateFunction_AGGREGATE_FUNCTION_COUNT_STAR,
				Distinct:   true,
				ResultType: stringResult,
			},
			wantText: "COUNT_STAR must not be DISTINCT",
		},
		{
			name: "missing argument",
			aggregate: &providerv1.AggregateCall{
				Function:   providerv1.AggregateFunction_AGGREGATE_FUNCTION_SUM,
				ResultType: stringResult,
			},
			wantText: "requires exactly one argument",
		},
		{
			name: "missing result type",
			aggregate: &providerv1.AggregateCall{
				Function:  providerv1.AggregateFunction_AGGREGATE_FUNCTION_SUM,
				Arguments: fieldArgument,
			},
			wantText: "result_type: type descriptor is required",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			expression := &providerv1.Expression{Kind: &providerv1.Expression_Aggregate{
				Aggregate: test.aggregate,
			}}
			err := validateExpressionValues(expression)
			if err == nil || !strings.Contains(err.Error(), test.wantText) {
				t.Fatalf(
					"validateExpressionValues() error = %v, want text %q",
					err,
					test.wantText,
				)
			}
		})
	}
}

func TestServerRejectsDeclaredLiteralMismatch(t *testing.T) {
	connection := &queryTestConnection{queryFunc: func(
		context.Context,
		*providerv1.QueryRequest,
	) (ResultStream, error) {
		t.Fatal("Query reached provider with an invalid literal")
		return nil, nil
	}}
	server := NewServer(&serverTestProvider{})
	connectionID := addServerTestConnection(t, server, connection)
	request := newQueryTestRequest(connectionID)
	request.Filter = &providerv1.Expression{Kind: &providerv1.Expression_Literal{
		Literal: &providerv1.Literal{
			Value: &kublingv1.Value{Kind: &kublingv1.Value_StringValue{
				StringValue: "not-an-integer",
			}},
			DeclaredType: &kublingv1.TypeDescriptor{
				Type: kublingv1.ValueType_VALUE_TYPE_INTEGER,
			},
		},
	}}

	err := server.Query(request, &queryTestServerStream{ctx: context.Background()})
	if got := status.Code(err); got != codes.InvalidArgument {
		t.Fatalf("Query status = %s, want %s: %v", got, codes.InvalidArgument, err)
	}
}

func TestServerRejectsInvalidAggregateExpression(t *testing.T) {
	connection := &queryTestConnection{queryFunc: func(
		context.Context,
		*providerv1.QueryRequest,
	) (ResultStream, error) {
		t.Fatal("Query reached provider with an invalid aggregate")
		return nil, nil
	}}
	server := NewServer(&serverTestProvider{})
	connectionID := addServerTestConnection(t, server, connection)
	request := newQueryTestRequest(connectionID)
	request.Projections = []*providerv1.Projection{{
		OutputName: "total",
		Expression: &providerv1.Expression{Kind: &providerv1.Expression_Aggregate{
			Aggregate: &providerv1.AggregateCall{
				Function: providerv1.AggregateFunction_AGGREGATE_FUNCTION_SUM,
				Arguments: []*providerv1.Expression{{Kind: &providerv1.Expression_Field{
					Field: &providerv1.FieldReference{Name: "amount"},
				}}},
			},
		}},
	}}

	err := server.Query(request, &queryTestServerStream{ctx: context.Background()})
	if got := status.Code(err); got != codes.InvalidArgument {
		t.Fatalf("Query status = %s, want %s: %v", got, codes.InvalidArgument, err)
	}
}

func TestServerRejectsStructurallyInvalidProviderBatch(t *testing.T) {
	connection := &queryTestConnection{queryFunc: func(
		context.Context,
		*providerv1.QueryRequest,
	) (ResultStream, error) {
		return &queryTestResultStream{nextResults: []queryNextResult{{
			batch: &providerv1.TupleBatch{
				Fields: []*providerv1.Field{{
					Type: kublingv1.ValueType_VALUE_TYPE_STRING,
				}},
				Tuples: []*providerv1.Tuple{{Values: []*kublingv1.Value{{
					Kind: &kublingv1.Value_IntegerValue{IntegerValue: 1},
				}}}},
			},
		}}}, nil
	}}
	server := NewServer(&serverTestProvider{})
	connectionID := addServerTestConnection(t, server, connection)

	err := server.Query(
		newQueryTestRequest(connectionID),
		&queryTestServerStream{ctx: context.Background()},
	)
	if got := status.Code(err); got != codes.Internal {
		t.Fatalf("Query status = %s, want %s: %v", got, codes.Internal, err)
	}
}

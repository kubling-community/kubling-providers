package provider

import (
	"context"
	"strings"
	"testing"

	grpcfeatures "github.com/kubling-community/kubling-grpc/sdk-go/features"
	kublingv1 "github.com/kubling-community/kubling-grpc/sdk-go/kubling/v1"
	providerv1 "github.com/kubling-community/kubling-providers/sdk-go/kubling/provider/v1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestValidateOutputBatchFeatures(t *testing.T) {
	tests := []struct {
		name                  string
		batch                 *providerv1.TupleBatch
		acceptedFeatures      []string
		lobTransportAvailable bool
		wantError             string
	}{
		{
			name: "legacy value needs no feature",
			batch: &providerv1.TupleBatch{
				Fields: []*providerv1.Field{{
					Name: "value",
					Type: kublingv1.ValueType_VALUE_TYPE_STRING,
				}},
				Tuples: []*providerv1.Tuple{{Values: []*kublingv1.Value{{
					Kind: &kublingv1.Value_StringValue{StringValue: "value"},
				}}}},
			},
		},
		{
			name:      "array field requires acceptance even when value is null",
			batch:     arrayFeatureBatch(nullTestValue()),
			wantError: grpcfeatures.ArrayValuesV1,
		},
		{
			name:             "accepted array passes",
			batch:            arrayFeatureBatch(integerArrayTestValue()),
			acceptedFeatures: []string{grpcfeatures.ArrayValuesV1},
		},
		{
			name: "nested spatial value requires both features",
			batch: arrayFeatureBatch(&kublingv1.Value{Kind: &kublingv1.Value_ArrayValue{
				ArrayValue: &kublingv1.ArrayValue{
					ElementType: &kublingv1.TypeDescriptor{Type: kublingv1.ValueType_VALUE_TYPE_GEOMETRY},
					Elements: []*kublingv1.Value{{Kind: &kublingv1.Value_GeometryWithCrs{
						GeometryWithCrs: &kublingv1.SpatialValue{Wkb: []byte{1}},
					}}},
				},
			}}),
			acceptedFeatures: []string{grpcfeatures.ArrayValuesV1},
			wantError:        grpcfeatures.SpatialValuesV1,
		},
		{
			name: "nested spatial value passes when both features are accepted",
			batch: arrayFeatureBatch(&kublingv1.Value{Kind: &kublingv1.Value_ArrayValue{
				ArrayValue: &kublingv1.ArrayValue{
					ElementType: &kublingv1.TypeDescriptor{Type: kublingv1.ValueType_VALUE_TYPE_GEOMETRY},
					Elements: []*kublingv1.Value{{Kind: &kublingv1.Value_GeometryWithCrs{
						GeometryWithCrs: &kublingv1.SpatialValue{Wkb: []byte{1}},
					}}},
				},
			}}),
			acceptedFeatures: []string{
				grpcfeatures.ArrayValuesV1,
				grpcfeatures.SpatialValuesV1,
			},
		},
		{
			name:             "LOB reference remains unavailable before provider transport",
			batch:            lobFeatureBatch(),
			acceptedFeatures: []string{grpcfeatures.LobReadV1},
			wantError:        "requires provider LOB transport",
		},
		{
			name:                  "LOB reference passes with acceptance and provider transport",
			batch:                 lobFeatureBatch(),
			acceptedFeatures:      []string{grpcfeatures.LobReadV1},
			lobTransportAvailable: true,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			err := validateOutputBatchFeatures(
				test.batch,
				test.acceptedFeatures,
				test.lobTransportAvailable,
			)
			if test.wantError == "" {
				if err != nil {
					t.Fatalf("validateOutputBatchFeatures() error = %v", err)
				}
				return
			}
			if err == nil || !strings.Contains(err.Error(), test.wantError) {
				t.Fatalf(
					"validateOutputBatchFeatures() error = %v, want text %q",
					err,
					test.wantError,
				)
			}
		})
	}
}

func TestPrepareOutputBatchScopesProviderLobReferences(t *testing.T) {
	original := lobFeatureBatch()
	prepared := prepareOutputBatch(original, "connection-1")
	if prepared == original {
		t.Fatal("prepareOutputBatch() returned provider-owned batch")
	}
	originalReference := original.GetTuples()[0].GetValues()[0].GetLobReference()
	if originalReference.GetSessionId() != "" {
		t.Fatalf("original LOB session_id = %q, want empty", originalReference.GetSessionId())
	}
	preparedReference := prepared.GetTuples()[0].GetValues()[0].GetLobReference()
	if got := preparedReference.GetSessionId(); got != "connection-1" {
		t.Fatalf("prepared LOB session_id = %q, want connection-1", got)
	}
}

func TestServerQueryEnforcesAcceptedOutputFeatures(t *testing.T) {
	for _, test := range []struct {
		name             string
		acceptedFeatures []string
		wantCode         codes.Code
		wantResponses    int
	}{
		{name: "rejects unaccepted array", wantCode: codes.Internal},
		{
			name:             "streams accepted array",
			acceptedFeatures: []string{grpcfeatures.ArrayValuesV1},
			wantCode:         codes.OK,
			wantResponses:    1,
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			connection := &queryTestConnection{queryFunc: func(
				context.Context,
				*providerv1.QueryRequest,
			) (ResultStream, error) {
				return &queryTestResultStream{nextResults: []queryNextResult{{
					batch: arrayFeatureBatch(integerArrayTestValue()),
				}}}, nil
			}}
			server := NewServer(&serverTestProvider{})
			connectionID := addServerTestConnection(t, server, connection)
			request := newQueryTestRequest(connectionID)
			request.AcceptedFeatures = test.acceptedFeatures
			stream := &queryTestServerStream{ctx: context.Background()}

			err := server.Query(request, stream)
			if got := status.Code(err); got != test.wantCode {
				t.Fatalf("Query status = %s, want %s", got, test.wantCode)
			}
			if len(stream.responses) != test.wantResponses {
				t.Fatalf(
					"Query response count = %d, want %d",
					len(stream.responses),
					test.wantResponses,
				)
			}
		})
	}
}

func TestServerInsertEnforcesAcceptedGeneratedValueFeatures(t *testing.T) {
	for _, test := range []struct {
		name             string
		acceptedFeatures []string
		wantCode         codes.Code
	}{
		{name: "rejects unaccepted array", wantCode: codes.Internal},
		{
			name:             "returns accepted array",
			acceptedFeatures: []string{grpcfeatures.ArrayValuesV1},
			wantCode:         codes.OK,
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			connection := &mutationTestConnection{insertFunc: func(
				context.Context,
				*providerv1.InsertRequest,
			) (*providerv1.InsertResponse, error) {
				return &providerv1.InsertResponse{
					GeneratedValues: arrayFeatureBatch(integerArrayTestValue()),
				}, nil
			}}
			server := NewServer(&serverTestProvider{})
			connectionID := addServerTestConnection(t, server, connection)

			_, err := server.Insert(context.Background(), &providerv1.InsertRequest{
				ConnectionId:     connectionID,
				Rows:             &providerv1.TupleBatch{},
				AcceptedFeatures: test.acceptedFeatures,
			})
			if got := status.Code(err); got != test.wantCode {
				t.Fatalf("Insert status = %s, want %s", got, test.wantCode)
			}
		})
	}
}

func arrayFeatureBatch(value *kublingv1.Value) *providerv1.TupleBatch {
	integerType := &kublingv1.TypeDescriptor{Type: kublingv1.ValueType_VALUE_TYPE_INTEGER}
	elementType := integerType
	if arrayValue := value.GetArrayValue(); arrayValue.GetElementType() != nil {
		elementType = arrayValue.GetElementType()
	}
	return &providerv1.TupleBatch{
		Fields: []*providerv1.Field{{
			Name: "numbers",
			Type: kublingv1.ValueType_VALUE_TYPE_ARRAY,
			TypeDescriptor: &kublingv1.TypeDescriptor{
				Type:        kublingv1.ValueType_VALUE_TYPE_ARRAY,
				ElementType: elementType,
			},
		}},
		Tuples: []*providerv1.Tuple{{Values: []*kublingv1.Value{value}}},
	}
}

func integerArrayTestValue() *kublingv1.Value {
	return &kublingv1.Value{Kind: &kublingv1.Value_ArrayValue{ArrayValue: &kublingv1.ArrayValue{
		ElementType: &kublingv1.TypeDescriptor{Type: kublingv1.ValueType_VALUE_TYPE_INTEGER},
		Elements: []*kublingv1.Value{{
			Kind: &kublingv1.Value_IntegerValue{IntegerValue: 7},
		}},
	}}}
}

func nullTestValue() *kublingv1.Value {
	return &kublingv1.Value{Kind: &kublingv1.Value_NullValue{
		NullValue: &kublingv1.NullValue{},
	}}
}

func lobFeatureBatch() *providerv1.TupleBatch {
	expiresAtUnixMs := int64(1_800_000_000_000)
	return &providerv1.TupleBatch{
		Fields: []*providerv1.Field{{
			Name: "payload",
			Type: kublingv1.ValueType_VALUE_TYPE_BLOB,
		}},
		Tuples: []*providerv1.Tuple{{Values: []*kublingv1.Value{{
			Kind: &kublingv1.Value_LobReference{LobReference: &kublingv1.LobReference{
				LobId:           "provider-lob",
				Type:            kublingv1.ValueType_VALUE_TYPE_BLOB,
				ExpiresAtUnixMs: &expiresAtUnixMs,
			}},
		}}}},
	}
}

package gatewaypb_test

import (
	"testing"

	kublingv1 "github.com/kubling-community/kubling-grpc/sdk-go/kubling/v1"
	gatewaypb "github.com/kubling-community/kubling-providers/providers/host/internal/gatewaypb"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
)

func TestAgentGatewayConnectIsBidirectional(t *testing.T) {
	service := gatewaypb.File_kubling_host_v1_agent_gateway_proto.Services().
		ByName(protoreflect.Name("AgentGatewayService"))
	if service == nil {
		t.Fatal("AgentGatewayService descriptor is missing")
	}
	method := service.Methods().ByName(protoreflect.Name("Connect"))
	if method == nil {
		t.Fatal("Connect descriptor is missing")
	}
	if !method.IsStreamingClient() || !method.IsStreamingServer() {
		t.Fatalf(
			"Connect streaming = client:%t server:%t, want bidirectional",
			method.IsStreamingClient(),
			method.IsStreamingServer(),
		)
	}
}

func TestAgentEnvelopePreservesTypedRows(t *testing.T) {
	original := &gatewaypb.ConnectRequest{
		Payload: &gatewaypb.ConnectRequest_ScanBatch{
			ScanBatch: &gatewaypb.ScanBatch{
				ScanId:     "scan-1",
				BatchIndex: 2,
				Rows: []*gatewaypb.ScanRow{
					{
						Values: []*kublingv1.Value{
							{
								Kind: &kublingv1.Value_StringValue{
									StringValue: "host-a",
								},
							},
						},
					},
				},
			},
		},
	}

	encoded, err := proto.Marshal(original)
	if err != nil {
		t.Fatalf("Marshal() error = %v", err)
	}
	decoded := &gatewaypb.ConnectRequest{}
	if err := proto.Unmarshal(encoded, decoded); err != nil {
		t.Fatalf("Unmarshal() error = %v", err)
	}
	if !proto.Equal(decoded, original) {
		t.Fatalf("round trip = %v, want %v", decoded, original)
	}
}

func TestEnvelopeFieldNumbersRemainStable(t *testing.T) {
	request := (&gatewaypb.ConnectRequest{}).ProtoReflect().Descriptor().Fields()
	assertFieldNumber(t, request, "hello", 1)
	assertFieldNumber(t, request, "heartbeat", 2)
	assertFieldNumber(t, request, "scan_batch", 3)
	assertFieldNumber(t, request, "scan_finished", 4)

	response := (&gatewaypb.ConnectResponse{}).ProtoReflect().Descriptor().Fields()
	assertFieldNumber(t, response, "session_accepted", 1)
	assertFieldNumber(t, response, "heartbeat_acknowledged", 2)
	assertFieldNumber(t, response, "scan_request", 3)
	assertFieldNumber(t, response, "cancel_scan", 4)
}

func assertFieldNumber(
	t *testing.T,
	fields protoreflect.FieldDescriptors,
	name protoreflect.Name,
	want protoreflect.FieldNumber,
) {
	t.Helper()
	field := fields.ByName(name)
	if field == nil {
		t.Fatalf("field %q is missing", name)
	}
	if got := field.Number(); got != want {
		t.Fatalf("field %q number = %d, want %d", name, got, want)
	}
}

package transport

import (
	"context"
	"errors"
	"net/netip"
	"testing"
	"time"

	"github.com/kubling-community/kubling-providers/providers/bacnet/internal/config"
	"github.com/kubling-community/kubling-providers/providers/bacnet/internal/discovery"
	bacnet "github.com/otfabric/go-bacnet"
	"github.com/otfabric/go-bacnet/client"
	"github.com/otfabric/go-bacnet/service"
)

func TestNewClientBindsReportsStatusAndCloses(t *testing.T) {
	created, err := NewClient(config.Attachment{
		ID:                  "test",
		Bind:                netip.MustParseAddrPort("127.0.0.1:0"),
		APDUTimeout:         time.Second,
		Retries:             1,
		SegmentTimeout:      time.Second,
		ObservationTTL:      time.Minute,
		MaxObservations:     16,
		MaxPathsPerInstance: 2,
	})
	if err != nil {
		t.Fatalf("NewClient() error = %v", err)
	}
	status := created.Status()
	if !status.LocalEndpoint.IsValid() || status.LocalEndpoint.Port() == 0 || status.ForeignDevice {
		t.Fatalf("Status() = %#v", status)
	}
	if err := created.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}
	if err := created.Close(); err != nil {
		t.Fatalf("second Close() error = %v", err)
	}
}

func TestReadIdentityUsesRPMDecodesTextAndReportsMissingProperties(t *testing.T) {
	reader := &stubPropertyReader{rpm: []service.ReadAccessResult{{
		Properties: []service.PropertyResult{
			{
				Property: bacnet.PropertyReference{Identifier: bacnet.PropertyVendorIdentifier},
				Value:    bacnet.UnsignedValue(4242),
			},
			{
				Property: bacnet.PropertyReference{Identifier: bacnet.PropertyModelName},
				Value: bacnet.ApplicationValue{
					Kind: bacnet.ValueCharacterString,
					Character: bacnet.CharacterString{
						Encoding: bacnet.CharacterEncodingISO88591,
						Value:    "VAV-\xc9",
					},
				},
			},
			{
				Property: bacnet.PropertyReference{Identifier: propertyFirmwareRevision},
				Value: bacnet.ApplicationValue{
					Kind:      bacnet.ValueCharacterString,
					Character: bacnet.CharacterString{Value: "2.0.1"},
				},
			},
		},
	}}}
	identity, issues, err := readIdentity(context.Background(), reader, client.Target{}, 1201)
	if err != nil || len(issues) != 4 {
		t.Fatalf("readIdentity() = %#v, %#v, %v", identity, issues, err)
	}
	if identity.VendorIdentifier != 4242 || identity.ModelName != "VAV-É" || identity.FirmwareRevision != "2.0.1" {
		t.Fatalf("identity = %#v", identity)
	}
}

func TestReadIdentityFallsBackToIndividualProperties(t *testing.T) {
	reader := &stubPropertyReader{
		rpmErr: errors.New("RPM unsupported"),
		values: map[bacnet.PropertyIdentifier]bacnet.ApplicationValue{
			bacnet.PropertyVendorIdentifier: bacnet.UnsignedValue(77),
			bacnet.PropertyModelName: {
				Kind:      bacnet.ValueCharacterString,
				Character: bacnet.CharacterString{Value: "Controller"},
			},
		},
	}
	identity, issues, err := readIdentity(context.Background(), reader, client.Target{}, 7)
	if err != nil {
		t.Fatalf("readIdentity() error = %v", err)
	}
	if identity.VendorIdentifier != 77 || identity.ModelName != "Controller" || len(issues) == 0 {
		t.Fatalf("readIdentity() = %#v, %#v", identity, issues)
	}
}

func TestTargetForRouteSupportsDirectAndRoutedDevices(t *testing.T) {
	direct, err := targetForRoute(discovery.Route{
		Endpoint: netip.MustParseAddrPort("192.0.2.7:47808"),
		MaxAPDU:  480,
	})
	if err != nil || direct.Endpoint.String() != "192.0.2.7:47808" || direct.MaxAPDU != 480 {
		t.Fatalf("direct target = %#v, %v", direct, err)
	}
	routed, err := targetForRoute(discovery.Route{
		Endpoint:      netip.MustParseAddrPort("192.0.2.1:47808"),
		RemoteNetwork: 42,
		MAC:           []byte{0x0a},
	})
	if err != nil || routed.Address.Network() != 42 || routed.Address.MAC().String() != "0a" {
		t.Fatalf("routed target = %#v, %v", routed, err)
	}
}

func TestReadPropertiesFallsBackWhenRPMIsUnsupported(t *testing.T) {
	object := bacnet.ObjectIdentifier{Type: bacnet.ObjectTypeAnalogValue, Instance: 7}
	reader := &stubBatchPropertyReader{
		batchErr: &bacnet.RejectError{Reason: bacnet.RejectReasonUnrecognizedService},
		values: map[bacnet.PropertyIdentifier]bacnet.ApplicationValue{
			bacnet.PropertyObjectName:   textValue("Zone temperature"),
			bacnet.PropertyPresentValue: bacnet.RealValue(22.25),
		},
	}
	results, err := readProperties(
		context.Background(),
		reader,
		client.Target{},
		[]service.ReadAccessSpecification{{
			Object: object,
			Properties: []bacnet.PropertyReference{
				{Identifier: bacnet.PropertyObjectName},
				{Identifier: bacnet.PropertyPresentValue},
			},
		}},
	)
	if err != nil {
		t.Fatalf("readProperties() error = %v", err)
	}
	if len(results) != 1 || len(results[0].Properties) != 2 || reader.individualReads != 2 {
		t.Fatalf("results = %#v, individual reads = %d", results, reader.individualReads)
	}
}

func TestReadObjectListEnforcesLimitBeforeReadingEntries(t *testing.T) {
	reader := &stubBatchPropertyReader{
		values: map[bacnet.PropertyIdentifier]bacnet.ApplicationValue{
			bacnet.PropertyObjectList: bacnet.UnsignedValue(3),
		},
	}
	_, err := readObjectList(context.Background(), reader, client.Target{}, 1201, 2)
	if !errors.Is(err, bacnet.ErrLimitExceeded) {
		t.Fatalf("readObjectList() error = %v, want ErrLimitExceeded", err)
	}
	if reader.batchReads != 0 {
		t.Fatalf("RPM batch reads = %d, want 0", reader.batchReads)
	}
}

type stubPropertyReader struct {
	rpm    []service.ReadAccessResult
	rpmErr error
	values map[bacnet.PropertyIdentifier]bacnet.ApplicationValue
}

type stubBatchPropertyReader struct {
	batch           []service.ReadAccessResult
	batchErr        error
	values          map[bacnet.PropertyIdentifier]bacnet.ApplicationValue
	batchReads      int
	individualReads int
}

func (s *stubBatchPropertyReader) ReadProperty(
	_ context.Context,
	_ client.Target,
	_ bacnet.ObjectIdentifier,
	property bacnet.PropertyReference,
) (bacnet.ApplicationValue, error) {
	s.individualReads++
	value, exists := s.values[property.Identifier]
	if !exists {
		return bacnet.ApplicationValue{}, errors.New("property unavailable")
	}
	return value, nil
}

func (s *stubBatchPropertyReader) ReadPropertyMultiple(
	context.Context,
	client.Target,
	[]service.ReadAccessSpecification,
) ([]service.ReadAccessResult, error) {
	return s.batch, s.batchErr
}

func (s *stubBatchPropertyReader) ReadPropertyMultipleBatched(
	context.Context,
	client.Target,
	[]service.ReadAccessSpecification,
	client.RPMBatchOptions,
) ([]service.ReadAccessResult, error) {
	s.batchReads++
	return s.batch, s.batchErr
}

func textValue(value string) bacnet.ApplicationValue {
	return bacnet.ApplicationValue{
		Kind:      bacnet.ValueCharacterString,
		Character: bacnet.CharacterString{Value: value},
	}
}

func (s *stubPropertyReader) ReadProperty(
	_ context.Context,
	_ client.Target,
	_ bacnet.ObjectIdentifier,
	property bacnet.PropertyReference,
) (bacnet.ApplicationValue, error) {
	value, exists := s.values[property.Identifier]
	if !exists {
		return bacnet.ApplicationValue{}, errors.New("property unavailable")
	}
	return value, nil
}

func (s *stubPropertyReader) ReadPropertyMultiple(
	context.Context,
	client.Target,
	[]service.ReadAccessSpecification,
) ([]service.ReadAccessResult, error) {
	return s.rpm, s.rpmErr
}

// Package transport adapts go-bacnet to the provider's narrow discovery API.
package transport

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/netip"
	"slices"
	"strings"
	"time"

	"github.com/kubling-community/kubling-providers/providers/bacnet/internal/config"
	"github.com/kubling-community/kubling-providers/providers/bacnet/internal/discovery"
	bacnet "github.com/otfabric/go-bacnet"
	"github.com/otfabric/go-bacnet/bip"
	"github.com/otfabric/go-bacnet/client"
	"github.com/otfabric/go-bacnet/service"
)

const (
	propertyApplicationSoftwareVersion bacnet.PropertyIdentifier = 12
	propertyFirmwareRevision           bacnet.PropertyIdentifier = 44
)

var identityProperties = []bacnet.PropertyIdentifier{
	bacnet.PropertyVendorIdentifier,
	bacnet.PropertyVendorName,
	bacnet.PropertyModelName,
	propertyFirmwareRevision,
	propertyApplicationSoftwareVersion,
	bacnet.PropertyProtocolVersion,
	bacnet.PropertyProtocolRevision,
}

// Client owns one long-lived go-bacnet client and UDP socket.
type Client struct {
	attachment    config.Attachment
	client        *client.Client
	foreignDevice bool
}

// NewClient opens one physical BACnet/IP attachment.
func NewClient(attachment config.Attachment) (discovery.Client, error) {
	options := []client.Option{
		client.WithLocalAddr(attachment.Bind.String()),
		client.WithTransactionOptions(
			attachment.APDUTimeout,
			attachment.Retries,
			attachment.SegmentTimeout,
		),
		client.WithRegistryOptions(client.RegistryOptions{
			MaxObservations:     attachment.MaxObservations,
			MaxPathsPerInstance: attachment.MaxPathsPerInstance,
			ObservationTTL:      attachment.ObservationTTL,
		}),
		client.WithLogger(slog.Default().With("attachment", attachment.ID)),
	}
	if attachment.Interface != "" {
		options = append(options, client.WithInterface(attachment.Interface))
	}
	foreignDevice := attachment.ForeignDevice != nil
	if foreignDevice {
		options = append(options, client.WithForeignDevice(client.ForeignDeviceConfig{
			BBMD: bip.NewEndpoint(attachment.ForeignDevice.BBMD),
			TTL:  attachment.ForeignDevice.TTL,
		}))
	}
	implementation, err := client.New(options...)
	if err != nil {
		return nil, err
	}
	return &Client{
		attachment:    attachment,
		client:        implementation,
		foreignDevice: foreignDevice,
	}, nil
}

// Discover sends all probes for one logical scope and returns only responses
// observed during that bounded window.
func (c *Client) Discover(
	ctx context.Context,
	configuration config.Discovery,
) ([]discovery.Observation, error) {
	startedAt := time.Now()
	options := client.DiscoveryOptions{
		LowLimit:  configuration.LowDeviceInstance,
		HighLimit: configuration.HighDeviceInstance,
	}
	var (
		sent bool
		err  error
	)
	if configuration.Broadcast {
		broadcast := bip.NewEndpoint(netip.AddrPortFrom(
			netip.AddrFrom4([4]byte{255, 255, 255, 255}),
			47808,
		))
		err = errors.Join(err, c.client.SendWhoIs(ctx, broadcast, true, options))
		sent = true
	}
	for _, target := range configuration.Targets {
		directedOptions := options
		if target.RemoteNetwork != nil {
			directedOptions.Address = bacnet.RemoteBroadcast(*target.RemoteNetwork)
		}
		sendErr := c.client.SendWhoIs(
			ctx,
			bip.NewEndpoint(target.Endpoint),
			false,
			directedOptions,
		)
		if sendErr != nil {
			err = errors.Join(err, fmt.Errorf("Who-Is to %s: %w", target.Endpoint, sendErr))
		}
		sent = true
	}
	if sent {
		timer := time.NewTimer(configuration.Window)
		select {
		case <-ctx.Done():
			if !timer.Stop() {
				<-timer.C
			}
			return nil, ctx.Err()
		case <-timer.C:
		}
	}

	observations := c.client.Devices()
	result := make([]discovery.Observation, 0, len(observations))
	for _, observation := range observations {
		if observation.LastSeen.Before(startedAt) ||
			!deviceInBounds(
				observation.Instance,
				configuration.LowDeviceInstance,
				configuration.HighDeviceInstance,
			) {
			continue
		}
		target := observation.Target()
		route := discovery.Route{
			Endpoint: target.Endpoint.Addr,
			MaxAPDU:  target.MaxAPDU,
		}
		if target.Address.Scope() == bacnet.AddressRemoteStation {
			route.RemoteNetwork = target.Address.Network()
			route.MAC = target.Address.MAC().Bytes()
		}
		if !configuration.Broadcast && !matchesTarget(route, configuration.Targets) {
			continue
		}
		converted := discovery.Observation{
			DeviceInstance: observation.Instance,
			Route:          route,
			LastSeen:       observation.LastSeen.UTC(),
		}
		if observation.Capabilities.VendorID.Known {
			converted.VendorKnown = true
			converted.VendorIdentifier = uint32(observation.Capabilities.VendorID.Value)
		}
		result = append(result, converted)
	}
	slices.SortFunc(result, func(left discovery.Observation, right discovery.Observation) int {
		if left.DeviceInstance < right.DeviceInstance {
			return -1
		}
		if left.DeviceInstance > right.DeviceInstance {
			return 1
		}
		return strings.Compare(left.Route.Canonical(), right.Route.Canonical())
	})
	return result, err
}

// Identify reads Device Object properties, falling back to individual reads
// when ReadPropertyMultiple is unavailable.
func (c *Client) Identify(
	ctx context.Context,
	observation discovery.Observation,
) (discovery.Identity, []discovery.Issue, error) {
	target, err := targetForRoute(observation.Route)
	if err != nil {
		return discovery.Identity{}, nil, err
	}
	identity, issues, err := readIdentity(ctx, c.client, target, observation.DeviceInstance)
	if identity.VendorIdentifier == 0 && observation.VendorKnown {
		identity.VendorIdentifier = observation.VendorIdentifier
	}
	return identity, issues, err
}

// ReadObjectList reads the array length first so exceeding the provider bound
// is an explicit error instead of a silently truncated result.
func (c *Client) ReadObjectList(
	ctx context.Context,
	route discovery.Route,
	deviceInstance uint32,
	maxItems int,
) ([]bacnet.ObjectIdentifier, error) {
	if maxItems <= 0 {
		return nil, fmt.Errorf("object-list limit must be positive")
	}
	target, err := targetForRoute(route)
	if err != nil {
		return nil, err
	}
	return readObjectList(ctx, c.client, target, deviceInstance, maxItems)
}

func readObjectList(
	ctx context.Context,
	reader propertyBatchReader,
	target client.Target,
	deviceInstance uint32,
	maxItems int,
) ([]bacnet.ObjectIdentifier, error) {
	device := bacnet.ObjectIdentifier{Type: bacnet.ObjectTypeDevice, Instance: deviceInstance}
	zero := uint32(0)
	countValue, err := reader.ReadProperty(ctx, target, device, bacnet.PropertyReference{
		Identifier: bacnet.PropertyObjectList,
		ArrayIndex: &zero,
	})
	if err != nil {
		return nil, fmt.Errorf("read object-list length: %w", err)
	}
	count, err := bacnet.AsUnsigned(countValue)
	if err != nil {
		return nil, fmt.Errorf("decode object-list length: %w", err)
	}
	if count > uint64(maxItems) {
		return nil, fmt.Errorf(
			"%w: device %d exposes %d objects; query limit is %d",
			bacnet.ErrLimitExceeded,
			deviceInstance,
			count,
			maxItems,
		)
	}
	if count == 0 {
		return []bacnet.ObjectIdentifier{}, nil
	}
	specification := service.ReadAccessSpecification{Object: device}
	for index := uint32(1); uint64(index) <= count; index++ {
		value := index
		specification.Properties = append(specification.Properties, bacnet.PropertyReference{
			Identifier: bacnet.PropertyObjectList,
			ArrayIndex: &value,
		})
	}
	results, err := readProperties(ctx, reader, target, []service.ReadAccessSpecification{specification})
	if err != nil {
		return nil, fmt.Errorf("read object-list entries: %w", err)
	}
	objects := make([]bacnet.ObjectIdentifier, 0, count)
	for _, result := range results {
		for _, property := range result.Properties {
			if property.Err != nil {
				return nil, fmt.Errorf("read object-list element %d: %w", len(objects)+1, property.Err)
			}
			identifier, decodeErr := bacnet.AsObjectID(property.Value)
			if decodeErr != nil {
				return nil, fmt.Errorf("decode object-list element %d: %w", len(objects)+1, decodeErr)
			}
			objects = append(objects, identifier)
		}
	}
	if uint64(len(objects)) != count {
		return nil, fmt.Errorf("object-list returned %d entries, expected %d", len(objects), count)
	}
	return objects, nil
}

// ReadProperties groups read-only requests with adaptive RPM and falls back to
// individual ReadProperty calls only when the peer rejects RPM itself.
func (c *Client) ReadProperties(
	ctx context.Context,
	route discovery.Route,
	specifications []service.ReadAccessSpecification,
) ([]service.ReadAccessResult, error) {
	target, err := targetForRoute(route)
	if err != nil {
		return nil, err
	}
	return readProperties(ctx, c.client, target, specifications)
}

// WriteProperty performs one confirmed BACnet WriteProperty request. The
// underlying client deliberately does not retransmit writes whose outcome may
// be unknown.
func (c *Client) WriteProperty(
	ctx context.Context,
	route discovery.Route,
	object bacnet.ObjectIdentifier,
	property bacnet.PropertyReference,
	value bacnet.ApplicationValue,
	priority *uint8,
) error {
	target, err := targetForRoute(route)
	if err != nil {
		return err
	}
	return c.client.WriteProperty(ctx, target, object, property, value, priority)
}

// Status returns current attachment and FDR state.
func (c *Client) Status() discovery.AttachmentSnapshot {
	return discovery.AttachmentSnapshot{
		LocalEndpoint:           c.client.LocalEndpoint().Addr,
		ForeignDevice:           c.foreignDevice,
		ForeignDeviceRegistered: c.foreignDevice && c.client.ForeignDeviceRegistered(),
	}
}

// Close releases the attachment socket.
func (c *Client) Close() error {
	return c.client.Close()
}

type propertyReader interface {
	ReadProperty(
		context.Context,
		client.Target,
		bacnet.ObjectIdentifier,
		bacnet.PropertyReference,
	) (bacnet.ApplicationValue, error)
	ReadPropertyMultiple(
		context.Context,
		client.Target,
		[]service.ReadAccessSpecification,
	) ([]service.ReadAccessResult, error)
}

type propertyBatchReader interface {
	propertyReader
	ReadPropertyMultipleBatched(
		context.Context,
		client.Target,
		[]service.ReadAccessSpecification,
		client.RPMBatchOptions,
	) ([]service.ReadAccessResult, error)
}

func readProperties(
	ctx context.Context,
	reader propertyBatchReader,
	target client.Target,
	specifications []service.ReadAccessSpecification,
) ([]service.ReadAccessResult, error) {
	if len(specifications) == 0 {
		return nil, nil
	}
	propertyCount := 0
	for index, specification := range specifications {
		if len(specification.Properties) == 0 {
			return nil, fmt.Errorf("read specification %d has no properties", index)
		}
		propertyCount += len(specification.Properties)
	}
	results, err := reader.ReadPropertyMultipleBatched(
		ctx,
		target,
		specifications,
		client.RPMBatchOptions{MaxBatches: propertyCount},
	)
	if err == nil {
		return results, nil
	}
	var reject *bacnet.RejectError
	if !errors.As(err, &reject) || reject.Reason != bacnet.RejectReasonUnrecognizedService {
		return results, err
	}

	results = make([]service.ReadAccessResult, 0, len(specifications))
	for _, specification := range specifications {
		result := service.ReadAccessResult{Object: specification.Object}
		for _, property := range specification.Properties {
			value, readErr := reader.ReadProperty(ctx, target, specification.Object, property)
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

func readIdentity(
	ctx context.Context,
	reader propertyReader,
	target client.Target,
	deviceInstance uint32,
) (discovery.Identity, []discovery.Issue, error) {
	deviceObject := bacnet.ObjectIdentifier{
		Type:     bacnet.ObjectTypeDevice,
		Instance: deviceInstance,
	}
	properties := make([]bacnet.PropertyReference, 0, len(identityProperties))
	for _, identifier := range identityProperties {
		properties = append(properties, bacnet.PropertyReference{Identifier: identifier})
	}
	results, rpmErr := reader.ReadPropertyMultiple(ctx, target, []service.ReadAccessSpecification{{
		Object:     deviceObject,
		Properties: properties,
	}})
	values := make(map[bacnet.PropertyIdentifier]bacnet.ApplicationValue, len(identityProperties))
	issues := make([]discovery.Issue, 0)
	if rpmErr == nil {
		reported := make(map[bacnet.PropertyIdentifier]struct{}, len(identityProperties))
		for _, result := range results {
			for _, property := range result.Properties {
				reported[property.Property.Identifier] = struct{}{}
				if property.Err != nil {
					issues = append(issues, propertyIssue(property.Property.Identifier, property.Err))
					continue
				}
				values[property.Property.Identifier] = property.Value
			}
		}
		for _, identifier := range identityProperties {
			if _, exists := reported[identifier]; !exists {
				issues = append(
					issues,
					propertyIssue(identifier, errors.New("property was not returned")),
				)
			}
		}
	} else {
		var fallbackErr error
		for _, identifier := range identityProperties {
			value, readErr := reader.ReadProperty(
				ctx,
				target,
				deviceObject,
				bacnet.PropertyReference{Identifier: identifier},
			)
			if readErr != nil {
				fallbackErr = errors.Join(fallbackErr, readErr)
				issues = append(issues, propertyIssue(identifier, readErr))
				continue
			}
			values[identifier] = value
		}
		if len(values) == 0 {
			return discovery.Identity{}, issues, errors.Join(rpmErr, fallbackErr)
		}
	}
	identity, decodeIssues := decodeIdentity(values)
	issues = append(issues, decodeIssues...)
	return identity, issues, nil
}

func decodeIdentity(
	values map[bacnet.PropertyIdentifier]bacnet.ApplicationValue,
) (discovery.Identity, []discovery.Issue) {
	result := discovery.Identity{}
	issues := make([]discovery.Issue, 0)
	for identifier, value := range values {
		switch identifier {
		case bacnet.PropertyVendorIdentifier:
			number, err := bacnet.AsUnsigned(value)
			if err != nil {
				issues = append(issues, propertyIssue(identifier, err))
			} else {
				result.VendorIdentifier = uint32(number)
			}
		case bacnet.PropertyVendorName:
			result.VendorName = decodeText(identifier, value, &issues)
		case bacnet.PropertyModelName:
			result.ModelName = decodeText(identifier, value, &issues)
		case propertyFirmwareRevision:
			result.FirmwareRevision = decodeText(identifier, value, &issues)
		case propertyApplicationSoftwareVersion:
			result.ApplicationSoftwareVersion = decodeText(identifier, value, &issues)
		case bacnet.PropertyProtocolVersion:
			number, err := bacnet.AsUnsigned(value)
			if err != nil {
				issues = append(issues, propertyIssue(identifier, err))
			} else {
				result.ProtocolVersion = uint32(number)
			}
		case bacnet.PropertyProtocolRevision:
			number, err := bacnet.AsUnsigned(value)
			if err != nil {
				issues = append(issues, propertyIssue(identifier, err))
			} else {
				result.ProtocolRevision = uint32(number)
			}
		}
	}
	return result, issues
}

func decodeText(
	identifier bacnet.PropertyIdentifier,
	value bacnet.ApplicationValue,
	issues *[]discovery.Issue,
) string {
	if value.Kind != bacnet.ValueCharacterString {
		*issues = append(*issues, propertyIssue(identifier, bacnet.ErrMalformed))
		return ""
	}
	result, err := value.Character.Text()
	if err != nil {
		*issues = append(*issues, propertyIssue(identifier, err))
		return ""
	}
	return strings.TrimSpace(result)
}

func propertyIssue(identifier bacnet.PropertyIdentifier, err error) discovery.Issue {
	return discovery.Issue{
		Code:       discovery.IssuePropertyUnavailable,
		Message:    fmt.Sprintf("property %s: %v", identifier, err),
		PropertyID: uint32(identifier),
	}
}

func targetForRoute(route discovery.Route) (client.Target, error) {
	if !route.Endpoint.IsValid() || !route.Endpoint.Addr().Is4() {
		return client.Target{}, fmt.Errorf("invalid BACnet route endpoint %q", route.Endpoint)
	}
	result := client.Target{
		Endpoint: bip.NewEndpoint(route.Endpoint),
		MaxAPDU:  route.MaxAPDU,
	}
	if route.RemoteNetwork != 0 {
		mac, err := bacnet.NewMAC(route.MAC)
		if err != nil {
			return client.Target{}, err
		}
		if mac.IsZero() {
			return client.Target{}, fmt.Errorf("routed BACnet target requires a MAC")
		}
		result.Address = bacnet.RemoteStation(route.RemoteNetwork, mac)
	}
	return result, nil
}

func matchesTarget(route discovery.Route, targets []config.DiscoveryTarget) bool {
	for _, target := range targets {
		if route.Endpoint != target.Endpoint {
			continue
		}
		if target.RemoteNetwork == nil && route.RemoteNetwork == 0 {
			return true
		}
		if target.RemoteNetwork != nil && route.RemoteNetwork == *target.RemoteNetwork {
			return true
		}
	}
	return false
}

func deviceInBounds(instance uint32, low *uint32, high *uint32) bool {
	return (low == nil || instance >= *low) && (high == nil || instance <= *high)
}

var _ discovery.Client = (*Client)(nil)

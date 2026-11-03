package config

import (
	"encoding/hex"
	"fmt"
	"net/netip"
	"regexp"
	"slices"
	"strings"
	"time"
)

const (
	defaultBACnetBindAddress  = "0.0.0.0:47808"
	defaultAPDUTimeout        = 3 * time.Second
	defaultSegmentTimeout     = 2 * time.Second
	defaultRetryCount         = 3
	defaultObservationTTL     = 30 * time.Minute
	defaultMaxObservations    = 4096
	defaultMaxPaths           = 8
	defaultDiscoveryInterval  = time.Minute
	defaultDiscoveryWindow    = 3 * time.Second
	maximumDeviceInstance     = uint32(4_194_303)
	maximumDiscoveryTargets   = 256
	maximumStaticDevices      = 4096
	maximumForeignDeviceTTL   = 65_535 * time.Second
	minimumForeignDeviceTTL   = 2 * time.Second
	maximumDiscoveryInterval  = 24 * time.Hour
	maximumDiscoveryWindow    = time.Minute
	minimumDiscoveryWindow    = 100 * time.Millisecond
	maximumTransactionTimeout = time.Minute
)

var (
	attachmentIDPattern = regexp.MustCompile(`^[a-z][a-z0-9-]{0,62}$`)
	namespacePattern    = regexp.MustCompile(`^[a-z][a-z0-9_.-]{0,126}$`)
)

// Attachment describes one long-lived BACnet/IP data-link endpoint.
type Attachment struct {
	ID                  string
	Interface           string
	Bind                netip.AddrPort
	APDUTimeout         time.Duration
	Retries             int
	SegmentTimeout      time.Duration
	ObservationTTL      time.Duration
	MaxObservations     int
	MaxPathsPerInstance int
	ForeignDevice       *ForeignDevice
}

// ForeignDevice configures registration with one BBMD.
type ForeignDevice struct {
	BBMD netip.AddrPort
	TTL  time.Duration
}

// Scope is one logical BACnet device universe attached to a physical socket.
type Scope struct {
	Namespace     string
	ID            string
	AttachmentID  string
	Mutations     MutationPolicy
	Discovery     Discovery
	StaticDevices []StaticDevice
}

// Key returns the stable namespace-qualified scope identifier.
func (s Scope) Key() string {
	return s.Namespace + "/" + s.ID
}

// MutationPolicy controls whether a logical scope accepts BACnet property
// writes and the priority used for commandable properties.
type MutationPolicy struct {
	Enabled  bool
	Priority uint8
}

// Discovery controls periodic Who-Is probes for a scope.
type Discovery struct {
	Broadcast          bool
	Interval           time.Duration
	Window             time.Duration
	LowDeviceInstance  *uint32
	HighDeviceInstance *uint32
	Targets            []DiscoveryTarget
}

// DiscoveryTarget sends Who-Is to one direct peer or BACnet router.
type DiscoveryTarget struct {
	Endpoint      netip.AddrPort
	RemoteNetwork *uint16
}

// StaticDevice gives a device a canonical route without relying on Who-Is.
type StaticDevice struct {
	DeviceInstance uint32
	Endpoint       netip.AddrPort
	RemoteNetwork  *uint16
	MAC            []byte
}

type fileAttachment struct {
	ID            string             `yaml:"id"`
	Interface     string             `yaml:"interface"`
	Bind          string             `yaml:"bind"`
	Transaction   fileTransaction    `yaml:"transaction"`
	Registry      fileRegistry       `yaml:"registry"`
	ForeignDevice *fileForeignDevice `yaml:"foreignDevice"`
}

type fileTransaction struct {
	APDUTimeout    string `yaml:"apduTimeout"`
	Retries        *int   `yaml:"retries"`
	SegmentTimeout string `yaml:"segmentTimeout"`
}

type fileRegistry struct {
	ObservationTTL      string `yaml:"observationTTL"`
	MaxObservations     *int   `yaml:"maxObservations"`
	MaxPathsPerInstance *int   `yaml:"maxPathsPerInstance"`
}

type fileForeignDevice struct {
	BBMD string `yaml:"bbmd"`
	TTL  string `yaml:"ttl"`
}

type fileScope struct {
	Namespace     string             `yaml:"namespace"`
	ID            string             `yaml:"id"`
	Attachment    string             `yaml:"attachment"`
	Mutations     fileMutations      `yaml:"mutations"`
	Discovery     fileDiscovery      `yaml:"discovery"`
	StaticDevices []fileStaticDevice `yaml:"staticDevices"`
}

type fileMutations struct {
	Enabled  bool   `yaml:"enabled"`
	Priority *uint8 `yaml:"priority"`
}

type fileDiscovery struct {
	Broadcast          bool                  `yaml:"broadcast"`
	Interval           string                `yaml:"interval"`
	Window             string                `yaml:"window"`
	LowDeviceInstance  *uint32               `yaml:"lowDeviceInstance"`
	HighDeviceInstance *uint32               `yaml:"highDeviceInstance"`
	Targets            []fileDiscoveryTarget `yaml:"targets"`
}

type fileDiscoveryTarget struct {
	Endpoint      string  `yaml:"endpoint"`
	RemoteNetwork *uint16 `yaml:"remoteNetwork"`
}

type fileStaticDevice struct {
	DeviceInstance uint32  `yaml:"deviceInstance"`
	Endpoint       string  `yaml:"endpoint"`
	RemoteNetwork  *uint16 `yaml:"remoteNetwork"`
	MAC            string  `yaml:"mac"`
}

func normalizeTopology(
	attachmentFiles []fileAttachment,
	scopeFiles []fileScope,
) ([]Attachment, []Scope, error) {
	attachments := make([]Attachment, 0, len(attachmentFiles))
	byID := make(map[string]Attachment, len(attachmentFiles))
	for index, input := range attachmentFiles {
		attachment, err := normalizeAttachment(input)
		if err != nil {
			return nil, nil, fmt.Errorf("attachments[%d]: %w", index, err)
		}
		if _, exists := byID[attachment.ID]; exists {
			return nil, nil, fmt.Errorf("attachments[%d]: duplicate id %q", index, attachment.ID)
		}
		for _, existing := range attachments {
			if bindAddressesConflict(existing.Bind, attachment.Bind) {
				return nil, nil, fmt.Errorf(
					"attachments %q and %q have conflicting bind addresses %s and %s",
					existing.ID,
					attachment.ID,
					existing.Bind,
					attachment.Bind,
				)
			}
		}
		attachments = append(attachments, attachment)
		byID[attachment.ID] = attachment
	}
	slices.SortFunc(attachments, func(left Attachment, right Attachment) int {
		return strings.Compare(left.ID, right.ID)
	})

	scopes := make([]Scope, 0, len(scopeFiles))
	seenScopes := make(map[string]struct{}, len(scopeFiles))
	for index, input := range scopeFiles {
		scope, err := normalizeScope(input, byID)
		if err != nil {
			return nil, nil, fmt.Errorf("scopes[%d]: %w", index, err)
		}
		if _, exists := seenScopes[scope.Key()]; exists {
			return nil, nil, fmt.Errorf("scopes[%d]: duplicate scope %q", index, scope.Key())
		}
		seenScopes[scope.Key()] = struct{}{}
		scopes = append(scopes, scope)
	}
	slices.SortFunc(scopes, func(left Scope, right Scope) int {
		return strings.Compare(left.Key(), right.Key())
	})
	return attachments, scopes, nil
}

func normalizeAttachment(input fileAttachment) (Attachment, error) {
	id := strings.TrimSpace(input.ID)
	if !attachmentIDPattern.MatchString(id) {
		return Attachment{}, fmt.Errorf("invalid id %q", input.ID)
	}
	interfaceName := strings.TrimSpace(input.Interface)
	if strings.ContainsAny(interfaceName, " \t\r\n") {
		return Attachment{}, fmt.Errorf("interface %q contains whitespace", input.Interface)
	}
	bind, err := parseIPv4Endpoint(input.Bind, defaultBACnetBindAddress, true, "bind")
	if err != nil {
		return Attachment{}, err
	}
	apduTimeout, err := normalizeBoundedDuration(
		input.Transaction.APDUTimeout,
		defaultAPDUTimeout,
		minimumDiscoveryWindow,
		maximumTransactionTimeout,
		"transaction.apduTimeout",
	)
	if err != nil {
		return Attachment{}, err
	}
	segmentTimeout, err := normalizeBoundedDuration(
		input.Transaction.SegmentTimeout,
		defaultSegmentTimeout,
		minimumDiscoveryWindow,
		maximumTransactionTimeout,
		"transaction.segmentTimeout",
	)
	if err != nil {
		return Attachment{}, err
	}
	retries := defaultRetryCount
	if input.Transaction.Retries != nil {
		retries = *input.Transaction.Retries
	}
	if retries < 0 || retries > 10 {
		return Attachment{}, fmt.Errorf("transaction.retries must be between 0 and 10")
	}
	observationTTL, err := normalizeBoundedDuration(
		input.Registry.ObservationTTL,
		defaultObservationTTL,
		time.Second,
		maximumDiscoveryInterval,
		"registry.observationTTL",
	)
	if err != nil {
		return Attachment{}, err
	}
	maxObservations, err := normalizePositiveLimit(
		input.Registry.MaxObservations,
		defaultMaxObservations,
		1_000_000,
		"registry.maxObservations",
	)
	if err != nil {
		return Attachment{}, err
	}
	maxPaths, err := normalizePositiveLimit(
		input.Registry.MaxPathsPerInstance,
		defaultMaxPaths,
		256,
		"registry.maxPathsPerInstance",
	)
	if err != nil {
		return Attachment{}, err
	}
	var foreignDevice *ForeignDevice
	if input.ForeignDevice != nil {
		bbmd, parseErr := parseIPv4Endpoint(input.ForeignDevice.BBMD, "", false, "foreignDevice.bbmd")
		if parseErr != nil {
			return Attachment{}, parseErr
		}
		ttl, parseErr := normalizeBoundedDuration(
			input.ForeignDevice.TTL,
			60*time.Second,
			minimumForeignDeviceTTL,
			maximumForeignDeviceTTL,
			"foreignDevice.ttl",
		)
		if parseErr != nil {
			return Attachment{}, parseErr
		}
		if ttl%time.Second != 0 {
			return Attachment{}, fmt.Errorf("foreignDevice.ttl must use whole seconds")
		}
		foreignDevice = &ForeignDevice{BBMD: bbmd, TTL: ttl}
	}
	return Attachment{
		ID:                  id,
		Interface:           interfaceName,
		Bind:                bind,
		APDUTimeout:         apduTimeout,
		Retries:             retries,
		SegmentTimeout:      segmentTimeout,
		ObservationTTL:      observationTTL,
		MaxObservations:     maxObservations,
		MaxPathsPerInstance: maxPaths,
		ForeignDevice:       foreignDevice,
	}, nil
}

func normalizeScope(input fileScope, attachments map[string]Attachment) (Scope, error) {
	namespace := strings.TrimSpace(input.Namespace)
	if !namespacePattern.MatchString(namespace) {
		return Scope{}, fmt.Errorf("invalid namespace %q", input.Namespace)
	}
	id := strings.TrimSpace(input.ID)
	if !attachmentIDPattern.MatchString(id) {
		return Scope{}, fmt.Errorf("invalid id %q", input.ID)
	}
	attachmentID := strings.TrimSpace(input.Attachment)
	attachment, exists := attachments[attachmentID]
	if !exists {
		return Scope{}, fmt.Errorf("attachment %q does not exist", attachmentID)
	}
	discovery, err := normalizeDiscovery(input.Discovery)
	if err != nil {
		return Scope{}, err
	}
	mutations, err := normalizeMutationPolicy(input.Mutations)
	if err != nil {
		return Scope{}, err
	}
	staticDevices, err := normalizeStaticDevices(input.StaticDevices, discovery)
	if err != nil {
		return Scope{}, err
	}
	if !discovery.Broadcast && len(discovery.Targets) == 0 && len(staticDevices) == 0 {
		return Scope{}, fmt.Errorf("scope needs broadcast, a discovery target, or a static device")
	}
	if discovery.Broadcast && attachment.ForeignDevice == nil {
		if !attachment.Bind.Addr().IsUnspecified() || attachment.Bind.Port() != 47808 {
			return Scope{}, fmt.Errorf(
				"local broadcast requires attachment %q to bind 0.0.0.0:47808 or use foreignDevice",
				attachmentID,
			)
		}
	}
	return Scope{
		Namespace:     namespace,
		ID:            id,
		AttachmentID:  attachmentID,
		Mutations:     mutations,
		Discovery:     discovery,
		StaticDevices: staticDevices,
	}, nil
}

func normalizeMutationPolicy(input fileMutations) (MutationPolicy, error) {
	if !input.Enabled {
		if input.Priority != nil {
			return MutationPolicy{}, fmt.Errorf("mutations.priority requires mutations.enabled")
		}
		return MutationPolicy{}, nil
	}
	if input.Priority == nil {
		return MutationPolicy{}, fmt.Errorf("mutations.priority is required when mutations are enabled")
	}
	if *input.Priority < 1 || *input.Priority > 16 {
		return MutationPolicy{}, fmt.Errorf("mutations.priority must be between 1 and 16")
	}
	return MutationPolicy{Enabled: true, Priority: *input.Priority}, nil
}

func normalizeDiscovery(input fileDiscovery) (Discovery, error) {
	interval, err := normalizeBoundedDuration(
		input.Interval,
		defaultDiscoveryInterval,
		time.Second,
		maximumDiscoveryInterval,
		"discovery.interval",
	)
	if err != nil {
		return Discovery{}, err
	}
	window, err := normalizeBoundedDuration(
		input.Window,
		defaultDiscoveryWindow,
		minimumDiscoveryWindow,
		maximumDiscoveryWindow,
		"discovery.window",
	)
	if err != nil {
		return Discovery{}, err
	}
	if input.LowDeviceInstance != nil && *input.LowDeviceInstance > maximumDeviceInstance {
		return Discovery{}, fmt.Errorf("discovery.lowDeviceInstance exceeds %d", maximumDeviceInstance)
	}
	if input.HighDeviceInstance != nil && *input.HighDeviceInstance > maximumDeviceInstance {
		return Discovery{}, fmt.Errorf("discovery.highDeviceInstance exceeds %d", maximumDeviceInstance)
	}
	if input.LowDeviceInstance != nil && input.HighDeviceInstance != nil &&
		*input.LowDeviceInstance > *input.HighDeviceInstance {
		return Discovery{}, fmt.Errorf("discovery.lowDeviceInstance must not exceed highDeviceInstance")
	}
	if len(input.Targets) > maximumDiscoveryTargets {
		return Discovery{}, fmt.Errorf("discovery.targets exceeds limit %d", maximumDiscoveryTargets)
	}
	targets := make([]DiscoveryTarget, 0, len(input.Targets))
	seen := make(map[string]struct{}, len(input.Targets))
	for index, raw := range input.Targets {
		endpoint, parseErr := parseIPv4Endpoint(raw.Endpoint, "", false, "discovery.targets.endpoint")
		if parseErr != nil {
			return Discovery{}, fmt.Errorf("discovery.targets[%d]: %w", index, parseErr)
		}
		if parseErr = validateRemoteNetwork(raw.RemoteNetwork); parseErr != nil {
			return Discovery{}, fmt.Errorf("discovery.targets[%d]: %w", index, parseErr)
		}
		key := endpoint.String()
		if raw.RemoteNetwork != nil {
			key += fmt.Sprintf("/net=%d", *raw.RemoteNetwork)
		}
		if _, exists := seen[key]; exists {
			return Discovery{}, fmt.Errorf("discovery.targets[%d] duplicates %q", index, key)
		}
		seen[key] = struct{}{}
		targets = append(targets, DiscoveryTarget{
			Endpoint:      endpoint,
			RemoteNetwork: cloneUint16(raw.RemoteNetwork),
		})
	}
	slices.SortFunc(targets, func(left DiscoveryTarget, right DiscoveryTarget) int {
		return strings.Compare(discoveryTargetKey(left), discoveryTargetKey(right))
	})
	return Discovery{
		Broadcast:          input.Broadcast,
		Interval:           interval,
		Window:             window,
		LowDeviceInstance:  cloneUint32(input.LowDeviceInstance),
		HighDeviceInstance: cloneUint32(input.HighDeviceInstance),
		Targets:            targets,
	}, nil
}

func normalizeStaticDevices(input []fileStaticDevice, discovery Discovery) ([]StaticDevice, error) {
	if len(input) > maximumStaticDevices {
		return nil, fmt.Errorf("staticDevices exceeds limit %d", maximumStaticDevices)
	}
	devices := make([]StaticDevice, 0, len(input))
	seen := make(map[uint32]struct{}, len(input))
	for index, raw := range input {
		if raw.DeviceInstance > maximumDeviceInstance {
			return nil, fmt.Errorf("staticDevices[%d].deviceInstance exceeds %d", index, maximumDeviceInstance)
		}
		if !deviceInRange(raw.DeviceInstance, discovery.LowDeviceInstance, discovery.HighDeviceInstance) {
			return nil, fmt.Errorf("staticDevices[%d].deviceInstance is outside discovery bounds", index)
		}
		if _, exists := seen[raw.DeviceInstance]; exists {
			return nil, fmt.Errorf("staticDevices[%d] duplicates deviceInstance %d", index, raw.DeviceInstance)
		}
		seen[raw.DeviceInstance] = struct{}{}
		endpoint, err := parseIPv4Endpoint(raw.Endpoint, "", false, "staticDevices.endpoint")
		if err != nil {
			return nil, fmt.Errorf("staticDevices[%d]: %w", index, err)
		}
		if err = validateRemoteNetwork(raw.RemoteNetwork); err != nil {
			return nil, fmt.Errorf("staticDevices[%d]: %w", index, err)
		}
		mac, err := parseMAC(raw.MAC)
		if err != nil {
			return nil, fmt.Errorf("staticDevices[%d].mac: %w", index, err)
		}
		if raw.RemoteNetwork != nil && len(mac) == 0 {
			return nil, fmt.Errorf("staticDevices[%d].mac is required with remoteNetwork", index)
		}
		if raw.RemoteNetwork == nil && len(mac) != 0 {
			return nil, fmt.Errorf("staticDevices[%d].mac requires remoteNetwork", index)
		}
		devices = append(devices, StaticDevice{
			DeviceInstance: raw.DeviceInstance,
			Endpoint:       endpoint,
			RemoteNetwork:  cloneUint16(raw.RemoteNetwork),
			MAC:            append([]byte(nil), mac...),
		})
	}
	slices.SortFunc(devices, func(left StaticDevice, right StaticDevice) int {
		if left.DeviceInstance < right.DeviceInstance {
			return -1
		}
		if left.DeviceInstance > right.DeviceInstance {
			return 1
		}
		return 0
	})
	return devices, nil
}

func parseIPv4Endpoint(raw string, defaultValue string, allowZeroPort bool, field string) (netip.AddrPort, error) {
	value := strings.TrimSpace(raw)
	if value == "" {
		value = defaultValue
	}
	if value == "" {
		return netip.AddrPort{}, fmt.Errorf("%s is required", field)
	}
	var endpoint netip.AddrPort
	if address, err := netip.ParseAddr(value); err == nil {
		endpoint = netip.AddrPortFrom(address, 47808)
	} else {
		var parseErr error
		endpoint, parseErr = netip.ParseAddrPort(value)
		if parseErr != nil {
			return netip.AddrPort{}, fmt.Errorf("invalid %s %q: %w", field, value, parseErr)
		}
	}
	if !endpoint.Addr().Is4() {
		return netip.AddrPort{}, fmt.Errorf("%s %q must use IPv4", field, value)
	}
	if !allowZeroPort && endpoint.Port() == 0 {
		return netip.AddrPort{}, fmt.Errorf("%s %q must use a non-zero port", field, value)
	}
	if field != "bind" &&
		(!endpoint.Addr().IsGlobalUnicast() && !endpoint.Addr().IsLoopback()) {
		return netip.AddrPort{}, fmt.Errorf("%s %q must be a unicast address", field, value)
	}
	return endpoint, nil
}

func normalizeBoundedDuration(
	raw string,
	defaultValue time.Duration,
	minimum time.Duration,
	maximum time.Duration,
	field string,
) (time.Duration, error) {
	if strings.TrimSpace(raw) == "" {
		return defaultValue, nil
	}
	value, err := time.ParseDuration(raw)
	if err != nil {
		return 0, fmt.Errorf("invalid %s %q: %w", field, raw, err)
	}
	if value < minimum || value > maximum {
		return 0, fmt.Errorf("%s must be between %s and %s", field, minimum, maximum)
	}
	return value, nil
}

func normalizePositiveLimit(raw *int, defaultValue int, maximum int, field string) (int, error) {
	if raw == nil {
		return defaultValue, nil
	}
	if *raw < 1 || *raw > maximum {
		return 0, fmt.Errorf("%s must be between 1 and %d", field, maximum)
	}
	return *raw, nil
}

func validateRemoteNetwork(value *uint16) error {
	if value != nil && (*value == 0 || *value == 65_535) {
		return fmt.Errorf("remoteNetwork must be between 1 and 65534")
	}
	return nil
}

func parseMAC(raw string) ([]byte, error) {
	value := strings.TrimSpace(raw)
	if value == "" {
		return nil, nil
	}
	value = strings.ReplaceAll(value, ":", "")
	value = strings.ReplaceAll(value, "-", "")
	if len(value)%2 != 0 {
		return nil, fmt.Errorf("must contain complete hexadecimal octets")
	}
	decoded, err := hex.DecodeString(value)
	if err != nil {
		return nil, fmt.Errorf("must be hexadecimal: %w", err)
	}
	if len(decoded) < 1 || len(decoded) > 7 {
		return nil, fmt.Errorf("must contain between 1 and 7 octets")
	}
	return decoded, nil
}

func bindAddressesConflict(left netip.AddrPort, right netip.AddrPort) bool {
	return left.Port() == right.Port() &&
		(left.Addr().IsUnspecified() || right.Addr().IsUnspecified() || left.Addr() == right.Addr())
}

func deviceInRange(instance uint32, low *uint32, high *uint32) bool {
	return (low == nil || instance >= *low) && (high == nil || instance <= *high)
}

func discoveryTargetKey(target DiscoveryTarget) string {
	result := target.Endpoint.String()
	if target.RemoteNetwork != nil {
		result += fmt.Sprintf("/net=%d", *target.RemoteNetwork)
	}
	return result
}

func cloneAttachments(input []Attachment) []Attachment {
	result := append([]Attachment(nil), input...)
	for index := range result {
		if input[index].ForeignDevice != nil {
			copyValue := *input[index].ForeignDevice
			result[index].ForeignDevice = &copyValue
		}
	}
	return result
}

func cloneScopes(input []Scope) []Scope {
	result := append([]Scope(nil), input...)
	for index := range result {
		result[index].Discovery.LowDeviceInstance = cloneUint32(input[index].Discovery.LowDeviceInstance)
		result[index].Discovery.HighDeviceInstance = cloneUint32(input[index].Discovery.HighDeviceInstance)
		result[index].Discovery.Targets = append([]DiscoveryTarget(nil), input[index].Discovery.Targets...)
		for targetIndex := range result[index].Discovery.Targets {
			result[index].Discovery.Targets[targetIndex].RemoteNetwork = cloneUint16(
				input[index].Discovery.Targets[targetIndex].RemoteNetwork,
			)
		}
		result[index].StaticDevices = append([]StaticDevice(nil), input[index].StaticDevices...)
		for deviceIndex := range result[index].StaticDevices {
			result[index].StaticDevices[deviceIndex].RemoteNetwork = cloneUint16(
				input[index].StaticDevices[deviceIndex].RemoteNetwork,
			)
			result[index].StaticDevices[deviceIndex].MAC = append(
				[]byte(nil),
				input[index].StaticDevices[deviceIndex].MAC...,
			)
		}
	}
	return result
}

func cloneUint16(value *uint16) *uint16 {
	if value == nil {
		return nil
	}
	result := *value
	return &result
}

func cloneUint32(value *uint32) *uint32 {
	if value == nil {
		return nil
	}
	result := *value
	return &result
}

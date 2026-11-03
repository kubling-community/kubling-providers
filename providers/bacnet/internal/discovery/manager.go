package discovery

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"slices"
	"strings"
	"sync"
	"time"

	"github.com/kubling-community/kubling-providers/providers/bacnet/internal/config"
	"github.com/kubling-community/kubling-providers/providers/bacnet/internal/profile"
	bacnet "github.com/otfabric/go-bacnet"
	"github.com/otfabric/go-bacnet/service"
)

type attachmentState struct {
	client         Client
	observationTTL time.Duration
	discoveryMu    sync.Mutex
	useMu          sync.RWMutex
}

// Manager owns long-lived attachment clients and periodic logical-scope
// discovery. It never changes the compiled schema.
type Manager struct {
	catalog     *profile.Catalog
	scopes      map[string]config.Scope
	attachments map[string]*attachmentState
	registry    *registry

	mu      sync.Mutex
	started bool
	closed  bool
	cancel  context.CancelFunc
	wg      sync.WaitGroup
}

// New opens one BACnet client for every configured physical attachment.
func New(
	attachments []config.Attachment,
	scopes []config.Scope,
	catalog *profile.Catalog,
	factory Factory,
) (*Manager, error) {
	if catalog == nil {
		return nil, fmt.Errorf("BACnet profile catalog is required")
	}
	if factory == nil {
		return nil, fmt.Errorf("BACnet client factory is required")
	}
	result := &Manager{
		catalog:     catalog,
		scopes:      make(map[string]config.Scope, len(scopes)),
		attachments: make(map[string]*attachmentState, len(attachments)),
		registry:    newRegistry(scopes),
	}
	for _, attachment := range attachments {
		client, err := factory(attachment)
		if err != nil {
			closeErr := result.closeClients()
			return nil, errors.Join(
				fmt.Errorf("open BACnet attachment %q: %w", attachment.ID, err),
				closeErr,
			)
		}
		result.attachments[attachment.ID] = &attachmentState{
			client:         client,
			observationTTL: attachment.ObservationTTL,
		}
	}
	for _, scope := range scopes {
		if result.attachments[scope.AttachmentID] == nil {
			_ = result.closeClients()
			return nil, fmt.Errorf(
				"BACnet scope %q references unopened attachment %q",
				scope.Key(),
				scope.AttachmentID,
			)
		}
		result.scopes[scope.Key()] = scope
	}
	return result, nil
}

// Start launches one refresh loop per logical scope. The first refresh is
// immediate; startup does not wait for devices to respond.
func (m *Manager) Start(parent context.Context) error {
	if parent == nil {
		return fmt.Errorf("BACnet discovery parent context is required")
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.closed {
		return fmt.Errorf("BACnet discovery manager is closed")
	}
	if m.started {
		return nil
	}
	ctx, cancel := context.WithCancel(parent)
	m.cancel = cancel
	m.started = true
	for _, scope := range m.scopes {
		scope := scope
		m.wg.Add(1)
		go m.runScope(ctx, scope)
	}
	return nil
}

func (m *Manager) runScope(ctx context.Context, scope config.Scope) {
	defer m.wg.Done()
	for {
		if err := m.RefreshScope(ctx, scope.Namespace, scope.ID); err != nil &&
			!errors.Is(err, context.Canceled) {
			slog.Warn(
				"BACnet scope refresh failed",
				"namespace", scope.Namespace,
				"scope", scope.ID,
				"error", err,
			)
		}
		timer := time.NewTimer(scope.Discovery.Interval)
		select {
		case <-ctx.Done():
			if !timer.Stop() {
				<-timer.C
			}
			return
		case <-timer.C:
		}
	}
}

// RefreshScope refreshes one scope synchronously and atomically replaces its
// registry snapshot.
func (m *Manager) RefreshScope(ctx context.Context, namespace string, scopeID string) error {
	if ctx == nil {
		return fmt.Errorf("BACnet discovery context is required")
	}
	m.mu.Lock()
	closed := m.closed
	m.mu.Unlock()
	if closed {
		return fmt.Errorf("BACnet discovery manager is closed")
	}
	key := strings.TrimSpace(namespace) + "/" + strings.TrimSpace(scopeID)
	scope, exists := m.scopes[key]
	if !exists {
		return fmt.Errorf("BACnet scope %q does not exist", key)
	}
	attachment := m.attachments[scope.AttachmentID]
	if attachment == nil {
		return fmt.Errorf("BACnet attachment %q is unavailable", scope.AttachmentID)
	}
	attemptedAt := time.Now().UTC()
	attachment.discoveryMu.Lock()
	defer attachment.discoveryMu.Unlock()
	attachment.useMu.RLock()
	defer attachment.useMu.RUnlock()
	m.mu.Lock()
	closed = m.closed
	m.mu.Unlock()
	if closed {
		return fmt.Errorf("BACnet discovery manager is closed")
	}

	observations, discoverErr := attachment.client.Discover(ctx, scope.Discovery)
	if ctx.Err() != nil {
		return ctx.Err()
	}
	observations = append(observations, staticObservations(scope.StaticDevices)...)
	observations = normalizeObservations(observations, scope.Discovery)
	unique, duplicates := splitDuplicates(observations)

	issues := make([]Issue, 0)
	if discoverErr != nil {
		issues = append(issues, Issue{Code: IssueDiscoveryFailed, Message: discoverErr.Error()})
	}
	for _, duplicate := range duplicates {
		issues = append(issues, Issue{
			Code: IssueDuplicateInstance,
			Message: fmt.Sprintf(
				"device instance %d was observed on %d routes",
				duplicate.DeviceInstance,
				len(duplicate.Routes),
			),
		})
	}
	devices := make([]Device, 0, len(unique))
	for _, observation := range unique {
		device := Device{
			Namespace:    scope.Namespace,
			ScopeID:      scope.ID,
			AttachmentID: scope.AttachmentID,
			Instance:     observation.DeviceInstance,
			Route:        cloneRoute(observation.Route),
			LastSeen:     observation.LastSeen,
			Static:       observation.Static,
		}
		identity, identityIssues, identityErr := attachment.client.Identify(ctx, observation)
		if ctx.Err() != nil {
			return ctx.Err()
		}
		if identity.VendorIdentifier == 0 && observation.VendorKnown {
			identity.VendorIdentifier = observation.VendorIdentifier
		}
		device.Identity = identity
		device.Issues = append(device.Issues, identityIssues...)
		if identityErr != nil {
			device.Issues = append(device.Issues, Issue{
				Code:    IssueIdentityReadFailed,
				Message: identityErr.Error(),
			})
		} else if device.LastSeen.IsZero() {
			device.LastSeen = attemptedAt
		}
		selected, selectErr := m.catalog.Select(profile.DeviceIdentity{
			VendorIdentifier: identity.VendorIdentifier,
			ModelName:        identity.ModelName,
			FirmwareRevision: identity.FirmwareRevision,
			DeviceInstance:   observation.DeviceInstance,
			Namespace:        scope.Namespace,
			ScopeID:          scope.ID,
		})
		if selectErr != nil {
			device.Issues = append(device.Issues, Issue{
				Code:    IssueProfileSelection,
				Message: selectErr.Error(),
			})
		} else {
			device.ProfileID = selected.ID
		}
		devices = append(devices, device)
	}
	slices.SortFunc(devices, compareDevices)
	success := discoverErr == nil && len(duplicates) == 0
	m.registry.replace(
		scope,
		attemptedAt,
		attachment.observationTTL,
		devices,
		duplicates,
		issues,
		success,
	)
	if discoverErr != nil || len(duplicates) != 0 {
		duplicateErr := error(nil)
		if len(duplicates) != 0 {
			duplicateErr = fmt.Errorf("%d duplicate Device Instance conflicts", len(duplicates))
		}
		return errors.Join(discoverErr, duplicateErr)
	}
	return nil
}

// Snapshot returns a detached point-in-time device registry.
func (m *Manager) Snapshot() Snapshot {
	result := m.registry.snapshot()
	identifiers := make([]string, 0, len(m.attachments))
	for identifier := range m.attachments {
		identifiers = append(identifiers, identifier)
	}
	slices.Sort(identifiers)
	result.Attachments = make([]AttachmentSnapshot, 0, len(identifiers))
	for _, identifier := range identifiers {
		attachment := m.attachments[identifier]
		attachment.useMu.RLock()
		status := attachment.client.Status()
		attachment.useMu.RUnlock()
		status.ID = identifier
		result.Attachments = append(result.Attachments, status)
	}
	return result
}

// ReadObjectList reads one registered device through the attachment that owns
// its route. The query keeps using the detached Device selected from a single
// registry snapshot.
func (m *Manager) ReadObjectList(
	ctx context.Context,
	device Device,
	maxItems int,
) ([]bacnet.ObjectIdentifier, error) {
	attachment, release, err := m.acquireAttachment(device.AttachmentID)
	if err != nil {
		return nil, err
	}
	defer release()
	return attachment.client.ReadObjectList(ctx, device.Route, device.Instance, maxItems)
}

// ReadProperties reads properties from one registered device without exposing
// attachment ownership to the query executor.
func (m *Manager) ReadProperties(
	ctx context.Context,
	device Device,
	specifications []service.ReadAccessSpecification,
) ([]service.ReadAccessResult, error) {
	attachment, release, err := m.acquireAttachment(device.AttachmentID)
	if err != nil {
		return nil, err
	}
	defer release()
	return attachment.client.ReadProperties(ctx, device.Route, specifications)
}

// MutationPolicy returns the immutable mutation policy for one logical scope.
func (m *Manager) MutationPolicy(
	namespace string,
	scopeID string,
) (config.MutationPolicy, bool) {
	scope, exists := m.scopes[strings.TrimSpace(namespace)+"/"+strings.TrimSpace(scopeID)]
	if !exists {
		return config.MutationPolicy{}, false
	}
	return scope.Mutations, true
}

// WriteProperty sends one confirmed write through the attachment that owns
// the registered device route.
func (m *Manager) WriteProperty(
	ctx context.Context,
	device Device,
	object bacnet.ObjectIdentifier,
	property bacnet.PropertyReference,
	value bacnet.ApplicationValue,
	priority *uint8,
) error {
	attachment, release, err := m.acquireAttachment(device.AttachmentID)
	if err != nil {
		return err
	}
	defer release()
	return attachment.client.WriteProperty(
		ctx,
		device.Route,
		object,
		property,
		value,
		priority,
	)
}

func (m *Manager) acquireAttachment(
	identifier string,
) (*attachmentState, func(), error) {
	attachment := m.attachments[identifier]
	if attachment == nil {
		return nil, nil, fmt.Errorf("BACnet attachment %q is unavailable", identifier)
	}
	attachment.useMu.RLock()
	m.mu.Lock()
	closed := m.closed
	m.mu.Unlock()
	if closed {
		attachment.useMu.RUnlock()
		return nil, nil, fmt.Errorf("BACnet discovery manager is closed")
	}
	return attachment, attachment.useMu.RUnlock, nil
}

// Close stops refresh loops and then closes every physical attachment.
func (m *Manager) Close() error {
	m.mu.Lock()
	if m.closed {
		m.mu.Unlock()
		return nil
	}
	m.closed = true
	cancel := m.cancel
	m.mu.Unlock()
	if cancel != nil {
		cancel()
	}
	m.wg.Wait()
	return m.closeClients()
}

func (m *Manager) closeClients() error {
	identifiers := make([]string, 0, len(m.attachments))
	for identifier := range m.attachments {
		identifiers = append(identifiers, identifier)
	}
	slices.Sort(identifiers)
	var result error
	for _, identifier := range identifiers {
		attachment := m.attachments[identifier]
		attachment.discoveryMu.Lock()
		attachment.useMu.Lock()
		result = errors.Join(result, attachment.client.Close())
		attachment.useMu.Unlock()
		attachment.discoveryMu.Unlock()
	}
	return result
}

func staticObservations(devices []config.StaticDevice) []Observation {
	result := make([]Observation, 0, len(devices))
	for _, device := range devices {
		route := Route{Endpoint: device.Endpoint, MAC: append([]byte(nil), device.MAC...)}
		if device.RemoteNetwork != nil {
			route.RemoteNetwork = *device.RemoteNetwork
		}
		result = append(result, Observation{
			DeviceInstance: device.DeviceInstance,
			Route:          route,
			Static:         true,
		})
	}
	return result
}

func normalizeObservations(input []Observation, discovery config.Discovery) []Observation {
	byRoute := make(map[string]Observation, len(input))
	for _, observation := range input {
		if !deviceInBounds(
			observation.DeviceInstance,
			discovery.LowDeviceInstance,
			discovery.HighDeviceInstance,
		) || !observation.Route.Endpoint.IsValid() {
			continue
		}
		key := fmt.Sprintf("%d|%s", observation.DeviceInstance, observation.Route.Canonical())
		existing, exists := byRoute[key]
		if !exists || observation.LastSeen.After(existing.LastSeen) || observation.Static {
			observation.Route = cloneRoute(observation.Route)
			byRoute[key] = observation
		}
	}
	result := make([]Observation, 0, len(byRoute))
	for _, observation := range byRoute {
		result = append(result, observation)
	}
	slices.SortFunc(result, func(left Observation, right Observation) int {
		if left.DeviceInstance < right.DeviceInstance {
			return -1
		}
		if left.DeviceInstance > right.DeviceInstance {
			return 1
		}
		return strings.Compare(left.Route.Canonical(), right.Route.Canonical())
	})
	return result
}

func splitDuplicates(input []Observation) ([]Observation, []Duplicate) {
	byInstance := make(map[uint32][]Observation)
	for _, observation := range input {
		byInstance[observation.DeviceInstance] = append(
			byInstance[observation.DeviceInstance],
			observation,
		)
	}
	unique := make([]Observation, 0, len(input))
	duplicates := make([]Duplicate, 0)
	for instance, observations := range byInstance {
		if len(observations) == 1 {
			unique = append(unique, observations[0])
			continue
		}
		routes := make([]Route, 0, len(observations))
		for _, observation := range observations {
			routes = append(routes, cloneRoute(observation.Route))
		}
		slices.SortFunc(routes, func(left Route, right Route) int {
			return strings.Compare(left.Canonical(), right.Canonical())
		})
		duplicates = append(duplicates, Duplicate{DeviceInstance: instance, Routes: routes})
	}
	slices.SortFunc(unique, func(left Observation, right Observation) int {
		if left.DeviceInstance < right.DeviceInstance {
			return -1
		}
		if left.DeviceInstance > right.DeviceInstance {
			return 1
		}
		return 0
	})
	slices.SortFunc(duplicates, func(left Duplicate, right Duplicate) int {
		if left.DeviceInstance < right.DeviceInstance {
			return -1
		}
		if left.DeviceInstance > right.DeviceInstance {
			return 1
		}
		return 0
	})
	return unique, duplicates
}

func deviceInBounds(instance uint32, low *uint32, high *uint32) bool {
	return (low == nil || instance >= *low) && (high == nil || instance <= *high)
}

func compareDevices(left Device, right Device) int {
	if left.Instance < right.Instance {
		return -1
	}
	if left.Instance > right.Instance {
		return 1
	}
	return strings.Compare(left.Route.Canonical(), right.Route.Canonical())
}

func cloneRoute(input Route) Route {
	input.MAC = append([]byte(nil), input.MAC...)
	return input
}

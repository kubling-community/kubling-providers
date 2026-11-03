package discovery

import (
	"context"
	"errors"
	"net/netip"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/kubling-community/kubling-providers/providers/bacnet/internal/config"
	"github.com/kubling-community/kubling-providers/providers/bacnet/internal/profile"
	bacnet "github.com/otfabric/go-bacnet"
	"github.com/otfabric/go-bacnet/service"
)

func TestRefreshScopeBuildsDetachedProfileSelectedSnapshot(t *testing.T) {
	catalog := scopedProfileCatalog(t)
	fake := &fakeClient{
		observations: []Observation{{
			DeviceInstance:   1201,
			Route:            Route{Endpoint: netip.MustParseAddrPort("192.0.2.21:47808")},
			LastSeen:         time.Now().UTC(),
			VendorIdentifier: 4242,
			VendorKnown:      true,
		}},
		identities: map[uint32]Identity{
			1201: {
				VendorIdentifier: 4242,
				ModelName:        "VAV-1000",
				FirmwareRevision: "2.0.1",
			},
		},
		status: AttachmentSnapshot{LocalEndpoint: netip.MustParseAddrPort("127.0.0.1:47808")},
	}
	manager := newTestManager(t, catalog, fake, time.Hour, []config.Scope{testScope("madrid.dc1", "cooling")})
	t.Cleanup(func() { _ = manager.Close() })

	if err := manager.RefreshScope(context.Background(), "madrid.dc1", "cooling"); err != nil {
		t.Fatalf("RefreshScope() error = %v", err)
	}
	snapshot := manager.Snapshot()
	if snapshot.Generation != 1 || len(snapshot.Attachments) != 1 || len(snapshot.Scopes) != 1 {
		t.Fatalf("Snapshot() = %#v", snapshot)
	}
	if snapshot.Attachments[0].ID != "shared" {
		t.Fatalf("attachment = %#v", snapshot.Attachments[0])
	}
	scope := snapshot.Scopes[0]
	if len(scope.Devices) != 1 ||
		scope.Devices[0].ProfileID != "scoped-vav" ||
		scope.Devices[0].Identity.ModelName != "VAV-1000" ||
		scope.LastSuccess.IsZero() {
		t.Fatalf("scope = %#v", scope)
	}
	scope.Devices[0].Route.MAC = []byte{0xff}
	scope.Devices[0].Issues = append(scope.Devices[0].Issues, Issue{Code: "MUTATED"})
	fresh := manager.Snapshot().Scopes[0].Devices[0]
	if len(fresh.Route.MAC) != 0 || len(fresh.Issues) != 0 {
		t.Fatal("Snapshot() exposes mutable registry state")
	}
}

func TestRefreshScopeRejectsDuplicateDeviceInstances(t *testing.T) {
	fake := &fakeClient{observations: []Observation{
		{
			DeviceInstance: 7,
			Route:          Route{Endpoint: netip.MustParseAddrPort("192.0.2.7:47808")},
			LastSeen:       time.Now().UTC(),
		},
		{
			DeviceInstance: 7,
			Route:          Route{Endpoint: netip.MustParseAddrPort("192.0.2.8:47808")},
			LastSeen:       time.Now().UTC(),
		},
	}}
	manager := newTestManager(t, standardCatalog(t), fake, time.Hour, []config.Scope{testScope("test", "local")})
	t.Cleanup(func() { _ = manager.Close() })

	err := manager.RefreshScope(context.Background(), "test", "local")
	if err == nil || !strings.Contains(err.Error(), "duplicate Device Instance") {
		t.Fatalf("RefreshScope() error = %v", err)
	}
	scope := manager.Snapshot().Scopes[0]
	if len(scope.Devices) != 0 || len(scope.Duplicates) != 1 || scope.Duplicates[0].DeviceInstance != 7 {
		t.Fatalf("scope = %#v", scope)
	}
	if len(scope.Issues) != 1 || scope.Issues[0].Code != IssueDuplicateInstance {
		t.Fatalf("issues = %#v", scope.Issues)
	}
}

func TestRefreshScopeRetainsMissingDevicesUntilTTL(t *testing.T) {
	fake := &fakeClient{
		observations: []Observation{{
			DeviceInstance: 9,
			Route:          Route{Endpoint: netip.MustParseAddrPort("192.0.2.9:47808")},
			LastSeen:       time.Now().Add(-time.Second),
		}},
		identities: map[uint32]Identity{9: {}},
	}
	manager := newTestManager(t, standardCatalog(t), fake, time.Nanosecond, []config.Scope{testScope("test", "local")})
	t.Cleanup(func() { _ = manager.Close() })
	if err := manager.RefreshScope(context.Background(), "test", "local"); err != nil {
		t.Fatalf("first RefreshScope() error = %v", err)
	}
	fake.setObservations(nil)
	if err := manager.RefreshScope(context.Background(), "test", "local"); err != nil {
		t.Fatalf("second RefreshScope() error = %v", err)
	}
	if devices := manager.Snapshot().Scopes[0].Devices; len(devices) != 0 {
		t.Fatalf("expired devices = %#v", devices)
	}
}

func TestRefreshScopeRetainsRecentMissingDevices(t *testing.T) {
	fake := &fakeClient{
		observations: []Observation{{
			DeviceInstance: 9,
			Route:          Route{Endpoint: netip.MustParseAddrPort("192.0.2.9:47808")},
			LastSeen:       time.Now().UTC(),
		}},
		identities: map[uint32]Identity{9: {ModelName: "Controller"}},
	}
	manager := newTestManager(t, standardCatalog(t), fake, time.Hour, []config.Scope{testScope("test", "local")})
	t.Cleanup(func() { _ = manager.Close() })
	if err := manager.RefreshScope(context.Background(), "test", "local"); err != nil {
		t.Fatalf("first RefreshScope() error = %v", err)
	}
	fake.setObservations(nil)
	if err := manager.RefreshScope(context.Background(), "test", "local"); err != nil {
		t.Fatalf("second RefreshScope() error = %v", err)
	}
	devices := manager.Snapshot().Scopes[0].Devices
	if len(devices) != 1 || devices[0].Identity.ModelName != "Controller" {
		t.Fatalf("retained devices = %#v", devices)
	}
}

func TestRefreshScopeMergesPartialIdentityWithPreviousSnapshot(t *testing.T) {
	observation := Observation{
		DeviceInstance: 9,
		Route:          Route{Endpoint: netip.MustParseAddrPort("192.0.2.9:47808")},
		LastSeen:       time.Now().UTC(),
	}
	fake := &fakeClient{
		observations: []Observation{observation},
		identities: map[uint32]Identity{9: {
			VendorIdentifier: 77,
			ModelName:        "Controller",
			FirmwareRevision: "1.2.3",
		}},
	}
	manager := newTestManager(t, standardCatalog(t), fake, time.Hour, []config.Scope{testScope("test", "local")})
	t.Cleanup(func() { _ = manager.Close() })
	if err := manager.RefreshScope(context.Background(), "test", "local"); err != nil {
		t.Fatalf("first RefreshScope() error = %v", err)
	}
	fake.setIdentityResult(
		Identity{VendorIdentifier: 77},
		[]Issue{{Code: IssuePropertyUnavailable, PropertyID: 70, Message: "model unavailable"}},
		nil,
	)
	if err := manager.RefreshScope(context.Background(), "test", "local"); err != nil {
		t.Fatalf("second RefreshScope() error = %v", err)
	}
	device := manager.Snapshot().Scopes[0].Devices[0]
	if device.Identity.ModelName != "Controller" ||
		device.Identity.FirmwareRevision != "1.2.3" ||
		device.ProfileID == "" ||
		len(device.Issues) != 1 {
		t.Fatalf("merged device = %#v", device)
	}
}

func TestRefreshScopeReplacesPreviousRouteForSameInstance(t *testing.T) {
	fake := &fakeClient{
		observations: []Observation{{
			DeviceInstance: 9,
			Route:          Route{Endpoint: netip.MustParseAddrPort("192.0.2.9:47808")},
			LastSeen:       time.Now().UTC(),
		}},
		identities: map[uint32]Identity{9: {ModelName: "Controller"}},
	}
	manager := newTestManager(t, standardCatalog(t), fake, time.Hour, []config.Scope{testScope("test", "local")})
	t.Cleanup(func() { _ = manager.Close() })
	if err := manager.RefreshScope(context.Background(), "test", "local"); err != nil {
		t.Fatalf("first RefreshScope() error = %v", err)
	}
	replacement := netip.MustParseAddrPort("192.0.2.10:47808")
	fake.setObservations([]Observation{{
		DeviceInstance: 9,
		Route:          Route{Endpoint: replacement},
		LastSeen:       time.Now().UTC(),
	}})
	if err := manager.RefreshScope(context.Background(), "test", "local"); err != nil {
		t.Fatalf("second RefreshScope() error = %v", err)
	}
	devices := manager.Snapshot().Scopes[0].Devices
	if len(devices) != 1 || devices[0].Route.Endpoint != replacement {
		t.Fatalf("devices after route replacement = %#v", devices)
	}
}

func TestScopesSharingAnAttachmentSerializeDiscovery(t *testing.T) {
	fake := &fakeClient{
		entered: make(chan struct{}, 2),
		release: make(chan struct{}),
	}
	manager := newTestManager(
		t,
		standardCatalog(t),
		fake,
		time.Hour,
		[]config.Scope{testScope("test", "left"), testScope("test", "right")},
	)
	t.Cleanup(func() { _ = manager.Close() })

	done := make(chan error, 2)
	go func() { done <- manager.RefreshScope(context.Background(), "test", "left") }()
	select {
	case <-fake.entered:
	case <-time.After(time.Second):
		t.Fatal("first discovery did not start")
	}
	go func() { done <- manager.RefreshScope(context.Background(), "test", "right") }()
	select {
	case <-fake.entered:
		t.Fatal("shared attachment allowed overlapping discovery windows")
	case <-time.After(30 * time.Millisecond):
	}
	fake.release <- struct{}{}
	select {
	case <-fake.entered:
	case <-time.After(time.Second):
		t.Fatal("second discovery did not start after first completed")
	}
	fake.release <- struct{}{}
	for range 2 {
		if err := <-done; err != nil {
			t.Fatalf("RefreshScope() error = %v", err)
		}
	}
	if fake.maximumConcurrent() != 1 {
		t.Fatalf("maximum concurrent discoveries = %d", fake.maximumConcurrent())
	}
}

func TestNewClosesEarlierClientsWhenAFactoryFails(t *testing.T) {
	first := &fakeClient{}
	calls := 0
	_, err := New(
		[]config.Attachment{{ID: "first"}, {ID: "second"}},
		nil,
		standardCatalog(t),
		func(config.Attachment) (Client, error) {
			calls++
			if calls == 1 {
				return first, nil
			}
			return nil, errors.New("bind failed")
		},
	)
	if err == nil || first.closeCount() != 1 {
		t.Fatalf("New() = %v, close count = %d", err, first.closeCount())
	}
}

func TestStartAndCloseCancelActiveRefresh(t *testing.T) {
	fake := &fakeClient{
		entered: make(chan struct{}, 1),
		release: make(chan struct{}),
	}
	manager := newTestManager(
		t,
		standardCatalog(t),
		fake,
		time.Hour,
		[]config.Scope{testScope("test", "local")},
	)
	if err := manager.Start(context.Background()); err != nil {
		t.Fatalf("Start() error = %v", err)
	}
	select {
	case <-fake.entered:
	case <-time.After(time.Second):
		t.Fatal("periodic discovery did not start")
	}
	closed := make(chan error, 1)
	go func() { closed <- manager.Close() }()
	select {
	case err := <-closed:
		if err != nil {
			t.Fatalf("Close() error = %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("Close() did not cancel active discovery")
	}
	if fake.closeCount() != 1 {
		t.Fatalf("close count = %d", fake.closeCount())
	}
}

func TestStartRejectsNilContext(t *testing.T) {
	manager := newTestManager(t, standardCatalog(t), &fakeClient{}, time.Hour, nil)
	t.Cleanup(func() { _ = manager.Close() })
	if err := manager.Start(nil); err == nil {
		t.Fatal("Start(nil) error = nil")
	}
}

func TestManagerRoutesPhysicalReadsThroughOwningAttachment(t *testing.T) {
	object := bacnet.ObjectIdentifier{Type: bacnet.ObjectTypeAnalogValue, Instance: 4}
	fake := &fakeClient{
		readObjects: []bacnet.ObjectIdentifier{object},
		readResults: []service.ReadAccessResult{{Object: object}},
	}
	manager := newTestManager(t, standardCatalog(t), fake, time.Hour, nil)
	t.Cleanup(func() { _ = manager.Close() })
	device := Device{
		AttachmentID: "shared",
		Instance:     1201,
		Route:        Route{Endpoint: netip.MustParseAddrPort("192.0.2.21:47808")},
	}
	objects, err := manager.ReadObjectList(context.Background(), device, 128)
	if err != nil || len(objects) != 1 || objects[0] != object {
		t.Fatalf("ReadObjectList() = %v, %v", objects, err)
	}
	results, err := manager.ReadProperties(
		context.Background(),
		device,
		[]service.ReadAccessSpecification{{Object: object, Properties: []bacnet.PropertyReference{{Identifier: bacnet.PropertyObjectName}}}},
	)
	if err != nil || len(results) != 1 || results[0].Object != object {
		t.Fatalf("ReadProperties() = %v, %v", results, err)
	}
	priority := uint8(16)
	writeValue := bacnet.RealValue(21.5)
	if err := manager.WriteProperty(
		context.Background(),
		device,
		object,
		bacnet.PropertyReference{Identifier: bacnet.PropertyPresentValue},
		writeValue,
		&priority,
	); err != nil {
		t.Fatalf("WriteProperty() error = %v", err)
	}
	fake.mu.Lock()
	defer fake.mu.Unlock()
	if fake.readRoute.Canonical() != device.Route.Canonical() ||
		fake.readRoute.MaxAPDU != device.Route.MaxAPDU ||
		fake.readInstance != device.Instance || fake.readLimit != 128 {
		t.Fatalf("routed read = route %#v instance %d limit %d", fake.readRoute, fake.readInstance, fake.readLimit)
	}
	if fake.writeObject != object ||
		fake.writeProperty.Identifier != bacnet.PropertyPresentValue ||
		fake.writeValue.Kind != bacnet.ValueReal || fake.writeValue.Real != 21.5 ||
		fake.writePriority == nil || *fake.writePriority != priority {
		t.Fatalf(
			"routed write = object %#v property %#v value %#v priority %v",
			fake.writeObject,
			fake.writeProperty,
			fake.writeValue,
			fake.writePriority,
		)
	}
}

func TestManagerReturnsScopeMutationPolicy(t *testing.T) {
	scope := testScope("test", "local")
	scope.Mutations = config.MutationPolicy{Enabled: true, Priority: 12}
	manager := newTestManager(t, standardCatalog(t), &fakeClient{}, time.Hour, []config.Scope{scope})
	t.Cleanup(func() { _ = manager.Close() })

	policy, exists := manager.MutationPolicy("test", "local")
	if !exists || !policy.Enabled || policy.Priority != 12 {
		t.Fatalf("MutationPolicy() = %#v, %v", policy, exists)
	}
	if _, exists := manager.MutationPolicy("test", "missing"); exists {
		t.Fatal("MutationPolicy() found an unknown scope")
	}
}

type fakeClient struct {
	mu             sync.Mutex
	observations   []Observation
	identities     map[uint32]Identity
	discoverErr    error
	identityErr    error
	identityIssues []Issue
	status         AttachmentSnapshot
	closed         int
	active         int
	maximum        int
	entered        chan struct{}
	release        chan struct{}
	readObjects    []bacnet.ObjectIdentifier
	readResults    []service.ReadAccessResult
	readRoute      Route
	readInstance   uint32
	readLimit      int
	writeObject    bacnet.ObjectIdentifier
	writeProperty  bacnet.PropertyReference
	writeValue     bacnet.ApplicationValue
	writePriority  *uint8
}

func (f *fakeClient) Discover(ctx context.Context, _ config.Discovery) ([]Observation, error) {
	f.mu.Lock()
	f.active++
	if f.active > f.maximum {
		f.maximum = f.active
	}
	entered := f.entered
	release := f.release
	observations := append([]Observation(nil), f.observations...)
	err := f.discoverErr
	f.mu.Unlock()
	defer func() {
		f.mu.Lock()
		f.active--
		f.mu.Unlock()
	}()
	if entered != nil {
		entered <- struct{}{}
	}
	if release != nil {
		select {
		case <-release:
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
	return observations, err
}

func (f *fakeClient) Identify(_ context.Context, observation Observation) (Identity, []Issue, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.identities[observation.DeviceInstance], append([]Issue(nil), f.identityIssues...), f.identityErr
}

func (f *fakeClient) ReadObjectList(
	_ context.Context,
	route Route,
	instance uint32,
	limit int,
) ([]bacnet.ObjectIdentifier, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.readRoute = route
	f.readInstance = instance
	f.readLimit = limit
	return append([]bacnet.ObjectIdentifier(nil), f.readObjects...), nil
}

func (f *fakeClient) ReadProperties(
	_ context.Context,
	route Route,
	_ []service.ReadAccessSpecification,
) ([]service.ReadAccessResult, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.readRoute = route
	return append([]service.ReadAccessResult(nil), f.readResults...), nil
}

func (f *fakeClient) WriteProperty(
	_ context.Context,
	route Route,
	object bacnet.ObjectIdentifier,
	property bacnet.PropertyReference,
	value bacnet.ApplicationValue,
	priority *uint8,
) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.readRoute = route
	f.writeObject = object
	f.writeProperty = property
	f.writeValue = value.Clone()
	if priority != nil {
		copied := *priority
		f.writePriority = &copied
	} else {
		f.writePriority = nil
	}
	return nil
}

func (f *fakeClient) Status() AttachmentSnapshot {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.status
}

func (f *fakeClient) Close() error {
	f.mu.Lock()
	f.closed++
	f.mu.Unlock()
	return nil
}

func (f *fakeClient) setObservations(observations []Observation) {
	f.mu.Lock()
	f.observations = append([]Observation(nil), observations...)
	f.mu.Unlock()
}

func (f *fakeClient) setIdentityResult(identity Identity, issues []Issue, err error) {
	f.mu.Lock()
	if f.identities == nil {
		f.identities = make(map[uint32]Identity)
	}
	for instance := range f.identities {
		f.identities[instance] = identity
	}
	f.identityIssues = append([]Issue(nil), issues...)
	f.identityErr = err
	f.mu.Unlock()
}

func (f *fakeClient) maximumConcurrent() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.maximum
}

func (f *fakeClient) closeCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.closed
}

func newTestManager(
	t *testing.T,
	catalog *profile.Catalog,
	fake Client,
	ttl time.Duration,
	scopes []config.Scope,
) *Manager {
	t.Helper()
	manager, err := New(
		[]config.Attachment{{ID: "shared", ObservationTTL: ttl}},
		scopes,
		catalog,
		func(config.Attachment) (Client, error) { return fake, nil },
	)
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	return manager
}

func testScope(namespace string, id string) config.Scope {
	return config.Scope{
		Namespace:    namespace,
		ID:           id,
		AttachmentID: "shared",
		Discovery: config.Discovery{
			Broadcast: true,
			Interval:  time.Hour,
			Window:    time.Millisecond,
		},
	}
}

func standardCatalog(t *testing.T) *profile.Catalog {
	t.Helper()
	catalog, err := profile.Load(nil)
	if err != nil {
		t.Fatalf("profile.Load() error = %v", err)
	}
	return catalog
}

func scopedProfileCatalog(t *testing.T) *profile.Catalog {
	t.Helper()
	path := filepath.Join(t.TempDir(), "scoped.yaml")
	if err := os.WriteFile(path, []byte(`
apiVersion: kubling.io/bacnet-profile/v1
id: scoped-vav
extends: [bacnet-standard]
selectors:
  - namespace: madrid.dc1
    scopeId: cooling
    deviceInstance: 1201
`), 0o600); err != nil {
		t.Fatalf("os.WriteFile() error = %v", err)
	}
	catalog, err := profile.Load([]string{path})
	if err != nil {
		t.Fatalf("profile.Load() error = %v", err)
	}
	return catalog
}

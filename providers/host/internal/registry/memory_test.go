package registry

import (
	"context"
	"errors"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"github.com/kubling-community/kubling-providers/providers/host/internal/model"
)

func TestMemoryOpenSessionReplacesOnlyTheSameHost(t *testing.T) {
	registry := newMemory(sequentialSessionIDs())
	startedAt := time.Date(2026, time.October, 8, 8, 0, 0, 0, time.UTC)
	registration := testRegistration("fleet-a", "agent-a")

	first, err := registry.OpenSession(
		context.Background(),
		registration,
		testWindow(startedAt),
	)
	if err != nil {
		t.Fatalf("OpenSession(first) error = %v", err)
	}
	registration.Hostname = "renamed-host"
	registration.Attributes["zone"] = "changed"
	reconnectedAt := startedAt.Add(time.Minute)
	second, err := registry.OpenSession(
		context.Background(),
		registration,
		testWindow(reconnectedAt),
	)
	if err != nil {
		t.Fatalf("OpenSession(second) error = %v", err)
	}

	if first.Session.Generation != 1 || second.Session.Generation != 2 {
		t.Fatalf(
			"session generations = %d, %d; want 1, 2",
			first.Session.Generation,
			second.Session.Generation,
		)
	}
	if !second.Identity.EnrolledAt.Equal(startedAt) {
		t.Fatalf("second enrollment = %v, want %v", second.Identity.EnrolledAt, startedAt)
	}
	resolvedFirst, err := registry.ResolveSession(context.Background(), first.Session.ID)
	if err != nil {
		t.Fatalf("ResolveSession(first) error = %v", err)
	}
	if !resolvedFirst.Session.DisconnectedAt.Equal(reconnectedAt) {
		t.Fatalf(
			"first disconnected at = %v, want %v",
			resolvedFirst.Session.DisconnectedAt,
			reconnectedAt,
		)
	}

	active, err := registry.SnapshotActive(
		context.Background(),
		"fleet-a",
		reconnectedAt,
	)
	if err != nil {
		t.Fatalf("SnapshotActive() error = %v", err)
	}
	if len(active) != 1 || active[0].Session.ID != second.Session.ID {
		t.Fatalf("active snapshots = %v, want only second session", active)
	}

	listed, err := registry.List(context.Background(), "fleet-a")
	if err != nil {
		t.Fatalf("List() error = %v", err)
	}
	if len(listed) != 1 || listed[0].Identity.Hostname != "renamed-host" {
		t.Fatalf("listed hosts = %v", listed)
	}
	if got := listed[0].Identity.Attributes["zone"]; got != "changed" {
		t.Fatalf("listed zone = %q, want changed", got)
	}
}

func TestMemoryRenewSessionAndExpiry(t *testing.T) {
	registry := newMemory(sequentialSessionIDs())
	startedAt := time.Date(2026, time.October, 8, 8, 0, 0, 0, time.UTC)
	snapshot, err := registry.OpenSession(
		context.Background(),
		testRegistration("fleet-a", "agent-a"),
		testWindow(startedAt),
	)
	if err != nil {
		t.Fatalf("OpenSession() error = %v", err)
	}

	renewedAt := startedAt.Add(5 * time.Second)
	renewed, err := registry.RenewSession(
		context.Background(),
		snapshot.Session.ID,
		testWindow(renewedAt),
	)
	if err != nil {
		t.Fatalf("RenewSession() error = %v", err)
	}
	if !renewed.Session.LastSeenAt.Equal(renewedAt) {
		t.Fatalf("renewed last seen = %v, want %v", renewed.Session.LastSeenAt, renewedAt)
	}

	active, err := registry.SnapshotActive(
		context.Background(),
		"fleet-a",
		renewed.Session.ExpiresAt.Add(-time.Nanosecond),
	)
	if err != nil || len(active) != 1 {
		t.Fatalf("SnapshotActive(before expiry) = %v, %v; want one host", active, err)
	}
	active, err = registry.SnapshotActive(
		context.Background(),
		"fleet-a",
		renewed.Session.ExpiresAt,
	)
	if err != nil || len(active) != 0 {
		t.Fatalf("SnapshotActive(at expiry) = %v, %v; want no hosts", active, err)
	}

	_, err = registry.RenewSession(
		context.Background(),
		snapshot.Session.ID,
		testWindow(renewed.Session.ExpiresAt),
	)
	if !errors.Is(err, ErrSessionExpired) {
		t.Fatalf("RenewSession(expired) error = %v, want ErrSessionExpired", err)
	}
}

func TestMemorySnapshotsAreSortedFilteredAndDetached(t *testing.T) {
	registry := newMemory(sequentialSessionIDs())
	startedAt := time.Date(2026, time.October, 8, 8, 0, 0, 0, time.UTC)
	for _, key := range []model.HostKey{
		{Namespace: "fleet-b", ID: "agent-a"},
		{Namespace: "fleet-a", ID: "agent-b"},
		{Namespace: "fleet-a", ID: "agent-a"},
	} {
		if _, err := registry.OpenSession(
			context.Background(),
			testRegistration(key.Namespace, key.ID),
			testWindow(startedAt),
		); err != nil {
			t.Fatalf("OpenSession(%v) error = %v", key, err)
		}
	}

	listed, err := registry.List(context.Background(), "fleet-a")
	if err != nil {
		t.Fatalf("List() error = %v", err)
	}
	if len(listed) != 2 || listed[0].Identity.Key.ID != "agent-a" ||
		listed[1].Identity.Key.ID != "agent-b" {
		t.Fatalf("List() order = %v", listed)
	}
	listed[0].Identity.Attributes["zone"] = "mutated"
	listed[0].Session.ID = "mutated"

	again, err := registry.List(context.Background(), "fleet-a")
	if err != nil {
		t.Fatalf("List(second) error = %v", err)
	}
	if again[0].Identity.Attributes["zone"] != "test" ||
		again[0].Session.ID == "mutated" {
		t.Fatalf("List() returned aliased registry state: %v", again[0])
	}
}

func testRegistration(namespace string, agentID string) Registration {
	return Registration{
		Namespace:          namespace,
		AgentIdentifier:    agentID,
		Hostname:           agentID + ".example",
		AgentVersion:       "test",
		ProtocolVersion:    model.ProtocolVersion{Major: 1},
		SchemaVersion:      "host-v1",
		MaxConcurrentScans: 1,
		Attributes:         map[string]string{"zone": "test"},
	}
}

func testWindow(observedAt time.Time) SessionWindow {
	return SessionWindow{
		ObservedAt: observedAt,
		ExpiresAt:  observedAt.Add(30 * time.Second),
	}
}

func sequentialSessionIDs() sessionIDSource {
	var sequence atomic.Uint64
	return func() (string, error) {
		return fmt.Sprintf("session-%d", sequence.Add(1)), nil
	}
}

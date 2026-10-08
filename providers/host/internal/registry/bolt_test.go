package registry

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/kubling-community/kubling-providers/providers/host/internal/hostschema"
	"github.com/kubling-community/kubling-providers/providers/host/internal/model"
)

func TestBoltRegistryPersistsIdentityAndGenerationAcrossRestart(t *testing.T) {
	stateDirectory := t.TempDir()
	connectedAt := time.Date(2026, time.October, 8, 12, 0, 0, 0, time.UTC)
	registration := boltTestRegistration()

	first, err := openBolt(
		stateDirectory,
		connectedAt.Add(-time.Second),
		func() (string, error) { return "session-1", nil },
	)
	if err != nil {
		t.Fatalf("openBolt(first) error = %v", err)
	}
	snapshot, err := first.OpenSession(
		context.Background(),
		registration,
		SessionWindow{ObservedAt: connectedAt, ExpiresAt: connectedAt.Add(time.Minute)},
	)
	if err != nil {
		t.Fatalf("OpenSession(first) error = %v", err)
	}
	if snapshot.Session.Generation != 1 {
		t.Fatalf("first generation = %d, want 1", snapshot.Session.Generation)
	}
	if err := first.Close(); err != nil {
		t.Fatalf("Close(first) error = %v", err)
	}

	restartedAt := connectedAt.Add(10 * time.Second)
	second, err := openBolt(
		stateDirectory,
		restartedAt,
		func() (string, error) { return "session-2", nil },
	)
	if err != nil {
		t.Fatalf("openBolt(second) error = %v", err)
	}
	t.Cleanup(func() { _ = second.Close() })
	active, err := second.SnapshotActive(context.Background(), "fleet-a", restartedAt)
	if err != nil {
		t.Fatalf("SnapshotActive(after restart) error = %v", err)
	}
	if len(active) != 0 {
		t.Fatalf("active snapshots after restart = %v, want none", active)
	}
	listed, err := second.List(context.Background(), "fleet-a")
	if err != nil {
		t.Fatalf("List(after restart) error = %v", err)
	}
	if len(listed) != 1 || listed[0].Identity.EnrolledAt != connectedAt ||
		listed[0].Session == nil || !listed[0].Session.DisconnectedAt.Equal(restartedAt) {
		t.Fatalf("persisted host after restart = %v", listed)
	}

	registration.Hostname = "host-a-renamed"
	registration.Attributes = map[string]string{"rack": "b"}
	reconnected, err := second.OpenSession(
		context.Background(),
		registration,
		SessionWindow{
			ObservedAt: restartedAt.Add(time.Second),
			ExpiresAt:  restartedAt.Add(time.Minute),
		},
	)
	if err != nil {
		t.Fatalf("OpenSession(second) error = %v", err)
	}
	if reconnected.Session.Generation != 2 ||
		reconnected.Identity.EnrolledAt != connectedAt ||
		reconnected.Identity.Hostname != "host-a-renamed" ||
		reconnected.Identity.Attributes["rack"] != "b" {
		t.Fatalf("reconnected host = %v", reconnected)
	}
}

func TestBoltRegistryRenewsAndClosesTransactionally(t *testing.T) {
	at := time.Date(2026, time.October, 8, 12, 0, 0, 0, time.UTC)
	registry, err := openBolt(
		t.TempDir(),
		at.Add(-time.Second),
		func() (string, error) { return "session-1", nil },
	)
	if err != nil {
		t.Fatalf("openBolt() error = %v", err)
	}
	t.Cleanup(func() { _ = registry.Close() })
	snapshot, err := registry.OpenSession(
		context.Background(),
		boltTestRegistration(),
		SessionWindow{ObservedAt: at, ExpiresAt: at.Add(time.Minute)},
	)
	if err != nil {
		t.Fatalf("OpenSession() error = %v", err)
	}
	renewed, err := registry.RenewSession(
		context.Background(),
		snapshot.Session.ID,
		SessionWindow{
			ObservedAt: at.Add(10 * time.Second),
			ExpiresAt:  at.Add(70 * time.Second),
		},
	)
	if err != nil {
		t.Fatalf("RenewSession() error = %v", err)
	}
	if !renewed.Session.LastSeenAt.Equal(at.Add(10 * time.Second)) {
		t.Fatalf("renewed last seen = %v", renewed.Session.LastSeenAt)
	}
	if err := registry.CloseSession(
		context.Background(),
		snapshot.Session.ID,
		at.Add(20*time.Second),
	); err != nil {
		t.Fatalf("CloseSession() error = %v", err)
	}
	active, err := registry.SnapshotActive(
		context.Background(),
		"fleet-a",
		at.Add(21*time.Second),
	)
	if err != nil || len(active) != 0 {
		t.Fatalf("SnapshotActive(after close) = %v, %v", active, err)
	}
	if _, err := registry.RenewSession(
		context.Background(),
		snapshot.Session.ID,
		SessionWindow{
			ObservedAt: at.Add(30 * time.Second),
			ExpiresAt:  at.Add(90 * time.Second),
		},
	); !errors.Is(err, ErrSessionNotCurrent) {
		t.Fatalf("RenewSession(closed) error = %v, want ErrSessionNotCurrent", err)
	}
}

func TestBoltRegistryUsesPrivateDatabasePermissions(t *testing.T) {
	stateDirectory := t.TempDir()
	registry, err := OpenBolt(stateDirectory, time.Now().UTC())
	if err != nil {
		t.Fatalf("OpenBolt() error = %v", err)
	}
	if err := registry.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}
	info, err := os.Stat(filepath.Join(stateDirectory, registryDatabaseFilename))
	if err != nil {
		t.Fatalf("Stat(registry) error = %v", err)
	}
	if permissions := info.Mode().Perm(); permissions != 0o600 {
		t.Fatalf("registry permissions = %o, want 600", permissions)
	}
}

func boltTestRegistration() Registration {
	return Registration{
		Namespace:          "fleet-a",
		AgentIdentifier:    "agent-a",
		Hostname:           "host-a",
		AgentVersion:       "test",
		ProtocolVersion:    model.ProtocolVersion{Major: 1},
		SchemaVersion:      hostschema.Version,
		MaxConcurrentScans: 1,
		Attributes:         map[string]string{"rack": "a"},
	}
}

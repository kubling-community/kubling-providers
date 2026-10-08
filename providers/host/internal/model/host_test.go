package model

import (
	"testing"
	"time"
)

func TestAgentSessionActiveAtUsesExclusiveExpiry(t *testing.T) {
	connectedAt := time.Date(2026, time.October, 5, 9, 0, 0, 0, time.UTC)
	expiresAt := time.Date(2026, time.October, 5, 10, 0, 0, 0, time.UTC)
	session := AgentSession{ConnectedAt: connectedAt, ExpiresAt: expiresAt}

	if session.ActiveAt(connectedAt.Add(-time.Nanosecond)) {
		t.Fatal("ActiveAt() before connection = true, want false")
	}
	if !session.ActiveAt(connectedAt) {
		t.Fatal("ActiveAt() at connection = false, want true")
	}
	if !session.ActiveAt(expiresAt.Add(-time.Nanosecond)) {
		t.Fatal("ActiveAt() before expiry = false, want true")
	}
	if session.ActiveAt(expiresAt) {
		t.Fatal("ActiveAt() at expiry = true, want false")
	}
	if (AgentSession{}).ActiveAt(expiresAt) {
		t.Fatal("ActiveAt() with no expiry = true, want false")
	}
}

func TestAgentSessionActiveAtUsesStreamDisconnect(t *testing.T) {
	disconnectedAt := time.Date(2026, time.October, 5, 10, 0, 0, 0, time.UTC)
	session := AgentSession{
		ConnectedAt:    disconnectedAt.Add(-time.Minute),
		ExpiresAt:      disconnectedAt.Add(time.Minute),
		DisconnectedAt: disconnectedAt,
	}

	if !session.ActiveAt(disconnectedAt.Add(-time.Nanosecond)) {
		t.Fatal("ActiveAt() before disconnect = false, want true")
	}
	if session.ActiveAt(disconnectedAt) {
		t.Fatal("ActiveAt() at disconnect = true, want false")
	}
}

func TestHostSnapshotCloneDoesNotAliasMutableState(t *testing.T) {
	snapshot := HostSnapshot{
		Identity: HostIdentity{
			Attributes: map[string]string{"zone": "a"},
		},
		Session: &AgentSession{ID: "session-1"},
	}

	cloned := snapshot.Clone()
	cloned.Identity.Attributes["zone"] = "b"
	cloned.Session.ID = "session-2"

	if got := snapshot.Identity.Attributes["zone"]; got != "a" {
		t.Fatalf("source attribute = %q, want a", got)
	}
	if got := snapshot.Session.ID; got != "session-1" {
		t.Fatalf("source session ID = %q, want session-1", got)
	}
}

package registry

import (
	"context"
	"time"

	"github.com/kubling-community/kubling-providers/providers/host/internal/model"
)

// Registration is the protocol-neutral identity presented after bootstrap
// credentials have selected and authorized a namespace. Namespace is an
// authorization result and must not be trusted directly from an agent payload.
type Registration struct {
	Namespace          string
	AgentIdentifier    string
	Hostname           string
	AgentVersion       string
	ProtocolVersion    model.ProtocolVersion
	SchemaVersion      string
	MaxConcurrentScans uint32
	Attributes         map[string]string
}

// SessionWindow supplies the authoritative observation and lease-expiry times
// chosen by the Agent Gateway.
type SessionWindow struct {
	ObservedAt time.Time
	ExpiresAt  time.Time
}

// Registry owns durable host identity and renewable agent sessions. Transport
// streams and query delivery are deliberately outside this persistence
// boundary.
type Registry interface {
	// OpenSession creates or recovers a durable host, issues a fresh opaque
	// session and atomically supersedes any older session for the same HostKey.
	OpenSession(
		context.Context,
		Registration,
		SessionWindow,
	) (model.HostSnapshot, error)

	// RenewSession extends the lease of the matching current session.
	RenewSession(
		context.Context,
		string,
		SessionWindow,
	) (model.HostSnapshot, error)

	// CloseSession marks only the matching stream session as disconnected. A
	// late close from a replaced stream must not affect the current session.
	CloseSession(context.Context, string, time.Time) error

	// ResolveSession resolves an opaque session identifier without renewing it.
	ResolveSession(context.Context, string) (model.HostSnapshot, error)

	// SnapshotActive returns a fixed query roster containing at most one active
	// session per host, optionally restricted to one namespace.
	SnapshotActive(
		context.Context,
		string,
		time.Time,
	) ([]model.HostSnapshot, error)

	// List returns durable host identities and their latest session state,
	// optionally restricted to one namespace. It also includes offline hosts.
	List(context.Context, string) ([]model.HostSnapshot, error)
}

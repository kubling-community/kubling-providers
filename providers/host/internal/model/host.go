package model

import "time"

// HostKey is the durable identity of one host inside a fleet namespace.
type HostKey struct {
	Namespace string
	ID        string
}

// HostIdentity is durable agent identity, independent from any current lease.
type HostIdentity struct {
	Key        HostKey
	Hostname   string
	EnrolledAt time.Time
	Attributes map[string]string
}

// ProtocolVersion identifies the Agent Gateway protocol negotiated by a
// session. The protobuf package carries the major version; the explicit value
// lets the gateway reject peers that cannot satisfy the required revision.
type ProtocolVersion struct {
	Major uint32
	Minor uint32
}

// AgentSession is one authenticated, renewable connection from an agent.
// Generation increases for every replacement session of the same host so late
// messages from an older stream cannot be accepted as current work.
type AgentSession struct {
	ID                 string
	Host               HostKey
	Generation         uint64
	AgentVersion       string
	ProtocolVersion    ProtocolVersion
	SchemaVersion      string
	MaxConcurrentScans uint32
	ConnectedAt        time.Time
	LastSeenAt         time.Time
	ExpiresAt          time.Time
	DisconnectedAt     time.Time
}

// ActiveAt reports whether the session could receive work at the supplied
// instant. A closed stream becomes inactive immediately; lease expiry covers
// silent network partitions and crashed agents.
func (s AgentSession) ActiveAt(at time.Time) bool {
	if s.ConnectedAt.IsZero() || at.Before(s.ConnectedAt) ||
		s.ExpiresAt.IsZero() || !at.Before(s.ExpiresAt) {
		return false
	}
	return s.DisconnectedAt.IsZero() || at.Before(s.DisconnectedAt)
}

// HostSnapshot combines durable identity with its current session, if any.
type HostSnapshot struct {
	Identity HostIdentity
	Session  *AgentSession
}

// Clone returns a snapshot whose mutable fields do not alias registry state.
func (s HostSnapshot) Clone() HostSnapshot {
	cloned := s
	cloned.Identity.Attributes = cloneAttributes(s.Identity.Attributes)
	if s.Session != nil {
		session := *s.Session
		cloned.Session = &session
	}
	return cloned
}

func cloneAttributes(attributes map[string]string) map[string]string {
	if attributes == nil {
		return nil
	}
	cloned := make(map[string]string, len(attributes))
	for key, value := range attributes {
		cloned[key] = value
	}
	return cloned
}

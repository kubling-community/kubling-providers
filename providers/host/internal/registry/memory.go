package registry

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"math"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/kubling-community/kubling-providers/providers/host/internal/model"
)

var (
	ErrSessionNotFound   = errors.New("agent session not found")
	ErrSessionNotCurrent = errors.New("agent session is not current")
	ErrSessionExpired    = errors.New("agent session has expired")
)

const maxSessionIDAttempts = 8

type sessionIDSource func() (string, error)

// Memory is a concurrency-safe in-memory implementation of Registry.
type Memory struct {
	mu          sync.RWMutex
	identities  map[model.HostKey]model.HostIdentity
	sessions    map[string]model.AgentSession
	current     map[model.HostKey]string
	generations map[model.HostKey]uint64
	sessionIDs  sessionIDSource
}

// NewMemory creates an empty registry with cryptographically random opaque
// session identifiers.
func NewMemory() *Memory {
	return newMemory(randomSessionID)
}

func newMemory(sessionIDs sessionIDSource) *Memory {
	return &Memory{
		identities:  make(map[model.HostKey]model.HostIdentity),
		sessions:    make(map[string]model.AgentSession),
		current:     make(map[model.HostKey]string),
		generations: make(map[model.HostKey]uint64),
		sessionIDs:  sessionIDs,
	}
}

func (m *Memory) OpenSession(
	ctx context.Context,
	registration Registration,
	window SessionWindow,
) (model.HostSnapshot, error) {
	if err := ctx.Err(); err != nil {
		return model.HostSnapshot{}, err
	}
	if err := validateRegistration(registration); err != nil {
		return model.HostSnapshot{}, err
	}
	if err := validateSessionWindow(window); err != nil {
		return model.HostSnapshot{}, err
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	sessionID, err := m.uniqueSessionIDLocked()
	if err != nil {
		return model.HostSnapshot{}, err
	}
	key := model.HostKey{
		Namespace: registration.Namespace,
		ID:        registration.AgentIdentifier,
	}
	if previousID := m.current[key]; previousID != "" {
		previous := m.sessions[previousID]
		if window.ObservedAt.Before(previous.LastSeenAt) {
			return model.HostSnapshot{}, errors.New("agent session observation moved backwards")
		}
	}
	if m.generations[key] == math.MaxUint64 {
		return model.HostSnapshot{}, errors.New("agent session generation overflow")
	}
	generation := m.generations[key] + 1

	if previousID := m.current[key]; previousID != "" {
		previous := m.sessions[previousID]
		markDisconnected(&previous, window.ObservedAt)
		m.sessions[previousID] = previous
	}

	identity, exists := m.identities[key]
	if !exists {
		identity = model.HostIdentity{
			Key:        key,
			EnrolledAt: window.ObservedAt,
		}
	}
	identity.Hostname = registration.Hostname
	identity.Attributes = cloneAttributes(registration.Attributes)

	session := model.AgentSession{
		ID:                 sessionID,
		Host:               key,
		Generation:         generation,
		AgentVersion:       registration.AgentVersion,
		ProtocolVersion:    registration.ProtocolVersion,
		SchemaVersion:      registration.SchemaVersion,
		MaxConcurrentScans: registration.MaxConcurrentScans,
		ConnectedAt:        window.ObservedAt,
		LastSeenAt:         window.ObservedAt,
		ExpiresAt:          window.ExpiresAt,
	}
	m.identities[key] = identity
	m.sessions[sessionID] = session
	m.current[key] = sessionID
	m.generations[key] = generation

	return model.HostSnapshot{
		Identity: identity,
		Session:  &session,
	}.Clone(), nil
}

func (m *Memory) RenewSession(
	ctx context.Context,
	sessionID string,
	window SessionWindow,
) (model.HostSnapshot, error) {
	if err := ctx.Err(); err != nil {
		return model.HostSnapshot{}, err
	}
	if strings.TrimSpace(sessionID) == "" {
		return model.HostSnapshot{}, errors.New("agent session ID is required")
	}
	if err := validateSessionWindow(window); err != nil {
		return model.HostSnapshot{}, err
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	session, exists := m.sessions[sessionID]
	if !exists {
		return model.HostSnapshot{}, ErrSessionNotFound
	}
	if m.current[session.Host] != sessionID || !session.DisconnectedAt.IsZero() {
		return model.HostSnapshot{}, ErrSessionNotCurrent
	}
	if window.ObservedAt.Before(session.LastSeenAt) {
		return model.HostSnapshot{}, errors.New("agent session observation moved backwards")
	}
	if !session.ActiveAt(window.ObservedAt) {
		return model.HostSnapshot{}, ErrSessionExpired
	}
	if !window.ExpiresAt.After(session.ExpiresAt) {
		return model.HostSnapshot{}, errors.New("agent session renewal must advance lease expiry")
	}

	session.LastSeenAt = window.ObservedAt
	session.ExpiresAt = window.ExpiresAt
	m.sessions[sessionID] = session
	identity := m.identities[session.Host]
	return model.HostSnapshot{
		Identity: identity,
		Session:  &session,
	}.Clone(), nil
}

func (m *Memory) CloseSession(
	ctx context.Context,
	sessionID string,
	disconnectedAt time.Time,
) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if strings.TrimSpace(sessionID) == "" {
		return errors.New("agent session ID is required")
	}
	if disconnectedAt.IsZero() {
		return errors.New("agent session disconnect time is required")
	}

	m.mu.Lock()
	defer m.mu.Unlock()
	session, exists := m.sessions[sessionID]
	if !exists {
		return ErrSessionNotFound
	}
	if disconnectedAt.Before(session.ConnectedAt) {
		return errors.New("agent session disconnect precedes connection")
	}
	markDisconnected(&session, disconnectedAt)
	m.sessions[sessionID] = session
	return nil
}

func (m *Memory) ResolveSession(
	ctx context.Context,
	sessionID string,
) (model.HostSnapshot, error) {
	if err := ctx.Err(); err != nil {
		return model.HostSnapshot{}, err
	}
	m.mu.RLock()
	defer m.mu.RUnlock()
	session, exists := m.sessions[sessionID]
	if !exists {
		return model.HostSnapshot{}, ErrSessionNotFound
	}
	identity := m.identities[session.Host]
	return model.HostSnapshot{
		Identity: identity,
		Session:  &session,
	}.Clone(), nil
}

func (m *Memory) SnapshotActive(
	ctx context.Context,
	namespace string,
	at time.Time,
) ([]model.HostSnapshot, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if at.IsZero() {
		return nil, errors.New("fleet snapshot time is required")
	}
	if namespace != "" && strings.TrimSpace(namespace) != namespace {
		return nil, errors.New("fleet namespace must not contain surrounding whitespace")
	}

	m.mu.RLock()
	defer m.mu.RUnlock()
	snapshots := make([]model.HostSnapshot, 0, len(m.current))
	for key, sessionID := range m.current {
		if namespace != "" && key.Namespace != namespace {
			continue
		}
		session := m.sessions[sessionID]
		if !session.ActiveAt(at) {
			continue
		}
		snapshots = append(snapshots, model.HostSnapshot{
			Identity: m.identities[key],
			Session:  &session,
		}.Clone())
	}
	sortSnapshots(snapshots)
	return snapshots, nil
}

func (m *Memory) List(
	ctx context.Context,
	namespace string,
) ([]model.HostSnapshot, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if namespace != "" && strings.TrimSpace(namespace) != namespace {
		return nil, errors.New("fleet namespace must not contain surrounding whitespace")
	}

	m.mu.RLock()
	defer m.mu.RUnlock()
	snapshots := make([]model.HostSnapshot, 0, len(m.identities))
	for key, identity := range m.identities {
		if namespace != "" && key.Namespace != namespace {
			continue
		}
		snapshot := model.HostSnapshot{Identity: identity}
		if sessionID := m.current[key]; sessionID != "" {
			session := m.sessions[sessionID]
			snapshot.Session = &session
		}
		snapshots = append(snapshots, snapshot.Clone())
	}
	sortSnapshots(snapshots)
	return snapshots, nil
}

func (m *Memory) uniqueSessionIDLocked() (string, error) {
	for range maxSessionIDAttempts {
		sessionID, err := m.sessionIDs()
		if err != nil {
			return "", fmt.Errorf("generate agent session ID: %w", err)
		}
		if strings.TrimSpace(sessionID) == "" {
			return "", errors.New("generated agent session ID is empty")
		}
		if _, exists := m.sessions[sessionID]; !exists {
			return sessionID, nil
		}
	}
	return "", errors.New("generate unique agent session ID: collision limit reached")
}

func validateRegistration(registration Registration) error {
	if strings.TrimSpace(registration.Namespace) == "" {
		return errors.New("fleet namespace is required")
	}
	if strings.TrimSpace(registration.Namespace) != registration.Namespace {
		return errors.New("fleet namespace must not contain surrounding whitespace")
	}
	if strings.TrimSpace(registration.AgentIdentifier) == "" {
		return errors.New("agent identifier is required")
	}
	if strings.TrimSpace(registration.AgentIdentifier) != registration.AgentIdentifier {
		return errors.New("agent identifier must not contain surrounding whitespace")
	}
	if strings.TrimSpace(registration.AgentVersion) == "" {
		return errors.New("agent version is required")
	}
	if registration.ProtocolVersion.Major == 0 {
		return errors.New("agent protocol major version is required")
	}
	if strings.TrimSpace(registration.SchemaVersion) == "" {
		return errors.New("agent schema version is required")
	}
	if registration.MaxConcurrentScans == 0 {
		return errors.New("agent max concurrent scans must be positive")
	}
	return nil
}

func validateSessionWindow(window SessionWindow) error {
	if window.ObservedAt.IsZero() {
		return errors.New("agent session observation time is required")
	}
	if window.ExpiresAt.IsZero() || !window.ExpiresAt.After(window.ObservedAt) {
		return errors.New("agent session expiry must follow observation time")
	}
	return nil
}

func markDisconnected(session *model.AgentSession, at time.Time) {
	if session.DisconnectedAt.IsZero() || at.Before(session.DisconnectedAt) {
		session.DisconnectedAt = at
	}
}

func sortSnapshots(snapshots []model.HostSnapshot) {
	sort.Slice(snapshots, func(i, j int) bool {
		left := snapshots[i].Identity.Key
		right := snapshots[j].Identity.Key
		if left.Namespace == right.Namespace {
			return left.ID < right.ID
		}
		return left.Namespace < right.Namespace
	})
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

func randomSessionID() (string, error) {
	var value [16]byte
	if _, err := rand.Read(value[:]); err != nil {
		return "", err
	}
	return hex.EncodeToString(value[:]), nil
}

var _ Registry = (*Memory)(nil)

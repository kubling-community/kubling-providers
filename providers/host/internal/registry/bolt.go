package registry

import (
	"context"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/kubling-community/kubling-providers/providers/host/internal/model"
	bolt "go.etcd.io/bbolt"
)

const (
	registryDatabaseFilename = "registry.db"
	registryOpenTimeout      = 2 * time.Second
	registrySchemaVersion    = uint64(1)
)

var (
	metaBucket        = []byte("meta")
	identitiesBucket  = []byte("identities")
	sessionsBucket    = []byte("sessions")
	currentBucket     = []byte("current")
	generationsBucket = []byte("generations")
	schemaVersionKey  = []byte("schema-version")
)

// Bolt is a transactional durable Registry. Live transport streams remain in
// the gateway directory and are deliberately not persisted.
type Bolt struct {
	database   *bolt.DB
	sessionIDs sessionIDSource
}

// OpenBolt opens the durable registry inside stateDirectory. Sessions that
// were live before this process started are marked disconnected because their
// transport streams cannot survive a provider restart.
func OpenBolt(stateDirectory string, openedAt time.Time) (*Bolt, error) {
	return openBolt(stateDirectory, openedAt, randomSessionID)
}

func openBolt(
	stateDirectory string,
	openedAt time.Time,
	sessionIDs sessionIDSource,
) (*Bolt, error) {
	stateDirectory = strings.TrimSpace(stateDirectory)
	if stateDirectory == "" {
		return nil, errors.New("host-provider state directory is required")
	}
	if openedAt.IsZero() {
		return nil, errors.New("host-provider registry open time is required")
	}
	if sessionIDs == nil {
		return nil, errors.New("agent session ID source is required")
	}
	if err := os.MkdirAll(stateDirectory, 0o700); err != nil {
		return nil, fmt.Errorf("create host-provider state directory: %w", err)
	}
	path := filepath.Join(stateDirectory, registryDatabaseFilename)
	database, err := bolt.Open(path, 0o600, &bolt.Options{Timeout: registryOpenTimeout})
	if err != nil {
		return nil, fmt.Errorf("open host-provider registry: %w", err)
	}
	registry := &Bolt{database: database, sessionIDs: sessionIDs}
	if err := registry.initialize(openedAt.UTC()); err != nil {
		_ = database.Close()
		return nil, err
	}
	return registry, nil
}

func (b *Bolt) initialize(openedAt time.Time) error {
	if err := b.database.Update(func(transaction *bolt.Tx) error {
		for _, name := range [][]byte{
			metaBucket,
			identitiesBucket,
			sessionsBucket,
			currentBucket,
			generationsBucket,
		} {
			if _, err := transaction.CreateBucketIfNotExists(name); err != nil {
				return err
			}
		}
		meta := transaction.Bucket(metaBucket)
		serializedVersion := meta.Get(schemaVersionKey)
		switch {
		case serializedVersion == nil:
			if err := meta.Put(schemaVersionKey, encodeUint64(registrySchemaVersion)); err != nil {
				return err
			}
		case len(serializedVersion) != 8 || binary.BigEndian.Uint64(serializedVersion) != registrySchemaVersion:
			return fmt.Errorf("unsupported host-provider registry schema version")
		}

		current := transaction.Bucket(currentBucket)
		sessions := transaction.Bucket(sessionsBucket)
		return current.ForEach(func(_, sessionID []byte) error {
			serialized := sessions.Get(sessionID)
			if serialized == nil {
				return errors.New("host-provider registry current session is missing")
			}
			session, err := decodeSession(serialized)
			if err != nil {
				return err
			}
			disconnectedAt := openedAt
			if disconnectedAt.Before(session.LastSeenAt) {
				disconnectedAt = session.LastSeenAt
			}
			markDisconnected(&session, disconnectedAt)
			return putJSON(sessions, sessionID, session)
		})
	}); err != nil {
		return fmt.Errorf("initialize host-provider registry: %w", err)
	}
	return nil
}

// Close flushes and closes the durable registry.
func (b *Bolt) Close() error {
	if b == nil || b.database == nil {
		return nil
	}
	if err := b.database.Close(); err != nil {
		return fmt.Errorf("close host-provider registry: %w", err)
	}
	return nil
}

func (b *Bolt) OpenSession(
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

	var snapshot model.HostSnapshot
	err := b.database.Update(func(transaction *bolt.Tx) error {
		if err := ctx.Err(); err != nil {
			return err
		}
		key := model.HostKey{
			Namespace: registration.Namespace,
			ID:        registration.AgentIdentifier,
		}
		hostKey := encodeHostKey(key)
		identities := transaction.Bucket(identitiesBucket)
		sessions := transaction.Bucket(sessionsBucket)
		current := transaction.Bucket(currentBucket)
		generations := transaction.Bucket(generationsBucket)

		if previousID := current.Get(hostKey); previousID != nil {
			previous, err := sessionFromBucket(sessions, previousID)
			if err != nil {
				return err
			}
			if window.ObservedAt.Before(previous.LastSeenAt) {
				return errors.New("agent session observation moved backwards")
			}
			markDisconnected(&previous, window.ObservedAt)
			if err := putJSON(sessions, previousID, previous); err != nil {
				return err
			}
		}

		generation, err := generationFromBucket(generations, hostKey)
		if err != nil {
			return err
		}
		if generation == math.MaxUint64 {
			return errors.New("agent session generation overflow")
		}
		generation++

		identity := model.HostIdentity{Key: key, EnrolledAt: window.ObservedAt}
		if serialized := identities.Get(hostKey); serialized != nil {
			if err := json.Unmarshal(serialized, &identity); err != nil {
				return fmt.Errorf("decode host identity: %w", err)
			}
		}
		identity.Hostname = registration.Hostname
		identity.Attributes = cloneAttributes(registration.Attributes)

		sessionID, err := b.uniqueSessionID(sessions)
		if err != nil {
			return err
		}
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
		if err := putJSON(identities, hostKey, identity); err != nil {
			return err
		}
		if err := putJSON(sessions, []byte(sessionID), session); err != nil {
			return err
		}
		if err := current.Put(hostKey, []byte(sessionID)); err != nil {
			return err
		}
		if err := generations.Put(hostKey, encodeUint64(generation)); err != nil {
			return err
		}
		snapshot = model.HostSnapshot{Identity: identity, Session: &session}.Clone()
		return nil
	})
	if err != nil {
		return model.HostSnapshot{}, fmt.Errorf("persist agent session: %w", err)
	}
	return snapshot, nil
}

func (b *Bolt) RenewSession(
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

	var snapshot model.HostSnapshot
	err := b.database.Update(func(transaction *bolt.Tx) error {
		if err := ctx.Err(); err != nil {
			return err
		}
		sessions := transaction.Bucket(sessionsBucket)
		session, err := sessionFromBucket(sessions, []byte(sessionID))
		if errors.Is(err, ErrSessionNotFound) {
			return ErrSessionNotFound
		}
		if err != nil {
			return err
		}
		hostKey := encodeHostKey(session.Host)
		if string(transaction.Bucket(currentBucket).Get(hostKey)) != sessionID ||
			!session.DisconnectedAt.IsZero() {
			return ErrSessionNotCurrent
		}
		if window.ObservedAt.Before(session.LastSeenAt) {
			return errors.New("agent session observation moved backwards")
		}
		if !session.ActiveAt(window.ObservedAt) {
			return ErrSessionExpired
		}
		if !window.ExpiresAt.After(session.ExpiresAt) {
			return errors.New("agent session renewal must advance lease expiry")
		}
		session.LastSeenAt = window.ObservedAt
		session.ExpiresAt = window.ExpiresAt
		if err := putJSON(sessions, []byte(sessionID), session); err != nil {
			return err
		}
		identity, err := identityFromBucket(
			transaction.Bucket(identitiesBucket),
			hostKey,
		)
		if err != nil {
			return err
		}
		snapshot = model.HostSnapshot{Identity: identity, Session: &session}.Clone()
		return nil
	})
	if err != nil {
		return model.HostSnapshot{}, err
	}
	return snapshot, nil
}

func (b *Bolt) CloseSession(
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
	return b.database.Update(func(transaction *bolt.Tx) error {
		if err := ctx.Err(); err != nil {
			return err
		}
		sessions := transaction.Bucket(sessionsBucket)
		session, err := sessionFromBucket(sessions, []byte(sessionID))
		if err != nil {
			return err
		}
		if disconnectedAt.Before(session.ConnectedAt) {
			return errors.New("agent session disconnect precedes connection")
		}
		markDisconnected(&session, disconnectedAt)
		return putJSON(sessions, []byte(sessionID), session)
	})
}

func (b *Bolt) ResolveSession(
	ctx context.Context,
	sessionID string,
) (model.HostSnapshot, error) {
	if err := ctx.Err(); err != nil {
		return model.HostSnapshot{}, err
	}
	var snapshot model.HostSnapshot
	err := b.database.View(func(transaction *bolt.Tx) error {
		session, err := sessionFromBucket(
			transaction.Bucket(sessionsBucket),
			[]byte(sessionID),
		)
		if err != nil {
			return err
		}
		identity, err := identityFromBucket(
			transaction.Bucket(identitiesBucket),
			encodeHostKey(session.Host),
		)
		if err != nil {
			return err
		}
		snapshot = model.HostSnapshot{Identity: identity, Session: &session}.Clone()
		return nil
	})
	if err != nil {
		return model.HostSnapshot{}, err
	}
	return snapshot, nil
}

func (b *Bolt) SnapshotActive(
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

	var snapshots []model.HostSnapshot
	err := b.database.View(func(transaction *bolt.Tx) error {
		current := transaction.Bucket(currentBucket)
		sessions := transaction.Bucket(sessionsBucket)
		identities := transaction.Bucket(identitiesBucket)
		return current.ForEach(func(hostKey, sessionID []byte) error {
			if err := ctx.Err(); err != nil {
				return err
			}
			session, err := sessionFromBucket(sessions, sessionID)
			if err != nil {
				return err
			}
			if !session.ActiveAt(at) {
				return nil
			}
			identity, err := identityFromBucket(identities, hostKey)
			if err != nil {
				return err
			}
			if namespace != "" && identity.Key.Namespace != namespace {
				return nil
			}
			snapshots = append(snapshots, model.HostSnapshot{
				Identity: identity,
				Session:  &session,
			}.Clone())
			return nil
		})
	})
	if err != nil {
		return nil, err
	}
	sortSnapshots(snapshots)
	return snapshots, nil
}

func (b *Bolt) List(
	ctx context.Context,
	namespace string,
) ([]model.HostSnapshot, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if namespace != "" && strings.TrimSpace(namespace) != namespace {
		return nil, errors.New("fleet namespace must not contain surrounding whitespace")
	}

	var snapshots []model.HostSnapshot
	err := b.database.View(func(transaction *bolt.Tx) error {
		identities := transaction.Bucket(identitiesBucket)
		current := transaction.Bucket(currentBucket)
		sessions := transaction.Bucket(sessionsBucket)
		return identities.ForEach(func(hostKey, serialized []byte) error {
			if err := ctx.Err(); err != nil {
				return err
			}
			var identity model.HostIdentity
			if err := json.Unmarshal(serialized, &identity); err != nil {
				return fmt.Errorf("decode host identity: %w", err)
			}
			if namespace != "" && identity.Key.Namespace != namespace {
				return nil
			}
			snapshot := model.HostSnapshot{Identity: identity}
			if sessionID := current.Get(hostKey); sessionID != nil {
				session, err := sessionFromBucket(sessions, sessionID)
				if err != nil {
					return err
				}
				snapshot.Session = &session
			}
			snapshots = append(snapshots, snapshot.Clone())
			return nil
		})
	})
	if err != nil {
		return nil, err
	}
	sortSnapshots(snapshots)
	return snapshots, nil
}

func (b *Bolt) uniqueSessionID(sessions *bolt.Bucket) (string, error) {
	for range maxSessionIDAttempts {
		sessionID, err := b.sessionIDs()
		if err != nil {
			return "", fmt.Errorf("generate agent session ID: %w", err)
		}
		if strings.TrimSpace(sessionID) == "" {
			return "", errors.New("generated agent session ID is empty")
		}
		if sessions.Get([]byte(sessionID)) == nil {
			return sessionID, nil
		}
	}
	return "", errors.New("generate unique agent session ID: collision limit reached")
}

func encodeHostKey(key model.HostKey) []byte {
	namespace := []byte(key.Namespace)
	encoded := make([]byte, 4+len(namespace)+len(key.ID))
	binary.BigEndian.PutUint32(encoded[:4], uint32(len(namespace)))
	copy(encoded[4:], namespace)
	copy(encoded[4+len(namespace):], key.ID)
	return encoded
}

func encodeUint64(value uint64) []byte {
	encoded := make([]byte, 8)
	binary.BigEndian.PutUint64(encoded, value)
	return encoded
}

func generationFromBucket(bucket *bolt.Bucket, key []byte) (uint64, error) {
	serialized := bucket.Get(key)
	if serialized == nil {
		return 0, nil
	}
	if len(serialized) != 8 {
		return 0, errors.New("invalid persisted host generation")
	}
	return binary.BigEndian.Uint64(serialized), nil
}

func identityFromBucket(
	bucket *bolt.Bucket,
	key []byte,
) (model.HostIdentity, error) {
	serialized := bucket.Get(key)
	if serialized == nil {
		return model.HostIdentity{}, errors.New("persisted host identity is missing")
	}
	var identity model.HostIdentity
	if err := json.Unmarshal(serialized, &identity); err != nil {
		return model.HostIdentity{}, fmt.Errorf("decode host identity: %w", err)
	}
	return identity, nil
}

func sessionFromBucket(
	bucket *bolt.Bucket,
	key []byte,
) (model.AgentSession, error) {
	serialized := bucket.Get(key)
	if serialized == nil {
		return model.AgentSession{}, ErrSessionNotFound
	}
	return decodeSession(serialized)
}

func decodeSession(serialized []byte) (model.AgentSession, error) {
	var session model.AgentSession
	if err := json.Unmarshal(serialized, &session); err != nil {
		return model.AgentSession{}, fmt.Errorf("decode agent session: %w", err)
	}
	return session, nil
}

func putJSON[T any](bucket *bolt.Bucket, key []byte, value T) error {
	serialized, err := json.Marshal(value)
	if err != nil {
		return err
	}
	return bucket.Put(key, serialized)
}

var _ Registry = (*Bolt)(nil)

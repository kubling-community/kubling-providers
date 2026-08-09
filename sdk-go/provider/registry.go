package provider

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"sync"
)

const connectionIDBytes = 16

// connectionRegistry owns the logical connections opened through the SDK.
type connectionRegistry struct {
	mu          sync.RWMutex
	connections map[string]*managedConnection
}

// managedConnection coordinates operations with connection closure.
type managedConnection struct {
	mu         sync.RWMutex
	connection Connection
	closed     bool
}

func newConnectionRegistry() *connectionRegistry {
	return &connectionRegistry{
		connections: make(map[string]*managedConnection),
	}
}

// add registers a connection and returns its opaque public identifier.
func (r *connectionRegistry) add(connection Connection) (string, error) {
	for {
		id, err := newConnectionID()
		if err != nil {
			return "", err
		}

		r.mu.Lock()

		if _, exists := r.connections[id]; exists {
			r.mu.Unlock()
			continue
		}

		r.connections[id] = &managedConnection{
			connection: connection,
		}

		r.mu.Unlock()

		return id, nil
	}
}

// acquire returns a connection and a function that must be called when the
// operation using it has finished.
func (r *connectionRegistry) acquire(
	id string,
) (Connection, func(), bool) {
	r.mu.RLock()
	managed, exists := r.connections[id]
	r.mu.RUnlock()

	if !exists {
		return nil, nil, false
	}

	managed.mu.RLock()

	if managed.closed {
		managed.mu.RUnlock()
		return nil, nil, false
	}

	release := func() {
		managed.mu.RUnlock()
	}

	return managed.connection, release, true
}

// close removes and closes a connection.
//
// Removing it first prevents new operations from acquiring it. The exclusive
// connection lock waits for operations already in progress to finish.
func (r *connectionRegistry) close(
	ctx context.Context,
	id string,
) (bool, error) {
	r.mu.Lock()

	managed, exists := r.connections[id]
	if exists {
		delete(r.connections, id)
	}

	r.mu.Unlock()

	if !exists {
		return false, nil
	}

	return true, managed.close(ctx)
}

// closeAll removes and closes every registered connection.
func (r *connectionRegistry) closeAll(ctx context.Context) error {
	r.mu.Lock()

	connections := make([]*managedConnection, 0, len(r.connections))
	for _, managed := range r.connections {
		connections = append(connections, managed)
	}

	clear(r.connections)

	r.mu.Unlock()

	var closeErrors []error

	for _, managed := range connections {
		if err := managed.close(ctx); err != nil {
			closeErrors = append(closeErrors, err)
		}
	}

	return errors.Join(closeErrors...)
}

func (c *managedConnection) close(ctx context.Context) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.closed {
		return nil
	}

	c.closed = true

	return c.connection.Close(ctx)
}

func newConnectionID() (string, error) {
	value := make([]byte, connectionIDBytes)

	if _, err := rand.Read(value); err != nil {
		return "", err
	}

	return hex.EncodeToString(value), nil
}

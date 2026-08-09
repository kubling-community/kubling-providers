package provider

import (
	"context"
	"encoding/hex"
	"errors"
	"runtime"
	"sync"
	"testing"
	"time"
)

type registryTestConnection struct {
	Connection

	mu            sync.Mutex
	closeCount    int
	closeContexts []context.Context
	closeErr      error
}

func (c *registryTestConnection) Close(ctx context.Context) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.closeCount++
	c.closeContexts = append(c.closeContexts, ctx)

	return c.closeErr
}

func (c *registryTestConnection) closeSnapshot() (
	int,
	[]context.Context,
) {
	c.mu.Lock()
	defer c.mu.Unlock()

	contexts := append([]context.Context(nil), c.closeContexts...)

	return c.closeCount, contexts
}

func TestConnectionRegistryAddAndAcquire(t *testing.T) {
	registry := newConnectionRegistry()
	connection := &registryTestConnection{}

	connectionID, err := registry.add(connection)
	if err != nil {
		t.Fatalf("add connection: %v", err)
	}

	if len(connectionID) != connectionIDBytes*2 {
		t.Fatalf(
			"connection ID length = %d, want %d",
			len(connectionID),
			connectionIDBytes*2,
		)
	}

	decodedID, err := hex.DecodeString(connectionID)
	if err != nil {
		t.Fatalf("connection ID is not hexadecimal: %v", err)
	}

	if len(decodedID) != connectionIDBytes {
		t.Fatalf(
			"decoded connection ID length = %d, want %d",
			len(decodedID),
			connectionIDBytes,
		)
	}

	acquired, release, found := registry.acquire(connectionID)
	if !found {
		t.Fatal("acquire registered connection: not found")
	}
	if acquired != connection {
		t.Fatalf("acquired connection = %p, want %p", acquired, connection)
	}
	if release == nil {
		t.Fatal("acquire registered connection returned nil release")
	}

	release()
}

func TestConnectionRegistryAcquireMissing(t *testing.T) {
	registry := newConnectionRegistry()

	connection, release, found := registry.acquire("missing")

	if found {
		t.Fatal("acquire missing connection: found")
	}
	if connection != nil {
		t.Fatalf("acquire missing connection = %v, want nil", connection)
	}
	if release != nil {
		t.Fatal("acquire missing connection returned a release function")
	}
}

func TestConnectionRegistryClose(t *testing.T) {
	registry := newConnectionRegistry()
	connection := &registryTestConnection{}
	ctx := context.WithValue(
		context.Background(),
		registryContextKey{},
		"close",
	)

	connectionID, err := registry.add(connection)
	if err != nil {
		t.Fatalf("add connection: %v", err)
	}

	found, err := registry.close(ctx, connectionID)
	if err != nil {
		t.Fatalf("close connection: %v", err)
	}
	if !found {
		t.Fatal("close registered connection: not found")
	}

	closeCount, closeContexts := connection.closeSnapshot()
	if closeCount != 1 {
		t.Fatalf("connection close count = %d, want 1", closeCount)
	}
	if len(closeContexts) != 1 || closeContexts[0] != ctx {
		t.Fatal("connection did not receive the close context")
	}

	acquired, release, found := registry.acquire(connectionID)
	if found {
		release()
		t.Fatal("acquire closed connection: found")
	}
	if acquired != nil || release != nil {
		t.Fatal("acquire closed connection returned connection state")
	}

	found, err = registry.close(context.Background(), connectionID)
	if err != nil {
		t.Fatalf("close connection again: %v", err)
	}
	if found {
		t.Fatal("close connection again: found")
	}

	closeCount, _ = connection.closeSnapshot()
	if closeCount != 1 {
		t.Fatalf("connection close count = %d after second close, want 1", closeCount)
	}
}

func TestConnectionRegistryClosePropagatesError(t *testing.T) {
	closeErr := errors.New("close failed")
	registry := newConnectionRegistry()
	connection := &registryTestConnection{
		closeErr: closeErr,
	}

	connectionID, err := registry.add(connection)
	if err != nil {
		t.Fatalf("add connection: %v", err)
	}

	found, err := registry.close(context.Background(), connectionID)
	if !found {
		t.Fatal("close registered connection: not found")
	}
	if !errors.Is(err, closeErr) {
		t.Fatalf("close error = %v, want %v", err, closeErr)
	}

	_, _, found = registry.acquire(connectionID)
	if found {
		t.Fatal("connection remained registered after close error")
	}
}

func TestConnectionRegistryCloseWaitsForRelease(t *testing.T) {
	registry := newConnectionRegistry()
	connection := &registryTestConnection{}

	connectionID, err := registry.add(connection)
	if err != nil {
		t.Fatalf("add connection: %v", err)
	}

	_, release, found := registry.acquire(connectionID)
	if !found {
		t.Fatal("acquire registered connection: not found")
	}

	released := false
	defer func() {
		if !released {
			release()
		}
	}()

	type closeResult struct {
		found bool
		err   error
	}

	result := make(chan closeResult, 1)
	go func() {
		found, err := registry.close(context.Background(), connectionID)
		result <- closeResult{
			found: found,
			err:   err,
		}
	}()

	waitForConnectionRemoval(t, registry, connectionID)

	select {
	case result := <-result:
		t.Fatalf(
			"close completed before release: found=%t, err=%v",
			result.found,
			result.err,
		)
	default:
	}

	closeCount, _ := connection.closeSnapshot()
	if closeCount != 0 {
		t.Fatalf("connection close count before release = %d, want 0", closeCount)
	}

	release()
	released = true

	select {
	case result := <-result:
		if result.err != nil {
			t.Fatalf("close connection: %v", result.err)
		}
		if !result.found {
			t.Fatal("close registered connection: not found")
		}
	case <-time.After(time.Second):
		t.Fatal("close did not complete after release")
	}

	closeCount, _ = connection.closeSnapshot()
	if closeCount != 1 {
		t.Fatalf("connection close count = %d, want 1", closeCount)
	}
}

func TestConnectionRegistryCloseAll(t *testing.T) {
	registry := newConnectionRegistry()
	connections := []*registryTestConnection{
		{},
		{},
		{},
	}
	connectionIDs := make([]string, 0, len(connections))

	for _, connection := range connections {
		connectionID, err := registry.add(connection)
		if err != nil {
			t.Fatalf("add connection: %v", err)
		}

		connectionIDs = append(connectionIDs, connectionID)
	}

	if err := registry.closeAll(context.Background()); err != nil {
		t.Fatalf("close all connections: %v", err)
	}

	for index, connection := range connections {
		closeCount, _ := connection.closeSnapshot()
		if closeCount != 1 {
			t.Errorf(
				"connection %d close count = %d, want 1",
				index,
				closeCount,
			)
		}

		_, _, found := registry.acquire(connectionIDs[index])
		if found {
			t.Errorf("connection %d remained registered", index)
		}
	}

	if err := registry.closeAll(context.Background()); err != nil {
		t.Fatalf("close all connections again: %v", err)
	}

	for index, connection := range connections {
		closeCount, _ := connection.closeSnapshot()
		if closeCount != 1 {
			t.Errorf(
				"connection %d close count after second closeAll = %d, want 1",
				index,
				closeCount,
			)
		}
	}
}

func TestConnectionRegistryCloseAllJoinsErrors(t *testing.T) {
	firstCloseErr := errors.New("first close failed")
	secondCloseErr := errors.New("second close failed")
	registry := newConnectionRegistry()
	connections := []*registryTestConnection{
		{closeErr: firstCloseErr},
		{},
		{closeErr: secondCloseErr},
	}
	connectionIDs := make([]string, 0, len(connections))

	for _, connection := range connections {
		connectionID, err := registry.add(connection)
		if err != nil {
			t.Fatalf("add connection: %v", err)
		}

		connectionIDs = append(connectionIDs, connectionID)
	}

	err := registry.closeAll(context.Background())
	if !errors.Is(err, firstCloseErr) {
		t.Errorf("closeAll error = %v, missing %v", err, firstCloseErr)
	}
	if !errors.Is(err, secondCloseErr) {
		t.Errorf("closeAll error = %v, missing %v", err, secondCloseErr)
	}

	for index, connection := range connections {
		closeCount, _ := connection.closeSnapshot()
		if closeCount != 1 {
			t.Errorf(
				"connection %d close count = %d, want 1",
				index,
				closeCount,
			)
		}

		_, _, found := registry.acquire(connectionIDs[index])
		if found {
			t.Errorf("connection %d remained registered", index)
		}
	}
}

type registryContextKey struct{}

func waitForConnectionRemoval(
	t *testing.T,
	registry *connectionRegistry,
	connectionID string,
) {
	t.Helper()

	timeout := time.NewTimer(time.Second)
	defer timeout.Stop()

	for {
		_, release, found := registry.acquire(connectionID)
		if !found {
			return
		}

		release()

		select {
		case <-timeout.C:
			t.Fatal("connection was not removed while close waited")
		default:
			runtime.Gosched()
		}
	}
}

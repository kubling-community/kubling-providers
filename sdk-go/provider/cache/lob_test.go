package cache

import (
	"context"
	"sync/atomic"
	"testing"

	kublingv1 "github.com/kubling-community/kubling-grpc/sdk-go/kubling/v1"
	providerv1 "github.com/kubling-community/kubling-providers/sdk-go/kubling/provider/v1"
	providersdk "github.com/kubling-community/kubling-providers/sdk-go/provider"
)

type testLobConnection struct {
	*testConnection
	readCalls    atomic.Int32
	releaseCalls atomic.Int32
}

func (c *testLobConnection) ReadLob(
	context.Context,
	*providerv1.ReadLobRequest,
) (providersdk.LobStream, error) {
	c.readCalls.Add(1)
	return nil, nil
}

func (c *testLobConnection) ReleaseLob(
	context.Context,
	*providerv1.ReleaseLobRequest,
) error {
	c.releaseCalls.Add(1)
	return nil
}

func TestOpenPreservesOptionalLobConnection(t *testing.T) {
	underlying := &testLobConnection{testConnection: &testConnection{}}
	cachedProvider, _ := Wrap(&testProvider{connection: underlying}, Config{})
	connection, err := cachedProvider.Open(context.Background())
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	lobConnection, ok := connection.(providersdk.LobConnection)
	if !ok {
		t.Fatal("Open() removed LobConnection support")
	}

	if _, err := lobConnection.ReadLob(
		context.Background(),
		&providerv1.ReadLobRequest{LobId: "lob-1"},
	); err != nil {
		t.Fatalf("ReadLob() error = %v", err)
	}
	if err := lobConnection.ReleaseLob(
		context.Background(),
		&providerv1.ReleaseLobRequest{LobId: "lob-1"},
	); err != nil {
		t.Fatalf("ReleaseLob() error = %v", err)
	}
	if underlying.readCalls.Load() != 1 || underlying.releaseCalls.Load() != 1 {
		t.Fatalf(
			"underlying LOB calls = (%d, %d), want (1, 1)",
			underlying.readCalls.Load(),
			underlying.releaseCalls.Load(),
		)
	}

	ordinaryProvider, _ := Wrap(
		&testProvider{connection: &testConnection{}},
		Config{},
	)
	ordinary, err := ordinaryProvider.Open(context.Background())
	if err != nil {
		t.Fatalf("ordinary Open() error = %v", err)
	}
	if _, ok := ordinary.(providersdk.LobConnection); ok {
		t.Fatal("ordinary connection unexpectedly implements LobConnection")
	}
}

func TestQueryDoesNotCacheLobReferences(t *testing.T) {
	var queryCalls atomic.Int32
	expiresAtUnixMs := int64(1_800_000_000_000)
	batch := &providerv1.TupleBatch{
		Tuples: []*providerv1.Tuple{{Values: []*kublingv1.Value{{
			Kind: &kublingv1.Value_LobReference{LobReference: &kublingv1.LobReference{
				LobId:           "lob-1",
				Type:            kublingv1.ValueType_VALUE_TYPE_BLOB,
				ExpiresAtUnixMs: &expiresAtUnixMs,
			}},
		}}}},
	}
	connection := &testConnection{queryFunc: func(
		context.Context,
		*providerv1.QueryRequest,
	) (providersdk.ResultStream, error) {
		queryCalls.Add(1)
		return &testStream{batches: []*providerv1.TupleBatch{batch}}, nil
	}}
	cachedProvider, _ := Wrap(&testProvider{connection: connection}, Config{})
	cachedConnection := openTestConnection(t, cachedProvider, "source")

	queryAndClose(t, cachedConnection, testQuery("transport-1", "TASK"))
	queryAndClose(t, cachedConnection, testQuery("transport-2", "TASK"))
	if got := queryCalls.Load(); got != 2 {
		t.Fatalf("underlying Query calls = %d, want 2", got)
	}
}

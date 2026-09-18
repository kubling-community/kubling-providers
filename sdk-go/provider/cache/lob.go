package cache

import (
	"context"

	providerv1 "github.com/kubling-community/kubling-providers/sdk-go/kubling/provider/v1"
	providersdk "github.com/kubling-community/kubling-providers/sdk-go/provider"
)

type lobCachedConnection struct {
	*cachedConnection
	lobConnection providersdk.LobConnection
}

func (c *lobCachedConnection) ReadLob(
	ctx context.Context,
	request *providerv1.ReadLobRequest,
) (providersdk.LobStream, error) {
	return c.lobConnection.ReadLob(ctx, request)
}

func (c *lobCachedConnection) ReleaseLob(
	ctx context.Context,
	request *providerv1.ReleaseLobRequest,
) error {
	return c.lobConnection.ReleaseLob(ctx, request)
}

var _ providersdk.LobConnection = (*lobCachedConnection)(nil)

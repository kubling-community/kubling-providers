package cache

import (
	"context"

	providerv1 "github.com/kubling-community/kubling-providers/sdk-go/kubling/provider/v1"
)

func (c *cachedConnection) Insert(
	ctx context.Context,
	request *providerv1.InsertRequest,
) (*providerv1.InsertResponse, error) {
	c.transactionMu.Lock()
	defer c.transactionMu.Unlock()

	response, err := c.Connection.Insert(ctx, request)
	if err == nil {
		c.recordMutationLocked(entityName(request))
	}

	return response, err
}

func (c *cachedConnection) Update(
	ctx context.Context,
	request *providerv1.UpdateRequest,
) (*providerv1.UpdateResponse, error) {
	c.transactionMu.Lock()
	defer c.transactionMu.Unlock()

	response, err := c.Connection.Update(ctx, request)
	if err == nil {
		c.recordMutationLocked(entityName(request))
	}

	return response, err
}

func (c *cachedConnection) Delete(
	ctx context.Context,
	request *providerv1.DeleteRequest,
) (*providerv1.DeleteResponse, error) {
	c.transactionMu.Lock()
	defer c.transactionMu.Unlock()

	response, err := c.Connection.Delete(ctx, request)
	if err == nil {
		c.recordMutationLocked(entityName(request))
	}

	return response, err
}

type entityRequest interface {
	GetEntity() *providerv1.EntityReference
}

func entityName(request entityRequest) string {
	if request == nil {
		return ""
	}

	entity, err := normalizedEntityKey(request.GetEntity())
	if err != nil {
		return ""
	}

	return entity
}

func (c *cachedConnection) recordMutationLocked(entity string) {
	if c.transactionActive {
		if entity == "" {
			c.pendingAll = true
			c.pendingEntities = nil
			return
		}
		if c.pendingAll {
			return
		}
		if c.pendingEntities == nil {
			c.pendingEntities = make(map[string]struct{})
		}
		c.pendingEntities[entity] = struct{}{}
		return
	}

	if entity == "" {
		c.state.invalidateAll()
		return
	}
	c.state.invalidateEntities([]string{entity})
}

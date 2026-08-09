package cache

import "context"

func (c *cachedConnection) Begin(ctx context.Context) error {
	c.transactionMu.Lock()
	defer c.transactionMu.Unlock()

	wasActive := c.transactionActive
	if err := c.Connection.Begin(ctx); err != nil {
		return err
	}

	c.transactionActive = true
	if !wasActive {
		c.clearPendingLocked()
	}

	return nil
}

func (c *cachedConnection) Commit(ctx context.Context) error {
	c.transactionMu.Lock()
	defer c.transactionMu.Unlock()

	err := c.Connection.Commit(ctx)
	c.invalidatePendingLocked()
	if err != nil {
		return err
	}

	c.transactionActive = false
	c.clearPendingLocked()

	return nil
}

func (c *cachedConnection) Rollback(ctx context.Context) error {
	c.transactionMu.Lock()
	defer c.transactionMu.Unlock()

	if err := c.Connection.Rollback(ctx); err != nil {
		c.invalidatePendingLocked()
		return err
	}

	c.transactionActive = false
	c.clearPendingLocked()

	return nil
}

func (c *cachedConnection) InTransaction(
	ctx context.Context,
) (bool, error) {
	c.transactionMu.Lock()
	defer c.transactionMu.Unlock()

	active, err := c.Connection.InTransaction(ctx)
	if err != nil {
		return false, err
	}
	if c.transactionActive && !active {
		c.invalidatePendingLocked()
	}

	c.transactionActive = active
	if !active {
		c.clearPendingLocked()
	}

	return active, nil
}

func (c *cachedConnection) Close(ctx context.Context) error {
	c.transactionMu.Lock()
	defer c.transactionMu.Unlock()

	err := c.Connection.Close(ctx)
	if c.transactionActive {
		c.invalidatePendingLocked()
	}
	c.transactionActive = false
	c.clearPendingLocked()

	return err
}

func (c *cachedConnection) invalidatePendingLocked() {
	if c.pendingAll {
		c.state.invalidateAll()
		return
	}
	if len(c.pendingEntities) == 0 {
		return
	}

	entities := make([]string, 0, len(c.pendingEntities))
	for entity := range c.pendingEntities {
		entities = append(entities, entity)
	}
	c.state.invalidateEntities(entities)
}

func (c *cachedConnection) clearPendingLocked() {
	c.pendingEntities = nil
	c.pendingAll = false
}

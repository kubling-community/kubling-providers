// Package cache provides an optional read-through query cache for providers.
//
// Cached providers preserve the provider SDK interfaces and can be passed
// directly to provider.NewServer. Successful SDK mutations are invalidated
// automatically. Callers must still invalidate source-side changes.
package cache

import (
	"context"
	"fmt"
	"strings"
	"time"

	providerv1 "github.com/kubling-community/kubling-providers/sdk-go/kubling/provider/v1"
	providersdk "github.com/kubling-community/kubling-providers/sdk-go/provider"
)

const (
	defaultTTL           = 30 * time.Second
	defaultMaxEntries    = 1_024
	defaultMaxBytes      = 64 << 20
	defaultMaxEntryBytes = 8 << 20
)

// Dependency describes cached entities affected by a change to another entity.
type Dependency struct {
	Entity     *providerv1.EntityReference
	Dependents []*providerv1.EntityReference
}

// Config controls the local in-process cache.
//
// Zero values use conservative defaults.
type Config struct {
	TTL           time.Duration
	MaxEntries    int
	MaxBytes      int64
	MaxEntryBytes int64
	// Dependencies maps a changed entity to additional query entities whose
	// cached results depend on it. Dependencies are expanded transitively.
	Dependencies []Dependency
}

// Invalidation identifies entities whose cached queries are no longer valid.
type Invalidation struct {
	Entities []*providerv1.EntityReference
}

// Provider wraps a provider implementation with query caching.
type Provider struct {
	implementation providersdk.Provider
	state          *cacheState
}

// Controller invalidates cached data owned by a wrapped Provider.
type Controller struct {
	state *cacheState
}

// Wrap adds an in-process query cache to implementation.
//
// The returned Controller must be notified after any external source event
// that can change cached query results. Insert, Update, and Delete invalidate
// their affected entities automatically after successful execution.
//
//	cachedProvider, invalidator := cache.Wrap(implementation, cache.Config{
//		TTL: 30 * time.Second,
//	})
//	server := provider.NewServer(cachedProvider)
//	_ = invalidator.Invalidate(ctx, cache.Invalidation{Entities: []*providerv1.EntityReference{
//		{Name: "SYSTEM", Namespace: "rack-01"},
//		{Name: "THERMAL", Namespace: "rack-01"},
//	}})
func Wrap(
	implementation providersdk.Provider,
	config Config,
) (*Provider, *Controller) {
	normalized := normalizeConfig(config)
	state := newCacheState(normalized)

	return &Provider{
			implementation: implementation,
			state:          state,
		}, &Controller{
			state: state,
		}
}

// Capabilities delegates to the wrapped provider.
func (p *Provider) Capabilities(
	ctx context.Context,
) (*providersdk.Capabilities, error) {
	return p.implementation.Capabilities(ctx)
}

// Health delegates to the wrapped provider.
func (p *Provider) Health(
	ctx context.Context,
) (*providerv1.HealthResponse, error) {
	return p.implementation.Health(ctx)
}

// Schema delegates to the wrapped provider when it exposes a schema.
func (p *Provider) Schema(ctx context.Context) (string, error) {
	schemaProvider, ok := p.implementation.(providersdk.SchemaProvider)
	if !ok {
		return "", nil
	}

	return schemaProvider.Schema(ctx)
}

// Metadata delegates to the wrapped provider when it exposes structured
// relational metadata.
func (p *Provider) Metadata(
	ctx context.Context,
) (*providersdk.Metadata, error) {
	metadataProvider, ok := p.implementation.(providersdk.MetadataProvider)
	if !ok {
		return nil, nil
	}

	return metadataProvider.Metadata(ctx)
}

// Open delegates connection creation and wraps successful connections.
func (p *Provider) Open(
	ctx context.Context,
) (providersdk.Connection, error) {
	connection, err := p.implementation.Open(ctx)
	if err != nil || connection == nil {
		return connection, err
	}

	return &cachedConnection{
		Connection: connection,
		state:      p.state,
	}, nil
}

// Invalidate invalidates selected entities in the provider data universe.
func (c *Controller) Invalidate(
	ctx context.Context,
	invalidation Invalidation,
) error {
	if err := contextError(ctx); err != nil {
		return err
	}
	if len(invalidation.Entities) == 0 {
		return fmt.Errorf("at least one entity is required")
	}
	entities := make([]string, 0, len(invalidation.Entities))
	for _, entity := range invalidation.Entities {
		key, err := normalizedEntityKey(entity)
		if err != nil {
			return err
		}
		entities = append(entities, key)
	}

	c.state.invalidateEntities(entities)

	return nil
}

// InvalidateAll invalidates every cached query owned by this controller.
func (c *Controller) InvalidateAll(ctx context.Context) error {
	if err := contextError(ctx); err != nil {
		return err
	}

	c.state.invalidateAll()

	return nil
}

type normalizedConfig struct {
	ttl           time.Duration
	maxEntries    int
	maxBytes      int64
	maxEntryBytes int64
	dependencies  map[string][]string
}

func normalizeConfig(config Config) normalizedConfig {
	ttl := config.TTL
	if ttl <= 0 {
		ttl = defaultTTL
	}

	maxEntries := config.MaxEntries
	if maxEntries <= 0 {
		maxEntries = defaultMaxEntries
	}

	maxBytes := config.MaxBytes
	if maxBytes <= 0 {
		maxBytes = defaultMaxBytes
	}

	maxEntryBytes := config.MaxEntryBytes
	if maxEntryBytes <= 0 {
		maxEntryBytes = defaultMaxEntryBytes
	}
	if maxEntryBytes > maxBytes {
		maxEntryBytes = maxBytes
	}

	return normalizedConfig{
		ttl:           ttl,
		maxEntries:    maxEntries,
		maxBytes:      maxBytes,
		maxEntryBytes: maxEntryBytes,
		dependencies:  normalizeDependencies(config.Dependencies),
	}
}

func normalizeDependencies(
	dependencies []Dependency,
) map[string][]string {
	normalized := make(map[string][]string, len(dependencies))
	for _, dependency := range dependencies {
		entity, err := normalizedEntityKey(dependency.Entity)
		if err != nil {
			continue
		}

		seen := make(map[string]struct{}, len(dependency.Dependents))
		for _, reference := range dependency.Dependents {
			dependent, err := normalizedEntityKey(reference)
			if err != nil || dependent == entity {
				continue
			}
			if _, exists := seen[dependent]; exists {
				continue
			}
			seen[dependent] = struct{}{}
			normalized[entity] = append(normalized[entity], dependent)
		}
	}

	return normalized
}

func contextError(ctx context.Context) error {
	if ctx == nil {
		return nil
	}

	return ctx.Err()
}

func normalizedEntityKey(entity *providerv1.EntityReference) (string, error) {
	if entity == nil {
		return "", fmt.Errorf("entity is required")
	}

	name := strings.ToUpper(strings.TrimSpace(entity.GetName()))
	if name == "" {
		return "", fmt.Errorf("entity name is required")
	}

	namespace := entity.GetNamespace()
	var key strings.Builder
	fmt.Fprintf(&key, "%d:%s/", len(namespace), namespace)
	fmt.Fprintf(&key, "%d:%s", len(name), name)

	return key.String(), nil
}

var (
	_ providersdk.Provider         = (*Provider)(nil)
	_ providersdk.SchemaProvider   = (*Provider)(nil)
	_ providersdk.MetadataProvider = (*Provider)(nil)
)

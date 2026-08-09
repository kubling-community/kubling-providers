package cache

import (
	"crypto/sha256"
	"sync"

	providerv1 "github.com/kubling-community/kubling-providers/sdk-go/kubling/provider/v1"
)

type generation struct {
	global uint64
	entity uint64
}

type queryKey struct {
	entity string
	digest [sha256.Size]byte
	generation
}

type cacheState struct {
	mu sync.Mutex

	store             *memoryStore
	globalGeneration  uint64
	entityGenerations map[string]uint64
	maxEntryBytes     int64
	dependencies      map[string][]string
}

func newCacheState(config normalizedConfig) *cacheState {
	return &cacheState{
		store: newMemoryStore(
			config.maxEntries,
			config.maxBytes,
			config.ttl,
		),
		entityGenerations: make(map[string]uint64),
		maxEntryBytes:     config.maxEntryBytes,
		dependencies:      config.dependencies,
	}
}

func (s *cacheState) lookup(
	entity string,
	digest [sha256.Size]byte,
) (queryCapture, cachedResult, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()

	capture := queryCapture{
		key: queryKey{
			entity:     entity,
			digest:     digest,
			generation: s.generation(entity),
		},
	}
	result, found := s.store.get(capture.key)

	return capture, result, found
}

func (s *cacheState) storeIfCurrent(
	capture queryCapture,
	result cachedResult,
) {
	if result.size > s.maxEntryBytes {
		return
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	current := s.generation(capture.key.entity)
	if current != capture.key.generation {
		return
	}

	s.store.set(capture.key, result)
}

func (s *cacheState) invalidateEntities(
	entities []string,
) {
	entities = s.expandEntities(entities)

	s.mu.Lock()
	defer s.mu.Unlock()

	for _, entity := range entities {
		s.entityGenerations[entity]++
	}
}

func (s *cacheState) expandEntities(entities []string) []string {
	queue := append([]string(nil), entities...)
	seen := make(map[string]struct{}, len(queue))
	expanded := make([]string, 0, len(queue))

	for len(queue) > 0 {
		entity := queue[0]
		queue = queue[1:]
		if entity == "" {
			continue
		}
		if _, exists := seen[entity]; exists {
			continue
		}

		seen[entity] = struct{}{}
		expanded = append(expanded, entity)
		queue = append(queue, s.dependencies[entity]...)
	}

	return expanded
}

func (s *cacheState) invalidateAll() {
	s.mu.Lock()
	s.globalGeneration++
	s.mu.Unlock()
}

func (s *cacheState) generation(
	entity string,
) generation {
	return generation{
		global: s.globalGeneration,
		entity: s.entityGenerations[entity],
	}
}

type queryCapture struct {
	key queryKey
}

type cachedResult struct {
	batches []*providerv1.TupleBatch
	size    int64
}

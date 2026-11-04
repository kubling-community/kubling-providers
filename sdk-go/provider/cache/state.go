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

	store               *memoryStore
	globalGeneration    uint64
	entityGenerations   map[string]uint64
	maxEntryBytes       int64
	dependencies        map[string][]string
	branchInvalidations map[string]branchInvalidation
}

type branchInvalidation struct {
	entities []string
	all      bool
}

func newCacheState(config normalizedConfig) *cacheState {
	return &cacheState{
		store: newMemoryStore(
			config.maxEntries,
			config.maxBytes,
			config.ttl,
		),
		entityGenerations:   make(map[string]uint64),
		maxEntryBytes:       config.maxEntryBytes,
		dependencies:        config.dependencies,
		branchInvalidations: make(map[string]branchInvalidation),
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

func (s *cacheState) retainBranchInvalidation(
	branch *providerv1.TransactionBranchId,
	entities map[string]struct{},
	all bool,
) {
	s.mu.Lock()
	defer s.mu.Unlock()

	key := cachedBranchKey(branch)
	previous := s.branchInvalidations[key]
	if all || previous.all {
		s.branchInvalidations[key] = branchInvalidation{all: true}
		return
	}

	merged := make(map[string]struct{}, len(previous.entities)+len(entities))
	for _, entity := range previous.entities {
		merged[entity] = struct{}{}
	}
	for entity := range entities {
		merged[entity] = struct{}{}
	}

	retained := branchInvalidation{}
	if len(merged) > 0 {
		retained.entities = make([]string, 0, len(merged))
		for entity := range merged {
			retained.entities = append(retained.entities, entity)
		}
	}
	s.branchInvalidations[key] = retained
}

func (s *cacheState) resolveBranchInvalidation(
	branch *providerv1.TransactionBranchId,
	invalidate bool,
) {
	key := cachedBranchKey(branch)
	s.mu.Lock()
	pending, found := s.branchInvalidations[key]
	delete(s.branchInvalidations, key)
	s.mu.Unlock()

	if !invalidate {
		return
	}
	if !found || pending.all {
		s.invalidateAll()
		return
	}
	if len(pending.entities) > 0 {
		s.invalidateEntities(pending.entities)
	}
}

func (s *cacheState) discardBranchInvalidation(
	branch *providerv1.TransactionBranchId,
) {
	s.mu.Lock()
	delete(s.branchInvalidations, cachedBranchKey(branch))
	s.mu.Unlock()
}

func cachedBranchKey(branch *providerv1.TransactionBranchId) string {
	return branch.GetResourceManagerId() + "\x00" + string(branch.GetBranchId())
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
	outcome *providerv1.QueryOutcome
	size    int64
}

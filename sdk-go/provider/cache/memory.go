package cache

import (
	"container/list"
	"sync"
	"time"
)

type memoryEntry struct {
	key       queryKey
	result    cachedResult
	expiresAt time.Time
}

type memoryStore struct {
	mu sync.Mutex

	entries    map[queryKey]*list.Element
	recency    *list.List
	bytes      int64
	maxEntries int
	maxBytes   int64
	ttl        time.Duration
	now        func() time.Time
}

func newMemoryStore(
	maxEntries int,
	maxBytes int64,
	ttl time.Duration,
) *memoryStore {
	return &memoryStore{
		entries:    make(map[queryKey]*list.Element),
		recency:    list.New(),
		maxEntries: maxEntries,
		maxBytes:   maxBytes,
		ttl:        ttl,
		now:        time.Now,
	}
}

func (s *memoryStore) get(key queryKey) (cachedResult, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()

	element, found := s.entries[key]
	if !found {
		return cachedResult{}, false
	}

	entry := element.Value.(*memoryEntry)
	if !s.now().Before(entry.expiresAt) {
		s.remove(element)
		return cachedResult{}, false
	}

	s.recency.MoveToFront(element)

	return entry.result, true
}

func (s *memoryStore) set(key queryKey, result cachedResult) {
	if result.size > s.maxBytes {
		return
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	if existing, found := s.entries[key]; found {
		s.remove(existing)
	}

	entry := &memoryEntry{
		key:       key,
		result:    result,
		expiresAt: s.now().Add(s.ttl),
	}
	element := s.recency.PushFront(entry)
	s.entries[key] = element
	s.bytes += result.size

	for len(s.entries) > s.maxEntries || s.bytes > s.maxBytes {
		s.remove(s.recency.Back())
	}
}

func (s *memoryStore) remove(element *list.Element) {
	if element == nil {
		return
	}

	entry := element.Value.(*memoryEntry)
	delete(s.entries, entry.key)
	s.recency.Remove(element)
	s.bytes -= entry.result.size
}

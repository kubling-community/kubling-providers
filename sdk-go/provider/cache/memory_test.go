package cache

import (
	"testing"
	"time"
)

func TestMemoryStoreExpiresAndEvictsLeastRecentlyUsed(t *testing.T) {
	now := time.Date(2026, 8, 5, 12, 0, 0, 0, time.UTC)
	store := newMemoryStore(2, 10, time.Minute)
	store.now = func() time.Time { return now }

	first := testQueryKey(1)
	second := testQueryKey(2)
	third := testQueryKey(3)

	store.set(first, cachedResult{size: 4})
	store.set(second, cachedResult{size: 4})
	if _, found := store.get(first); !found {
		t.Fatal("first entry was not cached")
	}

	store.set(third, cachedResult{size: 4})
	if _, found := store.get(second); found {
		t.Fatal("least recently used entry was not evicted")
	}
	if _, found := store.get(first); !found {
		t.Fatal("recently used entry was evicted")
	}
	if _, found := store.get(third); !found {
		t.Fatal("new entry was not cached")
	}

	now = now.Add(time.Minute)
	if _, found := store.get(first); found {
		t.Fatal("expired entry was returned")
	}
}

func TestMemoryStoreRejectsOversizedEntry(t *testing.T) {
	store := newMemoryStore(2, 10, time.Minute)
	key := testQueryKey(1)

	store.set(key, cachedResult{size: 11})

	if _, found := store.get(key); found {
		t.Fatal("oversized entry was cached")
	}
}

func testQueryKey(value byte) queryKey {
	key := queryKey{entity: "4:TASK"}
	key.digest[0] = value

	return key
}

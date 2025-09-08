package lru_cache

import (
	"container/list"
	"errors"
	"fmt"
	block_location "hunddb/model/block_location"
	"sync"
	"testing"
)

var (
	ErrKeyNotFound = errors.New("key not found")
)

// TODO: Add capacity of lru to config, validating it and setting the default value

// LRUCache is a generic Least Recently Used cache implementation (Used for read path cache and for block manager)
// K is the key type (string for records, disk location for blocks)
// V is the value type (any value for records, block data for disk blocks)
type LRUCache[K string | block_location.BlockLocation, V any] struct {
	capacity uint32
	size     uint32

	// Map for O(1) access: key -> list element
	cache_map map[K]*list.Element

	// Doubly linked list to maintain LRU order of:
	// Front = Most Recently Used, Back = Least Recently Used
	cache_list *list.List

	mutex sync.RWMutex
}

// listItem represents what we store in the list elements
type listItem[K comparable, V any] struct {
	key   K
	value V
}

// NewLRUCache creates a new LRU cache with the specified capacity
func NewLRUCache[K string | block_location.BlockLocation, V any](capacity uint32) *LRUCache[K, V] {
	return &LRUCache[K, V]{
		capacity:   capacity,
		size:       0,
		cache_map:  make(map[K]*list.Element),
		cache_list: list.New(),
	}
}

// Get retrieves a value from the cache and marks it as recently used
func (lru *LRUCache[K, V]) Get(key K) (V, error) {
	lru.mutex.Lock()
	defer lru.mutex.Unlock()

	node, exists := lru.cache_map[key]
	if !exists {
		var emptyVal V
		return emptyVal, ErrKeyNotFound
	}
	lru.cache_list.MoveToFront(node)
	return node.Value.(*listItem[K, V]).value, nil
}

// Put adds or updates a key-value pair in the cache
func (lru *LRUCache[K, V]) Put(key K, value V) error {
	lru.mutex.Lock()
	defer lru.mutex.Unlock()

	node, exists := lru.cache_map[key]

	if exists {
		// Update existing item and move to front
		node.Value.(*listItem[K, V]).value = value
		lru.cache_list.MoveToFront(node)
		return nil
	}

	if lru.size >= lru.capacity {
		// Remove least recently used item
		backElement := lru.cache_list.Back()
		delete(lru.cache_map, backElement.Value.(*listItem[K, V]).key)
		lru.cache_list.Remove(backElement)
		lru.size--
	}

	// Create new item and add to front
	newItem := &listItem[K, V]{
		key:   key,
		value: value,
	}
	node = lru.cache_list.PushFront(newItem)
	lru.cache_map[key] = node
	lru.size++
	return nil
}

// Remove removes a key from the cache
func (lru *LRUCache[K, V]) Remove(key K) error {
	lru.mutex.Lock()
	defer lru.mutex.Unlock()

	element, exists := lru.cache_map[key]
	if !exists {
		return ErrKeyNotFound
	}
	lru.cache_list.Remove(element)
	delete(lru.cache_map, key)
	lru.size--
	return nil
}

// Size returns the current number of items in the cache
func (lru *LRUCache[K, V]) Size() uint32 {
	lru.mutex.RLock()
	defer lru.mutex.RUnlock()

	return lru.size
}

// Capacity returns the maximum capacity of the cache
func (lru *LRUCache[K, V]) Capacity() uint32 {
	lru.mutex.RLock()
	defer lru.mutex.RUnlock()

	return lru.capacity
}

// Contains checks if a key exists in the cache without affecting its position
func (lru *LRUCache[K, V]) Contains(key K) bool {
	lru.mutex.RLock()
	defer lru.mutex.RUnlock()

	_, exists := lru.cache_map[key]
	return exists
}

// Peek gets a value without marking it as recently used
func (lru *LRUCache[K, V]) Peek(key K) (V, error) {
	lru.mutex.RLock()
	defer lru.mutex.RUnlock()

	element, exists := lru.cache_map[key]
	if !exists {
		var emptyValue V
		return emptyValue, ErrKeyNotFound
	}

	item := element.Value.(*listItem[K, V])
	return item.value, nil
}

// TestLRUCache_Concurrency stress-tests the cache with concurrent reads and writes.
// Run this test with the -race flag to detect race conditions.
func TestLRUCache_Concurrency(t *testing.T) {
	cache := NewLRUCache[string, int](10)

	var wg sync.WaitGroup
	numGoroutines := 100
	itemsPerGoroutine := 50

	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)
		go func(goroutineID int) {
			defer wg.Done()
			for j := 0; j < itemsPerGoroutine; j++ {
				key := fmt.Sprintf("key-%d-%d", goroutineID, j)
				value := goroutineID*1000 + j

				// Perform a write
				err := cache.Put(key, value)
				if err != nil {
					t.Errorf("Goroutine %d failed to put key %s: %v", goroutineID, key, err)
					return
				}

				// Perform a read
				retrieved, err := cache.Get(key)
				if err != nil {
					// It's possible the key was evicted by another goroutine, so an error isn't a failure.
					// We just want to ensure there are no panics or race conditions.
					continue
				}
				if retrieved != value {
					t.Errorf("Goroutine %d got incorrect value for key %s", goroutineID, key)
				}
			}
		}(i)
	}

	wg.Wait()
}

func (lru *LRUCache[K, V]) SetCapacity(newCapacity uint32) {
	lru.mutex.Lock()
	defer lru.mutex.Unlock()
	lru.capacity = newCapacity
}

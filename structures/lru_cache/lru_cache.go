package lru_cache

import (
	"container/list"
	"errors"
)

var (
	ErrKeyNotFound = errors.New("key not found")
)

// TODO: Figure out the type of the block disk location for the key (string | disk location).

// TODO: Add capacity of lru to config, validating it and setting the default value

// LRUCache is a generic Least Recently Used cache implementation (Used for read path and for block manager)
// K is the key type (string for records, disk location for blocks)
// V is the value type (any value for records, block data for disk blocks)
type LRUCache[K comparable, V any] struct {
	capacity uint32
	size     uint32

	// Map for O(1) access: key -> list element
	cache_map map[K]*list.Element

	// Doubly linked list to maintain LRU order of:
	// Front = Most Recently Used, Back = Least Recently Used
	cache_list *list.List
}

// listItem represents what we store in the list elements
type listItem[K comparable, V any] struct {
	key   K
	value V
}

// NewLRUCache creates a new LRU cache with the specified capacity
func NewLRUCache[K comparable, V any](capacity uint32) *LRUCache[K, V] {
	return &LRUCache[K, V]{
		capacity:   capacity,
		size:       0,
		cache_map:  make(map[K]*list.Element),
		cache_list: list.New(),
	}
}

// Get retrieves a value from the cache and marks it as recently used
func (lru *LRUCache[K, V]) Get(key K) (V, error) {
	node, exists := lru.cache_map[key]
	if !exists {
		var zero V
		return zero, ErrKeyNotFound
	}
	lru.cache_list.MoveToFront(node)
	return node.Value.(*listItem[K, V]).value, nil
}

// Put adds or updates a key-value pair in the cache
func (lru *LRUCache[K, V]) Put(key K, value V) error {
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
	return lru.size
}

// Capacity returns the maximum capacity of the cache
func (lru *LRUCache[K, V]) Capacity() uint32 {
	return lru.capacity
}

// Contains checks if a key exists in the cache without affecting its position
func (lru *LRUCache[K, V]) Contains(key K) bool {
	_, exists := lru.cache_map[key]
	return exists
}

// Peek gets a value without marking it as recently used
func (lru *LRUCache[K, V]) Peek(key K) (V, error) {
	element, exists := lru.cache_map[key]
	if !exists {
		var zero V
		return zero, ErrKeyNotFound
	}

	item := element.Value.(*listItem[K, V])
	return item.value, nil
}

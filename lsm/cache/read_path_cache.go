package cache

import (
	lru_cache "hunddb/lsm/lru_cache"
	model "hunddb/model/record"
	"hunddb/utils/config"
)

// Configuration variables loaded from config file
var (
	READ_PATH_CACHE_CAPACITY uint64 
)

// init loads Cache configuration from config file
func init() {
	cfg := config.GetConfig()
	// Always use config - no fallbacks here
	READ_PATH_CACHE_CAPACITY = cfg.Cache.ReadPathCapacity
}

// ReadPathCache wraps the LRU cache for the read path
// This cache stores actual key-value pairs read from SSTables
type ReadPathCache struct {
	cache *lru_cache.LRUCache[string, *model.Record]
}

// NewReadPathCache creates a new cache for the read path
func NewReadPathCache() *ReadPathCache {
	return &ReadPathCache{
		cache: lru_cache.NewLRUCache[string, *model.Record](uint32(READ_PATH_CACHE_CAPACITY)),
	}
}

// Get retrieves a value from the cache
func (rpc *ReadPathCache) Get(key string) (*model.Record, error) {
	return rpc.cache.Get(key)
}

// Put stores a value in the cache
func (rpc *ReadPathCache) Put(key string, value *model.Record) error {
	return rpc.cache.Put(key, value)
}

// Remove removes a key from the cache
func (rpc *ReadPathCache) Remove(key string) error {
	return rpc.cache.Remove(key)
}

// Contains checks if a key exists without affecting LRU order
func (rpc *ReadPathCache) Contains(key string) bool {
	return rpc.cache.Contains(key)
}

// Invalidate removes a key from cache (used when data is updated/deleted)
func (rpc *ReadPathCache) Invalidate(key string) {
	rpc.cache.Remove(key) // Ignore error if key doesn't exist
}

// Size returns current cache size
func (rpc *ReadPathCache) Size() uint32 {
	return rpc.cache.Size()
}

// Capacity returns cache capacity
func (rpc *ReadPathCache) Capacity() uint32 {
	return rpc.cache.Capacity()
}

func (rpc *ReadPathCache) SetCapacity(newCapacity uint32) {
	rpc.cache.SetCapacity(newCapacity)
}

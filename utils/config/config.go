package config

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"sync"
)

// DBConfig holds all database configuration parameters
type DBConfig struct {
	LSM struct {
		MaxLevels         uint64 `json:"max_levels"`
		MaxTablesPerLevel uint64 `json:"max_tables_per_level"`
		MaxMemtables      uint64 `json:"max_memtables"`
		CompactionType    string `json:"compaction_type"`
		LSMPath           string `json:"lsm_path"`
	} `json:"lsm"`

	Cache struct {
		ReadPathCapacity uint64 `json:"read_path_capacity"`
	} `json:"cache"`

	WAL struct {
		BlockSize uint64 `json:"block_size"`
		LogSize   uint64 `json:"log_size"`
	} `json:"wal"`

	SSTable struct {
		CompressionEnabled bool   `json:"compression_enabled"`
		UseSeparateFiles   bool   `json:"use_separate_files"`
		SparseStepIndex    uint64 `json:"sparse_step_index"`
	} `json:"sstable"`

	Memtable struct {
		Capacity     uint64 `json:"capacity"`
		MemtableType string `json:"memtable_type"` // "btree", "skiplist", "hashmap"
	} `json:"memtable"`

	BloomFilter struct {
		FalsePositiveRate float64 `json:"false_positive_rate"`
	} `json:"bloom_filter"`

	BlockManager struct {
		BlockSize uint64 `json:"block_size"`
		CacheSize uint64 `json:"cache_size"`
	} `json:"block_manager"`

	CRC struct {
		Size uint64 `json:"size"`
	} `json:"crc"`

	TokenBucket struct {
		Capacity       uint16 `json:"capacity"`
		RefillInterval uint   `json:"refill_interval"`
		RefillAmount   uint16 `json:"refill_amount"`
	} `json:"token_bucket"`
}

var (
	instance *DBConfig
	once     sync.Once
)

// GetConfig returns the singleton config instance
func GetConfig() *DBConfig {
	once.Do(func() {
		instance = loadConfig()
	})
	return instance
}

// loadConfig loads configuration from JSON file or creates default
func loadConfig() *DBConfig {
	// Get absolute path to this source file's directory (utils/config/)
	_, filename, _, _ := runtime.Caller(0)
	configDir := filepath.Dir(filename)
	configPath := filepath.Join(configDir, "app.json")

	// Check if config file exists
	if _, err := os.Stat(configPath); os.IsNotExist(err) {
		// Create default config
		defaultConfig := getDefaultConfig()
		saveConfigToFile(defaultConfig, configPath)
		return defaultConfig
	}

	// Read existing config file
	data, err := os.ReadFile(configPath)
	if err != nil {
		fmt.Printf("Warning: Failed to read config file, using defaults: %v\n", err)
		return getDefaultConfig()
	}

	// Parse JSON
	var config DBConfig
	if err := json.Unmarshal(data, &config); err != nil {
		fmt.Printf("Warning: Failed to parse config file, using defaults: %v\n", err)
		return getDefaultConfig()
	}

	return &config
}

// getDefaultConfig returns default configuration values
func getDefaultConfig() *DBConfig {
	config := &DBConfig{}

	// LSM defaults
	config.LSM.MaxLevels = 7
	config.LSM.MaxTablesPerLevel = 4
	config.LSM.MaxMemtables = 4
	config.LSM.CompactionType = "size"
	config.LSM.LSMPath = "lsm.db"

	// Cache defaults
	config.Cache.ReadPathCapacity = 1000

	// WAL defaults
	config.WAL.LogSize = 16

	// SSTable defaults
	config.SSTable.CompressionEnabled = true
	config.SSTable.UseSeparateFiles = true
	config.SSTable.SparseStepIndex = 10

	// Memtable defaults
	config.Memtable.Capacity = 1000
	config.Memtable.MemtableType = "btree" // btree, skiplist, hashmap

	// BloomFilter defaults
	config.BloomFilter.FalsePositiveRate = 0.01 // 1%

	// BlockManager defaults
	config.BlockManager.BlockSize = 4096 // 4KB
	config.BlockManager.CacheSize = 100

	// CRC defaults
	config.CRC.Size = 4

	// TokenBucket defaults
	config.TokenBucket.Capacity = 10
	config.TokenBucket.RefillInterval = 20
	config.TokenBucket.RefillAmount = 1

	return config
}

// saveConfigToFile saves config to JSON file
func saveConfigToFile(config *DBConfig, filePath string) error {
	// Create directory if it doesn't exist
	dir := filepath.Dir(filePath)
	if err := os.MkdirAll(dir, 0755); err != nil {
		return fmt.Errorf("failed to create config directory: %v", err)
	}

	// Marshal to JSON with indentation
	data, err := json.MarshalIndent(config, "", "  ")
	if err != nil {
		return fmt.Errorf("failed to marshal config: %v", err)
	}

	// Write to file
	if err := os.WriteFile(filePath, data, 0644); err != nil {
		return fmt.Errorf("failed to write config file: %v", err)
	}

	return nil
}

// UpdateConfig updates the configuration (for future use)
func UpdateConfig(newConfig *DBConfig) error {
	configPath := "config/app.json"

	// Validate config (basic validation)
	if err := validateConfig(newConfig); err != nil {
		return err
	}

	// Save to file
	if err := saveConfigToFile(newConfig, configPath); err != nil {
		return err
	}

	// Update singleton instance (for hot reload)
	instance = newConfig

	return nil
}

// validateConfig performs basic validation on config values
func validateConfig(config *DBConfig) error {
	if config.LSM.MaxLevels < 1 {
		return fmt.Errorf("max_levels must be at least 1")
	}
	if config.LSM.MaxTablesPerLevel < 1 {
		return fmt.Errorf("max_tables_per_level must be at least 1")
	}
	if config.LSM.MaxMemtables < 1 {
		return fmt.Errorf("max_memtables must be at least 1")
	}
	if config.LSM.CompactionType != "size" && config.LSM.CompactionType != "level" {
		return fmt.Errorf("compaction_type must be either 'size' or 'leveled'")
	}
	if config.LSM.LSMPath == "" {
		return fmt.Errorf("lsm_path cannot be empty")
	}
	if config.WAL.LogSize < 1 {
		return fmt.Errorf("wal_log_size must be at least 1")
	}

	// SSTable validation
	if config.SSTable.SparseStepIndex < 1 {
		return fmt.Errorf("sparse_step_index must be at least 1")
	}

	// Memtable validation
	if config.Memtable.Capacity < 1 {
		return fmt.Errorf("memtable_capacity must be at least 1")
	}
	memtableTypes := []string{"btree", "skiplist", "hashmap"}
	validType := false
	for _, t := range memtableTypes {
		if config.Memtable.MemtableType == t {
			validType = true
			break
		}
	}
	if !validType {
		return fmt.Errorf("memtable_type must be one of: btree, skiplist, hashmap")
	}

	// BloomFilter validation
	if config.BloomFilter.FalsePositiveRate <= 0 || config.BloomFilter.FalsePositiveRate >= 1 {
		return fmt.Errorf("false_positive_rate must be between 0 and 1")
	}

	// BlockManager validation
	if config.BlockManager.BlockSize < 1024 {
		return fmt.Errorf("block_manager_block_size must be at least 1024")
	}

	// CRC validation
	if config.CRC.Size < 1 {
		return fmt.Errorf("crc_size must be at least 1")
	}

	return nil
}

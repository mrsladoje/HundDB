package config

import (
	"os"
	"testing"
)

func TestGetConfig(t *testing.T) {
	// Clean up any existing config
	os.Remove("config/app.json")
	defer os.RemoveAll("config")

	// Get config (should create default)
	cfg := GetConfig()

	// Test default values
	if cfg.LSM.MaxLevels != 7 {
		t.Errorf("Expected MaxLevels to be 7, got %d", cfg.LSM.MaxLevels)
	}

	if cfg.LSM.CompactionType != "size" {
		t.Errorf("Expected CompactionType to be 'size', got %s", cfg.LSM.CompactionType)
	}

	if cfg.Cache.ReadPathCapacity != 1000 {
		t.Errorf("Expected ReadPathCapacity to be 1000, got %d", cfg.Cache.ReadPathCapacity)
	}

	if cfg.BlockManager.BlockSize != 4096 {
		t.Errorf("Expected BlockManager BlockSize to be 4096, got %d", cfg.BlockManager.BlockSize)
	}
}

func TestUpdateConfig(t *testing.T) {
	// Clean up
	os.Remove("config/app.json")
	defer os.RemoveAll("config")

	// Create new config with different values
	newConfig := getDefaultConfig()
	newConfig.LSM.MaxLevels = 10
	newConfig.Cache.ReadPathCapacity = 2000
	newConfig.BlockManager.BlockSize = 8192

	// Update config
	err := UpdateConfig(newConfig)
	if err != nil {
		t.Errorf("Failed to update config: %v", err)
	}

	// Get updated config
	cfg := GetConfig()
	if cfg.LSM.MaxLevels != 10 {
		t.Errorf("Expected updated MaxLevels to be 10, got %d", cfg.LSM.MaxLevels)
	}

	if cfg.Cache.ReadPathCapacity != 2000 {
		t.Errorf("Expected updated ReadPathCapacity to be 2000, got %d", cfg.Cache.ReadPathCapacity)
	}
}

func TestValidateConfig(t *testing.T) {
	// Test invalid config
	invalidConfig := getDefaultConfig()
	invalidConfig.LSM.MaxLevels = 0 // Invalid

	err := validateConfig(invalidConfig)
	if err == nil {
		t.Error("Expected validation error for MaxLevels = 0")
	}

	// Test valid config
	validConfig := getDefaultConfig()
	err = validateConfig(validConfig)
	if err != nil {
		t.Errorf("Expected valid config to pass validation, got error: %v", err)
	}
}

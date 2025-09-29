package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"hunddb/lsm"
	model "hunddb/model/record"
	"os"
	"path/filepath"
	"strings"

	"github.com/wailsapp/wails/v2/pkg/runtime"
)

var (
	ErrKeyNotFound = errors.New("key not found")
)

// App struct - application layer wrapper for LSM
type App struct {
	ctx context.Context
	lsm *lsm.LSM
}

// NewApp creates a new App application struct and loads the LSM instance
func NewApp() *App {
	lsmInstance := lsm.LoadLSM()

	return &App{
		lsm: lsmInstance,
	}
}

// startup is called when the app starts up
func (a *App) startup(ctx context.Context) {
	a.ctx = ctx
	runtime.WindowMaximise(a.ctx)
}

/*
Helper method to convert Record to a serializable map.
We use this because wails has trouble serializing complex types directly.
*/
func (a *App) recordToMap(record *model.Record) map[string]interface{} {
	if record == nil {
		return nil
	}

	return map[string]interface{}{
		"key":       record.Key,
		"value":     string(record.Value),
		"timestamp": record.Timestamp,
		"deleted":   record.Tombstone,
	}
}

// CheckExistingData checks if there are existing database files with actual data
func (a *App) CheckExistingData() (bool, error) {
	// Get current config to check paths
	configData, err := a.GetConfig()
	if err != nil {
		return false, err
	}

	var config map[string]interface{}
	if err := json.Unmarshal([]byte(configData), &config); err != nil {
		return false, err
	}

	// Check LSM path - only if file exists AND has size > 0
	lsmConfig, ok := config["lsm"].(map[string]interface{})
	if ok {
		if lsmPath, exists := lsmConfig["lsm_path"].(string); exists && lsmPath != "" {
			if stat, err := os.Stat(lsmPath); err == nil && stat.Size() > 0 {
				return true, nil
			}
		}
	}

	// Check WAL path - only if file exists AND has size > 0
	walConfig, ok := config["wal"].(map[string]interface{})
	if ok {
		if walPath, exists := walConfig["wal_path"].(string); exists && walPath != "" {
			if stat, err := os.Stat(walPath); err == nil && stat.Size() > 0 {
				return true, nil
			}
		}
	}

	// Check for SSTable files with actual data
	if lsmConfig != nil {
		if lsmPath, exists := lsmConfig["lsm_path"].(string); exists && lsmPath != "" {
			dir := filepath.Dir(lsmPath)
			if entries, err := os.ReadDir(dir); err == nil {
				for _, entry := range entries {
					if !entry.IsDir() {
						name := entry.Name()
						// Check for database files with actual size
						if strings.HasSuffix(name, ".sst") ||
							strings.HasSuffix(name, ".db") ||
							strings.Contains(name, "lsm") {
							fullPath := filepath.Join(dir, name)
							if stat, err := os.Stat(fullPath); err == nil && stat.Size() > 0 {
								return true, nil
							}
						}
					}
				}
			}
		}
	}

	return false, nil
}

// LSM Wrapper Methods - Convert complex types to Wails-compatible types

// Get retrieves a record by key from the LSM
func (a *App) Get(key string) (map[string]interface{}, error) {
	record, errorEncounteredInCheck, isErrorEncountered := a.lsm.Get(key)
	var err error
	err = nil
	if isErrorEncountered {
		if isKeyNotFoundError(errorEncounteredInCheck) {
			// Key not found is not considered an error in this context
			return nil, nil
		}

		err = errorEncounteredInCheck
	}
	if record == nil {
		return nil, err
	}

	return a.recordToMap(record), nil
}

// Put stores a key-value pair in the LSM
func (a *App) Put(key string, value string) error {
	valueBytes := []byte(value)

	err := a.lsm.Put(key, valueBytes)
	if err != nil {
		return fmt.Errorf("error storing record: %v", err)
	}

	return nil
}

// Delete deletes a key in the LSM
func (a *App) Delete(key string) (bool, error) {
	record, err := a.Get(key)
	if (err == nil && record == nil) || record["deleted"] == true {
		return false, nil
	}
	keyExists, err := a.lsm.Delete(key)
	if err != nil {
		return false, fmt.Errorf("error deleting record: %v", err)
	}
	return keyExists, nil
}

// IsDataLost checks if data was lost during LSM loading
func (a *App) IsDataLost() bool {
	return a.lsm.IsDataLost()
}

// PersistLSM manually triggers LSM persistence
func (a *App) PersistLSM() error {
	return a.lsm.PersistLSM()
}

// PrefixScan scans for keys with the given prefix using pagination
func (a *App) PrefixScan(prefix string, pageSize int, pageNumber int) ([]string, error) {
	if pageSize <= 0 {
		pageSize = 5 // Default page size
	}
	if pageNumber < 0 {
		pageNumber = 0 // Default to first page
	}

	keys, err := a.lsm.PrefixScan(prefix, pageSize, pageNumber)
	if err != nil {
		return nil, fmt.Errorf("error scanning prefix '%s': %v", prefix, err)
	}

	return keys, nil
}

// RangeScan scans for keys within the given range using pagination
func (a *App) RangeScan(rangeStart string, rangeEnd string, pageSize int, pageNumber int) ([]string, error) {
	if pageSize <= 0 {
		pageSize = 5 // Default page size
	}
	if pageNumber < 0 {
		pageNumber = 0 // Default to first page
	}

	keys, err := a.lsm.RangeScan(rangeStart, rangeEnd, pageSize, pageNumber)
	if err != nil {
		return nil, fmt.Errorf("error scanning range [%s, %s): %v", rangeStart, rangeEnd, err)
	}

	return keys, nil
}

// PrefixIterate retrieves the next record for a given prefix and key
func (a *App) PrefixIterate(prefix string, key string) (map[string]interface{}, error) {
	record, err := a.lsm.GetNextForPrefix(prefix, key)
	if err != nil {
		return nil, err
	}
	return a.recordToMap(record), nil
}

func (a *App) RangeIterate(rangeStart string, rangeEnd string, key string) (map[string]interface{}, error) {
	record, err := a.lsm.GetNextForRange(rangeStart, rangeEnd, key)
	if err != nil {
		return nil, err
	}
	return a.recordToMap(record), nil
}

// GetSSTableLevels returns the current SSTable level structure from the LSM
func (a *App) GetSSTableLevels() [][]int {
	return a.lsm.GetLevels()
}

// GetSSTableStats returns statistics about the SSTable structure
func (a *App) GetSSTableStats() map[string]interface{} {
	levels := a.lsm.GetLevels()

	totalSSTables := 0
	maxTablesPerLevel := 0

	for _, level := range levels {
		totalSSTables += len(level)
		if len(level) > maxTablesPerLevel {
			maxTablesPerLevel = len(level)
		}
	}

	return map[string]interface{}{
		"totalLevels":       len(levels),
		"totalSSTables":     totalSSTables,
		"maxTablesPerLevel": maxTablesPerLevel,
		"levelDetails":      levels,
	}
}

// Helper function to check if an error is or contains ErrKeyNotFound
func isKeyNotFoundError(err error) bool {
	if err == nil {
		return false
	}

	// Direct comparison
	if errors.Is(err, ErrKeyNotFound) {
		return true
	}

	// Check if the error message contains the ErrKeyNotFound message
	// This handles cases like "some context: key not found"
	return strings.Contains(err.Error(), ErrKeyNotFound.Error())
}

// SaveConfig saves the provided configuration to app.json file
// configJSON: JSON string containing the complete configuration
func (a *App) SaveConfig(configJSON string) error {
	configPath := "utils/config/app.json"

	// Validate JSON format by trying to parse it
	var config map[string]interface{}
	if err := json.Unmarshal([]byte(configJSON), &config); err != nil {
		return fmt.Errorf("invalid JSON format: %w", err)
	}

	// Write the configuration to file
	if err := os.WriteFile(configPath, []byte(configJSON), 0644); err != nil {
		return fmt.Errorf("failed to write config file: %w", err)
	}

	return nil
}

// GetConfig retrieves the current configuration from app.json file
func (a *App) GetConfig() (string, error) {
	configPath := "utils/config/app.json"

	data, err := os.ReadFile(configPath)
	if err != nil {
		return "", fmt.Errorf("failed to read config file: %w", err)
	}

	return string(data), nil
}

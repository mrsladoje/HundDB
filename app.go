package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"hunddb/lsm"
	"hunddb/lsm/sstable"
	model "hunddb/model/record"
	"hunddb/probabilistic/count_min_sketch"
	"hunddb/probabilistic/hyperloglog"
	"hunddb/probabilistic/independent_bloom_filter"
	"hunddb/probabilistic/sim_hash"
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

// CheckSSTableIntegrity checks the integrity of a specific SSTable
func (a *App) CheckSSTableIntegrity(sstableIndex int) map[string]interface{} {
	passed, corruptBlocks, fatalError, err := sstable.CheckIntegrity(sstableIndex)

	// Convert corrupt blocks to a format that Wails can handle
	corruptBlocksData := make([]map[string]interface{}, len(corruptBlocks))
	for i, block := range corruptBlocks {
		corruptBlocksData[i] = map[string]interface{}{
			"filePath":   block.FilePath,
			"blockIndex": block.BlockIndex,
		}
	}

	result := map[string]interface{}{
		"sstableIndex":  sstableIndex,
		"passed":        passed,
		"corruptBlocks": corruptBlocksData,
		"fatalError":    fatalError,
		"error":         nil,
		"timestamp":     "now", // Will be set by frontend
	}

	if err != nil {
		result["error"] = err.Error()
	}

	return result
}

// ======== PROBABILISTIC DATA STRUCTURES ========

// Count-Min Sketch Functions
func (a *App) CreateCountMinSketch(name string, epsilon, delta float64) (string, error) {
	cms := count_min_sketch.NewCMS(epsilon, delta)
	if err := cms.SaveToDisk(name); err != nil {
		return "", fmt.Errorf("failed to save Count-Min Sketch: %w", err)
	}
	return fmt.Sprintf("Count-Min Sketch '%s' created successfully (Epsilon: %.4f, Delta: %.4f)", name, epsilon, delta), nil
}

func (a *App) LoadCountMinSketch(name string) (string, error) {
	_, err := count_min_sketch.LoadCountMinSketchFromDisk(name)
	if err != nil {
		return "", fmt.Errorf("failed to load Count-Min Sketch: %w", err)
	}
	return fmt.Sprintf("Count-Min Sketch '%s' loaded successfully", name), nil
}

func (a *App) AddToCountMinSketch(name, item string) (string, error) {
	cms, err := count_min_sketch.LoadCountMinSketchFromDisk(name)
	if err != nil {
		return "", fmt.Errorf("failed to load Count-Min Sketch: %w", err)
	}

	cms.Add([]byte(item))

	if err := cms.SaveToDisk(name); err != nil {
		return "", fmt.Errorf("failed to save Count-Min Sketch: %w", err)
	}

	return fmt.Sprintf("Added '%s' to Count-Min Sketch '%s'", item, name), nil
}

func (a *App) QueryCountMinSketch(name, item string) (int, error) {
	cms, err := count_min_sketch.LoadCountMinSketchFromDisk(name)
	if err != nil {
		return 0, fmt.Errorf("failed to load Count-Min Sketch: %w", err)
	}

	count := cms.Count([]byte(item))
	return int(count), nil
}

// HyperLogLog Functions
func (a *App) CreateHyperLogLog(name string, precision int) (string, error) {
	hll, err := hyperloglog.NewHLL(uint8(precision))
	if err != nil {
		return "", fmt.Errorf("failed to create HyperLogLog: %w", err)
	}
	if err := hll.SaveToDisk(name); err != nil {
		return "", fmt.Errorf("failed to save HyperLogLog: %w", err)
	}
	return fmt.Sprintf("HyperLogLog '%s' created successfully (Precision: %d)", name, precision), nil
}

func (a *App) LoadHyperLogLog(name string) (string, error) {
	_, err := hyperloglog.LoadHyperLogLogFromDisk(name)
	if err != nil {
		return "", fmt.Errorf("failed to load HyperLogLog: %w", err)
	}
	return fmt.Sprintf("HyperLogLog '%s' loaded successfully", name), nil
}

func (a *App) AddToHyperLogLog(name, item string) (string, error) {
	hll, err := hyperloglog.LoadHyperLogLogFromDisk(name)
	if err != nil {
		return "", fmt.Errorf("failed to load HyperLogLog: %w", err)
	}

	hll.Add([]byte(item))

	if err := hll.SaveToDisk(name); err != nil {
		return "", fmt.Errorf("failed to save HyperLogLog: %w", err)
	}

	return fmt.Sprintf("Added '%s' to HyperLogLog '%s'", item, name), nil
}

func (a *App) EstimateHyperLogLog(name string) (int, error) {
	hll, err := hyperloglog.LoadHyperLogLogFromDisk(name)
	if err != nil {
		return 0, fmt.Errorf("failed to load HyperLogLog: %w", err)
	}

	estimate := hll.Estimate()
	return int(estimate), nil
}

// SimHash Functions
func (a *App) ComputeSimHashFingerprint(document string) (string, error) {
	fingerprint := sim_hash.NewSimHashFingerprintFromText(document)
	return fingerprint.String(), nil
}

func (a *App) SaveSimHashFingerprint(name string, fingerprint string) (string, error) {
	fp := &sim_hash.SimHashFingerprint{}
	if err := fp.UnmarshalText([]byte(fingerprint)); err != nil {
		return "", fmt.Errorf("failed to parse fingerprint: %w", err)
	}

	if err := fp.SaveToDisk(name); err != nil {
		return "", fmt.Errorf("failed to save SimHash fingerprint: %w", err)
	}

	return fmt.Sprintf("SimHash fingerprint '%s' saved successfully", name), nil
}

func (a *App) LoadSimHashFingerprint(name string) (string, error) {
	fp := &sim_hash.SimHashFingerprint{}
	if err := fp.LoadFromDisk(name); err != nil {
		return "", fmt.Errorf("failed to load SimHash fingerprint: %w", err)
	}

	return fp.String(), nil
}

func (a *App) CompareSimHashFingerprints(fp1, fp2 string) (int, error) {
	fingerprint1 := &sim_hash.SimHashFingerprint{}
	if err := fingerprint1.UnmarshalText([]byte(fp1)); err != nil {
		return 0, fmt.Errorf("failed to parse first fingerprint: %w", err)
	}

	fingerprint2 := &sim_hash.SimHashFingerprint{}
	if err := fingerprint2.UnmarshalText([]byte(fp2)); err != nil {
		return 0, fmt.Errorf("failed to parse second fingerprint: %w", err)
	}

	distance := fingerprint1.HammingDistance(*fingerprint2)
	return int(distance), nil
}

func (a *App) CompareDocumentsSimilarity(document1, document2 string) (map[string]interface{}, error) {
	// Compute fingerprints for both documents
	fingerprint1 := sim_hash.NewSimHashFingerprintFromText(document1)
	fingerprint2 := sim_hash.NewSimHashFingerprintFromText(document2)

	// Calculate Hamming distance
	distance := fingerprint1.HammingDistance(fingerprint2)

	// Determine similarity based on common thresholds
	var similarity string
	var similarityPercentage float64

	if distance == 0 {
		similarity = "Identical"
		similarityPercentage = 100.0
	} else if distance <= 3 {
		similarity = "Very Similar"
		similarityPercentage = 98.0
	} else if distance <= 6 {
		similarity = "Similar"
		similarityPercentage = 90.0
	} else if distance <= 12 {
		similarity = "Somewhat Similar"
		similarityPercentage = 75.0
	} else if distance <= 20 {
		similarity = "Slightly Similar"
		similarityPercentage = 50.0
	} else if distance <= 30 {
		similarity = "Different"
		similarityPercentage = 25.0
	} else {
		similarity = "Very Different"
		similarityPercentage = 5.0
	}

	return map[string]interface{}{
		"distance":             int(distance),
		"similarity":           similarity,
		"similarityPercentage": similarityPercentage,
		"fingerprint1":         fingerprint1.String(),
		"fingerprint2":         fingerprint2.String(),
	}, nil
}

// Bloom Filter Functions
func (a *App) CreateBloomFilter(name string, capacity int, falsePositiveRate float64) (string, error) {
	bf := independent_bloom_filter.NewIndependentBloomFilter(capacity, falsePositiveRate)
	if err := bf.SaveToDisk(name); err != nil {
		return "", fmt.Errorf("failed to save Bloom Filter: %w", err)
	}
	return fmt.Sprintf("Bloom Filter '%s' created successfully (Capacity: %d, FP Rate: %.4f)", name, capacity, falsePositiveRate), nil
}

func (a *App) LoadBloomFilter(name string) (string, error) {
	bf := &independent_bloom_filter.IndependentBloomFilter{}
	if err := bf.LoadFromDisk(name); err != nil {
		return "", fmt.Errorf("failed to load Bloom Filter: %w", err)
	}
	return fmt.Sprintf("Bloom Filter '%s' loaded successfully", name), nil
}

func (a *App) AddToBloomFilter(name, item string) (string, error) {
	bf := &independent_bloom_filter.IndependentBloomFilter{}
	if err := bf.LoadFromDisk(name); err != nil {
		return "", fmt.Errorf("failed to load Bloom Filter: %w", err)
	}

	bf.Add([]byte(item))

	if err := bf.SaveToDisk(name); err != nil {
		return "", fmt.Errorf("failed to save Bloom Filter: %w", err)
	}

	return fmt.Sprintf("Added '%s' to Bloom Filter '%s'", item, name), nil
}

func (a *App) TestBloomFilter(name, item string) (bool, error) {
	bf := &independent_bloom_filter.IndependentBloomFilter{}
	if err := bf.LoadFromDisk(name); err != nil {
		return false, fmt.Errorf("failed to load Bloom Filter: %w", err)
	}

	exists := bf.Contains([]byte(item))
	return exists, nil
}

package sstable

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	record "hunddb/model/record"
)

// Test helper functions

func createTestRecords(count int) []record.Record {
	records := make([]record.Record, count)
	for i := 0; i < count; i++ {
		records[i] = *record.NewRecord(
			fmt.Sprintf("key_%03d", i),
			[]byte(fmt.Sprintf("value_%03d", i)),
			uint64(time.Now().Unix())+uint64(i),
			false,
			false,
		)
	}
	return records
}

func createTestRecordsWithTombstones(count int) []record.Record {
	records := make([]record.Record, count)
	for i := 0; i < count; i++ {
		tombstone := i%3 == 0 // Every third record is a tombstone
		var value []byte
		if !tombstone {
			value = []byte(fmt.Sprintf("value_%03d", i))
		}

		records[i] = *record.NewRecord(
			fmt.Sprintf("key_%03d", i),
			value,
			uint64(time.Now().Unix())+uint64(i),
			tombstone,
			false,
		)
	}
	return records
}

func createLargeTestRecords(count int) []record.Record {
	records := make([]record.Record, count)
	largeValue := make([]byte, 2048) // 2KB value to test block boundaries
	for i := 0; i < len(largeValue); i++ {
		largeValue[i] = byte(i % 256)
	}

	for i := 0; i < count; i++ {
		records[i] = *record.NewRecord(
			fmt.Sprintf("large_key_%03d", i),
			largeValue,
			uint64(time.Now().Unix())+uint64(i),
			false,
			false,
		)
	}
	return records
}

func setupTestDir(t *testing.T) string {
	tmpDir, err := os.MkdirTemp("", "sstable_test_")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}

	// Change to test directory
	oldDir, _ := os.Getwd()
	os.Chdir(tmpDir)

	t.Cleanup(func() {
		os.Chdir(oldDir)
		os.RemoveAll(tmpDir)
	})

	return tmpDir
}

func fileExists(filepath string) bool {
	_, err := os.Stat(filepath)
	return !os.IsNotExist(err)
}

func getFileSize(filepath string) int64 {
	info, err := os.Stat(filepath)
	if err != nil {
		return 0
	}
	return info.Size()
}

// Test cases

func TestPersistMemtable_BasicFunctionality(t *testing.T) {
	testDir := setupTestDir(t)

	// Save original config values
	originalUseSeparateFiles := USE_SEPARATE_FILES
	originalCompressionEnabled := COMPRESSION_ENABLED
	originalSparseStepIndex := SPARSE_STEP_INDEX

	defer func() {
		USE_SEPARATE_FILES = originalUseSeparateFiles
		COMPRESSION_ENABLED = originalCompressionEnabled
		SPARSE_STEP_INDEX = originalSparseStepIndex
	}()

	// Test with separate files enabled
	USE_SEPARATE_FILES = true
	COMPRESSION_ENABLED = false
	SPARSE_STEP_INDEX = 10

	records := createTestRecords(50)
	err := PersistMemtable(records, 1, 0)

	if err != nil {
		t.Errorf("PersistMemtable failed: %v", err)
	}

	// Verify all component files were created
	expectedFiles := []string{
		"sstable_1.db",          // Config file
		"sstable_1_data.db",     // Data component
		"sstable_1_index.db",    // Index component
		"sstable_1_summary.db",  // Summary component
		"sstable_1_filter.db",   // Filter component
		"sstable_1_metadata.db", // Metadata component
	}

	for _, filename := range expectedFiles {
		filepath := filepath.Join(testDir, filename)
		if !fileExists(filepath) {
			t.Errorf("Expected file %s was not created", filename)
		} else {
			size := getFileSize(filepath)
			if size == 0 {
				t.Errorf("File %s is empty", filename)
			}
			t.Logf("Created file %s with size %d bytes", filename, size)
		}
	}
}

func TestPersistMemtable_SingleFileMode(t *testing.T) {
	testDir := setupTestDir(t)

	// Save original config values
	originalUseSeparateFiles := USE_SEPARATE_FILES
	originalCompressionEnabled := COMPRESSION_ENABLED
	originalSparseStepIndex := SPARSE_STEP_INDEX

	defer func() {
		USE_SEPARATE_FILES = originalUseSeparateFiles
		COMPRESSION_ENABLED = originalCompressionEnabled
		SPARSE_STEP_INDEX = originalSparseStepIndex
	}()

	// Test with single file mode
	USE_SEPARATE_FILES = false
	COMPRESSION_ENABLED = false
	SPARSE_STEP_INDEX = 5

	records := createTestRecords(25)
	err := PersistMemtable(records, 2, 1)

	if err != nil {
		t.Errorf("PersistMemtable failed: %v", err)
	}

	// Verify only the main file was created
	mainFile := filepath.Join(testDir, "sstable_2.db")
	if !fileExists(mainFile) {
		t.Errorf("Expected main file sstable_2.db was not created")
	}

	// Verify component files were NOT created
	componentFiles := []string{
		"sstable_2_data.db",
		"sstable_2_index.db",
		"sstable_2_summary.db",
		"sstable_2_filter.db",
		"sstable_2_metadata.db",
	}

	for _, filename := range componentFiles {
		filepath := filepath.Join(testDir, filename)
		if fileExists(filepath) {
			t.Errorf("Component file %s should not exist in single file mode", filename)
		}
	}

	// Verify the main file is larger (contains all components)
	size := getFileSize(mainFile)
	if size < 5*int64(BLOCK_SIZE) { // Should be at least 5 blocks
		t.Errorf("Main file size %d seems too small for all components", size)
	}
}

func TestPersistMemtable_WithCompression(t *testing.T) {
	setupTestDir(t)

	// Save original config values
	originalUseSeparateFiles := USE_SEPARATE_FILES
	originalCompressionEnabled := COMPRESSION_ENABLED
	originalSparseStepIndex := SPARSE_STEP_INDEX

	defer func() {
		USE_SEPARATE_FILES = originalUseSeparateFiles
		COMPRESSION_ENABLED = originalCompressionEnabled
		SPARSE_STEP_INDEX = originalSparseStepIndex
	}()

	// Test with compression enabled
	USE_SEPARATE_FILES = true
	COMPRESSION_ENABLED = true
	SPARSE_STEP_INDEX = 8

	records := createTestRecords(40)
	err := PersistMemtable(records, 3, 2)

	if err != nil {
		t.Errorf("PersistMemtable with compression failed: %v", err)
	}

	// Basic file existence check
	if !fileExists("sstable_3.db") {
		t.Errorf("Config file was not created")
	}
}

func TestPersistMemtable_WithTombstones(t *testing.T) {
	setupTestDir(t)

	// Save original config values
	originalUseSeparateFiles := USE_SEPARATE_FILES
	originalCompressionEnabled := COMPRESSION_ENABLED
	originalSparseStepIndex := SPARSE_STEP_INDEX

	defer func() {
		USE_SEPARATE_FILES = originalUseSeparateFiles
		COMPRESSION_ENABLED = originalCompressionEnabled
		SPARSE_STEP_INDEX = originalSparseStepIndex
	}()

	USE_SEPARATE_FILES = true
	COMPRESSION_ENABLED = false
	SPARSE_STEP_INDEX = 10

	records := createTestRecordsWithTombstones(30)
	err := PersistMemtable(records, 4, 0)

	if err != nil {
		t.Errorf("PersistMemtable with tombstones failed: %v", err)
	}

	// Verify files were created
	if !fileExists("sstable_4_data.db") {
		t.Errorf("Data file was not created")
	}
}

func TestPersistMemtable_LargeRecords(t *testing.T) {
	setupTestDir(t)

	// Save original config values
	originalUseSeparateFiles := USE_SEPARATE_FILES
	originalCompressionEnabled := COMPRESSION_ENABLED
	originalSparseStepIndex := SPARSE_STEP_INDEX
	originalBlockSize := BLOCK_SIZE

	defer func() {
		USE_SEPARATE_FILES = originalUseSeparateFiles
		COMPRESSION_ENABLED = originalCompressionEnabled
		SPARSE_STEP_INDEX = originalSparseStepIndex
		BLOCK_SIZE = originalBlockSize
	}()

	USE_SEPARATE_FILES = true
	COMPRESSION_ENABLED = false
	SPARSE_STEP_INDEX = 5

	// Create records with large values to test block boundary handling
	records := createLargeTestRecords(10)
	err := PersistMemtable(records, 5, 1)

	if err != nil {
		t.Errorf("PersistMemtable with large records failed: %v", err)
	}

	// Verify data file is appropriately large
	dataSize := getFileSize("sstable_5_data.db")
	expectedMinSize := int64(len(records)) * 2048 // At least the raw data size
	if dataSize < expectedMinSize {
		t.Errorf("Data file size %d is smaller than expected minimum %d", dataSize, expectedMinSize)
	}
}

func TestPersistMemtable_SingleRecord(t *testing.T) {
	setupTestDir(t)

	// Save original config values
	originalUseSeparateFiles := USE_SEPARATE_FILES
	originalCompressionEnabled := COMPRESSION_ENABLED
	originalSparseStepIndex := SPARSE_STEP_INDEX

	defer func() {
		USE_SEPARATE_FILES = originalUseSeparateFiles
		COMPRESSION_ENABLED = originalCompressionEnabled
		SPARSE_STEP_INDEX = originalSparseStepIndex
	}()

	USE_SEPARATE_FILES = true
	COMPRESSION_ENABLED = false
	SPARSE_STEP_INDEX = 10

	// Test with single record
	records := createTestRecords(1)
	err := PersistMemtable(records, 7, 0)

	if err != nil {
		t.Errorf("PersistMemtable with single record failed: %v", err)
	}

	// Verify all files were created
	expectedFiles := []string{
		"sstable_7.db",
		"sstable_7_data.db",
		"sstable_7_index.db",
		"sstable_7_summary.db",
		"sstable_7_filter.db",
		"sstable_7_metadata.db",
	}

	for _, filename := range expectedFiles {
		if !fileExists(filename) {
			t.Errorf("Expected file %s was not created", filename)
		}
	}
}

func TestPersistMemtable_DifferentSparseStepIndexes(t *testing.T) {
	setupTestDir(t)

	// Save original config values
	originalUseSeparateFiles := USE_SEPARATE_FILES
	originalCompressionEnabled := COMPRESSION_ENABLED
	originalSparseStepIndex := SPARSE_STEP_INDEX

	defer func() {
		USE_SEPARATE_FILES = originalUseSeparateFiles
		COMPRESSION_ENABLED = originalCompressionEnabled
		SPARSE_STEP_INDEX = originalSparseStepIndex
	}()

	USE_SEPARATE_FILES = true
	COMPRESSION_ENABLED = false

	// Test different sparse step indexes
	testCases := []int{1, 5, 10, 25}

	for i, stepIndex := range testCases {
		SPARSE_STEP_INDEX = stepIndex

		records := createTestRecords(100)
		err := PersistMemtable(records, 8+i, 0)

		if err != nil {
			t.Errorf("PersistMemtable failed with sparse step index %d: %v", stepIndex, err)
		}

		configFile := fmt.Sprintf("sstable_%d.db", 8+i)
		if !fileExists(configFile) {
			t.Errorf("Config file %s was not created", configFile)
		}
	}
}

func TestPersistMemtable_DifferentLevels(t *testing.T) {
	setupTestDir(t)

	// Save original config values
	originalUseSeparateFiles := USE_SEPARATE_FILES
	originalCompressionEnabled := COMPRESSION_ENABLED
	originalSparseStepIndex := SPARSE_STEP_INDEX

	defer func() {
		USE_SEPARATE_FILES = originalUseSeparateFiles
		COMPRESSION_ENABLED = originalCompressionEnabled
		SPARSE_STEP_INDEX = originalSparseStepIndex
	}()

	USE_SEPARATE_FILES = true
	COMPRESSION_ENABLED = false
	SPARSE_STEP_INDEX = 10

	// Test different levels (though level might not affect file creation directly)
	levels := []int{0, 1, 2, 5}

	for i, level := range levels {
		records := createTestRecords(20)
		err := PersistMemtable(records, 12+i, level)

		if err != nil {
			t.Errorf("PersistMemtable failed with level %d: %v", level, err)
		}

		configFile := fmt.Sprintf("sstable_%d.db", 12+i)
		if !fileExists(configFile) {
			t.Errorf("Config file %s was not created for level %d", configFile, level)
		}
	}
}

func TestPersistMemtable_ConfigurationCombinations(t *testing.T) {
	setupTestDir(t)

	// Save original config values
	originalUseSeparateFiles := USE_SEPARATE_FILES
	originalCompressionEnabled := COMPRESSION_ENABLED
	originalSparseStepIndex := SPARSE_STEP_INDEX

	defer func() {
		USE_SEPARATE_FILES = originalUseSeparateFiles
		COMPRESSION_ENABLED = originalCompressionEnabled
		SPARSE_STEP_INDEX = originalSparseStepIndex
	}()

	// Test all combinations of boolean configurations
	configurations := []struct {
		separateFiles bool
		compression   bool
		name          string
	}{
		{true, true, "separate_files_compressed"},
		{true, false, "separate_files_uncompressed"},
		{false, true, "single_file_compressed"},
		{false, false, "single_file_uncompressed"},
	}

	for i, config := range configurations {
		USE_SEPARATE_FILES = config.separateFiles
		COMPRESSION_ENABLED = config.compression
		SPARSE_STEP_INDEX = 10

		records := createTestRecords(30)
		err := PersistMemtable(records, 16+i, 0)

		if err != nil {
			t.Errorf("PersistMemtable failed for config %s: %v", config.name, err)
			continue
		}

		// Verify appropriate files exist
		mainFile := fmt.Sprintf("sstable_%d.db", 16+i)
		if !fileExists(mainFile) {
			t.Errorf("Main config file missing for config %s", config.name)
		}

		// Check component files based on configuration
		if config.separateFiles {
			componentFiles := []string{
				fmt.Sprintf("sstable_%d_data.db", 16+i),
				fmt.Sprintf("sstable_%d_index.db", 16+i),
				fmt.Sprintf("sstable_%d_summary.db", 16+i),
				fmt.Sprintf("sstable_%d_filter.db", 16+i),
				fmt.Sprintf("sstable_%d_metadata.db", 16+i),
			}

			for _, compFile := range componentFiles {
				if !fileExists(compFile) {
					t.Errorf("Component file %s missing for config %s", compFile, config.name)
				}
			}
		}

		t.Logf("Configuration %s tested successfully", config.name)
	}
}

// Benchmark tests

func BenchmarkPersistMemtable_Small(b *testing.B) {
	testDir := setupTestDir(&testing.T{})
	defer os.RemoveAll(testDir)

	// Save original config values
	originalUseSeparateFiles := USE_SEPARATE_FILES
	originalCompressionEnabled := COMPRESSION_ENABLED
	originalSparseStepIndex := SPARSE_STEP_INDEX

	defer func() {
		USE_SEPARATE_FILES = originalUseSeparateFiles
		COMPRESSION_ENABLED = originalCompressionEnabled
		SPARSE_STEP_INDEX = originalSparseStepIndex
	}()

	USE_SEPARATE_FILES = true
	COMPRESSION_ENABLED = false
	SPARSE_STEP_INDEX = 10

	records := createTestRecords(100)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		err := PersistMemtable(records, i, 0)
		if err != nil {
			b.Fatalf("PersistMemtable failed: %v", err)
		}
	}
}

func BenchmarkPersistMemtable_Large(b *testing.B) {
	testDir := setupTestDir(&testing.T{})
	defer os.RemoveAll(testDir)

	// Save original config values
	originalUseSeparateFiles := USE_SEPARATE_FILES
	originalCompressionEnabled := COMPRESSION_ENABLED
	originalSparseStepIndex := SPARSE_STEP_INDEX

	defer func() {
		USE_SEPARATE_FILES = originalUseSeparateFiles
		COMPRESSION_ENABLED = originalCompressionEnabled
		SPARSE_STEP_INDEX = originalSparseStepIndex
	}()

	USE_SEPARATE_FILES = true
	COMPRESSION_ENABLED = false
	SPARSE_STEP_INDEX = 10

	records := createTestRecords(10000)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		err := PersistMemtable(records, i, 0)
		if err != nil {
			b.Fatalf("PersistMemtable failed: %v", err)
		}
	}
}

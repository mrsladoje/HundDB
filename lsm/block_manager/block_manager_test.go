package block_manager

import (
	"bytes"
	block_location "hunddb/model/block_location"
	"os"
	"sync"
	"testing"
)

// Helper function to create a temporary test file
func createTestFile(t testing.TB, content []byte) (string, func()) {
	tmpFile, err := os.CreateTemp("", "block_manager_test_*.dat")
	if err != nil {
		t.Fatalf("Failed to create temp file: %v", err)
	}

	if content != nil {
		if _, err := tmpFile.Write(content); err != nil {
			tmpFile.Close()
			os.Remove(tmpFile.Name())
			t.Fatalf("Failed to write to temp file: %v", err)
		}
	}

	tmpFile.Close()

	return tmpFile.Name(), func() {
		os.Remove(tmpFile.Name())
	}
}

func resetBlockManager() {
	instance = nil
	once = sync.Once{}
}

func TestGetBlockManager_Singleton(t *testing.T) {
	resetBlockManager()

	bm1 := GetBlockManager()
	bm2 := GetBlockManager()

	if bm1 == nil {
		t.Fatal("Expected non nil BlockManager")
	}

	if bm1 != bm2 {
		t.Error("Expected singleton pattern - both instances should be identical")
	}

	if bm1.GetBlockSize() == 0 {
		t.Error("Expected non-zero block size")
	}
}

func TestBlockManager_WriteAndReadBlock(t *testing.T) {
	resetBlockManager()
	bm := GetBlockManager()

	testData := []byte("test data for block manager")
	// Pad data to block size for consistent testing
	blockSize := int(bm.GetBlockSize())
	if len(testData) < blockSize {
		padding := make([]byte, blockSize-len(testData))
		testData = append(testData, padding...)
	}

	tmpFile, cleanup := createTestFile(t, nil)
	defer cleanup()

	location := block_location.BlockLocation{
		FilePath:   tmpFile,
		BlockIndex: 0,
	}

	// Test writing a block
	err := bm.WriteBlock(location, testData)
	if err != nil {
		t.Fatalf("Failed to write block: %v", err)
	}

	// Test reading the block back
	block, err := bm.ReadBlock(location)
	if err != nil {
		t.Fatalf("Failed to read block: %v", err)
	}

	if len(block) == 0 {
		t.Fatal("Expected non-empty block data")
	}

	if !bytes.Equal(block, testData) {
		t.Errorf("Block data mismatch")
	}
}

func TestBlockManager_ReadNonExistentFile(t *testing.T) {
	resetBlockManager()
	bm := GetBlockManager()

	location := block_location.BlockLocation{
		FilePath:   "/nonexistent/path/file.dat",
		BlockIndex: 0,
	}

	_, err := bm.ReadBlock(location)
	if err == nil {
		t.Error("Expected error when reading from non-existent file")
	}
}

func TestBlockManager_MultipleBlocksInFile(t *testing.T) {
	resetBlockManager()
	bm := GetBlockManager()

	blockSize := int(bm.GetBlockSize())

	block1Data := make([]byte, blockSize)
	block2Data := make([]byte, blockSize)
	block3Data := make([]byte, blockSize)

	for i := range blockSize {
		block1Data[i] = 0xAA
		block2Data[i] = 0xBB
		block3Data[i] = 0xCC
	}

	tmpFile, cleanup := createTestFile(t, nil)
	defer cleanup()

	locations := []block_location.BlockLocation{
		{FilePath: tmpFile, BlockIndex: 0},
		{FilePath: tmpFile, BlockIndex: 1},
		{FilePath: tmpFile, BlockIndex: 2},
	}

	testData := [][]byte{block1Data, block2Data, block3Data}

	for i, location := range locations {
		err := bm.WriteBlock(location, testData[i])
		if err != nil {
			t.Fatalf("Failed to write block %d: %v", i, err)
		}
	}

	for i, location := range locations {
		block, err := bm.ReadBlock(location)
		if err != nil {
			t.Fatalf("Failed to read block %d: %v", i, err)
		}

		if !bytes.Equal(block, testData[i]) {
			t.Errorf("Block %d data mismatch", i)
		}
	}
}

func TestBlockManager_CacheIntegration(t *testing.T) {
	resetBlockManager()
	bm := GetBlockManager()

	testData := make([]byte, int(bm.GetBlockSize()))
	for i := range testData {
		testData[i] = 0xDD
	}

	tmpFile, cleanup := createTestFile(t, nil)
	defer cleanup()

	location := block_location.BlockLocation{
		FilePath:   tmpFile,
		BlockIndex: 0,
	}

	err := bm.WriteBlock(location, testData)
	if err != nil {
		t.Fatalf("Failed to write block: %v", err)
	}

	block1, err := bm.ReadBlock(location)
	if err != nil {
		t.Fatalf("Failed to read block (first time): %v", err)
	}

	block2, err := bm.ReadBlock(location)
	if err != nil {
		t.Fatalf("Failed to read block (second time): %v", err)
	}

	if !bytes.Equal(block1, block2) {
		t.Error("Cached and fresh data should be identical")
	}

	if !bytes.Equal(block1, testData) {
		t.Error("Block data should match written data")
	}
}

func TestBlockManager_WriteToExistingFile(t *testing.T) {
	resetBlockManager()
	bm := GetBlockManager()

	blockSize := int(bm.GetBlockSize())

	// Create file with existing data
	existingData := make([]byte, blockSize*2)
	for i := range existingData {
		existingData[i] = 0xFF
	}

	tmpFile, cleanup := createTestFile(t, existingData)
	defer cleanup()

	newData := make([]byte, blockSize)
	for i := range newData {
		newData[i] = 0x00
	}

	location := block_location.BlockLocation{
		FilePath:   tmpFile,
		BlockIndex: 1,
	}

	err := bm.WriteBlock(location, newData)
	if err != nil {
		t.Fatalf("Failed to write to existing file: %v", err)
	}

	block, err := bm.ReadBlock(location)
	if err != nil {
		t.Fatalf("Failed to read written block: %v", err)
	}

	if !bytes.Equal(block, newData) {
		t.Error("Written block data mismatch")
	}

	location0 := block_location.BlockLocation{
		FilePath:   tmpFile,
		BlockIndex: 0,
	}

	block0, err := bm.ReadBlock(location0)
	if err != nil {
		t.Fatalf("Failed to read block 0: %v", err)
	}

	expectedBlock0Data := existingData[:blockSize]
	if !bytes.Equal(block0, expectedBlock0Data) {
		t.Error("Original block 0 data should be preserved")
	}
}

// ---- New Concurrency and Thread-Safety Tests ----

// TestBlockManager_ConcurrentReads tests that multiple goroutines can read
// from the same file concurrently without issues.
func TestBlockManager_ConcurrentReads(t *testing.T) {
	resetBlockManager()
	bm := GetBlockManager()
	blockSize := int(bm.GetBlockSize())
	numBlocks := 10

	// Prepare a file with multiple distinct blocks
	var fileContent []byte
	expectedBlocks := make([][]byte, numBlocks)
	for i := 0; i < numBlocks; i++ {
		blockData := make([]byte, blockSize)
		blockData[0] = byte(i) // Make each block unique
		expectedBlocks[i] = blockData
		fileContent = append(fileContent, blockData...)
	}

	tmpFile, cleanup := createTestFile(t, fileContent)
	defer cleanup()

	var wg sync.WaitGroup
	numGoroutines := 20

	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)
		go func(goroutineID int) {
			defer wg.Done()
			blockIndex := goroutineID % numBlocks
			location := block_location.BlockLocation{FilePath: tmpFile, BlockIndex: uint64(blockIndex)}

			readData, err := bm.ReadBlock(location)
			if err != nil {
				t.Errorf("Goroutine %d failed to read block %d: %v", goroutineID, blockIndex, err)
				return
			}
			if !bytes.Equal(readData, expectedBlocks[blockIndex]) {
				t.Errorf("Goroutine %d read incorrect data for block %d", goroutineID, blockIndex)
			}
		}(i)
	}

	wg.Wait()
}

// TestBlockManager_ConcurrentWritesDifferentFiles tests that multiple goroutines can write
// to different files concurrently without interfering with each other.
func TestBlockManager_ConcurrentWritesDifferentFiles(t *testing.T) {
	resetBlockManager()
	bm := GetBlockManager()
	blockSize := int(bm.GetBlockSize())
	numGoroutines := 5

	var wg sync.WaitGroup
	files := make([]string, numGoroutines)
	cleanups := make([]func(), numGoroutines)
	expectedData := make([][]byte, numGoroutines)

	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)
		go func(goroutineID int) {
			defer wg.Done()
			data := make([]byte, blockSize)
			data[0] = byte(goroutineID) // Unique data for each goroutine
			expectedData[goroutineID] = data

			file, cleanupFunc := createTestFile(t, nil)
			files[goroutineID] = file
			cleanups[goroutineID] = cleanupFunc

			location := block_location.BlockLocation{FilePath: file, BlockIndex: 0}
			if err := bm.WriteBlock(location, data); err != nil {
				t.Errorf("Goroutine %d failed to write block: %v", goroutineID, err)
			}
		}(i)
	}
	wg.Wait()

	// Cleanup and verification
	for i := 0; i < numGoroutines; i++ {
		defer cleanups[i]()
		location := block_location.BlockLocation{FilePath: files[i], BlockIndex: 0}
		readData, err := bm.ReadBlock(location)
		if err != nil {
			t.Fatalf("Failed to read back data from file %d: %v", i, err)
		}
		if !bytes.Equal(readData, expectedData[i]) {
			t.Errorf("Data mismatch for file %d", i)
		}
	}
}

// TestBlockManager_RemoveFileMutex tests that the mutex for a file is correctly
// removed from the internal map to prevent memory leaks.
func TestBlockManager_RemoveFileMutex(t *testing.T) {
	resetBlockManager()
	bm := GetBlockManager()

	tmpFile, cleanup := createTestFile(t, nil)
	defer cleanup()

	// Access the file to ensure a mutex is created
	location := block_location.BlockLocation{FilePath: tmpFile, BlockIndex: 0}
	_, err := bm.ReadBlock(location)
	if err != nil {
		t.Fatalf("Failed to read block to create mutex: %v", err)
	}

	// Verify the mutex exists in the map
	if _, ok := bm.fileMutexes.Load(tmpFile); !ok {
		t.Fatal("Mutex was not created in the map after file access")
	}

	// Remove the mutex
	bm.RemoveFileMutex(tmpFile)

	// Verify the mutex no longer exists in the map
	if _, ok := bm.fileMutexes.Load(tmpFile); ok {
		t.Error("Mutex was not removed from the map after calling RemoveFileMutex")
	}
}

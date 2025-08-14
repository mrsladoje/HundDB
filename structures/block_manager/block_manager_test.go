package block_manager

import (
	"bytes"
	mdl "hunddb/model"
	"os"
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

func TestGetBlockManager_Singleton(t *testing.T) {
	instance = nil

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
	instance = nil
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

	location := mdl.BlockLocation{
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
	instance = nil
	bm := GetBlockManager()

	location := mdl.BlockLocation{
		FilePath:   "/nonexistent/path/file.dat",
		BlockIndex: 0,
	}

	_, err := bm.ReadBlock(location)
	if err == nil {
		t.Error("Expected error when reading from non-existent file")
	}
}

func TestBlockManager_MultipleBlocksInFile(t *testing.T) {
	instance = nil
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

	locations := []mdl.BlockLocation{
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
	instance = nil
	bm := GetBlockManager()

	testData := make([]byte, int(bm.GetBlockSize()))
	for i := range testData {
		testData[i] = 0xDD
	}

	tmpFile, cleanup := createTestFile(t, nil)
	defer cleanup()

	location := mdl.BlockLocation{
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
	instance = nil
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

	location := mdl.BlockLocation{
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

	location0 := mdl.BlockLocation{
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

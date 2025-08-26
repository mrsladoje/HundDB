package crc

import (
	"encoding/binary"
	"hash/crc32"
	"math"
)

// TODO: Displace CRC_SIZE to config, load BLOCK_SIZE from config
const CRC_SIZE = 4
const BLOCK_SIZE = 1024 * uint64(4)

// AddCRCToData adds a CRC32 checksum to the beginning of the block data.
func AddCRCToBlockData(data []byte) []byte {
	if len(data) < CRC_SIZE {
		return data // Safety check
	}

	// Calculate CRC for everything after the CRC field
	crc := crc32.ChecksumIEEE(data[CRC_SIZE:])

	// Put CRC at the beginning using little endian
	binary.LittleEndian.PutUint32(data[:CRC_SIZE], crc)

	return data
}

/*
Adds CRC at the beginning of each block therefore making the the data ready to be written.
*/
func AddCRCsToData(serializedData []byte) []byte {

	if len(serializedData) < CRC_SIZE {
		return serializedData // Safety check
	}

	dataPerBlock := BLOCK_SIZE - CRC_SIZE
	numBlocks := int(math.Ceil(float64(len(serializedData)) / float64(dataPerBlock)))

	finalBytes := make([]byte, 0, uint64(numBlocks)*BLOCK_SIZE)

	for i := uint64(0); i < uint64(len(serializedData)); i += dataPerBlock {

		block := make([]byte, BLOCK_SIZE)

		end := uint64(i) + dataPerBlock
		if end > uint64(len(serializedData)) {
			end = uint64(len(serializedData))
		}

		copy(block[CRC_SIZE:], serializedData[i:end])
		block = AddCRCToBlockData(block)

		finalBytes = append(finalBytes, block...)
	}

	return finalBytes
}

func SizeAfterAddingCRCs(originalSize uint64) uint64 {
	dataPerBlock := BLOCK_SIZE - CRC_SIZE
	numBlocks := int(math.Ceil(float64(originalSize) / float64(dataPerBlock)))

	return uint64(numBlocks) * BLOCK_SIZE
}

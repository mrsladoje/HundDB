package hyperloglog

import (
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"fmt"
	"math"
	"math/bits"
	"os"
	"path/filepath"
	"sync"

	block_manager "hunddb/lsm/block_manager"
	crc_util "hunddb/utils/crc"
)

const (
	HLL_MIN_PRECISION = 4
	HLL_MAX_PRECISION = 16
)

// HLL is a probabilistic data structure used to estimate the cardinality of a set.
// It works with uint32 for efficiency given the data size in our project.
// This implementation is thread-safe using RWMutex.
type HLL struct {
	m   uint32       // Size of the register array, 2^p
	p   uint8        // Precision, determines the number of bits used for the bucket index
	reg []uint8      // Array of registers, each storing the maximum number of trailing zero bits + 1
	mu  sync.RWMutex // RWMutex for thread-safety
}

// NewHLL creates a new instance of a HyperLogLog.
// precision: the precision parameter, must be between 4 and 16.
func NewHLL(precision uint8) (*HLL, error) {
	if precision < HLL_MIN_PRECISION || precision > HLL_MAX_PRECISION {
		return nil, errors.New("precision out of bounds")
	}
	m := uint32(1 << precision) // Equivalent to 2^p
	return &HLL{
		m:   m,
		p:   precision,
		reg: make([]uint8, m),
	}, nil
}

// Add inserts an element into the HyperLogLog by updating the corresponding register.
// item: the element to be added to the HyperLogLog.
// This method is thread-safe and uses a write lock.
func (hll *HLL) Add(item []byte) {
	hll.mu.Lock()
	defer hll.mu.Unlock()

	rawHash := sha256.Sum256(item)
	hash := binary.BigEndian.Uint64(rawHash[:8])
	regBucket := firstKbits(hash, hll.p)
	zeroCount := trailingZeroBits(hash) + 1
	if zeroCount > hll.reg[regBucket] {
		hll.reg[regBucket] = zeroCount
	}
}

// Estimate estimates the cardinality of the set represented by the HyperLogLog.
// This method is thread-safe and uses a read lock.
func (hll *HLL) Estimate() float64 {
	hll.mu.RLock()
	defer hll.mu.RUnlock()

	sum := 0.0
	for _, val := range hll.reg {
		sum += 1.0 / math.Pow(2.0, float64(val))
	}
	alpha := 0.7213 / (1.0 + 1.079/float64(hll.m))
	estimate := alpha * float64(hll.m*hll.m) / sum

	emptyRegs := hll.emptyCountUnsafe() // Use unsafe version since we already hold the lock
	if estimate <= 2.5*float64(hll.m) && emptyRegs > 0 {
		return float64(hll.m) * math.Log(float64(hll.m)/float64(emptyRegs))
	} else if estimate > 1.0/30.0*math.Pow(2.0, 32.0) {
		return -math.Pow(2.0, 32.0) * math.Log(1.0-estimate/math.Pow(2.0, 32.0))
	}
	return estimate
}

// emptyCount returns the number of registers that are zero.
// This method is thread-safe and uses a read lock.
func (hll *HLL) emptyCount() int {
	hll.mu.RLock()
	defer hll.mu.RUnlock()
	return hll.emptyCountUnsafe()
}

// emptyCountUnsafe returns the number of registers that are zero without locking.
// This is used internally when the caller already holds a lock.
func (hll *HLL) emptyCountUnsafe() int {
	count := 0
	for _, val := range hll.reg {
		if val == 0 {
			count++
		}
	}
	return count
}

// GetPrecision returns the precision of the HyperLogLog.
// This method is thread-safe and uses a read lock.
func (hll *HLL) GetPrecision() uint8 {
	hll.mu.RLock()
	defer hll.mu.RUnlock()
	return hll.p
}

// GetSize returns the size of the register array.
// This method is thread-safe and uses a read lock.
func (hll *HLL) GetSize() uint32 {
	hll.mu.RLock()
	defer hll.mu.RUnlock()
	return hll.m
}

// firstKbits returns the first k bits of the value.
func firstKbits(value uint64, k uint8) uint64 {
	return value >> (64 - k)
}

// trailingZeroBits returns the number of trailing zero bits in the value.
func trailingZeroBits(value uint64) uint8 {
	return uint8(bits.TrailingZeros64(value))
}

// Serialize serializes the HyperLogLog into a byte slice.
// The byte slice returned contains the size of the uint8 slice, precision as the uint8, the uint8 slice.
// The format is as follows:
// - 4 bytes for the size of the register array (m)
// - 1 byte for the number of of bits used for the bucket index (p)
// - m bytes, representing the slice of registers
// This method is thread-safe and uses a read lock.
func (hll *HLL) Serialize() []byte {
	hll.mu.RLock()
	defer hll.mu.RUnlock()

	// totalSize is the size of the whole hyperloglog structure
	totalSize := 5 + hll.m
	data := make([]byte, totalSize)

	binary.LittleEndian.PutUint32(data[0:4], hll.m)
	data[4] = hll.p
	copy(data[5:], hll.reg)

	return data
}

// Deserialize initializes an HLL structure from a byte slice.
// The byte slice returned contains the size of the uint8 slice, precision as the uint8, the uint8 slice.
// The format is as follows:
// - 4 bytes for the size of the register array (m)
// - 1 byte for the number of of bits used for the bucket index (p)
// - m bytes, representing the slice of registers
func Deserialize(data []byte) *HLL {
	m := binary.LittleEndian.Uint32(data[0:4])
	p := data[4]
	reg := make([]uint8, m)
	copy(reg, data[5:])
	return &HLL{
		m:   m,
		p:   p,
		reg: reg,
		mu:  sync.RWMutex{}, // Initialize the mutex
	}
}

// SaveToDisk saves the HyperLogLog to disk with the given name
func (hll *HLL) SaveToDisk(name string) error {
	// Construct the file path relative to the current working directory
	filename := filepath.Join("probabilistic", fmt.Sprintf("hyperloglog_%s.db", name))

	// Ensure the directory exists
	dir := filepath.Dir(filename)
	if err := os.MkdirAll(dir, os.ModePerm); err != nil {
		return fmt.Errorf("failed to create directory: %v", err)
	}

	// Serialize the HLL data
	serializedData := hll.Serialize()

	// Create file data: [size (8B) + serialized data]
	totalSize := 8 + len(serializedData)
	fileData := make([]byte, totalSize)

	// Write size header
	binary.LittleEndian.PutUint64(fileData[0:8], uint64(len(serializedData)))

	// Copy serialized data
	copy(fileData[8:], serializedData)

	// Add CRC blocks and write to disk
	dataWithCRC := crc_util.AddCRCsToData(fileData)

	blockManager := block_manager.GetBlockManager()
	return blockManager.WriteToDisk(dataWithCRC, filename, 0)
}

// LoadHyperLogLogFromDisk loads a HyperLogLog from disk with the given name
func LoadHyperLogLogFromDisk(name string) (*HLL, error) {
	// Construct the file path relative to the current working directory
	filename := filepath.Join("probabilistic", fmt.Sprintf("hyperloglog_%s.db", name))
	blockManager := block_manager.GetBlockManager()

	// Read size header (first 8 bytes)
	sizeBytes, _, err := blockManager.ReadFromDisk(filename, 0, 8)
	if err != nil {
		return nil, fmt.Errorf("file not found or corrupted: %v", err)
	}

	dataSize := binary.LittleEndian.Uint64(sizeBytes)

	// Read serialized data starting from offset 8 + CRC_SIZE
	// (BlockManager accounts for CRC internally)
	serializedData, _, err := blockManager.ReadFromDisk(filename, 8+4, dataSize)
	if err != nil {
		return nil, fmt.Errorf("failed to read data: %v", err)
	}

	// Deserialize and return
	return Deserialize(serializedData), nil
}

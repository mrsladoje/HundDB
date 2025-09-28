package token_bucket

import (
	"encoding/binary"
	"hunddb/lsm/block_manager"
	"time"
)

const (
	TOKEN_CAPACITY        uint16 = 5  // Maximum amount of tokens
	REFILL_INTERVAL       uint64 = 20 // Refills tokens on every passed interval
	REFILL_AMOUNT         uint16 = 1  // Amount to be refilled
	LAST_RESET_SIZE       uint16 = 8  // Size of LastReset in bytes
	REMAINING_TOKENS_SIZE uint16 = 2  // Size of RemainingTokens in bytes
	FILEPATH              string = "lsm/token_bucket/token_bucket.db"
)

type TokenBucket struct {
	LastReset       time.Time                   // Time of last reset
	RemainingTokens uint16                      // Current available tokens
	BlockManager    *block_manager.BlockManager // Block manager
}

// NewTokenBucket creates a new TokenBucket instance
func NewTokenBucket(capacity uint8, refillInterval uint64) *TokenBucket {
	BlockManager := block_manager.GetBlockManager()
	data, _, _ := BlockManager.ReadFromDisk(FILEPATH, 0, uint64(LAST_RESET_SIZE+REMAINING_TOKENS_SIZE))

	return deserialize(data)
}

// Serialize TokenBucket to bytes
func (tb *TokenBucket) serialize() []byte {
	buffer := make([]byte, 10)

	binary.LittleEndian.PutUint64(buffer[0:8], uint64(tb.LastReset.Unix()))
	binary.LittleEndian.PutUint16(buffer[8:10], tb.RemainingTokens)

	return buffer
}

// Deserialize bytes to TokenBucket
func deserialize(data []byte) *TokenBucket {
	// Validate data length
	if len(data) < int(LAST_RESET_SIZE+REMAINING_TOKENS_SIZE) {
		// Return default TokenBucket if data is faulty
		return &TokenBucket{
			LastReset:       time.Now(),
			RemainingTokens: TOKEN_CAPACITY,
			BlockManager:    block_manager.GetBlockManager(),
		}
	}

	lastReset := time.Unix(int64(binary.LittleEndian.Uint64(data[0:LAST_RESET_SIZE])), 0)
	remainingTokens := binary.LittleEndian.Uint16(data[LAST_RESET_SIZE : LAST_RESET_SIZE+REMAINING_TOKENS_SIZE])

	return &TokenBucket{
		LastReset:       lastReset,
		RemainingTokens: remainingTokens,
		BlockManager:    block_manager.GetBlockManager(),
	}
}

// Save to disk through BlockManager
func (tb *TokenBucket) SaveToDisk() error {
	serializedData := tb.serialize()
	return tb.BlockManager.WriteToDisk(serializedData, FILEPATH, 0)
}

// AllowRequest gives user permission for using system.
// It also increases remainingTokens if enough time passes.
func (tb *TokenBucket) AllowRequest() bool {
	currentTime := time.Now()
	diff := currentTime.Sub(tb.LastReset).Seconds()

	// Calculate how many refill intervals have passed
	intervalsPassed := int(diff) / int(REFILL_INTERVAL)

	// Add tokens for each interval passed
	tb.RemainingTokens += uint16(intervalsPassed) * REFILL_AMOUNT

	// Cap at maximum capacity
	if tb.RemainingTokens > TOKEN_CAPACITY {
		tb.RemainingTokens = TOKEN_CAPACITY
	}

	// Update last reset time if any intervals passed
	if intervalsPassed > 0 {
		tb.LastReset = tb.LastReset.Add(time.Duration(intervalsPassed*int(REFILL_INTERVAL)) * time.Second)
	}

	// Check if tokens available
	if tb.RemainingTokens > 0 {
		tb.RemainingTokens--

		return true
	}

	return false
}

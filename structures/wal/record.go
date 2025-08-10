package wal

// TODO: This is just an interface, should be removed when all structures are put together
type Record struct {
	key       []byte
	value     []byte
	timestamp uint64
	tombstone bool
}

func NewRecord(key []byte, value []byte, timestamp uint64, tombstone bool) *Record {
	return &Record{
		key:       key,
		value:     value,
		timestamp: timestamp,
		tombstone: tombstone,
	}
}

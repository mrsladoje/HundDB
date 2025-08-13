package model

// BlockLocation uniquely identifies a block on disk by combining file path and block index
type BlockLocation struct {
	FilePath   string
	BlockIndex uint64
}

// Block represents a block of data from disk
type Block struct {
	Location BlockLocation
	Data     []byte
}

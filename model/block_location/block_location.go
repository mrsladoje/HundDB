package block_location

// BlockLocation uniquely identifies a block on disk by combining file path and block index
type BlockLocation struct {
	FilePath   string
	BlockIndex uint64
}

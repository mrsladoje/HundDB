package model

type Record struct {
	Key       []byte
	Value     []byte
	Active    bool
	Timestamp uint64
}

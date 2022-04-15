package memtable

import (
	"encoding/binary"
	"leveldb-go/internal_key"
)

type Batch struct {
	internalKey *internal_key.InternalKey
	uKeySize    uint64
	uValue      []byte
}

func newBatch(internalKey *internal_key.InternalKey, uValue []byte) *Batch {
	uKeySize := uint64(len(internalKey.UKey))
	return &Batch{
		internalKey: internalKey,
		uKeySize:    uKeySize,
		uValue:      uValue,
	}
}

func (batch *Batch) Encode() []byte {
	internalKeyBuf := batch.internalKey.EncodeTo()
	memKeyBufSize := len(internalKeyBuf) + len(batch.uValue) + 8
	memKeyBuf := make([]byte, memKeyBufSize)
	binary.BigEndian.PutUint64(memKeyBuf, batch.uKeySize)
	copy(memKeyBuf[8:], internalKeyBuf)
	copy(memKeyBuf[len(internalKeyBuf)+8:], batch.uValue)
	return memKeyBuf
}

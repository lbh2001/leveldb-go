package skiplist

import (
	"encoding/binary"
	"leveldb-go/internal_key"
)

type NodeKey struct {
	internalKey *internal_key.InternalKey
	uKeySize    uint64
	uValue      []byte
}

func newBatch(internalKey *internal_key.InternalKey, uValue []byte) *NodeKey {
	uKeySize := uint64(len(internalKey.UKey))
	return &NodeKey{
		internalKey: internalKey,
		uKeySize:    uKeySize,
		uValue:      uValue,
	}
}

func (nk *NodeKey) Encode() []byte {
	internalKeyBuf := nk.internalKey.EncodeTo()
	memKeyBufSize := len(internalKeyBuf) + len(nk.uValue) + 8
	memKeyBuf := make([]byte, memKeyBufSize)
	binary.BigEndian.PutUint64(memKeyBuf, nk.uKeySize)
	copy(memKeyBuf[8:], internalKeyBuf)
	copy(memKeyBuf[len(internalKeyBuf)+8:], nk.uValue)
	return memKeyBuf
}

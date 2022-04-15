package internal_key

import (
	"encoding/binary"
	"errors"
)

type ValueType uint8

const (
	ValueTypePut = ValueType(0)
	ValueTypeDel = ValueType(1)
)

var (
	errInvalidValueType = errors.New("Invalid internal key: type ")
	errInvalidKeyLength = errors.New("Invalid internal key: length ")
)

type InternalKey struct {
	UKey      []byte
	SeqNumber uint64
	VT        ValueType
}

func NewInternalKey(uKey []byte, seqNumber uint64, vt ValueType) *InternalKey {
	if vt > ValueTypeDel {
		panic(errInvalidValueType)
	}
	return &InternalKey{
		UKey:      uKey,
		SeqNumber: seqNumber,
		VT:        vt,
	}
}

func (ik *InternalKey) EncodeTo() []byte {
	buf := make([]byte, len(ik.UKey)+8)
	copy(buf, ik.UKey)
	binary.BigEndian.PutUint64(buf[len(ik.UKey):], (ik.SeqNumber<<8)|uint64(ik.VT))
	return buf
}

func DecodeFrom(encodedInternalKey []byte) (*InternalKey, error) {
	if len(encodedInternalKey) < 8 {
		return nil, errInvalidKeyLength
	}
	meta := binary.BigEndian.Uint64(encodedInternalKey[len(encodedInternalKey)-8:])
	seqNumber, vt := meta>>8, ValueType(meta&0xff)
	if vt > ValueTypeDel {
		return nil, errInvalidValueType
	}
	uKey := encodedInternalKey[:len(encodedInternalKey)-8]
	ik := NewInternalKey(uKey, seqNumber, vt)
	return ik, nil
}

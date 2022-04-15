package internal_key

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestInternalKeyEncodeAndDecode(t *testing.T) {
	internalKeys := make([]*InternalKey, 0)
	internalKeys = append(internalKeys, NewInternalKey([]byte("key1"), uint64(1), ValueTypePut))
	internalKeys = append(internalKeys, NewInternalKey([]byte("key2"), uint64(2), ValueTypePut))
	internalKeys = append(internalKeys, NewInternalKey([]byte("key3"), uint64(3), ValueTypePut))
	internalKeys = append(internalKeys, NewInternalKey([]byte("key2"), uint64(4), ValueTypeDel))
	for _, internalKey := range internalKeys {
		encodedBuffer := internalKey.EncodeTo()
		decodedIk, err := DecodeFrom(encodedBuffer)
		assert.Nil(t, err)
		assert.Equal(t, *internalKey, *decodedIk)
	}
}

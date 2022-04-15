package memtable

import (
	"github.com/stretchr/testify/assert"
	"leveldb-go/version"
	"strconv"
	"sync"
	"testing"
)

func TestSkipListInsert(t *testing.T) {
	s := newSkipList()
	insertDatas := []struct {
		uKey      []byte
		uValue    []byte
		seqNumber uint64
	}{
		{
			uKey:      []byte("key1"),
			uValue:    []byte("value1"),
			seqNumber: 1,
		},
		{
			uKey:      []byte("key2"),
			uValue:    []byte("value2"),
			seqNumber: 2,
		},
		{
			uKey:      []byte("key3"),
			uValue:    []byte("value3"),
			seqNumber: 3,
		},
		{
			uKey:      []byte("key2"),
			uValue:    []byte("value4"),
			seqNumber: 4,
		},
	}

	for _, insertData := range insertDatas {
		s.Put(insertData.uKey, insertData.uValue, insertData.seqNumber)
	}

	// general cases
	uValue1, ok := s.Get([]byte("key1"), 5)
	assert.Equal(t, true, ok)
	assert.Equal(t, []byte("value1"), uValue1)

	uValue3, ok := s.Get([]byte("key3"), 6)
	assert.Equal(t, true, ok)
	assert.Equal(t, []byte("value3"), uValue3)

	// ["key2"]'s value should be updated to ["value4"]
	uValue2, ok := s.Get([]byte("key2"), 7)
	assert.Equal(t, true, ok)
	assert.Equal(t, []byte("value4"), uValue2)

	// ["key4"] is not exist in skip list
	uValue4, ok := s.Get([]byte("key4"), 8)
	assert.Equal(t, false, ok)
	assert.Nil(t, uValue4)
}

func TestSkipListDelete(t *testing.T) {
	s := newSkipList()
	insertDatas := []struct {
		uKey      []byte
		uValue    []byte
		seqNumber uint64
	}{
		{
			uKey:      []byte("key1"),
			uValue:    []byte("value1"),
			seqNumber: 1,
		},
		{
			uKey:      []byte("key2"),
			uValue:    []byte("value2"),
			seqNumber: 2,
		},
		{
			uKey:      []byte("key3"),
			uValue:    []byte("value3"),
			seqNumber: 3,
		},
	}

	for _, insertData := range insertDatas {
		s.Put(insertData.uKey, insertData.uValue, insertData.seqNumber)
	}

	for _, insertData := range insertDatas {
		// before delete
		uValue, ok := s.Get(insertData.uKey, insertData.seqNumber+3)
		assert.Equal(t, true, ok)
		assert.Equal(t, insertData.uValue, uValue)
		s.Delete(insertData.uKey, insertData.seqNumber)
		// after delete
		uValue, ok = s.Get(insertData.uKey, insertData.seqNumber+4)
		assert.Equal(t, false, ok)
		assert.Nil(t, uValue)
	}
}

func TestAvoidDirtyRead(t *testing.T) {
	s := newSkipList()
	insertDatas := []struct {
		uKey      []byte
		uValue    []byte
		seqNumber uint64
	}{
		{
			uKey:      []byte("key"),
			uValue:    []byte("value1"),
			seqNumber: 1,
		},
		{
			uKey:      []byte("key"),
			uValue:    []byte("value2"),
			seqNumber: 2,
		},
		{
			uKey:      []byte("key"),
			uValue:    []byte("value3"),
			seqNumber: 4,
		},
	}
	// update "key".value from "value1" to "value3"
	for _, data := range insertDatas {
		s.Put(data.uKey, data.uValue, data.seqNumber)
	}
	// general read request
	value, ok := s.Get([]byte("key"), 5)
	assert.Equal(t, true, ok)
	assert.Equal(t, []byte("value3"), value)
	// stale read request, it can only get the data whose seqNumber < the given seqNumber
	value, ok = s.Get([]byte("key"), 3)
	assert.Equal(t, true, ok)
	assert.Equal(t, []byte("value2"), value)
}

// benchmark
func BenchmarkSkipListConcurrency(b *testing.B) {
	seq := version.NewAndInitialSeqNumber()
	b.StartTimer()
	s := newSkipList()
	var wg sync.WaitGroup
	for i := 0; i < b.N; i++ {
		wg.Add(1)
		go func(i int) {
			seqNumber := seq.GetSeqNumberAmount()
			defer wg.Done()
			key := []byte("key" + strconv.Itoa(i))
			val := []byte("value" + strconv.Itoa(i))
			s.Put(key, val, seqNumber)
		}(i)
	}
	wg.Wait()
	b.StopTimer()
}

func BenchmarkSkipListSerial(b *testing.B) {
	seq := version.NewAndInitialSeqNumber()
	b.StartTimer()
	s := newSkipList()
	for i := 0; i < b.N; i++ {
		seqNumber := seq.GetSeqNumberAmount()
		key := []byte("key" + strconv.Itoa(i))
		val := []byte("value" + strconv.Itoa(i))
		s.Put(key, val, seqNumber)
	}
	b.StopTimer()
}

package memtable

import (
	"leveldb-go/skiplist"
	"sync"
)

type MemTable struct {
	sl       *skiplist.SkipList
	memUsage uint64
	sync.Mutex
}

func NewMemTable() *MemTable {
	return &MemTable{
		sl:       skiplist.NewSkipList(),
		memUsage: uint64(0),
	}
}

func (mt *MemTable) Put(key, value []byte, seqNumber uint64) {
	mt.sl.Put(key, value, seqNumber)
	mt.Lock()
	mt.memUsage += uint64(len(key) + len(value) + 16)
	mt.Unlock()
}

func (mt *MemTable) Delete(key []byte, seqNumber uint64) {
	mt.sl.Delete(key, seqNumber)
	mt.Lock()
	// in delete option, len(user value) == 0
	mt.memUsage += uint64(len(key) + 16)
	mt.Unlock()
}

func (mt *MemTable) Get(key []byte) ([]byte, bool) {
	return mt.Get(key)
}

func (mt *MemTable) MemoryUsage() uint64 {
	mt.Lock()
	memUsage := mt.memUsage
	mt.Unlock()
	return memUsage
}

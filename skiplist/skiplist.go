package skiplist

import (
	"leveldb-go/internal_key"
	"math/rand"
	"strings"
	"sync"
)

const Branching int = 4

type SkipList struct {
	// a dummy node
	head *node
	// max height of all nodes in this skip list
	maxHeight int
	// used to ensure safety of concurrent R&W
	mu sync.RWMutex
}

func NewSkipList() *SkipList {
	return &SkipList{
		head:      newNode(nil, MaxLevel),
		maxHeight: 1,
	}
}

func (s *SkipList) Put(uKey, uValue []byte, seqNumber uint64) *node {
	return s.insertNode(uKey, uValue, seqNumber, internal_key.ValueTypePut)
}

func (s *SkipList) Delete(uKey []byte, seqNumber uint64) *node {
	return s.insertNode(uKey, []byte(""), seqNumber, internal_key.ValueTypeDel)
}

func (s *SkipList) Get(uKey []byte, seqNumber uint64) ([]byte, bool) {
	h := s.head
	for l := s.maxHeight - 1; l >= 0; l-- {
		for h.next[l] != nil && isBytesLess(h.next[l].getUKey(), uKey) {
			h = h.next[l]
		}
	}
	for h = h.next[0]; h != nil && isBytesEqual(h.getUKey(), uKey); h = h.next[0] {
		if h.getSeqNumber() > seqNumber {
			continue
		} else if h.getValueType() == internal_key.ValueTypeDel {
			break
		} else {
			return h.nk.uValue, true
		}
	}
	return nil, false
}

// insertNode returns such a node in skip list:
// - if node.uKey == the given uKey, return the node after
//   update its uValue
// - if there is no node whose uKey is equal to the given batch,
//   then create a new node to storage the given kv and return it
func (s *SkipList) insertNode(uKey, uValue []byte, seqNumber uint64, valueType internal_key.ValueType) *node {
	s.mu.Lock()
	defer s.mu.Unlock()
	// scan the given uKey in this skip list
	h, needUpdate := s.scan(uKey)
	for h.next[0] != nil {
		if isBytesEqual(h.next[0].getUKey(), uKey) && h.next[0].getSeqNumber() > seqNumber {
			h = h.next[0]
		} else {
			break
		}
	}
	for i := 0; i < len(h.next); i++ {
		needUpdate[i] = h
	}
	height := randomHeight()
	if height > s.maxHeight {
		// we promise that we increase at almost one level to skip list at once
		height = s.maxHeight + 1
		needUpdate[s.maxHeight] = s.head
		s.maxHeight = height
	}
	internalKey := internal_key.NewInternalKey(uKey, seqNumber, valueType)
	batch := newBatch(internalKey, uValue)
	n := newNode(batch, height)
	for l := height - 1; l >= 0; l-- {
		n.next[l] = needUpdate[l].next[l]
		needUpdate[l].next[l] = n
	}
	return n
}

// scan returns a []*node which maybe need to update
func (s *SkipList) scan(uKey []byte) (*node, []*node) {
	needUpdate := make([]*node, MaxLevel)
	// search the given uKey in this skip list
	h := s.head
	for l := s.maxHeight - 1; l >= 0; l-- {
		for h.next[l] != nil && isBytesLess(h.next[l].getUKey(), uKey) {
			h = h.next[l]
		}
		needUpdate[l] = h
	}
	return h, needUpdate
}

func isBytesEqual(a, b []byte) bool {
	strA, strB := string(a), string(b)
	return strings.Compare(strA, strB) == 0
}

func isBytesLess(a, b []byte) bool {
	strA, strB := string(a), string(b)
	return strings.Compare(strA, strB) == -1
}

func randomHeight() int {
	var level = 1
	for level < MaxLevel && rand.Intn(Branching) == 0 {
		level++
	}
	return level
}

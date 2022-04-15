package memtable

import "leveldb-go/internal_key"

const (
	MaxLevel int = 16
)

type node struct {
	batch *Batch
	next  []*node
}

func newNode(key *Batch, level int) *node {
	if level > MaxLevel {
		panic("invalid level: out of max level")
	}
	return &node{
		batch: key,
		next:  make([]*node, level),
	}
}

func (n *node) getNext(level int) *node {
	if level > len(n.next) {
		panic("invalid level: out of range node.len(next)")
	}
	return n.next[level]
}

func (n *node) setNext(level int, next *node) {
	if level > len(n.next) {
		panic("invalid level: out of range node.len(next)")
	}
	n.next[level] = next
}

func (n *node) getUKey() []byte {
	return n.batch.internalKey.UKey
}

func (n *node) updateUValue(seqNumber uint64, uValue []byte) {
	n.batch.internalKey.SeqNumber = seqNumber
	n.batch.uValue = uValue
}

func (n *node) getSeqNumber() uint64 {
	return n.batch.internalKey.SeqNumber
}

func (n *node) getValueType() internal_key.ValueType {
	return n.batch.internalKey.VT
}

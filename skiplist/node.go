package skiplist

import (
	"leveldb-go/internal_key"
)

const (
	MaxLevel int = 16
)

type node struct {
	nk   *NodeKey
	next []*node
}

func newNode(key *NodeKey, level int) *node {
	if level > MaxLevel {
		panic("invalid level: out of max level")
	}
	return &node{
		nk:   key,
		next: make([]*node, level),
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
	return n.nk.internalKey.UKey
}

func (n *node) updateUValue(seqNumber uint64, uValue []byte) {
	n.nk.internalKey.SeqNumber = seqNumber
	n.nk.uValue = uValue
}

func (n *node) getSeqNumber() uint64 {
	return n.nk.internalKey.SeqNumber
}

func (n *node) getValueType() internal_key.ValueType {
	return n.nk.internalKey.VT
}

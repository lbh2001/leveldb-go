package memtable

import (
	"math/rand"
	"strings"
)

func IsBytesEqual(a, b []byte) bool {
	strA, strB := string(a), string(b)
	return strings.Compare(strA, strB) == 0
}

func IsBytesLess(a, b []byte) bool {
	strA, strB := string(a), string(b)
	return strings.Compare(strA, strB) == -1
}

func RandomHeight() int {
	var level = 1
	for level < MaxLevel && rand.Intn(Branching) == 0 {
		level++
	}
	return level
}

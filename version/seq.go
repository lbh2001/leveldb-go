package version

import "sync"

// SeqNumber should be singleton
type SeqNumber struct {
	Amount uint64
	sync.Mutex
}

func NewAndInitialSeqNumber() *SeqNumber {
	return &SeqNumber{
		Amount: uint64(0),
	}
}

func (sn *SeqNumber) GetSeqNumberAmount() uint64 {
	sn.Lock()
	defer sn.Unlock()
	sn.Amount++
	return sn.Amount
}

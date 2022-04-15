package version

import (
	"github.com/stretchr/testify/assert"
	"sync"
	"testing"
)

func TestSeqNumberAmountIncrement(t *testing.T) {
	sn := NewAndInitialSeqNumber()
	var wg sync.WaitGroup
	var testDataAmount = 10000
	for i := 0; i < testDataAmount; i++ {
		wg.Add(1)
		go func() {
			sn.GetSeqNumberAmount()
			wg.Done()
		}()
	}
	wg.Wait()
	assert.Equal(t, uint64(testDataAmount+1), sn.GetSeqNumberAmount())
}

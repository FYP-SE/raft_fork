package stability

import (
	"sync"
	"testing"
)

// T4.6 (TASKS.md) -- graceful handover needs a leader's own score to change
// mid-test (start healthy, degrade, trigger a handover), which ConstScorer
// can't do (it's a fixed value type by design). Written first per the TDD
// convention, before Var lands.

func TestVar_ScoreReturnsInitialValue(t *testing.T) {
	v := NewVar(200)
	if got := v.Score(); got != 200 {
		t.Errorf("Score() = %d, want 200", got)
	}
}

func TestVar_SetChangesScore(t *testing.T) {
	v := NewVar(200)
	v.Set(50)
	if got := v.Score(); got != 50 {
		t.Errorf("Score() = %d, want 50 after Set", got)
	}
}

func TestVar_ImplementsScorer(t *testing.T) {
	var _ Scorer = NewVar(0)
}

func TestVar_ConcurrentSetAndScore(t *testing.T) {
	v := NewVar(0)
	var wg sync.WaitGroup
	for i := 0; i < 50; i++ {
		wg.Add(2)
		go func(n int) {
			defer wg.Done()
			v.Set(uint8(n))
		}(i)
		go func() {
			defer wg.Done()
			_ = v.Score()
		}()
	}
	wg.Wait() // run with -race to confirm no data race
}

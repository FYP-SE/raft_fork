package stability

import "sync"

// Var is a Scorer whose value can be changed at runtime via Set. Unlike
// ConstScorer (a fixed value type), it exists for tests that need to
// simulate a node's stability changing over time -- e.g. a healthy leader
// degrading to trigger a graceful handover (DESIGN.md §2.6).
type Var struct {
	mu sync.Mutex
	v  uint8
}

// NewVar returns a Var reporting the given initial score.
func NewVar(v uint8) *Var {
	return &Var{v: v}
}

// Set changes the score Var reports from subsequent Score calls.
func (s *Var) Set(v uint8) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.v = v
}

// Score implements Scorer.
func (s *Var) Score() uint8 {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.v
}

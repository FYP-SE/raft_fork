package stability

import (
	"sync"
	"testing"
)

// T4.1 (TASKS.md) -- unit tests for EWMAScorer math, written before the
// implementation (stability/ewma_scorer.go). See DESIGN.md §2.1 for the
// scorer formula this exercises:
//
//	each signal s_i normalised to [0,1] (1 = healthy) against fixed reference bounds
//	ewma_i <- alpha*s_i + (1-alpha)*ewma_i
//	score  = 255 * sum(w_i * ewma_i)

func defaultBounds() map[Signal]Bounds {
	return map[Signal]Bounds{
		SignalCPU:    {Min: 0, Max: 100},
		SignalMemory: {Min: 0, Max: 100},
		SignalFsync:  {Min: 0, Max: 500},
		SignalJitter: {Min: 0, Max: 200},
	}
}

func TestNewEWMAScorer_DefaultsFullyHealthy(t *testing.T) {
	s, err := NewEWMAScorer(DefaultEWMAConfig(defaultBounds()))
	if err != nil {
		t.Fatalf("NewEWMAScorer: %v", err)
	}
	if got := s.Score(); got != 255 {
		t.Errorf("fresh scorer Score() = %d, want 255 (no samples yet => assumed healthy)", got)
	}
}

func TestEWMAScorer_SingleSignalExactMath(t *testing.T) {
	// Single signal, weight 1.0, alpha 0.2, bounds [0,100] -- deterministic,
	// hand-computable EWMA step so this test pins the exact formula (not
	// just its converged tail behaviour).
	cfg := EWMAConfig{
		Alpha:   0.2,
		Weights: map[Signal]float64{SignalCPU: 1.0},
		Bounds:  map[Signal]Bounds{SignalCPU: {Min: 0, Max: 100}},
	}
	s, err := NewEWMAScorer(cfg)
	if err != nil {
		t.Fatalf("NewEWMAScorer: %v", err)
	}
	// value=50 -> normalised = 1 - (50-0)/(100-0) = 0.5
	// ewma_new = 0.2*0.5 + 0.8*1.0 (initial ewma=1.0, healthy) = 0.9
	// score = round(255*0.9) = round(229.5) = 230
	s.Sample(SignalCPU, 50)
	if got, want := s.Score(), uint8(230); got != want {
		t.Errorf("Score() after one sample = %d, want %d", got, want)
	}
}

func TestEWMAScorer_ConvergesTowardHealthyExtreme(t *testing.T) {
	s, err := NewEWMAScorer(DefaultEWMAConfig(defaultBounds()))
	if err != nil {
		t.Fatalf("NewEWMAScorer: %v", err)
	}
	for i := 0; i < 100; i++ {
		s.Sample(SignalCPU, 0)
		s.Sample(SignalMemory, 0)
		s.Sample(SignalFsync, 0)
		s.Sample(SignalJitter, 0)
	}
	if got := s.Score(); got != 255 {
		t.Errorf("Score() after sustained healthy samples = %d, want 255", got)
	}
}

func TestEWMAScorer_ConvergesTowardUnhealthyExtreme(t *testing.T) {
	s, err := NewEWMAScorer(DefaultEWMAConfig(defaultBounds()))
	if err != nil {
		t.Fatalf("NewEWMAScorer: %v", err)
	}
	for i := 0; i < 100; i++ {
		s.Sample(SignalCPU, 100)
		s.Sample(SignalMemory, 100)
		s.Sample(SignalFsync, 500)
		s.Sample(SignalJitter, 200)
	}
	if got := s.Score(); got != 0 {
		t.Errorf("Score() after sustained unhealthy samples = %d, want 0", got)
	}
}

func TestEWMAScorer_WeightedComposite(t *testing.T) {
	cfg := EWMAConfig{
		Alpha: 0.2,
		Weights: map[Signal]float64{
			SignalCPU:    0.5,
			SignalMemory: 0.5,
		},
		Bounds: map[Signal]Bounds{
			SignalCPU:    {Min: 0, Max: 100},
			SignalMemory: {Min: 0, Max: 100},
		},
	}
	s, err := NewEWMAScorer(cfg)
	if err != nil {
		t.Fatalf("NewEWMAScorer: %v", err)
	}
	for i := 0; i < 200; i++ {
		s.Sample(SignalCPU, 0)   // stays fully healthy
		s.Sample(SignalMemory, 100) // stays fully unhealthy
	}
	got := s.Score()
	// 0.5*255 (healthy half) + 0.5*0 (unhealthy half), rounded.
	if got < 125 || got > 130 {
		t.Errorf("Score() for half-healthy/half-unhealthy composite = %d, want ~127", got)
	}
}

func TestEWMAScorer_ClampsOutOfBoundValues(t *testing.T) {
	cfg := EWMAConfig{
		Alpha:   0.2,
		Weights: map[Signal]float64{SignalCPU: 1.0},
		Bounds:  map[Signal]Bounds{SignalCPU: {Min: 0, Max: 100}},
	}
	s, err := NewEWMAScorer(cfg)
	if err != nil {
		t.Fatalf("NewEWMAScorer: %v", err)
	}
	for i := 0; i < 100; i++ {
		s.Sample(SignalCPU, 1000) // far beyond Max -- must clamp, not go negative/overflow
	}
	if got := s.Score(); got != 0 {
		t.Errorf("Score() after far-out-of-bound samples = %d, want 0 (clamped)", got)
	}

	s2, err := NewEWMAScorer(cfg)
	if err != nil {
		t.Fatalf("NewEWMAScorer: %v", err)
	}
	for i := 0; i < 100; i++ {
		s2.Sample(SignalCPU, -1000) // far below Min -- must clamp to fully healthy
	}
	if got := s2.Score(); got != 255 {
		t.Errorf("Score() after far-below-bound samples = %d, want 255 (clamped)", got)
	}
}

func TestEWMAScorer_ThreadSafe(t *testing.T) {
	s, err := NewEWMAScorer(DefaultEWMAConfig(defaultBounds()))
	if err != nil {
		t.Fatalf("NewEWMAScorer: %v", err)
	}
	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func(n int) {
			defer wg.Done()
			for j := 0; j < 1000; j++ {
				s.Sample(SignalCPU, float64(j%100))
				_ = s.Score()
			}
		}(i)
	}
	wg.Wait()
	// No assertion beyond "run clean under -race" -- this test's job is to
	// be a race-detector target, not to check a specific converged value.
}

func TestNewEWMAScorer_RejectsBadConfig(t *testing.T) {
	cases := map[string]EWMAConfig{
		"alpha zero": {
			Alpha:   0,
			Weights: map[Signal]float64{SignalCPU: 1.0},
			Bounds:  map[Signal]Bounds{SignalCPU: {Min: 0, Max: 100}},
		},
		"alpha too big": {
			Alpha:   1.5,
			Weights: map[Signal]float64{SignalCPU: 1.0},
			Bounds:  map[Signal]Bounds{SignalCPU: {Min: 0, Max: 100}},
		},
		"weights don't sum to 1": {
			Alpha:   0.2,
			Weights: map[Signal]float64{SignalCPU: 0.3, SignalMemory: 0.3},
			Bounds: map[Signal]Bounds{
				SignalCPU:    {Min: 0, Max: 100},
				SignalMemory: {Min: 0, Max: 100},
			},
		},
		"missing bounds for weighted signal": {
			Alpha:   0.2,
			Weights: map[Signal]float64{SignalCPU: 1.0},
			Bounds:  map[Signal]Bounds{},
		},
		"degenerate bounds (min==max)": {
			Alpha:   0.2,
			Weights: map[Signal]float64{SignalCPU: 1.0},
			Bounds:  map[Signal]Bounds{SignalCPU: {Min: 50, Max: 50}},
		},
	}
	for name, cfg := range cases {
		t.Run(name, func(t *testing.T) {
			if _, err := NewEWMAScorer(cfg); err == nil {
				t.Errorf("NewEWMAScorer(%+v) = nil error, want error", cfg)
			}
		})
	}
}

func TestConstScorer(t *testing.T) {
	var c ConstScorer = 128
	if got := c.Score(); got != 128 {
		t.Errorf("ConstScorer(128).Score() = %d, want 128", got)
	}
	var zero ConstScorer
	if got := zero.Score(); got != 0 {
		t.Errorf("zero-value ConstScorer.Score() = %d, want 0", got)
	}
}

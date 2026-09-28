package stability

import (
	"fmt"
	"math"
	"sync"
)

// Signal identifies one of the fixed local signals DESIGN.md §2.1 composes
// into a stability score. The set is closed (v1: CPU, memory, WAL fsync
// latency, heartbeat/RTT jitter) -- Phase 8's Markov scorer is a different
// Scorer implementation behind the same interface, not an extension of this
// enum.
type Signal int

const (
	SignalCPU Signal = iota
	SignalMemory
	SignalFsync
	SignalJitter

	numSignals = 4
)

// Bounds are the reference min/max a raw sample is normalised against:
// value <= Min maps to 1.0 (fully healthy), value >= Max maps to 0.0 (fully
// unhealthy), linear in between. All four signals are "lower is healthier".
type Bounds struct {
	Min, Max float64
}

// EWMAConfig configures an EWMAScorer. Weights and Bounds only need entries
// for signals actually in use; Weights must sum to 1 across those entries.
type EWMAConfig struct {
	// Alpha is the EWMA smoothing factor in (0,1], applied per Sample call.
	Alpha float64
	// Weights maps each in-use signal to its share of the composite score;
	// must sum to 1.
	Weights map[Signal]float64
	// Bounds gives the reference range for each in-use signal (every signal
	// with a nonzero Weights entry must have a Bounds entry).
	Bounds map[Signal]Bounds
	// CriticalLevel: Critical() reports true when any weighted signal's
	// smoothed health (1 = at Bounds.Min, 0 = at Bounds.Max) is at or below
	// this. Must be in [0,1]; 0 in a hand-built config means only a signal
	// pinned at its bad bound counts. DefaultEWMAConfig uses 0.1.
	CriticalLevel float64
}

// DefaultEWMAConfig returns DESIGN.md §5's default alpha/weights (CPU .3,
// memory .2, fsync .3, jitter .2; alpha 0.2) for the given reference bounds,
// which are necessarily host-supplied (the raft library has no notion of
// what unit "CPU" or "fsync latency" are measured in).
func DefaultEWMAConfig(bounds map[Signal]Bounds) EWMAConfig {
	return EWMAConfig{
		Alpha:         0.2,
		CriticalLevel: 0.1,
		Weights: map[Signal]float64{
			SignalCPU:    0.3,
			SignalMemory: 0.2,
			SignalFsync:  0.3,
			SignalJitter: 0.2,
		},
		Bounds: bounds,
	}
}

func (cfg EWMAConfig) validate() error {
	if cfg.Alpha <= 0 || cfg.Alpha > 1 {
		return fmt.Errorf("stability: EWMAConfig.Alpha must be in (0,1], got %v", cfg.Alpha)
	}
	if cfg.CriticalLevel < 0 || cfg.CriticalLevel > 1 {
		return fmt.Errorf("stability: EWMAConfig.CriticalLevel must be in [0,1], got %v", cfg.CriticalLevel)
	}
	if len(cfg.Weights) == 0 {
		return fmt.Errorf("stability: EWMAConfig.Weights must not be empty")
	}
	var sum float64
	for sig, w := range cfg.Weights {
		if w < 0 {
			return fmt.Errorf("stability: EWMAConfig.Weights[%d] = %v, must be >= 0", sig, w)
		}
		sum += w
		b, ok := cfg.Bounds[sig]
		if !ok {
			return fmt.Errorf("stability: EWMAConfig.Bounds missing entry for weighted signal %d", sig)
		}
		if b.Min == b.Max {
			return fmt.Errorf("stability: EWMAConfig.Bounds[%d] is degenerate (Min == Max == %v)", sig, b.Min)
		}
	}
	const epsilon = 1e-6
	if math.Abs(sum-1) > epsilon {
		return fmt.Errorf("stability: EWMAConfig.Weights must sum to 1, got %v", sum)
	}
	return nil
}

// EWMAScorer is the DESIGN.md §2.1 "Scorer v1" composite: each signal is
// normalised to [0,1] against fixed reference bounds, smoothed with an EWMA,
// and combined via configured weights into a byte score. Thread-safe.
type EWMAScorer struct {
	cfg EWMAConfig

	mu   sync.Mutex
	ewma [numSignals]float64
}

// NewEWMAScorer builds an EWMAScorer from cfg, initialised as if every
// in-use signal had always been perfectly healthy (ewma_i = 1.0) -- a freshly
// started node reports itself as stable until it actually observes
// otherwise, rather than looking maximally unstable before its first sample.
func NewEWMAScorer(cfg EWMAConfig) (*EWMAScorer, error) {
	if err := cfg.validate(); err != nil {
		return nil, err
	}
	s := &EWMAScorer{cfg: cfg}
	for i := range s.ewma {
		s.ewma[i] = 1.0
	}
	return s, nil
}

// Sample feeds one raw observation of signal in. Non-blocking, safe for
// concurrent use with Score and other Sample calls.
func (s *EWMAScorer) Sample(signal Signal, value float64) {
	b, ok := s.cfg.Bounds[signal]
	if !ok {
		return // signal not configured for this scorer instance -- ignore
	}
	normalised := 1 - (value-b.Min)/(b.Max-b.Min)
	if normalised > 1 {
		normalised = 1
	} else if normalised < 0 {
		normalised = 0
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	s.ewma[signal] = s.cfg.Alpha*normalised + (1-s.cfg.Alpha)*s.ewma[signal]
}

// Score implements Scorer: the weighted EWMA composite, quantised to a byte.
func (s *EWMAScorer) Score() uint8 {
	s.mu.Lock()
	var composite float64
	for sig, w := range s.cfg.Weights {
		composite += w * s.ewma[sig]
	}
	s.mu.Unlock()

	scaled := math.Round(composite * 255)
	switch {
	case scaled <= 0:
		return 0
	case scaled >= 255:
		return 255
	default:
		return uint8(scaled)
	}
}

// Critical implements CriticalReporter: true if any signal with a nonzero
// weight has a smoothed health at or below CriticalLevel.
func (s *EWMAScorer) Critical() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	for sig, w := range s.cfg.Weights {
		if w > 0 && s.ewma[sig] <= s.cfg.CriticalLevel {
			return true
		}
	}
	return false
}

// Health returns one signal's smoothed health in [0,1] (1 = at Bounds.Min,
// 0 = at Bounds.Max), for observability.
func (s *EWMAScorer) Health(sig Signal) float64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.ewma[sig]
}

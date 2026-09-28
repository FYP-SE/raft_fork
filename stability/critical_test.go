package stability

import "testing"

// DESIGN_UPDATE.md D7 -- calibration and the critical-signal trigger
// (weighted score + critical trigger, chosen by Piyumi 2026-09-28).
//
// A weighted sum cannot push one bad signal below HandoverThreshold (a
// weight <= 0.3 lowers the score by at most ~77 of 255), so the score keeps
// ranking heirs and a separate Critical() report lets the leader hand over
// when any single signal is critical.

// saturate drives signal to its fully-bad bound until the EWMA has converged.
func saturate(s *EWMAScorer, sig Signal) {
	b := s.cfg.Bounds[sig]
	for i := 0; i < 200; i++ {
		s.Sample(sig, b.Max)
	}
}

// One fully degraded signal must lower the score by at least 2x the default
// HysteresisMargin (20), so a degraded heir is always replaceable.
func TestCalibration_OneBadSignalDropsScoreByTwiceMargin(t *testing.T) {
	const hysteresisMargin = 20
	for _, sig := range []Signal{SignalCPU, SignalMemory, SignalFsync, SignalJitter} {
		s, err := NewEWMAScorer(DefaultEWMAConfig(defaultBounds()))
		if err != nil {
			t.Fatal(err)
		}
		saturate(s, sig)
		if drop := 255 - int(s.Score()); drop < 2*hysteresisMargin {
			t.Errorf("signal %d fully bad: score dropped %d, want >= %d", sig, drop, 2*hysteresisMargin)
		}
	}
}

func TestCritical_FalseWhenHealthy(t *testing.T) {
	s, err := NewEWMAScorer(DefaultEWMAConfig(defaultBounds()))
	if err != nil {
		t.Fatal(err)
	}
	if s.Critical() {
		t.Fatal("fresh scorer reports Critical()")
	}
	// Half-bad on every signal: degraded, but nothing critical.
	for i := 0; i < 200; i++ {
		for sig, b := range s.cfg.Bounds {
			s.Sample(sig, (b.Min+b.Max)/2)
		}
	}
	if s.Critical() {
		t.Fatal("Critical() with every signal only half-bad")
	}
}

func TestCritical_AnySingleFullyBadSignal(t *testing.T) {
	for _, sig := range []Signal{SignalCPU, SignalMemory, SignalFsync, SignalJitter} {
		s, err := NewEWMAScorer(DefaultEWMAConfig(defaultBounds()))
		if err != nil {
			t.Fatal(err)
		}
		saturate(s, sig)
		if !s.Critical() {
			t.Errorf("signal %d fully bad: Critical() = false, want true", sig)
		}
	}
}

// Unweighted (unconfigured) signals never count.
func TestCritical_IgnoresUnweightedSignals(t *testing.T) {
	cfg := DefaultEWMAConfig(defaultBounds())
	cfg.Weights = map[Signal]float64{SignalCPU: 1}
	s, err := NewEWMAScorer(cfg)
	if err != nil {
		t.Fatal(err)
	}
	saturate(s, SignalFsync)
	if s.Critical() {
		t.Fatal("Critical() from a signal with no weight")
	}
}

func TestCriticalLevel_DefaultAndValidation(t *testing.T) {
	cfg := DefaultEWMAConfig(defaultBounds())
	if cfg.CriticalLevel != 0.1 {
		t.Fatalf("CriticalLevel = %v, want default 0.1", cfg.CriticalLevel)
	}
	cfg.CriticalLevel = 1.5
	if _, err := NewEWMAScorer(cfg); err == nil {
		t.Fatal("NewEWMAScorer accepted CriticalLevel 1.5")
	}
}

// Var can model a critical leader in raft's handover tests.
func TestVar_CriticalSettable(t *testing.T) {
	v := NewVar(200)
	if v.Critical() {
		t.Fatal("NewVar Critical() = true")
	}
	v.SetCritical(true)
	if !v.Critical() {
		t.Fatal("SetCritical(true) not reported")
	}
}

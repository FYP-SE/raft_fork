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

// One fully degraded signal must lower the score by more than the default
// HysteresisMargin (40 = ~3x the healthy-node noise p99 of 14 measured
// 2026-09-28), so a degraded heir is replaceable on score alone. The
// original "2x margin" headroom rule cannot hold at 40 (it would need 80;
// the drops are 51-77). Only the 0.3-weight signals (CPU, fsync: -77) also
// clear margin + noise (54); the 0.2-weight ones (memory, jitter: -51) do
// not by themselves -- a bad link is caught by D6 (in-sync) instead.
func TestCalibration_OneBadSignalDropsScoreBeyondMargin(t *testing.T) {
	const hysteresisMargin = 40
	for _, sig := range []Signal{SignalCPU, SignalMemory, SignalFsync, SignalJitter} {
		s, err := NewEWMAScorer(DefaultEWMAConfig(defaultBounds()))
		if err != nil {
			t.Fatal(err)
		}
		saturate(s, sig)
		if drop := 255 - int(s.Score()); drop <= hysteresisMargin {
			t.Errorf("signal %d fully bad: score dropped %d, want > %d", sig, drop, hysteresisMargin)
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

// Health exposes one signal's smoothed health for observability (per-signal
// metrics, 2026-09-28: the composite alone cannot say which signal costs a
// healthy node ~43 points).
func TestEWMAScorer_Health(t *testing.T) {
	s, err := NewEWMAScorer(DefaultEWMAConfig(defaultBounds()))
	if err != nil {
		t.Fatal(err)
	}
	if got := s.Health(SignalFsync); got != 1 {
		t.Fatalf("fresh Health(fsync) = %v, want 1", got)
	}
	saturate(s, SignalFsync)
	if got := s.Health(SignalFsync); got > 0.01 {
		t.Fatalf("Health(fsync) after saturation = %v, want ~0", got)
	}
	if got := s.Health(SignalCPU); got != 1 {
		t.Fatalf("Health(cpu) = %v, want untouched 1", got)
	}
}

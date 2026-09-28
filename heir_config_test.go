package raft

import (
	"testing"

	"go.etcd.io/raft/v3/stability"
)

// T4.1 (TASKS.md) -- Config surface tests, written before wiring the new
// fields into Config/validate in raft.go. See DESIGN.md §5 for the tunable
// defaults and CLAUDE.md constraint 4: "everything off by default" must be
// byte-identical to stock.

func TestConfig_HeirRaftFieldsDefaultOff(t *testing.T) {
	// Deliberately not newTestConfig: this test asserts Config's own
	// zero-value defaults (CLAUDE.md constraint 4), which must hold
	// regardless of the T4.8 RAFT_HEIRRAFT_FORCE_ON test wrapper that
	// newTestConfig applies (heir_conformance_test.go).
	cfg := &Config{
		ID:              1,
		ElectionTick:    10,
		HeartbeatTick:   1,
		Storage:         newTestMemoryStorage(withPeers(1)),
		MaxSizePerMsg:   noLimit,
		MaxInflightMsgs: 256,
	}
	if err := cfg.validate(); err != nil {
		t.Fatalf("validate() on a config with all HeirRaft fields unset: %v", err)
	}
	if cfg.StabilityScorer != nil {
		t.Errorf("StabilityScorer = %v, want nil by default", cfg.StabilityScorer)
	}
	if cfg.HeirElection || cfg.HeirLogPriority || cfg.GracefulHandover {
		t.Errorf("feature flags = (%v,%v,%v), want all false by default",
			cfg.HeirElection, cfg.HeirLogPriority, cfg.GracefulHandover)
	}
}

func TestConfig_ValidateFillsTunableDefaults(t *testing.T) {
	cfg := newTestConfig(1, 10, 1, newTestMemoryStorage(withPeers(1)))
	if err := cfg.validate(); err != nil {
		t.Fatalf("validate(): %v", err)
	}
	// Defaults per DESIGN.md §5.
	wantHysteresisMargin := uint8(20)
	wantMinHeirTenure := 10
	wantHeirJitter := 0.1
	wantNonHeirBackoff := 1.0 // DESIGN_UPDATE.md D3
	wantHandoverThreshold := uint8(64)
	wantDegradeWindow := 5
	wantHandoverCooldown := 10

	if cfg.FreshnessSlack != 1 {
		t.Errorf("FreshnessSlack = %d, want default 1", cfg.FreshnessSlack)
	}
	if cfg.HeirSyncGrace != cfg.ElectionTick {
		t.Errorf("HeirSyncGrace = %d, want default ElectionTick (%d)", cfg.HeirSyncGrace, cfg.ElectionTick)
	}
	if cfg.HysteresisMargin != wantHysteresisMargin {
		t.Errorf("HysteresisMargin = %d, want default %d", cfg.HysteresisMargin, wantHysteresisMargin)
	}
	if cfg.MinHeirTenure != wantMinHeirTenure {
		t.Errorf("MinHeirTenure = %d, want default %d", cfg.MinHeirTenure, wantMinHeirTenure)
	}
	if cfg.HeirJitter != wantHeirJitter {
		t.Errorf("HeirJitter = %v, want default %v", cfg.HeirJitter, wantHeirJitter)
	}
	if cfg.NonHeirBackoff != wantNonHeirBackoff {
		t.Errorf("NonHeirBackoff = %v, want default %v", cfg.NonHeirBackoff, wantNonHeirBackoff)
	}
	if cfg.HandoverThreshold != wantHandoverThreshold {
		t.Errorf("HandoverThreshold = %d, want default %d", cfg.HandoverThreshold, wantHandoverThreshold)
	}
	if cfg.DegradeWindow != wantDegradeWindow {
		t.Errorf("DegradeWindow = %d, want default %d", cfg.DegradeWindow, wantDegradeWindow)
	}
	if cfg.HandoverCooldown != wantHandoverCooldown {
		t.Errorf("HandoverCooldown = %d, want default %d", cfg.HandoverCooldown, wantHandoverCooldown)
	}
}

func TestConfig_ValidateRejectsFlagWithoutScorer(t *testing.T) {
	for _, name := range []string{"HeirElection", "HeirLogPriority", "GracefulHandover"} {
		t.Run(name, func(t *testing.T) {
			cfg := newTestConfig(1, 10, 1, newTestMemoryStorage(withPeers(1)))
			// Explicit, not just relying on newTestConfig's default: this
			// test's premise is "flag true, scorer absent," which must hold
			// regardless of RAFT_HEIRRAFT_FORCE_ON (heir_conformance_test.go).
			cfg.StabilityScorer = nil
			switch name {
			case "HeirElection":
				cfg.HeirElection = true
			case "HeirLogPriority":
				cfg.HeirLogPriority = true
			case "GracefulHandover":
				cfg.GracefulHandover = true
			}
			if err := cfg.validate(); err == nil {
				t.Errorf("validate() with %s=true and nil StabilityScorer = nil error, want error", name)
			}
		})
	}
}

func TestConfig_ValidateAcceptsFlagsWithScorer(t *testing.T) {
	cfg := newTestConfig(1, 10, 1, newTestMemoryStorage(withPeers(1)))
	cfg.StabilityScorer = stability.ConstScorer(200)
	cfg.HeirElection = true
	cfg.HeirLogPriority = true
	cfg.GracefulHandover = true
	if err := cfg.validate(); err != nil {
		t.Errorf("validate() with flags on and a scorer set: %v", err)
	}
}

func TestConfig_ValidateRejectsBadTunables(t *testing.T) {
	cases := map[string]func(*Config){
		"NonHeirBackoff below 1": func(c *Config) { c.NonHeirBackoff = 0.5 },
		"NonHeirBackoff above 2": func(c *Config) { c.NonHeirBackoff = 2.5 },
		"HeirJitter negative":    func(c *Config) { c.HeirJitter = -0.1 },
		"HeirJitter above 1":     func(c *Config) { c.HeirJitter = 1.5 },
		"MinHeirTenure negative": func(c *Config) { c.MinHeirTenure = -1 },
		"DegradeWindow negative": func(c *Config) { c.DegradeWindow = -1 },
		"HandoverCooldown negative": func(c *Config) { c.HandoverCooldown = -1 },
	}
	for name, mutate := range cases {
		t.Run(name, func(t *testing.T) {
			cfg := newTestConfig(1, 10, 1, newTestMemoryStorage(withPeers(1)))
			mutate(cfg)
			if err := cfg.validate(); err == nil {
				t.Errorf("validate() with %s = nil error, want error", name)
			}
		})
	}
}

// TestResetRandomizedElectionTimeout_DisabledModeNoOp is the regression test
// T4.1 requires: with all HeirRaft fields at their zero value (nil/false),
// resetRandomizedElectionTimeout's output distribution must be byte-identical
// to stock -- i.e. uniform in [electionTimeout, 2*electionTimeout). T4.1
// itself doesn't touch resetRandomizedElectionTimeout (that's T4.5); this
// test's job is to pin the *current* stock distribution now, as a sacred
// scaffold that T4.5 must keep green when it makes the function heir-aware.
func TestResetRandomizedElectionTimeout_DisabledModeNoOp(t *testing.T) {
	const electionTick = 10
	r := newTestRaft(1, electionTick, 1, newTestMemoryStorage(withPeers(1)))

	const iterations = 20000
	sum := 0
	seen := make(map[int]bool)
	for i := 0; i < iterations; i++ {
		r.resetRandomizedElectionTimeout()
		got := r.randomizedElectionTimeout
		if got < electionTick || got >= 2*electionTick {
			t.Fatalf("randomizedElectionTimeout = %d, want in [%d, %d)", got, electionTick, 2*electionTick)
		}
		sum += got
		seen[got] = true
	}

	// Stock behaviour is uniform over {electionTick, ..., 2*electionTick-1}
	// (electionTick distinct values) -- with 20000 draws over 10 buckets we
	// expect to see all of them, and the mean should sit close to the
	// theoretical (electionTick + (2*electionTick-1)) / 2.
	if len(seen) != electionTick {
		t.Errorf("saw %d distinct values over %d iterations, want all %d buckets hit", len(seen), iterations, electionTick)
	}
	mean := float64(sum) / float64(iterations)
	wantMean := float64(electionTick+(2*electionTick-1)) / 2
	if diff := mean - wantMean; diff < -0.2 || diff > 0.2 {
		t.Errorf("mean randomizedElectionTimeout = %v, want ~%v (uniform distribution)", mean, wantMean)
	}
}

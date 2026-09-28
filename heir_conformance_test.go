package raft

import (
	"os"
	"testing"

	"go.etcd.io/raft/v3/stability"
)

// T4.8 (TASKS.md) -- full-suite + upstream conformance, "all HeirRaft flags
// ON" pass. Rather than editing every test, this hooks the one shared
// config constructor (newTestConfig, used throughout raft_test.go and
// raft_paper_test.go) so an opt-in environment variable turns HeirRaft
// fully on for the *entire* package's test suite in one run -- exactly the
// "config-defaulting test wrapper" TASKS.md asks for.
//
// Deliberately scoped to the raft package's own unit/paper-conformance
// tests, not rafttest's datadriven interaction tests (see TASKS.md's result
// note and RESEARCH_LOG.md 2026-07-30 for why the latter isn't feasible to
// force this way: golden files pin exact output byte-for-byte, and turning
// HeirRaft on for all ~28 pre-existing testdata/*.txt files would produce
// mass, expected-but-unreviewed mismatches that would drown out any real
// regression signal. Multi-flag interaction at the interaction-test layer
// is instead covered by testdata/heir_all_flags_smoke.txt, which turns all
// three flags on *deliberately* for one representative cluster.
const heirRaftForceOnEnvVar = "RAFT_HEIRRAFT_FORCE_ON"

// maybeForceHeirRaftOn enables every HeirRaft feature flag (with a healthy
// constant scorer) when heirRaftForceOnEnvVar is set in the environment,
// and is a complete no-op otherwise. Normal `go test` runs (the env var
// unset) are therefore byte-for-byte unaffected: tests that assert the
// disabled-mode default (e.g. TestConfig_HeirRaftFieldsDefaultOff) keep
// passing, and every heir_*_test.go test that sets its own Config fields
// after calling newTestConfig continues to get exactly what it asked for,
// since those assignments run after this and simply win.
func maybeForceHeirRaftOn(cfg *Config) {
	if os.Getenv(heirRaftForceOnEnvVar) == "" {
		return
	}
	cfg.StabilityScorer = stability.ConstScorer(200)
	cfg.HeirElection = true
	cfg.HeirLease = true // DESIGN_UPDATE.md D2
	cfg.HeirLogPriority = true
	cfg.GracefulHandover = true
}

func TestMaybeForceHeirRaftOn_NoOpByDefault(t *testing.T) {
	t.Setenv(heirRaftForceOnEnvVar, "")
	cfg := &Config{}
	maybeForceHeirRaftOn(cfg)
	if cfg.StabilityScorer != nil || cfg.HeirElection || cfg.HeirLogPriority || cfg.GracefulHandover {
		t.Fatalf("cfg = %+v, want completely untouched when %s is unset", cfg, heirRaftForceOnEnvVar)
	}
}

func TestMaybeForceHeirRaftOn_EnablesEverythingWhenSet(t *testing.T) {
	t.Setenv(heirRaftForceOnEnvVar, "1")
	cfg := &Config{}
	maybeForceHeirRaftOn(cfg)
	if cfg.StabilityScorer == nil {
		t.Error("StabilityScorer = nil, want non-nil")
	}
	if !cfg.HeirElection || !cfg.HeirLogPriority || !cfg.GracefulHandover {
		t.Errorf("flags = (%v,%v,%v), want all true", cfg.HeirElection, cfg.HeirLogPriority, cfg.GracefulHandover)
	}
}

func TestMaybeForceHeirRaftOn_CallerOverrideAfterwardWins(t *testing.T) {
	t.Setenv(heirRaftForceOnEnvVar, "1")
	cfg := newTestConfig(1, 10, 1, newTestMemoryStorage(withPeers(1)))
	// Simulate what every heir_*_test.go test does: explicitly set fields
	// after newTestConfig returns. This must win regardless of the env var.
	cfg.HeirElection = false
	cfg.StabilityScorer = nil
	if cfg.HeirElection || cfg.StabilityScorer != nil {
		t.Fatal("explicit post-construction override was clobbered")
	}
}

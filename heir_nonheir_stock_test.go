package raft

import (
	"testing"

	"go.etcd.io/raft/v3/stability"
)

// DESIGN_UPDATE.md D3 -- non-heir followers use exactly stock timers.
//
// The v1 default NonHeirBackoff=1.5 pushed every non-heir to [1.5ET, 2.5ET),
// so whenever the heir failed (dead, lagging, lease-rejected) the cluster
// recovered *slower* than stock. With the default at 1.0 a non-heir's draw
// is stock's uniform [ET, 2ET), so the fallback path is exactly stock. The
// knob stays for ablation only.

func TestNonHeirTimer_DefaultEqualsStock(t *testing.T) {
	r := newTimeoutTestRaft(t, 10, 1)
	r.knownHeir = 2 // a live heir exists, and it isn't me (r.id == 1)
	assertVanillaDistribution(t, r)
}

func TestNonHeirTimer_DefaultBackoffIsOne(t *testing.T) {
	r := newTimeoutTestRaft(t, 10, 1)
	if r.nonHeirBackoff != 1.0 {
		t.Fatalf("nonHeirBackoff = %v, want default 1.0 (D3)", r.nonHeirBackoff)
	}
}

// The knob still works when set explicitly (ablation arm).
func TestNonHeirTimer_ExplicitBackoffStillApplies(t *testing.T) {
	cfg := newTestConfig(1, 10, 1, newTestMemoryStorage(withPeers(1, 2, 3)))
	cfg.StabilityScorer = stability.ConstScorer(0)
	cfg.HeirElection = true
	cfg.NonHeirBackoff = 1.5
	r := newRaft(cfg)
	r.knownHeir = 2

	for i := 0; i < timeoutSamples; i++ {
		r.resetRandomizedElectionTimeout()
		if got := r.randomizedElectionTimeout; got < 15 || got >= 25 {
			t.Fatalf("randomizedElectionTimeout = %d, want in [15,25) with NonHeirBackoff=1.5", got)
		}
	}
}

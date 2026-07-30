package raft

import (
	"testing"

	pb "go.etcd.io/raft/v3/raftpb"
	"go.etcd.io/raft/v3/stability"
)

// T4.5 (TASKS.md) -- crash-path biased election timeouts (DESIGN.md §2.5).
// Written before resetRandomizedElectionTimeout's three-way rule lands, per
// the project's TDD convention. Draws many samples per case (mirroring
// heir_config_test.go's T4.1 disabled-mode regression test) rather than a
// single draw, since the whole point of the mechanism is a *distribution*
// shift, not a fixed value.

const timeoutSamples = 20000

// newTimeoutTestRaft builds a single-voter raft with HeirElection on and the
// given electionTimeout/heartbeatTimeout, for exercising
// resetRandomizedElectionTimeout in isolation.
func newTimeoutTestRaft(t *testing.T, electionTimeout, heartbeatTimeout int) *raft {
	t.Helper()
	cfg := newTestConfig(1, electionTimeout, heartbeatTimeout, newTestMemoryStorage(withPeers(1, 2, 3)))
	cfg.StabilityScorer = stability.ConstScorer(0)
	cfg.HeirElection = true
	return newRaft(cfg)
}

func TestResetRandomizedElectionTimeout_HeirFiresFirst(t *testing.T) {
	// electionTimeout=50 (not 10) so the jitter window (HeirJitter=0.1 ->
	// delta=5) has enough granularity to actually vary across draws; with
	// electionTimeout=10, delta would be 1 and every draw degenerates to the
	// same value, which is a test-precision artifact, not a real problem
	// with the implementation (see TestResetRandomizedElectionTimeout_HeirRangeBelowNonHeirRange
	// for confirmation that the bound itself holds regardless).
	r := newTimeoutTestRaft(t, 50, 1)
	r.knownHeir = r.id // "I am the heir", and the belief is fresh (heirSeenTick=0)

	minSeen, maxSeen := r.electionTimeout*10, 0
	for i := 0; i < timeoutSamples; i++ {
		r.resetRandomizedElectionTimeout()
		got := r.randomizedElectionTimeout
		if got < r.electionTimeout {
			t.Fatalf("randomizedElectionTimeout = %d, want >= electionTimeout=%d", got, r.electionTimeout)
		}
		// Upper bound: electionTimeout + HeirJitter*electionTimeout (default
		// HeirJitter=0.1), so strictly less than the vanilla/non-heir minimum.
		if maxAllowed := r.electionTimeout + int(0.1*float64(r.electionTimeout)) + 1; got > maxAllowed {
			t.Fatalf("randomizedElectionTimeout = %d, want <= %d (heir jitter bound)", got, maxAllowed)
		}
		if got < minSeen {
			minSeen = got
		}
		if got > maxSeen {
			maxSeen = got
		}
	}
	// A real (non-degenerate) jitter range should have produced more than a
	// single fixed value across 20000 draws.
	if minSeen == maxSeen {
		t.Errorf("all %d draws were the same value %d -- jitter isn't being applied", timeoutSamples, minSeen)
	}
}

func TestResetRandomizedElectionTimeout_NonHeirBacksOff(t *testing.T) {
	r := newTimeoutTestRaft(t, 10, 1)
	r.knownHeir = 2 // a live heir exists, and it isn't me (r.id == 1)

	minSeen := r.electionTimeout * 10
	maxSeen := 0
	for i := 0; i < timeoutSamples; i++ {
		r.resetRandomizedElectionTimeout()
		got := r.randomizedElectionTimeout
		// Lower bound: NonHeirBackoff*electionTimeout (default 1.5), so
		// strictly greater than the heir's own upper bound above.
		if minAllowed := int(1.5 * float64(r.electionTimeout)); got < minAllowed {
			t.Fatalf("randomizedElectionTimeout = %d, want >= %d (non-heir backoff bound)", got, minAllowed)
		}
		if got < minSeen {
			minSeen = got
		}
		if got > maxSeen {
			maxSeen = got
		}
	}
	if minSeen == maxSeen {
		t.Errorf("all %d draws were the same value %d -- rand(electionTimeout) isn't being applied", timeoutSamples, minSeen)
	}
}

func TestResetRandomizedElectionTimeout_HeirRangeBelowNonHeirRange(t *testing.T) {
	// With the DESIGN.md §5 defaults (HeirJitter=0.1, NonHeirBackoff=1.5),
	// every possible heir-branch draw must be strictly below every possible
	// non-heir-branch draw, so the heir is structurally guaranteed to fire
	// first (DESIGN.md §2.5's whole point).
	r := newTimeoutTestRaft(t, 10, 1)

	r.knownHeir = r.id
	heirMax := 0
	for i := 0; i < timeoutSamples; i++ {
		r.resetRandomizedElectionTimeout()
		if r.randomizedElectionTimeout > heirMax {
			heirMax = r.randomizedElectionTimeout
		}
	}

	r.knownHeir = 2
	nonHeirMin := r.electionTimeout * 10
	for i := 0; i < timeoutSamples; i++ {
		r.resetRandomizedElectionTimeout()
		if r.randomizedElectionTimeout < nonHeirMin {
			nonHeirMin = r.randomizedElectionTimeout
		}
	}

	if heirMax >= nonHeirMin {
		t.Fatalf("heir range max (%d) >= non-heir range min (%d): heir isn't guaranteed to fire first", heirMax, nonHeirMin)
	}
}

// TestResetRandomizedElectionTimeout_NoAnnouncementIsVanilla is the T4.5
// "heir_stale_vanilla" case (TASKS.md explicitly calls for a statistical Go
// unit test here, not a datadriven golden file): with no heir ever known,
// the distribution must be indistinguishable from vanilla Raft's uniform
// [electionTimeout, 2*electionTimeout).
func TestResetRandomizedElectionTimeout_NoAnnouncementIsVanilla(t *testing.T) {
	r := newTimeoutTestRaft(t, 10, 1)
	// r.knownHeir left at None (zero value): no announcement ever recorded.
	assertVanillaDistribution(t, r)
}

// TestResetRandomizedElectionTimeout_StaleAnnouncementIsVanilla: a
// previously-known heir whose announcement is older than HeirStaleness
// heartbeat intervals must be treated exactly like "no heir known".
func TestResetRandomizedElectionTimeout_StaleAnnouncementIsVanilla(t *testing.T) {
	r := newTimeoutTestRaft(t, 10, 1)
	r.knownHeir = 2
	r.heirSeenTick = r.heirStaleness*r.heartbeatTimeout + 1 // just past the bound
	assertVanillaDistribution(t, r)
}

func TestResetRandomizedElectionTimeout_DisabledIsVanilla(t *testing.T) {
	cfg := newTestConfig(1, 10, 1, newTestMemoryStorage(withPeers(1, 2, 3)))
	// Explicit, not just left at newTestConfig's default: this test's premise
	// is HeirElection=false, which must hold regardless of
	// RAFT_HEIRRAFT_FORCE_ON (heir_conformance_test.go).
	cfg.HeirElection = false
	cfg.HeirLogPriority = false
	cfg.GracefulHandover = false
	cfg.StabilityScorer = nil
	r := newRaft(cfg)
	r.knownHeir = 2 // even if soft state somehow got set, disabled must win
	assertVanillaDistribution(t, r)
}

// TestStepFollower_RecomputesTimeoutOnAnnouncement is a regression test for a
// real bug found while writing T4.5's datadriven test: a follower that stays
// continuously in StateFollower across an entire term (the common case)
// never re-enters (*raft).reset -- stepFollower's MsgApp/MsgHeartbeat cases
// only zero electionElapsed, they don't call resetRandomizedElectionTimeout.
// So the very first randomizedElectionTimeout, drawn back when the node was
// constructed (long before any heir was ever selected), would otherwise
// never be recomputed, and the bias would silently never take effect for a
// node that never changes role/term. Fixed by recomputing the timeout (gated
// on HeirElection, to keep vanilla behaviour untouched) right after
// handleAppendEntries/handleHeartbeat run recordHeir.
func TestStepFollower_RecomputesTimeoutOnAnnouncement(t *testing.T) {
	newFollower := func(t *testing.T) *raft {
		t.Helper()
		cfg := newTestConfig(2, 50, 1, newTestMemoryStorage(withPeers(1, 2, 3)))
		cfg.StabilityScorer = stability.ConstScorer(0)
		cfg.HeirElection = true
		r := newRaft(cfg)
		r.becomeFollower(1, 1) // establish as a stable follower of leader 1, term 1
		return r
	}

	t.Run("heir announcement biases toward firing first", func(t *testing.T) {
		r := newFollower(t)
		if err := r.Step(&pb.Message{Type: pb.MsgHeartbeat.Enum(), From: new(uint64(1)), Term: new(r.Term), Heir: new(r.id)}); err != nil {
			t.Fatal(err)
		}
		maxAllowed := r.electionTimeout + int(0.1*float64(r.electionTimeout)) + 1
		if got := r.randomizedElectionTimeout; got < r.electionTimeout || got > maxAllowed {
			t.Fatalf("randomizedElectionTimeout = %d, want in [%d,%d] after learning self is heir", got, r.electionTimeout, maxAllowed)
		}
	})

	t.Run("non-heir announcement biases toward waiting", func(t *testing.T) {
		r := newFollower(t)
		if err := r.Step(&pb.Message{Type: pb.MsgHeartbeat.Enum(), From: new(uint64(1)), Term: new(r.Term), Heir: new(uint64(3))}); err != nil {
			t.Fatal(err)
		}
		minAllowed := int(1.5 * float64(r.electionTimeout))
		if got := r.randomizedElectionTimeout; got < minAllowed {
			t.Fatalf("randomizedElectionTimeout = %d, want >= %d after learning 3 (not me) is heir", got, minAllowed)
		}
	})
}

// assertVanillaDistribution runs the same 10-bucket/mean check as T4.1's
// heir_config_test.go disabled-mode regression test, confirming the branch
// under test reproduces vanilla Raft's uniform distribution exactly.
func assertVanillaDistribution(t *testing.T, r *raft) {
	t.Helper()
	const draws = timeoutSamples
	buckets := make([]int, r.electionTimeout)
	sum := 0
	for i := 0; i < draws; i++ {
		r.resetRandomizedElectionTimeout()
		got := r.randomizedElectionTimeout
		if got < r.electionTimeout || got >= 2*r.electionTimeout {
			t.Fatalf("randomizedElectionTimeout = %d, want in [%d,%d)", got, r.electionTimeout, 2*r.electionTimeout)
		}
		buckets[got-r.electionTimeout]++
		sum += got
	}
	for b, n := range buckets {
		if n == 0 {
			t.Errorf("bucket %d (value %d) never hit in %d draws", b, r.electionTimeout+b, draws)
		}
	}
	// The uniform draw is over [electionTimeout, 2*electionTimeout - 1]
	// (Intn(electionTimeout) is [0, electionTimeout-1]), so the true mean is
	// electionTimeout + (electionTimeout-1)/2, not 1.5*electionTimeout -- for
	// electionTimeout=10 that's 14.5, matching T4.1's own regression test.
	wantMean := float64(r.electionTimeout) + float64(r.electionTimeout-1)/2
	gotMean := float64(sum) / float64(draws)
	if diff := gotMean - wantMean; diff < -0.2 || diff > 0.2 {
		t.Errorf("mean = %v, want within 0.2 of %v", gotMean, wantMean)
	}
}

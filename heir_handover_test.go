package raft

import (
	"testing"

	"go.etcd.io/raft/v3/stability"
)

// T4.6 (TASKS.md) -- graceful handover. Written before maybeGracefulHandover
// lands in raft.go, per the project's TDD convention. Exercises the
// DESIGN.md §2.6 trigger conditions directly against a *raft built via
// newTestRaft, using stability.Var (T4.6's other new-this-task addition) so
// the leader's own score can change mid-test, and manipulating
// tracker.Progress by hand for the heir's side -- same style as
// heir_selection_test.go and heir_timeout_test.go before it. A companion
// datadriven test (testdata/heir_graceful_handover.txt) covers the
// end-to-end wire path: degrade -> MsgTimeoutNow -> heir leads next term.

// newHandoverTestRaft builds a 3-voter (ids 1,2,3) leader raft with
// HeirElection and GracefulHandover on, a mutable own-score Var (initially
// healthy), and the given DegradeWindow/HandoverCooldown for fast tests.
// Returns the raft and the Var so the test can degrade/recover the score.
func newHandoverTestRaft(t *testing.T, degradeWindow, handoverCooldown int) (*raft, *stability.Var) {
	t.Helper()
	v := stability.NewVar(200) // healthy by default
	cfg := newTestConfig(1, 10, 1, newTestMemoryStorage(withPeers(1, 2, 3)))
	cfg.StabilityScorer = v
	cfg.HeirElection = true
	cfg.GracefulHandover = true
	cfg.DegradeWindow = degradeWindow
	cfg.HandoverCooldown = handoverCooldown
	r := newRaft(cfg)
	r.becomeCandidate()
	r.becomeLeader()
	return r, v
}

// setEligibleHeir designates id as an already-selected, fully-eligible heir
// with the given stability score, bypassing selectHeir (which T4.6's tests
// don't need to exercise again).
func setEligibleHeir(r *raft, id uint64, score uint8) {
	r.heir = id
	pr := r.trk.Progress[id]
	pr.RecentActive = true
	pr.ScoreReported = true
	pr.StabilityScore = score
	pr.Match = r.raftLog.lastIndex()
	// In sync in every recent sample (DESIGN_UPDATE.md D5/D6).
	pr.LastAckTick = r.leaderTicks
	pr.HeirSyncHistory = 1<<heirEntryWindow - 1
}

func TestMaybeGracefulHandover_TriggersAfterDegradeWindow(t *testing.T) {
	r, v := newHandoverTestRaft(t, 3, 100)
	setEligibleHeir(r, 2, 255) // comfortably above threshold(64)+margin(20)
	v.Set(30)                 // below HandoverThreshold=64

	for i := 0; i < 2; i++ {
		r.maybeGracefulHandover()
		if r.leadTransferee != None {
			t.Fatalf("iteration %d: leadTransferee = %d, want None before degradeWindow=3 is reached", i, r.leadTransferee)
		}
	}
	r.maybeGracefulHandover() // 3rd consecutive degraded tick
	if r.leadTransferee != 2 {
		t.Fatalf("leadTransferee = %d, want 2 after degradeWindow reached", r.leadTransferee)
	}
}

func TestMaybeGracefulHandover_NoTriggerWithoutEligibleHeir(t *testing.T) {
	r, v := newHandoverTestRaft(t, 2, 100)
	// r.heir left at None: no heir has ever been selected.
	v.Set(10)
	for i := 0; i < 5; i++ {
		r.maybeGracefulHandover()
	}
	if r.leadTransferee != None {
		t.Fatalf("leadTransferee = %d, want None with no eligible heir", r.leadTransferee)
	}
}

func TestMaybeGracefulHandover_NoTriggerWithoutSufficientMargin(t *testing.T) {
	r, v := newHandoverTestRaft(t, 2, 100)
	// HandoverThreshold=64, HysteresisMargin=20 (default) => heir needs >= 84.
	setEligibleHeir(r, 2, 70) // above threshold alone, but under the margin
	v.Set(10)
	for i := 0; i < 5; i++ {
		r.maybeGracefulHandover()
	}
	if r.leadTransferee != None {
		t.Fatalf("leadTransferee = %d, want None when heir's score doesn't clear threshold+margin", r.leadTransferee)
	}
}

func TestMaybeGracefulHandover_RecoveryResetsDegradeTicks(t *testing.T) {
	r, v := newHandoverTestRaft(t, 3, 100)
	setEligibleHeir(r, 2, 255)
	v.Set(30)
	r.maybeGracefulHandover() // degradeTicks 0->1
	r.maybeGracefulHandover() // degradeTicks 1->2

	v.Set(200) // recovers before hitting degradeWindow=3
	r.maybeGracefulHandover()
	if r.degradeTicks != 0 {
		t.Fatalf("degradeTicks = %d, want 0 after recovering above threshold", r.degradeTicks)
	}
	if r.leadTransferee != None {
		t.Fatalf("leadTransferee = %d, want None -- recovery should have prevented the trigger", r.leadTransferee)
	}

	v.Set(30)
	for i := 0; i < 2; i++ {
		r.maybeGracefulHandover()
		if r.leadTransferee != None {
			t.Fatalf("iteration %d: leadTransferee = %d, want None -- degradeTicks restarted from 0 after recovery", i, r.leadTransferee)
		}
	}
	r.maybeGracefulHandover()
	if r.leadTransferee != 2 {
		t.Fatalf("leadTransferee = %d, want 2 after a fresh full degradeWindow", r.leadTransferee)
	}
}

func TestMaybeGracefulHandover_CooldownPreventsRetrigger(t *testing.T) {
	r, v := newHandoverTestRaft(t, 1, 5)
	setEligibleHeir(r, 2, 255)
	v.Set(30)

	r.maybeGracefulHandover()
	if r.leadTransferee != 2 {
		t.Fatalf("leadTransferee = %d, want 2 after first trigger", r.leadTransferee)
	}

	// Simulate the in-flight transfer having already concluded (aborted or
	// completed) so leadTransferee no longer blocks a second attempt on its
	// own -- isolates the cooldown check specifically. handoverCooldown is
	// in election timeouts (DESIGN.md §5), so the tick count that must
	// elapse is handoverCooldown*electionTimeout, not handoverCooldown
	// itself.
	r.leadTransferee = None
	cooldownTicks := r.handoverCooldown * r.electionTimeout

	for i := 0; i < cooldownTicks-1; i++ {
		r.maybeGracefulHandover()
		if r.leadTransferee != None {
			t.Fatalf("iteration %d: leadTransferee = %d, want None -- cooldown (%d ticks) hasn't elapsed yet", i, r.leadTransferee, cooldownTicks)
		}
	}
	r.maybeGracefulHandover() // cooldownTicks-th tick since the trigger: elapsed
	if r.leadTransferee != 2 {
		t.Fatalf("leadTransferee = %d, want 2 once cooldown has elapsed", r.leadTransferee)
	}
}

func TestMaybeGracefulHandover_DisabledIsNoOp(t *testing.T) {
	v := stability.NewVar(10)
	cfg := newTestConfig(1, 10, 1, newTestMemoryStorage(withPeers(1, 2, 3)))
	cfg.StabilityScorer = v
	cfg.HeirElection = true
	// Explicit, not just left at newTestConfig's default: this test's premise
	// is GracefulHandover=false, which must hold regardless of
	// RAFT_HEIRRAFT_FORCE_ON (heir_conformance_test.go).
	cfg.GracefulHandover = false
	r := newRaft(cfg)
	r.becomeCandidate()
	r.becomeLeader()
	setEligibleHeir(r, 2, 255)

	for i := 0; i < 10; i++ {
		r.maybeGracefulHandover()
	}
	if r.leadTransferee != None {
		t.Fatalf("leadTransferee = %d, want None when GracefulHandover is disabled", r.leadTransferee)
	}
}

func TestRaftReset_ClearsHandoverSoftState(t *testing.T) {
	r, _ := newHandoverTestRaft(t, 3, 100)
	r.degradeTicks = 2
	r.handoverCooldownRemaining = 42

	r.reset(r.Term + 1)

	if r.degradeTicks != 0 {
		t.Errorf("degradeTicks = %d, want 0 after reset", r.degradeTicks)
	}
	if r.handoverCooldownRemaining != 0 {
		t.Errorf("handoverCooldownRemaining = %d, want 0 after reset", r.handoverCooldownRemaining)
	}
}

// T5.2 (TASKS.md) -- gracefulHandoverCount, added for etcd's
// etcd_heirraft_graceful_handover_total Prometheus metric (the task text's
// "export from raft Status"). Mirrors heirChurn's precedent exactly
// (T4.4): a cumulative, monitoring-only counter, deliberately NOT reset in
// (*raft).reset.

func TestMaybeGracefulHandover_CountsOnlyActualTriggers(t *testing.T) {
	r, v := newHandoverTestRaft(t, 3, 100)
	setEligibleHeir(r, 2, 255)
	v.Set(30) // below HandoverThreshold=64

	for i := 0; i < 2; i++ {
		r.maybeGracefulHandover()
		if r.gracefulHandoverCount != 0 {
			t.Fatalf("iteration %d: gracefulHandoverCount = %d, want 0 before degradeWindow=3 is reached", i, r.gracefulHandoverCount)
		}
	}
	r.maybeGracefulHandover() // 3rd consecutive degraded tick: triggers
	if r.gracefulHandoverCount != 1 {
		t.Fatalf("gracefulHandoverCount = %d, want 1 after the trigger", r.gracefulHandoverCount)
	}

	// Further ticks are gated by the cooldown -- count must not double-tick.
	for i := 0; i < 5; i++ {
		r.maybeGracefulHandover()
	}
	if r.gracefulHandoverCount != 1 {
		t.Fatalf("gracefulHandoverCount = %d, want still 1 while cooldown gates further attempts", r.gracefulHandoverCount)
	}
}

func TestMaybeGracefulHandover_GatedCallsNeverCount(t *testing.T) {
	// No eligible heir at all -- every call below is gated before the
	// trigger point maybeGracefulHandover_TriggersAfterDegradeWindow reaches.
	r, v := newHandoverTestRaft(t, 2, 100)
	v.Set(10)
	for i := 0; i < 10; i++ {
		r.maybeGracefulHandover()
	}
	if r.gracefulHandoverCount != 0 {
		t.Fatalf("gracefulHandoverCount = %d, want 0 with no eligible heir ever", r.gracefulHandoverCount)
	}
}

func TestGracefulHandoverCount_NotResetOnTermChange(t *testing.T) {
	r, v := newHandoverTestRaft(t, 1, 100)
	setEligibleHeir(r, 2, 255)
	v.Set(30)
	r.maybeGracefulHandover()
	if r.gracefulHandoverCount != 1 {
		t.Fatalf("gracefulHandoverCount = %d, want 1 after trigger", r.gracefulHandoverCount)
	}

	r.reset(r.Term + 1)

	if r.gracefulHandoverCount != 1 {
		t.Fatalf("gracefulHandoverCount = %d, want unchanged (1) after reset -- it's a cumulative monitoring counter, not soft state", r.gracefulHandoverCount)
	}
}

func TestBasicStatus_ExposesGracefulHandoverCount(t *testing.T) {
	r, v := newHandoverTestRaft(t, 1, 100)
	setEligibleHeir(r, 2, 255)
	v.Set(30)
	r.maybeGracefulHandover()

	if got := getBasicStatus(r).GracefulHandoverCount; got != 1 {
		t.Fatalf("getBasicStatus(r).GracefulHandoverCount = %d, want 1", got)
	}
}

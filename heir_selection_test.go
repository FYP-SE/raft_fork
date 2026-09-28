package raft

import (
	"testing"

	pb "go.etcd.io/raft/v3/raftpb"
	"go.etcd.io/raft/v3/stability"
)

// T4.4 (TASKS.md) -- heir selection + announcement. Written before the
// selectHeir/heirEligible/changeHeir/recordHeir/currentHeir implementation
// lands in raft.go, per the project's TDD convention. Exercises the
// DESIGN.md §2.3 algorithm directly against a *raft built via newTestRaft,
// manipulating tracker.Progress by hand rather than driving a full
// interaction-test cluster -- this lets edge cases (lag boundary, margin
// overflow, ineligibility) be asserted precisely and fast. A companion
// datadriven test (testdata/heir_selection.txt) covers the end-to-end wire
// path: score -> selection -> Heir field on MsgHeartbeat -> status output.

// newHeirTestRaft builds a 3-voter (ids 1,2,3) leader raft with HeirElection
// on and a ConstScorer(0) (irrelevant to these tests -- individual follower
// scores are set directly on tracker.Progress instead of flowing through the
// wire, since these tests are about the selection algorithm, not dissemination).
func newHeirTestRaft(t *testing.T, extraVoters ...uint64) *raft {
	t.Helper()
	peers := append([]uint64{1, 2, 3}, extraVoters...)
	cfg := newTestConfig(1, 10, 1, newTestMemoryStorage(withPeers(peers...)))
	cfg.StabilityScorer = stability.ConstScorer(0)
	cfg.HeirElection = true
	r := newRaft(cfg)
	r.becomeCandidate()
	r.becomeLeader()
	// Far enough from 0 that an "inactive" follower (LastAckTick 0) is
	// outside the electionTimeout ack window.
	r.leaderTicks = 1000
	return r
}

// setProgress overwrites the leader's view of follower id's progress for
// test setup. "active" means it acked at the current leader tick (these
// tests call selectHeir directly, which does not advance the clock). An
// active, reported follower also gets a full in-sync sample history, so
// these T4.4 tests exercise score/margin/tenure/exclusion rules rather than
// the D5 warm-up, which heir_insync_test.go covers.
func setProgress(r *raft, id uint64, active, reported bool, score uint8, match uint64) {
	pr := r.trk.Progress[id]
	pr.RecentActive = active
	pr.ScoreReported = reported
	pr.StabilityScore = score
	pr.Match = match
	if active {
		pr.LastAckTick = r.leaderTicks
	} else {
		pr.LastAckTick = 0
	}
	if active && reported {
		pr.HeirSyncHistory = 1<<heirEntryWindow - 1
	} else {
		pr.HeirSyncHistory = 0
	}
}

// selectUntilGraceExpires runs selection for one grace period's worth of
// heartbeat intervals minus one: the last call at which a heir that just
// went out of sync must still be kept (DESIGN_UPDATE.md D6).
func selectUntilGraceExpires(r *raft) {
	for i := 0; i < r.heirSyncGrace/r.heartbeatTimeout-1; i++ {
		r.selectHeir()
	}
}

func TestSelectHeir_PicksTopEligibleScorer(t *testing.T) {
	r := newHeirTestRaft(t)
	last := r.raftLog.lastIndex()
	setProgress(r, 2, true, true, 100, last)
	setProgress(r, 3, true, true, 200, last)

	r.selectHeir()

	if r.heir != 3 {
		t.Fatalf("heir = %d, want 3 (highest score)", r.heir)
	}
	if r.heirChurn != 0 {
		t.Fatalf("heirChurn = %d, want 0 (first-ever pick isn't churn)", r.heirChurn)
	}
}

func TestSelectHeir_IgnoresLearner(t *testing.T) {
	cfg := newTestConfig(1, 10, 1, newTestMemoryStorage(withPeers(1, 2), withLearners(3)))
	cfg.StabilityScorer = stability.ConstScorer(0)
	cfg.HeirElection = true
	r := newRaft(cfg)
	r.becomeCandidate()
	r.becomeLeader()

	last := r.raftLog.lastIndex()
	setProgress(r, 2, true, true, 50, last)
	setProgress(r, 3, true, true, 255, last) // learner, highest score

	r.selectHeir()

	if r.heir != 2 {
		t.Fatalf("heir = %d, want 2 (learner 3 must never be picked despite higher score)", r.heir)
	}
}

func TestSelectHeir_IgnoresInactiveOrUnreported(t *testing.T) {
	r := newHeirTestRaft(t)
	last := r.raftLog.lastIndex()
	setProgress(r, 2, true, true, 50, last)
	setProgress(r, 3, false, true, 255, last) // highest score, but not RecentActive

	r.selectHeir()
	if r.heir != 2 {
		t.Fatalf("heir = %d, want 2 (inactive follower 3 must be excluded)", r.heir)
	}

	setProgress(r, 3, true, false, 255, last) // active now, but never reported a score
	r.selectHeir()
	if r.heir != 2 {
		t.Fatalf("heir = %d, want 2 (follower 3 with ScoreReported=false must be excluded)", r.heir)
	}
}

func TestSelectHeir_IgnoresLaggingFollower(t *testing.T) {
	r := newHeirTestRaft(t)
	r.freshnessSlack = 0 // zero tolerance: any lag behind the freshest disqualifies
	last := r.raftLog.lastIndex()
	setProgress(r, 2, true, true, 50, last)
	setProgress(r, 3, true, true, 255, 0) // behind by last-0, highest score
	r.trk.Progress[3].HeirSyncHistory = 0 // never in sync: no warm-up credit

	r.selectHeir()
	if r.heir != 2 {
		t.Fatalf("heir = %d, want 2 (follower 3 lags beyond freshnessSlack=0)", r.heir)
	}
}

func TestSelectHeir_HysteresisMarginBlocksNearbyChallenger(t *testing.T) {
	r := newHeirTestRaft(t)
	r.hysteresisMargin = 20
	r.minHeirTenure = 0 // isolate the margin check from the tenure check
	last := r.raftLog.lastIndex()

	setProgress(r, 2, true, true, 200, last)
	setProgress(r, 3, false, false, 0, last) // not yet eligible
	r.selectHeir()
	if r.heir != 2 {
		t.Fatalf("heir = %d, want 2 (only eligible follower)", r.heir)
	}

	// Follower 3 becomes eligible with a score only 10 above the heir's --
	// below the 20-point margin. Run selection several times; it must never
	// flip, no matter how many intervals pass.
	setProgress(r, 3, true, true, 210, last)
	for i := 0; i < 5; i++ {
		r.selectHeir()
		if r.heir != 2 {
			t.Fatalf("iteration %d: heir = %d, want 2 (10-point lead is under the 20-point margin)", i, r.heir)
		}
	}
	if r.heirChurn != 0 {
		t.Fatalf("heirChurn = %d, want 0 (margin must prevent any replacement)", r.heirChurn)
	}
}

func TestSelectHeir_HysteresisTenureDelaysReplacement(t *testing.T) {
	r := newHeirTestRaft(t)
	r.hysteresisMargin = 20
	r.minHeirTenure = 3
	last := r.raftLog.lastIndex()

	setProgress(r, 2, true, true, 100, last)
	setProgress(r, 3, false, false, 0, last)
	r.selectHeir() // heir = 2, tenure = 0
	if r.heir != 2 {
		t.Fatalf("heir = %d, want 2", r.heir)
	}

	// Follower 3 clears the margin (255 vs 100) but the heir's tenure hasn't
	// reached minHeirTenure=3 yet -- replacement must wait. Each call that
	// doesn't replace bumps heirTenure by one (0->1->2->3); the check runs
	// before the bump, so it takes minHeirTenure+1 calls since heir 2 was set
	// before a replacement is allowed.
	setProgress(r, 3, true, true, 255, last)
	for i := 0; i < 3; i++ {
		r.selectHeir()
		if r.heir != 2 {
			t.Fatalf("iteration %d: heir = %d, want 2 (tenure hasn't reached minHeirTenure=3 yet)", i, r.heir)
		}
	}
	if r.heirChurn != 0 {
		t.Fatalf("heirChurn = %d, want 0 before tenure is satisfied", r.heirChurn)
	}

	r.selectHeir() // heirTenure has now reached 3 -> replace
	if r.heir != 3 {
		t.Fatalf("heir = %d, want 3 (margin cleared and minHeirTenure reached)", r.heir)
	}
	if r.heirChurn != 1 {
		t.Fatalf("heirChurn = %d, want 1 after the one replacement", r.heirChurn)
	}
}

func TestSelectHeir_ReplacesHeirThatBecomesIneligible(t *testing.T) {
	r := newHeirTestRaft(t)
	r.hysteresisMargin = 200 // deliberately huge, so only ineligibility can force a change
	r.minHeirTenure = 1000
	last := r.raftLog.lastIndex()

	setProgress(r, 2, true, true, 255, last)
	setProgress(r, 3, true, true, 10, last)
	r.selectHeir()
	if r.heir != 2 {
		t.Fatalf("heir = %d, want 2", r.heir)
	}

	// Heir 2 goes inactive. Even though 3's score is far below what the
	// margin/tenure gates would ever allow, a heir out of sync for the whole
	// grace period must be replaced -- but not before (DESIGN_UPDATE.md D6;
	// v1 replaced it on the first bad sample).
	setProgress(r, 2, false, true, 255, last)
	selectUntilGraceExpires(r)
	if r.heir != 2 {
		t.Fatalf("heir = %d, want 2 still inside the grace period", r.heir)
	}
	r.selectHeir()
	if r.heir != 3 {
		t.Fatalf("heir = %d, want 3 (heir 2 became ineligible; must be replaced regardless of margin/tenure)", r.heir)
	}
	if r.heirChurn != 1 {
		t.Fatalf("heirChurn = %d, want 1", r.heirChurn)
	}
}

func TestSelectHeir_NoEligibleFollowerClearsHeir(t *testing.T) {
	r := newHeirTestRaft(t)
	last := r.raftLog.lastIndex()
	setProgress(r, 2, true, true, 100, last)
	setProgress(r, 3, true, true, 50, last)
	r.selectHeir()
	if r.heir != 2 {
		t.Fatalf("heir = %d, want 2", r.heir)
	}

	setProgress(r, 2, false, true, 100, last)
	setProgress(r, 3, false, true, 50, last)
	selectUntilGraceExpires(r)
	r.selectHeir()
	if r.heir != None {
		t.Fatalf("heir = %d, want None (no eligible follower left)", r.heir)
	}
	if r.heirChurn != 1 {
		t.Fatalf("heirChurn = %d, want 1 (losing a heir counts as churn)", r.heirChurn)
	}
}

func TestSelectHeir_DisabledIsNoOp(t *testing.T) {
	cfg := newTestConfig(1, 10, 1, newTestMemoryStorage(withPeers(1, 2, 3)))
	// Explicit, not just left at newTestConfig's default: DESIGN.md/CLAUDE.md
	// constraint 4, behaviour must be a no-op, regardless of
	// RAFT_HEIRRAFT_FORCE_ON (heir_conformance_test.go).
	cfg.HeirElection = false
	cfg.HeirLogPriority = false
	cfg.GracefulHandover = false
	cfg.StabilityScorer = nil
	r := newRaft(cfg)
	r.becomeCandidate()
	r.becomeLeader()
	last := r.raftLog.lastIndex()
	setProgress(r, 2, true, true, 255, last)

	r.selectHeir()

	if r.heir != None {
		t.Fatalf("heir = %d, want None when HeirElection is disabled", r.heir)
	}
}

func TestRaftReset_ClearsHeirSoftStateButNotChurn(t *testing.T) {
	r := newHeirTestRaft(t)
	r.heir = 2
	r.heirTenure = 7
	r.heirChurn = 3
	r.knownHeir = 9

	r.reset(r.Term + 1)

	if r.heir != None {
		t.Errorf("heir = %d, want None after reset", r.heir)
	}
	if r.heirTenure != 0 {
		t.Errorf("heirTenure = %d, want 0 after reset", r.heirTenure)
	}
	if r.knownHeir != None {
		t.Errorf("knownHeir = %d, want None after reset", r.knownHeir)
	}
	if r.heirChurn != 3 {
		t.Errorf("heirChurn = %d, want unchanged at 3 (cumulative metric, not soft state)", r.heirChurn)
	}
}

func TestRaft_HeirStamp(t *testing.T) {
	r := newHeirTestRaft(t)
	if got := r.heirStamp(); got != nil {
		t.Fatalf("heirStamp() = %v, want nil before any heir is selected", got)
	}
	r.heir = 2
	got := r.heirStamp()
	if got == nil || *got != 2 {
		t.Fatalf("heirStamp() = %v, want pointer to 2", got)
	}
}

func TestRaft_RecordHeirAndCurrentHeir(t *testing.T) {
	cfg := newTestConfig(2, 10, 1, newTestMemoryStorage(withPeers(1, 2, 3)))
	cfg.StabilityScorer = stability.ConstScorer(0)
	cfg.HeirElection = true
	r := newRaft(cfg)
	r.becomeFollower(1, 1)

	if got := r.currentHeir(); got != None {
		t.Fatalf("currentHeir() = %d, want None before any announcement", got)
	}

	r.recordHeir(newHeirMsg(3))
	if r.knownHeir != 3 {
		t.Fatalf("knownHeir = %d, want 3", r.knownHeir)
	}
	if got := r.currentHeir(); got != 3 {
		t.Fatalf("currentHeir() = %d, want 3", got)
	}
	// No tick-based expiry any more: see heir_term_persistence_test.go.
}

func TestRaft_RecordHeirIgnoresAbsentField(t *testing.T) {
	cfg := newTestConfig(2, 10, 1, newTestMemoryStorage(withPeers(1, 2, 3)))
	r := newRaft(cfg)
	r.becomeFollower(1, 1)
	r.knownHeir = 5

	r.recordHeir(&pb.Message{}) // no Heir field set at all

	if r.knownHeir != 5 {
		t.Fatalf("recordHeir must be a no-op when m.Heir is nil, got knownHeir=%d", r.knownHeir)
	}
}

// newHeirMsg builds a minimal message carrying a non-nil Heir field, as a
// leader's MsgHeartbeat/MsgApp would (DESIGN.md §2.4).
func newHeirMsg(heir uint64) *pb.Message {
	return &pb.Message{Heir: new(heir)}
}

// T5.2 (TASKS.md) regression test for a real bug found via live 3-node
// cluster testing (2026-07-31, RESEARCH_LOG.md): vanilla raft's own
// MsgCheckQuorum handling (stepLeader, unrelated to HeirRaft) periodically
// zeroes every Progress.RecentActive as bookkeeping for the *next*
// CheckQuorum window. With etcd's real defaults (HeartbeatTick=1, so
// heartbeatElapsed hits its threshold on literally every tick), that reset
// coincides with a selectHeir call once every electionTimeout/heartbeatTimeout
// ticks. tickHeartbeat used to run the CheckQuorum branch (and its
// RecentActive reset) BEFORE the heir-selection branch, so on the colliding
// tick selectHeir read a just-zeroed RecentActive and treated a perfectly
// healthy heir as suddenly ineligible -- bypassing hysteresis/tenure
// entirely via the "heir became ineligible" branch in selectHeir, and
// immediately reselecting it the very next tick once fresh responses came
// in. Observed live as heir_changes_total climbing by thousands within
// seconds, forever, on any real cluster with CheckQuorum on (etcd always
// sets it true) -- never caught by Phase 4's unit/datadriven tests because
// none of them exercised CheckQuorum=true (newTestConfig defaults it
// false, and no T4.x heir test opted in). Fixed by running heir selection
// before the CheckQuorum branch in tickHeartbeat, so it always sees the
// prior interval's settled RecentActive state.
func TestTickHeartbeat_CheckQuorumResetDoesNotChurnHealthyHeir(t *testing.T) {
	cfg := newTestConfig(1, 3, 1, newTestMemoryStorage(withPeers(1, 2, 3)))
	cfg.StabilityScorer = stability.ConstScorer(0)
	cfg.HeirElection = true
	cfg.CheckQuorum = true // the real etcd default (raftConfig in bootstrap.go); newTestConfig itself defaults this false
	r := newRaft(cfg)
	r.becomeCandidate()
	r.becomeLeader()

	// Mark both followers active and reporting, as real heartbeat responses
	// would -- mirrors setEligibleHeir's style (heir_handover_test.go).
	for _, id := range []uint64{2, 3} {
		pr := r.trk.Progress[id]
		pr.RecentActive = true
		pr.ScoreReported = true
		pr.StabilityScore = 200
		pr.Match = r.raftLog.lastIndex()
		pr.LastAckTick = r.leaderTicks
	}

	var churn int
	var everNone, everSet bool
	for i := 0; i < 60; i++ { // several multiples of electionTimeout=3
		r.tickHeartbeat()
		// No heir during the D5 warm-up (3 of 5 samples) is expected; once
		// one is set it must never drop back to None.
		if r.heir != None {
			everSet = true
		} else if everSet {
			everNone = true
		}
		// Re-mark active on every tick, as real MsgHeartbeatResp/MsgAppResp
		// handling (recordHeir's callers) would -- the bug reproduces even
		// with continuously live followers, since the reset+reselect both
		// happen inside the very same tick, before any response can arrive.
		for _, id := range []uint64{2, 3} {
			pr := r.trk.Progress[id]
			pr.RecentActive = true
			pr.ScoreReported = true
			pr.StabilityScore = 200
			pr.Match = r.raftLog.lastIndex()
			pr.LastAckTick = r.leaderTicks
		}
	}
	churn = int(r.heirChurn)
	if !everSet {
		t.Fatal("no heir was ever selected")
	}

	if everNone {
		t.Error("heir dropped to None at some point despite followers being continuously eligible -- CheckQuorum's RecentActive reset raced selectHeir")
	}
	if churn != 0 {
		t.Errorf("heirChurn = %d, want 0 -- a continuously-eligible heir must never churn", churn)
	}
}

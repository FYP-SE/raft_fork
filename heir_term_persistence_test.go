package raft

import (
	"testing"

	pb "go.etcd.io/raft/v3/raftpb"
	"go.etcd.io/raft/v3/stability"
)

// DESIGN_UPDATE.md D4 -- followers keep the heir for the whole term.
//
// v1 expired a follower's knownHeir after HeirStaleness (4) heartbeat
// intervals of silence from the leader: 400 ms at defaults, well before the
// 1000 ms election timeout. So by the time a crashed leader's heir actually
// campaigned it had already forgotten it was the heir, and the heir path in
// becomePreCandidate (gated on currentHeir()==r.id) never ran (2026-09-27
// audit). D4 removes the tick expiry: knownHeir is valid until
// the term changes (reset) or the leader announces a different heir,
// including an explicit heir=0 when it drops one.

func newPersistenceTestRaft(t *testing.T, id uint64) *raft {
	t.Helper()
	cfg := newTestConfig(id, 10, 1, newTestMemoryStorage(withPeers(1, 2, 3)))
	cfg.StabilityScorer = stability.ConstScorer(0)
	cfg.HeirElection = true
	cfg.PreVote = true
	r := newRaft(cfg)
	r.becomeFollower(1, 3)
	return r
}

// A follower that stops hearing from the leader still knows the heir right
// up to its own election timeout.
func TestHeirPersistence_FollowerKeepsHeirThroughSilence(t *testing.T) {
	r := newPersistenceTestRaft(t, 1)
	r.recordHeir(newHeirMsg(2))
	r.resetRandomizedElectionTimeout()

	for i := 0; i < r.randomizedElectionTimeout-1; i++ {
		r.tick()
		if got := r.currentHeir(); got != 2 {
			t.Fatalf("after %d silent ticks currentHeir() = %d, want 2 (no tick expiry)", i+1, got)
		}
	}
	if r.state != StateFollower {
		t.Fatalf("state = %v, want still StateFollower before its timeout", r.state)
	}
}

// The heir itself still knows it is the heir when its timer fires, so its
// PreCandidate round takes the heir path (it redraws a short heir timeout,
// DESIGN_UPDATE.md D1). This was dead code in v1 because the belief had
// already expired.
func TestHeirPersistence_HeirStillHeirWhenItCampaigns(t *testing.T) {
	r := newPersistenceTestRaft(t, 1)
	r.recordHeir(newHeirMsg(1))
	r.resetRandomizedElectionTimeout()

	for i := 0; i < 5*r.electionTimeout && r.state != StatePreCandidate; i++ {
		r.tick()
	}
	if r.state != StatePreCandidate {
		t.Fatalf("state = %v, want StatePreCandidate after the heir's timeout", r.state)
	}
	if got := r.currentHeir(); got != r.id {
		t.Fatalf("currentHeir() = %d, want self (%d) at campaign time", got, r.id)
	}
	if got := r.randomizedElectionTimeout; got < r.heirTimeout || got >= r.electionTimeout {
		t.Fatalf("randomizedElectionTimeout = %d after the round, want a heir redraw in [%d,%d)", got, r.heirTimeout, r.electionTimeout)
	}
}

// A new term still clears the belief (unchanged from v1).
func TestHeirPersistence_ClearedOnTermChange(t *testing.T) {
	r := newPersistenceTestRaft(t, 1)
	r.recordHeir(newHeirMsg(2))
	r.becomeFollower(r.Term+1, 3)
	if got := r.currentHeir(); got != None {
		t.Fatalf("currentHeir() = %d, want None after a term change", got)
	}
}

// The leader stamps an explicit 0 when HeirElection is on and it has no
// heir, so followers drop a heir the leader dropped.
func TestHeirPersistence_LeaderStampsExplicitZero(t *testing.T) {
	r := newPersistenceTestRaft(t, 1)
	r.heir = None
	got := r.heirStamp()
	if got == nil || *got != 0 {
		t.Fatalf("heirStamp() = %v, want pointer to 0 when HeirElection is on and no heir is set", got)
	}
	r.heir = 2
	if got := r.heirStamp(); got == nil || *got != 2 {
		t.Fatalf("heirStamp() = %v, want pointer to 2", got)
	}
}

// With HeirElection off the stamp stays nil, so the wire format is
// byte-identical to stock.
func TestHeirPersistence_DisabledStampIsNil(t *testing.T) {
	cfg := newTestConfig(1, 10, 1, newTestMemoryStorage(withPeers(1, 2, 3)))
	cfg.HeirElection = false
	cfg.HeirLogPriority = false
	cfg.GracefulHandover = false
	cfg.StabilityScorer = nil
	r := newRaft(cfg)
	if got := r.heirStamp(); got != nil {
		t.Fatalf("heirStamp() = %v, want nil when HeirElection is off", got)
	}
}

// A follower that receives heir=0 forgets its old heir.
func TestHeirPersistence_FollowerClearsOnExplicitZero(t *testing.T) {
	r := newPersistenceTestRaft(t, 1)
	r.recordHeir(newHeirMsg(2))
	r.recordHeir(newHeirMsg(0))
	if got := r.currentHeir(); got != None {
		t.Fatalf("currentHeir() = %d, want None after an explicit heir=0", got)
	}
}

// End to end through Step: a heartbeat from the leader with no heir clears
// the follower's belief.
func TestHeirPersistence_HeartbeatWithZeroClears(t *testing.T) {
	r := newPersistenceTestRaft(t, 1)
	r.recordHeir(newHeirMsg(2))
	zero := uint64(0)
	if err := r.Step(&pb.Message{
		From: new(uint64(3)), To: new(uint64(1)), Term: new(r.Term),
		Type: pb.MsgHeartbeat.Enum(), Heir: &zero,
	}); err != nil {
		t.Fatal(err)
	}
	if got := r.currentHeir(); got != None {
		t.Fatalf("currentHeir() = %d, want None after heartbeat with heir=0", got)
	}
}

// reset() must clear the old term's heir belief BEFORE drawing the new
// term's timeout. Otherwise the old heir draws the heir timeout into a term
// where it is no longer heir (with D1's short heir timeout: a premature
// election under the new leader), and old non-heirs draw the backoff.
// Found 2026-09-28 via a flaky heir_edge_log_behind_heir.txt.
func TestHeirPersistence_ResetDrawsWithoutOldTermHeir(t *testing.T) {
	r := newPersistenceTestRaft(t, 1)
	sawVanillaOnly := false
	for i := 0; i < timeoutSamples; i++ {
		r.knownHeir = r.id // heir in the old term
		r.reset(r.Term + 1)
		got := r.randomizedElectionTimeout
		if got < r.electionTimeout || got >= 2*r.electionTimeout {
			t.Fatalf("randomizedElectionTimeout = %d after a term change, want vanilla [%d,%d)",
				got, r.electionTimeout, 2*r.electionTimeout)
		}
		// The v1 heir path draws only ET or ET+1 at ET=10; anything above
		// that can only come from the vanilla draw.
		if got > r.electionTimeout+1 {
			sawVanillaOnly = true
		}
	}
	if !sawVanillaOnly {
		t.Fatalf("every draw after reset was in the heir window: the old term's heir belief was used")
	}
}

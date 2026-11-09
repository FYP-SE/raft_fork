package raft

import (
	"testing"
	"time"

	"go.etcd.io/raft/v3/stability"
)

// DESIGN_UPDATE.md E8 -- RTT-ranked heir selection. Same style as
// heir_selection_test.go: drive selectHeir directly with hand-set progress.

// fakeRTT is a PeerRTTProvider backed by a map; missing ids have no estimate.
type fakeRTT map[uint64]time.Duration

func (f fakeRTT) PeerRTT(id uint64) (time.Duration, bool) {
	d, ok := f[id]
	return d, ok
}

func newRTTHeirTestRaft(t *testing.T, rtts fakeRTT, extraVoters ...uint64) *raft {
	t.Helper()
	peers := append([]uint64{1, 2, 3}, extraVoters...)
	cfg := newTestConfig(1, 10, 1, newTestMemoryStorage(withPeers(peers...)))
	cfg.StabilityScorer = stability.ConstScorer(0)
	cfg.HeirElection = true
	cfg.HeirRTTRank = true
	cfg.PeerRTT = rtts
	r := newRaft(cfg)
	r.becomeCandidate()
	r.becomeLeader()
	r.leaderTicks = 1000
	return r
}

func TestHeirRTT_ConfigRequiresHeirElectionAndProvider(t *testing.T) {
	cfg := newTestConfig(1, 10, 1, newTestMemoryStorage(withPeers(1, 2, 3)))
	cfg.StabilityScorer = stability.ConstScorer(0)
	cfg.HeirElection = false // explicit: newTestConfig turns it on under RAFT_HEIRRAFT_FORCE_ON
	cfg.HeirRTTRank = true
	cfg.PeerRTT = fakeRTT{}
	if err := cfg.validate(); err == nil {
		t.Fatal("HeirRTTRank without HeirElection must be rejected")
	}
	cfg.HeirElection = true
	cfg.PeerRTT = nil
	if err := cfg.validate(); err == nil {
		t.Fatal("HeirRTTRank without PeerRTT must be rejected")
	}
	cfg.PeerRTT = fakeRTT{}
	if err := cfg.validate(); err != nil {
		t.Fatalf("valid config rejected: %v", err)
	}
	if cfg.HeirRTTMargin != 2*time.Millisecond {
		t.Fatalf("HeirRTTMargin default = %v, want 2ms", cfg.HeirRTTMargin)
	}
}

func TestHeirRTT_PicksNearestHealthyOverHigherScore(t *testing.T) {
	r := newRTTHeirTestRaft(t, fakeRTT{2: 80 * time.Millisecond, 3: 12 * time.Millisecond})
	last := r.raftLog.lastIndex()
	setProgress(r, 2, true, true, 245, last) // healthiest, far
	setProgress(r, 3, true, true, 230, last) // within margin 40, near

	r.selectHeir()
	if r.heir != 3 {
		t.Fatalf("heir = %d, want 3 (healthy and nearest)", r.heir)
	}
}

func TestHeirRTT_UnhealthyNearNodeNotPicked(t *testing.T) {
	r := newRTTHeirTestRaft(t, fakeRTT{2: 80 * time.Millisecond, 3: 12 * time.Millisecond})
	last := r.raftLog.lastIndex()
	setProgress(r, 2, true, true, 245, last)
	setProgress(r, 3, true, true, 180, last) // 65 below best: fails the gate

	r.selectHeir()
	if r.heir != 2 {
		t.Fatalf("heir = %d, want 2 (3 is nearer but fails the health gate)", r.heir)
	}
}

func TestHeirRTT_IneligibleNearNodeNotPicked(t *testing.T) {
	r := newRTTHeirTestRaft(t, fakeRTT{2: 80 * time.Millisecond, 3: 12 * time.Millisecond})
	r.freshnessSlack = 0
	last := r.raftLog.lastIndex()
	setProgress(r, 2, true, true, 240, last)
	setProgress(r, 3, true, true, 240, 0) // lagging: freshness gate
	r.trk.Progress[3].HeirSyncHistory = 0

	r.selectHeir()
	if r.heir != 2 {
		t.Fatalf("heir = %d, want 2 (3 is nearer but not in sync)", r.heir)
	}
}

func TestHeirRTT_UnknownRTTRankedLast(t *testing.T) {
	r := newRTTHeirTestRaft(t, fakeRTT{2: 90 * time.Millisecond}) // 3 unknown
	last := r.raftLog.lastIndex()
	setProgress(r, 2, true, true, 230, last)
	setProgress(r, 3, true, true, 245, last)

	r.selectHeir()
	if r.heir != 2 {
		t.Fatalf("heir = %d, want 2 (3 has no RTT estimate)", r.heir)
	}
}

func TestHeirRTT_NoEstimatesFallsBackToScoreThenID(t *testing.T) {
	r := newRTTHeirTestRaft(t, fakeRTT{}, 4)
	last := r.raftLog.lastIndex()
	setProgress(r, 2, true, true, 230, last)
	setProgress(r, 3, true, true, 245, last)
	setProgress(r, 4, true, true, 245, last)

	r.selectHeir()
	if r.heir != 3 {
		t.Fatalf("heir = %d, want 3 (no RTTs: highest score, then lowest ID)", r.heir)
	}
}

func TestHeirRTT_MarginBlocksSmallRTTGain(t *testing.T) {
	rtts := fakeRTT{2: 20 * time.Millisecond, 3: 40 * time.Millisecond}
	r := newRTTHeirTestRaft(t, rtts)
	r.minHeirTenure = 0
	last := r.raftLog.lastIndex()
	setProgress(r, 2, true, true, 240, last)
	setProgress(r, 3, true, true, 240, last)
	r.selectHeir()
	if r.heir != 2 {
		t.Fatalf("heir = %d, want 2", r.heir)
	}

	rtts[3] = 19 * time.Millisecond // 1 ms better: under the 2 ms margin
	for i := 0; i < 5; i++ {
		r.selectHeir()
		if r.heir != 2 {
			t.Fatalf("iteration %d: heir = %d, want 2 (1 ms gain is under the margin)", i, r.heir)
		}
	}

	rtts[3] = 10 * time.Millisecond // 10 ms better: replace
	r.selectHeir()
	if r.heir != 3 {
		t.Fatalf("heir = %d, want 3 (RTT gain beyond the margin)", r.heir)
	}
	if r.heirChurn != 1 {
		t.Fatalf("heirChurn = %d, want 1", r.heirChurn)
	}
}

func TestHeirRTT_TenureDelaysReplacement(t *testing.T) {
	rtts := fakeRTT{2: 20 * time.Millisecond, 3: 40 * time.Millisecond}
	r := newRTTHeirTestRaft(t, rtts)
	r.minHeirTenure = 3
	last := r.raftLog.lastIndex()
	setProgress(r, 2, true, true, 240, last)
	setProgress(r, 3, true, true, 240, last)
	r.selectHeir()

	rtts[3] = 5 * time.Millisecond
	for i := 0; i < 3; i++ {
		r.selectHeir()
		if r.heir != 2 {
			t.Fatalf("iteration %d: heir = %d, want 2 (tenure not reached)", i, r.heir)
		}
	}
	r.selectHeir()
	if r.heir != 3 {
		t.Fatalf("heir = %d, want 3 (tenure reached)", r.heir)
	}
}

func TestHeirRTT_KeptHeirThatTurnsUnhealthyIsReplaced(t *testing.T) {
	r := newRTTHeirTestRaft(t, fakeRTT{2: 10 * time.Millisecond, 3: 60 * time.Millisecond})
	r.minHeirTenure = 0
	last := r.raftLog.lastIndex()
	setProgress(r, 2, true, true, 240, last)
	setProgress(r, 3, true, true, 240, last)
	r.selectHeir()
	if r.heir != 2 {
		t.Fatalf("heir = %d, want 2", r.heir)
	}

	setProgress(r, 2, true, true, 150, last) // still in sync, now 90 below 3
	r.selectHeir()
	if r.heir != 3 {
		t.Fatalf("heir = %d, want 3 (heir 2 fell below the health floor)", r.heir)
	}
}

func TestHeirRTT_KeptHeirBrieflyOutOfSyncKeptDuringGrace(t *testing.T) {
	r := newRTTHeirTestRaft(t, fakeRTT{2: 10 * time.Millisecond, 3: 60 * time.Millisecond})
	r.minHeirTenure = 0
	last := r.raftLog.lastIndex()
	setProgress(r, 2, true, true, 240, last)
	setProgress(r, 3, true, true, 240, last)
	r.selectHeir()

	// Heir 2 stops acking: kept until the sync grace runs out.
	r.trk.Progress[2].LastAckTick = 0
	selectUntilGraceExpires(r)
	if r.heir != 2 {
		t.Fatalf("heir = %d, want 2 (inside the sync grace)", r.heir)
	}
	r.selectHeir()
	if r.heir != 3 {
		t.Fatalf("heir = %d, want 3 (grace expired)", r.heir)
	}
}

func TestHeirRTT_NoCandidatesClearsHeir(t *testing.T) {
	r := newRTTHeirTestRaft(t, fakeRTT{2: 10 * time.Millisecond})
	last := r.raftLog.lastIndex()
	setProgress(r, 2, true, true, 240, last)
	setProgress(r, 3, false, false, 0, last)
	r.selectHeir()
	if r.heir != 2 {
		t.Fatalf("heir = %d, want 2", r.heir)
	}
	r.trk.Progress[2].LastAckTick = 0
	selectUntilGraceExpires(r)
	r.selectHeir()
	if r.heir != None {
		t.Fatalf("heir = %d, want none (no eligible follower after grace)", r.heir)
	}
}

// Regression (laptop smoke 2026-11-09): a kept heir that drops below the
// eligibility history while its latest sample is in sync must not be
// replaced by a nearer challenger; only the sync grace may drop it.
func TestHeirRTT_KeptHeirTemporarilyIneligibleNotReplacedByRTT(t *testing.T) {
	rtts := fakeRTT{2: 40 * time.Millisecond, 3: 60 * time.Millisecond}
	r := newRTTHeirTestRaft(t, rtts)
	r.minHeirTenure = 0
	last := r.raftLog.lastIndex()
	setProgress(r, 2, true, true, 240, last)
	setProgress(r, 3, false, false, 0, last)
	r.selectHeir()
	if r.heir != 2 {
		t.Fatalf("heir = %d, want 2", r.heir)
	}

	// 3 becomes eligible and nearer; heir 2 has only its latest sample in
	// sync (history 0b1), so it is below heirEntryNeeded.
	setProgress(r, 3, true, true, 240, last)
	rtts[3] = 5 * time.Millisecond
	r.trk.Progress[2].HeirSyncHistory = 0
	r.selectHeir()
	if r.heir != 2 {
		t.Fatalf("heir = %d, want 2 (ineligible but in grace: RTT must not replace it)", r.heir)
	}
}

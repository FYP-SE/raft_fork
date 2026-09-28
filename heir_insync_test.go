package raft

import (
	"testing"

	pb "go.etcd.io/raft/v3/raftpb"
)

// DESIGN_UPDATE.md D5 + D6 -- freshness gate and Kafka-ISR-style in-sync rule.
//
// Per heartbeat interval the leader samples each voter follower. It is in
// sync if all hold:
//   - it acked (MsgAppResp/MsgHeartbeatResp) within the last electionTimeout,
//   - it has reported a stability score this term,
//   - Match >= committed (holds every committed entry),
//   - Match >= freshest follower's Match - FreshnessSlack (default 1).
// A follower can BECOME heir once in sync in >= 3 of its last 5 samples.
// The current heir is dropped only after being out of sync continuously for
// HeirSyncGrace ticks (default: electionTimeout). Among eligible followers:
// highest score, then lowest ID. Nobody eligible -> no heir (stock timing).
// Values chosen by Piyumi 2026-09-28.

// interval advances the leader's clock by one heartbeat interval and runs
// selection, as tickHeartbeat does.
func interval(r *raft) {
	r.leaderTicks += uint64(r.heartbeatTimeout)
	r.selectHeir()
}

// ack records a response from follower id at the current leader tick.
func ack(r *raft, id uint64, score uint8, match uint64) {
	pr := r.trk.Progress[id]
	pr.LastAckTick = r.leaderTicks
	pr.ScoreReported = true
	pr.StabilityScore = score
	pr.Match = match
}

// intervals runs n intervals, calling each(i) before each one to set acks.
func intervals(r *raft, n int, each func(i int)) {
	for i := 0; i < n; i++ {
		each(i)
		interval(r)
	}
}

func TestInSync_NeedsThreeOfFiveSamples(t *testing.T) {
	r := newHeirTestRaft(t)
	last := r.raftLog.lastIndex()
	for i := 0; i < 2; i++ {
		ack(r, 2, 200, last)
		interval(r)
		if r.heir != None {
			t.Fatalf("after %d in-sync samples heir = %d, want None (needs 3 of 5)", i+1, r.heir)
		}
	}
	ack(r, 2, 200, last)
	interval(r)
	if r.heir != 2 {
		t.Fatalf("after 3 in-sync samples heir = %d, want 2", r.heir)
	}
}

func TestInSync_ThreeOfFiveToleratesGaps(t *testing.T) {
	r := newHeirTestRaft(t)
	last := r.raftLog.lastIndex()
	// in, out (stale), in, out, in -> 3 of the last 5 -> eligible.
	intervals(r, 5, func(i int) {
		m := last
		if i%2 == 1 {
			m = 0 // behind committed/freshest in this sample
		}
		ack(r, 2, 200, m)
		ack(r, 3, 100, 0) // never fresh
	})
	if r.heir != 2 {
		t.Fatalf("heir = %d, want 2 (in sync in 3 of the last 5 samples)", r.heir)
	}
}

func TestInSync_StaleFollowerNeverSelected(t *testing.T) {
	r := newHeirTestRaft(t)
	last := r.raftLog.lastIndex()
	r.raftLog.commitTo(last)
	// 3 has the best score but never holds every committed entry.
	intervals(r, 20, func(int) {
		ack(r, 2, 50, last)
		ack(r, 3, 255, last-1)
	})
	if r.heir != 2 {
		t.Fatalf("heir = %d, want 2: a follower below the commit index must never be heir", r.heir)
	}
}

func TestInSync_FreshnessSlack(t *testing.T) {
	r := newHeirTestRaft(t, 4)
	// Uncommitted tail so only the slack rule separates them: freshest
	// follower (2) at 10, 3 at 9 (within slack 1), 4 at 8 (outside).
	r.raftLog.commitTo(r.raftLog.lastIndex())
	base := r.raftLog.committed
	intervals(r, 5, func(int) {
		ack(r, 2, 10, base+2)
		ack(r, 3, 200, base+1)
		ack(r, 4, 255, base)
	})
	if r.heir != 3 {
		t.Fatalf("heir = %d, want 3: 4 is 2 behind the freshest (slack 1), 3 is within slack and outscores 2", r.heir)
	}
}

func TestInSync_StrictSlackZero(t *testing.T) {
	r := newHeirTestRaft(t)
	r.freshnessSlack = 0
	r.raftLog.commitTo(r.raftLog.lastIndex())
	base := r.raftLog.committed
	intervals(r, 5, func(int) {
		ack(r, 2, 10, base+1)
		ack(r, 3, 255, base)
	})
	if r.heir != 2 {
		t.Fatalf("heir = %d, want 2: with slack 0 only the freshest follower qualifies", r.heir)
	}
}

func TestInSync_TiesGoToLowestID(t *testing.T) {
	r := newHeirTestRaft(t)
	last := r.raftLog.lastIndex()
	intervals(r, 3, func(int) {
		ack(r, 2, 200, last)
		ack(r, 3, 200, last)
	})
	if r.heir != 2 {
		t.Fatalf("heir = %d, want 2 (equal gate and score -> lowest ID)", r.heir)
	}
}

func TestInSync_SingleSlowAckNoChurn(t *testing.T) {
	r := newHeirTestRaft(t)
	last := r.raftLog.lastIndex()
	intervals(r, 5, func(int) {
		ack(r, 2, 200, last)
		ack(r, 3, 100, last)
	})
	if r.heir != 2 {
		t.Fatalf("setup: heir = %d, want 2", r.heir)
	}
	// One interval where 2 falls behind, then it is back.
	ack(r, 2, 200, 0)
	ack(r, 3, 100, last)
	interval(r)
	intervals(r, 3, func(int) {
		ack(r, 2, 200, last)
		ack(r, 3, 100, last)
	})
	if r.heir != 2 || r.heirChurn != 0 {
		t.Fatalf("heir = %d churn = %d, want 2 and 0: one bad sample must not cause churn", r.heir, r.heirChurn)
	}
}

func TestInSync_SilentHeirDroppedAfterAckAgePlusGrace(t *testing.T) {
	r := newHeirTestRaft(t)
	last := r.raftLog.lastIndex()
	intervals(r, 5, func(int) {
		ack(r, 2, 200, last)
		ack(r, 3, 100, last)
	})
	if r.heir != 2 {
		t.Fatalf("setup: heir = %d, want 2", r.heir)
	}
	// 2 goes silent; 3 keeps acking. 2 stays in sync until its last ack is
	// older than ET, then out of sync for the grace period (ET), then drops.
	et := r.electionTimeout / r.heartbeatTimeout
	grace := r.heirSyncGrace / r.heartbeatTimeout
	lastAck := r.trk.Progress[2].LastAckTick
	for i := 0; r.heir == 2 && i < 10*et; i++ {
		ack(r, 3, 100, last)
		interval(r)
	}
	if r.heir != 3 {
		t.Fatalf("heir = %d, want 2 replaced by 3", r.heir)
	}
	// Dropped on the first sample where its ack is older than ET + grace.
	age := int(r.leaderTicks - lastAck)
	if want := (et + grace) * r.heartbeatTimeout; age != want {
		t.Fatalf("heir dropped at ack age %d ticks, want %d (ET + grace)", age, want)
	}
}

func TestInSync_NobodyEligibleMeansNoHeir(t *testing.T) {
	r := newHeirTestRaft(t)
	last := r.raftLog.lastIndex()
	intervals(r, 5, func(int) {
		ack(r, 2, 200, last)
	})
	if r.heir != 2 {
		t.Fatalf("setup: heir = %d, want 2", r.heir)
	}
	for i := 0; i < 50; i++ {
		interval(r) // nobody acks
	}
	if r.heir != None {
		t.Fatalf("heir = %d, want None once no follower is in sync", r.heir)
	}
	if got := r.heirStamp(); got == nil || *got != 0 {
		t.Fatalf("heirStamp() = %v, want explicit 0", got)
	}
}

func TestInSync_ConfigDefaults(t *testing.T) {
	r := newHeirTestRaft(t)
	if r.freshnessSlack != 1 {
		t.Errorf("freshnessSlack = %d, want default 1", r.freshnessSlack)
	}
	if r.heirSyncGrace != r.electionTimeout {
		t.Errorf("heirSyncGrace = %d, want default electionTimeout (%d)", r.heirSyncGrace, r.electionTimeout)
	}

	cfg := newTestConfig(1, 10, 1, newTestMemoryStorage(withPeers(1, 2, 3)))
	cfg.FreshnessSlack = FreshnessSlackStrict
	cfg.HeirSyncGrace = -1
	if err := cfg.validate(); err == nil {
		t.Errorf("validate() accepted HeirSyncGrace=-1")
	}
	cfg.HeirSyncGrace = 0
	cfg.FreshnessSlack = -2
	if err := cfg.validate(); err == nil {
		t.Errorf("validate() accepted FreshnessSlack=-2")
	}
	cfg.FreshnessSlack = FreshnessSlackStrict
	if err := cfg.validate(); err != nil {
		t.Fatalf("validate(): %v", err)
	}
	if r2 := newRaft(cfg); r2.freshnessSlack != 0 {
		t.Errorf("FreshnessSlackStrict -> freshnessSlack = %d, want 0", r2.freshnessSlack)
	}
}

// The leader records the ack tick from real responses, not only in tests.
func TestInSync_AckTickFromResponses(t *testing.T) {
	r := newHeirTestRaft(t)
	r.leaderTicks = 42
	if err := r.Step(&pb.Message{From: new(uint64(2)), To: new(uint64(1)), Term: new(r.Term), Type: pb.MsgHeartbeatResp.Enum()}); err != nil {
		t.Fatal(err)
	}
	if got := r.trk.Progress[2].LastAckTick; got != 42 {
		t.Fatalf("LastAckTick after MsgHeartbeatResp = %d, want 42", got)
	}
	r.leaderTicks = 50
	if err := r.Step(&pb.Message{From: new(uint64(3)), To: new(uint64(1)), Term: new(r.Term), Type: pb.MsgAppResp.Enum(), Index: new(r.raftLog.lastIndex())}); err != nil {
		t.Fatal(err)
	}
	if got := r.trk.Progress[3].LastAckTick; got != 50 {
		t.Fatalf("LastAckTick after MsgAppResp = %d, want 50", got)
	}
}

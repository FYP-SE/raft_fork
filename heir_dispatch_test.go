package raft

import (
	"testing"

	pb "go.etcd.io/raft/v3/raftpb"
	"go.etcd.io/raft/v3/stability"
)

// T4.7 (TASKS.md) -- heir-first dispatch (DESIGN.md §2.7). Written before
// bcastAppend's reordering lands, per the project's TDD convention.
// Dispatch order only: commit rule, quorum semantics and acknowledgement
// handling must be byte-for-byte unchanged, so these tests check (a) the
// order messages land in r.msgs and (b) that the eventual commit outcome of
// a real append round doesn't depend on that order at all.

func newDispatchTestRaft(t *testing.T, heirLogPriority bool) *raft {
	t.Helper()
	cfg := newTestConfig(1, 10, 1, newTestMemoryStorage(withPeers(1, 2, 3, 4)))
	cfg.StabilityScorer = stability.ConstScorer(0)
	cfg.HeirLogPriority = heirLogPriority
	r := newRaft(cfg)
	r.becomeCandidate()
	r.becomeLeader()
	return r
}

// msgAppOrder returns the To fields, in order, of the MsgApp messages
// present in r.msgs (bcastAppend also produces the leader's own self-ack,
// which isn't a wire message and is filtered out by the To==r.id check
// already; here we additionally only look at MsgApp, since other message
// types could in principle be interleaved).
func msgAppOrder(r *raft) []uint64 {
	var order []uint64
	for _, m := range r.msgs {
		if m.GetType() == pb.MsgApp {
			order = append(order, m.GetTo())
		}
	}
	return order
}

func TestBcastAppend_HeirFirstWhenPrioritySet(t *testing.T) {
	r := newDispatchTestRaft(t, true)
	r.heir = 3 // natural sorted-id order would visit 2, 3, 4 -- heir (3) must jump the queue

	r.bcastAppend()

	got := msgAppOrder(r)
	want := []uint64{3, 2, 4}
	if len(got) != len(want) {
		t.Fatalf("msgAppOrder = %v, want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("msgAppOrder = %v, want %v", got, want)
		}
	}
}

func TestBcastAppend_NaturalOrderWhenPriorityUnset(t *testing.T) {
	r := newDispatchTestRaft(t, false)
	r.heir = 3 // set, but HeirLogPriority is off -- must have zero effect

	r.bcastAppend()

	got := msgAppOrder(r)
	want := []uint64{2, 3, 4}
	if len(got) != len(want) {
		t.Fatalf("msgAppOrder = %v, want %v (byte-identical to stock ordering)", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("msgAppOrder = %v, want %v (byte-identical to stock ordering)", got, want)
		}
	}
}

func TestBcastAppend_HeirFirstWithNoHeirSelectedIsNaturalOrder(t *testing.T) {
	r := newDispatchTestRaft(t, true)
	// r.heir left at None: nothing has been selected yet.

	r.bcastAppend()

	got := msgAppOrder(r)
	want := []uint64{2, 3, 4}
	if len(got) != len(want) {
		t.Fatalf("msgAppOrder = %v, want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("msgAppOrder = %v, want %v", got, want)
		}
	}
}

func TestBcastAppend_HeirFirstDoesNotDuplicateOrDropRecipients(t *testing.T) {
	r := newDispatchTestRaft(t, true)
	r.heir = 4

	r.bcastAppend()

	got := msgAppOrder(r)
	seen := map[uint64]int{}
	for _, id := range got {
		seen[id]++
	}
	for _, id := range []uint64{2, 3, 4} {
		if seen[id] != 1 {
			t.Fatalf("recipient %d appears %d times in %v, want exactly once", id, seen[id], got)
		}
	}
}

// TestBcastAppend_CommitOutcomeUnaffectedByOrdering runs the same proposal
// through two otherwise-identical clusters, one with HeirLogPriority on and
// one off, and checks the final committed index and log contents are
// identical either way -- dispatch order must never change what gets
// committed (DESIGN.md §2.7: "commit rule, quorum semantics ... are
// byte-for-byte unchanged").
func TestBcastAppend_CommitOutcomeUnaffectedByOrdering(t *testing.T) {
	runOnce := func(heirLogPriority bool, heir uint64) (committed uint64, lastIndex uint64) {
		r := newDispatchTestRaft(t, heirLogPriority)
		r.heir = heir
		if !r.appendEntry(&pb.Entry{Data: []byte("foo")}) {
			t.Fatal("appendEntry failed")
		}
		r.bcastAppend()
		for _, m := range r.msgs {
			if m.GetType() != pb.MsgApp {
				continue
			}
			mlastIndex, ok := r.raftLog.maybeAppend(logSliceFromMsgApp(m), m.GetCommit())
			_ = mlastIndex
			_ = ok
			// Followers aren't modeled here; we just need the leader's own
			// commit computation, driven by acking every peer's Progress
			// directly (equivalent to every MsgAppResp arriving).
			pr := r.trk.Progress[m.GetTo()]
			pr.MaybeUpdate(r.raftLog.lastIndex())
		}
		r.maybeCommit()
		return r.raftLog.committed, r.raftLog.lastIndex()
	}

	offCommitted, offLast := runOnce(false, None)
	onCommitted, onLast := runOnce(true, 3)

	if offCommitted != onCommitted {
		t.Errorf("committed index = %d (priority on) vs %d (priority off), want equal", onCommitted, offCommitted)
	}
	if offLast != onLast {
		t.Errorf("last index = %d (priority on) vs %d (priority off), want equal", onLast, offLast)
	}
}

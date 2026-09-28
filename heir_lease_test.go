package raft

import (
	"testing"

	pb "go.etcd.io/raft/v3/raftpb"
	"go.etcd.io/raft/v3/stability"
)

// DESIGN_UPDATE.md D2 -- heir lease.
//
// CheckQuorum's lease makes a voter ignore any (Pre)Vote while it has heard
// from a leader within electionTimeout. That capped D1: the heir fired at
// ~300 ms but was ignored until ~1000 ms. With HeirLease on, a voter skips
// the lease for exactly one sender -- the heir the leader announced to it --
// and only once the voter itself has not heard from the leader for
// HeirTimeout ticks. One vote per term and the log up-to-date check are
// unchanged, so a stale heir still loses, and a heir cut off from a leader
// that the voters still hear is still ignored.

// newLeaseVoter builds voter 3 in a 3-node cluster (leader 1, heir 2),
// following leader 1 at term 1 with CheckQuorum and PreVote on.
func newLeaseVoter(t *testing.T, heirLease bool) *raft {
	t.Helper()
	cfg := newTestConfig(3, 10, 1, newTestMemoryStorage(withPeers(1, 2, 3)))
	cfg.StabilityScorer = stability.ConstScorer(0)
	cfg.HeirElection = true
	cfg.HeirLease = heirLease
	cfg.CheckQuorum = true
	cfg.PreVote = true
	r := newRaft(cfg)
	r.becomeFollower(1, 1)
	r.recordHeir(newHeirMsg(2))
	return r
}

// voteReq is a (Pre)Vote from `from` for term 2 with a log as up to date as
// the voter's own (lastIndex/lastTerm), unless overridden.
func voteReq(r *raft, typ pb.MessageType, from uint64) *pb.Message {
	last := r.raftLog.lastEntryID()
	return &pb.Message{
		From: new(from), To: new(r.id), Term: new(r.Term + 1), Type: typ.Enum(),
		LogTerm: new(last.term), Index: new(last.index),
	}
}

// response returns the single vote response the voter sent, or nil if it
// ignored the request.
func response(t *testing.T, r *raft) *pb.Message {
	t.Helper()
	var out *pb.Message
	for _, m := range r.readMessages() {
		if typ := m.GetType(); typ == pb.MsgPreVoteResp || typ == pb.MsgVoteResp {
			if out != nil {
				t.Fatalf("more than one vote response")
			}
			out = m
		}
	}
	return out
}

func silence(r *raft, ticks int) {
	for i := 0; i < ticks; i++ {
		r.tick()
	}
	r.readMessages()
}

// (a) The known heir gets the vote once the voter has itself seen HeirTimeout
// ticks of leader silence -- long before the lease would expire.
func TestHeirLease_GrantsHeirAfterHeirTimeoutOfSilence(t *testing.T) {
	for _, typ := range []pb.MessageType{pb.MsgPreVote, pb.MsgVote} {
		r := newLeaseVoter(t, true)
		silence(r, r.heirTimeout)
		if err := r.Step(voteReq(r, typ, 2)); err != nil {
			t.Fatal(err)
		}
		resp := response(t, r)
		if resp == nil || resp.GetReject() {
			t.Fatalf("%v from heir after %d silent ticks: response %v, want a granted vote", typ, r.heirTimeout, resp)
		}
	}
}

// (b) A heir with a bad link to a leader the voter still hears is ignored.
func TestHeirLease_IgnoresHeirWhileVoterHearsLeader(t *testing.T) {
	r := newLeaseVoter(t, true)
	silence(r, r.heirTimeout-1)
	if err := r.Step(voteReq(r, pb.MsgPreVote, 2)); err != nil {
		t.Fatal(err)
	}
	if resp := response(t, r); resp != nil {
		t.Fatalf("PreVote from heir after %d silent ticks: got %v, want ignored (voter still in contact)", r.heirTimeout-1, resp)
	}
}

// (c) No exception for a non-heir.
func TestHeirLease_NonHeirStillHitsLease(t *testing.T) {
	r := newLeaseVoter(t, true)
	silence(r, r.electionTimeout-1)
	if err := r.Step(voteReq(r, pb.MsgPreVote, 1)); err != nil { // 1 is not the heir
		t.Fatal(err)
	}
	if resp := response(t, r); resp != nil {
		t.Fatalf("PreVote from non-heir inside the lease: got %v, want ignored", resp)
	}
}

// (d) The log up-to-date check still rejects a stale heir.
func TestHeirLease_LogCheckStillRejectsStaleHeir(t *testing.T) {
	r := newLeaseVoter(t, true)
	r.raftLog.append(&pb.Entry{Term: new(uint64(1)), Index: new(r.raftLog.lastIndex() + 1)})
	silence(r, r.heirTimeout)
	req := voteReq(r, pb.MsgPreVote, 2)
	req.Index = new(r.raftLog.lastIndex() - 1) // heir one entry behind
	if err := r.Step(req); err != nil {
		t.Fatal(err)
	}
	resp := response(t, r)
	if resp == nil || !resp.GetReject() {
		t.Fatalf("PreVote from log-behind heir: got %v, want an explicit rejection", resp)
	}
}

// (e) HeirLease off: stock lease behaviour for the heir too.
func TestHeirLease_OffKeepsStockLease(t *testing.T) {
	r := newLeaseVoter(t, false)
	silence(r, r.heirTimeout)
	if err := r.Step(voteReq(r, pb.MsgPreVote, 2)); err != nil {
		t.Fatal(err)
	}
	if resp := response(t, r); resp != nil {
		t.Fatalf("HeirLease off: got %v, want ignored inside the lease", resp)
	}
}

// One vote per term is unchanged: after granting the heir a real vote, the
// voter refuses a second candidate in the same term.
func TestHeirLease_OneVotePerTermUnchanged(t *testing.T) {
	r := newLeaseVoter(t, true)
	silence(r, r.heirTimeout)
	if err := r.Step(voteReq(r, pb.MsgVote, 2)); err != nil {
		t.Fatal(err)
	}
	if resp := response(t, r); resp == nil || resp.GetReject() {
		t.Fatalf("vote for heir: %v, want granted", resp)
	}
	other := voteReq(r, pb.MsgVote, 1)
	other.Term = new(r.Term) // same term the heir just won the vote in
	if err := r.Step(other); err != nil {
		t.Fatal(err)
	}
	if resp := response(t, r); resp == nil || !resp.GetReject() {
		t.Fatalf("second vote in the same term: %v, want rejected", resp)
	}
}

// Under ReadOnlyLeaseBased, D2 is disabled (lease reads rely on the full
// lease), the same way raft ignores MsgForgetLeader there.
func TestHeirLease_DisabledUnderLeaseBasedReads(t *testing.T) {
	cfg := newTestConfig(3, 10, 1, newTestMemoryStorage(withPeers(1, 2, 3)))
	cfg.StabilityScorer = stability.ConstScorer(0)
	cfg.HeirElection = true
	cfg.HeirLease = true
	cfg.CheckQuorum = true
	cfg.PreVote = true
	cfg.ReadOnlyOption = ReadOnlyLeaseBased
	r := newRaft(cfg)
	if r.heirLease {
		t.Fatal("heirLease = true under ReadOnlyLeaseBased, want disabled")
	}
	r.becomeFollower(1, 1)
	r.recordHeir(newHeirMsg(2))
	silence(r, r.heirTimeout)
	if err := r.Step(voteReq(r, pb.MsgPreVote, 2)); err != nil {
		t.Fatal(err)
	}
	if resp := response(t, r); resp != nil {
		t.Fatalf("ReadOnlyLeaseBased: got %v, want the heir ignored inside the lease", resp)
	}
}

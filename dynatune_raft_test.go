// Copyright 2026 The etcd Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package raft

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	pb "go.etcd.io/raft/v3/raftpb"
)

// Behaviour tests for the Dynatune wiring in raft.go (task C5):
//   A. switch off  => identical to stock (guards; these must pass before AND after)
//   B. leader side
//   C. follower side
//   D. elections
// The pure maths is covered in dynatune_test.go; Config in dynatune_config_test.go.
//
// Units: with Dynatune on, one tick is one millisecond, so the helpers use
// ElectionTick=1000 / HeartbeatTick=100 (etcd's defaults in ms).

// ------------------------------------------------------------------ helpers

type fakeClock struct{ now time.Time }

func newFakeClock() *fakeClock               { return &fakeClock{now: t0} }
func (c *fakeClock) Now() time.Time          { return c.now }
func (c *fakeClock) Advance(d time.Duration) { c.now = c.now.Add(d) }

const (
	dynElection  = 1000
	dynHeartbeat = 100
)

func dynConfig(id uint64, clk *fakeClock) *Config {
	cfg := newTestConfig(id, dynElection, dynHeartbeat, newTestMemoryStorage(withPeers(1, 2, 3)))
	cfg.Dynatune = true
	cfg.Clock = clk.Now
	cfg.MaxElectionMetricsCapacity = 100
	cfg.MinElectionMetricsCapacity = 3
	cfg.ElectionSafetyFactor = 3
	cfg.HeartbeatReachabilityGoal = 0.99
	cfg.K = 5
	cfg.OptimizeHeartbeatInterval = true
	return cfg
}

func newDynatuneRaft(id uint64, clk *fakeClock, mods ...func(*Config)) *raft {
	cfg := dynConfig(id, clk)
	for _, m := range mods {
		m(cfg)
	}
	return newRaft(cfg)
}

// newDynatuneLeader returns node 1 of {1,2,3} as leader of term 1, with its
// initial messages drained.
func newDynatuneLeader(clk *fakeClock, mods ...func(*Config)) *raft {
	r := newDynatuneRaft(1, clk, mods...)
	r.becomeCandidate()
	r.becomeLeader()
	r.advanceMessagesAfterAppend()
	r.readMessages()
	return r
}

// newDynatuneFollower returns node 2 of {1,2,3} as follower of leader 1, term 1.
func newDynatuneFollower(clk *fakeClock, mods ...func(*Config)) *raft {
	r := newDynatuneRaft(2, clk, mods...)
	r.becomeFollower(1, 1)
	return r
}

func filterType(msgs []*pb.Message, typ pb.MessageType) []*pb.Message {
	var out []*pb.Message
	for _, m := range msgs {
		if m.GetType() == typ {
			out = append(out, m)
		}
	}
	return out
}

func heartbeats(msgs []*pb.Message) []*pb.Message { return filterType(msgs, pb.MsgHeartbeat) }

// tickHeartbeats ticks r n times and returns the heartbeats sent on each tick.
func tickHeartbeats(r *raft, n int) [][]*pb.Message {
	out := make([][]*pb.Message, 0, n)
	for i := 0; i < n; i++ {
		r.tick()
		out = append(out, heartbeats(r.readMessages()))
	}
	return out
}

// countByTarget tallies heartbeats per destination over n ticks.
func countByTarget(r *raft, n int) map[uint64]int {
	counts := map[uint64]int{}
	for _, tick := range tickHeartbeats(r, n) {
		for _, m := range tick {
			counts[m.GetTo()]++
		}
	}
	return counts
}

func requireNoDynatuneFields(t *testing.T, m *pb.Message) {
	t.Helper()
	require.Nil(t, m.Rtt, "Rtt")
	require.Nil(t, m.SendTime, "SendTime")
	require.Nil(t, m.SequenceId, "SequenceId")
	require.Nil(t, m.HeartbeatInterval, "HeartbeatInterval")
}

// hbResp is a MsgHeartbeatResp from follower `from` to leader 1 (term 1).
func hbResp(from uint64, opts ...func(*pb.Message)) *pb.Message {
	m := &pb.Message{Type: pb.MsgHeartbeatResp.Enum(), From: new(from), To: new(uint64(1)), Term: new(uint64(1))}
	for _, o := range opts {
		o(m)
	}
	return m
}

func withSendTime(tm time.Time) func(*pb.Message) {
	return func(m *pb.Message) { m.SendTime = new(tm.UnixNano()) }
}

func withInterval(v int64) func(*pb.Message) {
	return func(m *pb.Message) { m.HeartbeatInterval = new(v) }
}

func msgBeat() *pb.Message { return &pb.Message{Type: pb.MsgBeat.Enum()} }

// hbMsg is a MsgHeartbeat from leader 1 to follower 2 (term 1) carrying the
// Dynatune fields.
func hbMsg(seq int64, rtt time.Duration, sendTime time.Time, commit uint64) *pb.Message {
	return &pb.Message{
		Type: pb.MsgHeartbeat.Enum(), From: new(uint64(1)), To: new(uint64(2)), Term: new(uint64(1)),
		Commit: new(commit), Context: []byte("ctx"),
		Rtt: new(int64(rtt)), SendTime: new(sendTime.UnixNano()), SequenceId: new(seq),
	}
}

// plainHb is a stock heartbeat: none of the Dynatune fields.
func plainHb(commit uint64) *pb.Message {
	return &pb.Message{
		Type: pb.MsgHeartbeat.Enum(), From: new(uint64(1)), To: new(uint64(2)), Term: new(uint64(1)),
		Commit: new(commit),
	}
}

func onlyResp(t *testing.T, msgs []*pb.Message) *pb.Message {
	t.Helper()
	resps := filterType(msgs, pb.MsgHeartbeatResp)
	require.Len(t, resps, 1)
	return resps[0]
}

// feed delivers one heartbeat per sequence id (send time = t0 + seq seconds, so
// timestamps are strictly increasing) and returns the responses.
func feed(t *testing.T, r *raft, rtt time.Duration, seqs ...int64) []*pb.Message {
	t.Helper()
	var resps []*pb.Message
	for _, s := range seqs {
		require.NoError(t, r.Step(hbMsg(s, rtt, t0.Add(time.Duration(s)*time.Second), 0)))
		resps = append(resps, onlyResp(t, r.readMessages()))
	}
	return resps
}

func requireStockTimeoutRange(t *testing.T, r *raft) {
	t.Helper()
	require.GreaterOrEqual(t, r.randomizedElectionTimeout, r.electionTimeout)
	require.Less(t, r.randomizedElectionTimeout, 2*r.electionTimeout)
}

// ============================================================================
// A. Dynatune OFF => identical to stock
// ============================================================================

func TestDynatuneOff_ZeroValueAllocatesNothing(t *testing.T) {
	cfg := &Config{
		ID: 1, ElectionTick: 10, HeartbeatTick: 1,
		Storage: newTestMemoryStorage(withPeers(1, 2, 3)), MaxSizePerMsg: noLimit, MaxInflightMsgs: 256,
	}
	require.NoError(t, cfg.validate())
	r := newRaft(cfg)
	r.becomeCandidate()
	r.becomeLeader()
	require.Nil(t, r.followerMetrics)
	require.Nil(t, r.leaderMetrics)
	require.Empty(t, r.heartbeatStates)
}

func TestDynatuneOff_LeaderHeartbeatCarriesNoDynatuneFields(t *testing.T) {
	r := newTestRaft(1, 10, 1, newTestMemoryStorage(withPeers(1, 2, 3)))
	r.becomeCandidate()
	r.becomeLeader()
	r.readMessages()
	require.NoError(t, r.Step(msgBeat()))
	hbs := heartbeats(r.readMessages())
	require.Len(t, hbs, 2)
	for _, m := range hbs {
		requireNoDynatuneFields(t, m)
	}
}

func TestDynatuneOff_FollowerResponseCarriesNoDynatuneFields(t *testing.T) {
	r := newTestRaft(2, 10, 1, newTestMemoryStorage(withPeers(1, 2, 3)))
	r.becomeFollower(1, 1)
	// even if a (Dynatune) leader stamps its heartbeat
	require.NoError(t, r.Step(hbMsg(1, ms(5), t0, 0)))
	requireNoDynatuneFields(t, onlyResp(t, r.readMessages()))
}

func TestDynatuneOff_LeaderKeepsTheLegacyScheduleOneBroadcastPerHeartbeatTick(t *testing.T) {
	r := newTestRaft(1, 10, 3, newTestMemoryStorage(withPeers(1, 2, 3)))
	r.becomeCandidate()
	r.becomeLeader()
	r.readMessages()
	var perTick []int
	for _, tick := range tickHeartbeats(r, 6) {
		perTick = append(perTick, len(tick))
	}
	require.Equal(t, []int{0, 0, 2, 0, 0, 2}, perTick, "both followers every 3rd tick, nothing else")
}

func TestDynatuneOff_FollowerIgnoresDynatuneFieldsOnHeartbeat(t *testing.T) {
	r := newTestRaft(2, 10, 1, newTestMemoryStorage(withPeers(1, 2, 3)))
	r.becomeFollower(1, 1)
	for i := int64(1); i <= 20; i++ {
		require.NoError(t, r.Step(hbMsg(i, ms(54.2), t0.Add(time.Duration(i)*time.Second), 0)))
		r.readMessages()
		requireStockTimeoutRange(t, r)
	}
	require.Nil(t, r.followerMetrics, "no metrics are collected when off")
}

func TestDynatuneOff_LeaderIgnoresDynatuneFieldsOnResponse(t *testing.T) {
	r := newTestRaft(1, 10, 3, newTestMemoryStorage(withPeers(1, 2, 3)))
	r.becomeCandidate()
	r.becomeLeader()
	r.readMessages()
	r.trk.Progress[2].RecentActive = false
	require.NotPanics(t, func() {
		require.NoError(t, r.Step(hbResp(2, withSendTime(t0), withInterval(1))))
	})
	require.True(t, r.trk.Progress[2].RecentActive, "legacy handling still runs")
	require.Empty(t, r.heartbeatStates)
	r.readMessages()
	var perTick []int
	for _, tick := range tickHeartbeats(r, 6) {
		perTick = append(perTick, len(tick))
	}
	require.Equal(t, []int{0, 0, 2, 0, 0, 2}, perTick, "a requested interval of 1 must not change the legacy schedule")
}

// ============================================================================
// B. Leader side
// ============================================================================

func TestDynatune_LeaderCreatesAScheduleForEachFollowerOnly(t *testing.T) {
	r := newDynatuneLeader(newFakeClock())
	require.Len(t, r.heartbeatStates, 2)
	for _, id := range []uint64{2, 3} {
		require.Equal(t, &heartbeatState{elapsed: 0, timeout: dynHeartbeat, sequenceId: 0}, r.heartbeatStates[id])
	}
	_, self := r.heartbeatStates[1]
	require.False(t, self)
}

func TestDynatune_HeartbeatIsStampedWithClockSequenceAndLastRTT(t *testing.T) {
	clk := newFakeClock()
	r := newDynatuneLeader(clk)

	require.NoError(t, r.Step(msgBeat())) // a non-tick broadcast must stamp too
	hbs := heartbeats(r.readMessages())
	require.Len(t, hbs, 2)
	for _, m := range hbs {
		require.NotNil(t, m.SendTime)
		require.Equal(t, t0.UnixNano(), m.GetSendTime())
		require.NotNil(t, m.SequenceId)
		require.Zero(t, m.GetSequenceId(), "no tick-driven send yet: current id")
		require.NotNil(t, m.Rtt)
		require.Zero(t, m.GetRtt(), "no RTT measured yet")
	}

	// node 2 answers 7 ms later; the leader measures RTT = now - SendTime.
	clk.Advance(7 * time.Millisecond)
	require.NoError(t, r.Step(hbResp(2, withSendTime(t0))))
	clk.Advance(time.Millisecond)
	require.NoError(t, r.Step(msgBeat()))
	for _, m := range heartbeats(r.readMessages()) {
		switch m.GetTo() {
		case 2:
			require.Equal(t, int64(7*time.Millisecond), m.GetRtt())
		case 3:
			require.Zero(t, m.GetRtt())
		default:
			t.Fatalf("unexpected target %d", m.GetTo())
		}
	}
}

func TestDynatune_FollowersHaveIndependentCadence(t *testing.T) {
	r := newDynatuneLeader(newFakeClock())
	r.heartbeatStates[2].timeout = 3
	r.heartbeatStates[3].timeout = 5
	counts := countByTarget(r, 15)
	require.Equal(t, 5, counts[2], "ticks 3,6,9,12,15")
	require.Equal(t, 3, counts[3], "ticks 5,10,15")
}

func TestDynatune_EachTickDrivenSendAdvancesThatFollowersSequenceId(t *testing.T) {
	r := newDynatuneLeader(newFakeClock())
	r.heartbeatStates[2].timeout = 1
	r.heartbeatStates[3].timeout = 1000 // stays silent
	var seqs []int64
	for _, tick := range tickHeartbeats(r, 3) {
		require.Len(t, tick, 1)
		seqs = append(seqs, tick[0].GetSequenceId())
	}
	require.Equal(t, []int64{1, 2, 3}, seqs, "id is incremented before the send")
	require.Equal(t, uint64(3), r.heartbeatStates[2].sequenceId)
	require.Equal(t, uint64(0), r.heartbeatStates[3].sequenceId)
}

func TestDynatune_AtMostOneHeartbeatPerFollowerPerTick(t *testing.T) {
	// HeartbeatTick=1 would make the legacy MsgBeat fire every tick; the
	// per-follower schedule must replace it, not add to it.
	r := newDynatuneLeader(newFakeClock(), func(c *Config) { c.HeartbeatTick = 1 })
	require.Len(t, r.heartbeatStates, 2, "precondition: the per-follower schedule is active")
	ticks := tickHeartbeats(r, 3)
	for i, tick := range ticks {
		require.Len(t, tick, 2, "tick %d: one heartbeat to each of the two followers", i+1)
	}
}

func TestDynatune_ProposalResetsEveryFollowersElapsedCounter(t *testing.T) {
	r := newDynatuneLeader(newFakeClock())
	r.heartbeatStates[2].timeout = 10
	r.heartbeatStates[3].timeout = 10
	for i, tick := range tickHeartbeats(r, 5) {
		require.Empty(t, tick, "tick %d", i+1)
	}
	// The proposal's MsgApp is itself a sign of life, so the counters restart.
	require.NoError(t, r.Step(&pb.Message{From: new(uint64(1)), Type: pb.MsgProp.Enum(), Entries: []*pb.Entry{{Data: []byte("x")}}}))
	r.readMessages()
	ticks := tickHeartbeats(r, 10)
	for i := 0; i < 9; i++ {
		require.Empty(t, ticks[i], "tick %d after the proposal", i+1)
	}
	require.Len(t, ticks[9], 2, "10th tick after the proposal")
}

func TestDynatune_PeerAddedMidTermGetsAScheduleAndHeartbeats(t *testing.T) {
	r := newDynatuneLeader(newFakeClock())
	r.applyConfChange((&pb.ConfChange{Type: pb.ConfChangeAddNode.Enum(), NodeId: new(uint64(4))}).AsV2())

	// a broadcast right away must not crash on the not-yet-scheduled peer
	require.NotPanics(t, func() { require.NoError(t, r.Step(msgBeat())) })
	var to []uint64
	for _, m := range heartbeats(r.readMessages()) {
		to = append(to, m.GetTo())
	}
	require.ElementsMatch(t, []uint64{2, 3, 4}, to)

	counts := countByTarget(r, dynHeartbeat)
	require.Equal(t, 1, counts[4], "the new peer is on the schedule")
	require.Equal(t, 1, counts[2])
	require.Contains(t, r.heartbeatStates, uint64(4))
}

func TestDynatune_RemovedPeerIsDroppedFromTheSchedule(t *testing.T) {
	r := newDynatuneLeader(newFakeClock())
	r.applyConfChange((&pb.ConfChange{Type: pb.ConfChangeRemoveNode.Enum(), NodeId: new(uint64(3))}).AsV2())
	counts := countByTarget(r, dynHeartbeat)
	require.Equal(t, 1, counts[2])
	require.Zero(t, counts[3], "no heartbeats to a removed peer")
	require.Contains(t, r.heartbeatStates, uint64(2), "precondition: the schedule is active for the remaining peer")
	require.NotContains(t, r.heartbeatStates, uint64(3))
}

func TestDynatune_HeartbeatResponseGivesRTTFromTheClock(t *testing.T) {
	clk := newFakeClock()
	r := newDynatuneLeader(clk)
	clk.Advance(12 * time.Millisecond)
	require.NoError(t, r.Step(hbResp(2, withSendTime(t0))))
	rtt, ok := r.leaderMetrics.getRTT(2)
	require.True(t, ok)
	require.Equal(t, 12*time.Millisecond, rtt)
	_, ok = r.leaderMetrics.getRTT(3)
	require.False(t, ok)
}

func TestDynatune_RequestedIntervalSetsThatFollowersTimeout(t *testing.T) {
	clk := newFakeClock()
	r := newDynatuneLeader(clk)

	require.NoError(t, r.Step(hbResp(2, withSendTime(clk.Now()), withInterval(40))))
	require.Equal(t, 40, r.heartbeatStates[2].timeout)
	require.Equal(t, dynHeartbeat, r.heartbeatStates[3].timeout, "other followers untouched")

	require.NoError(t, r.Step(hbResp(2, withSendTime(clk.Now()), withInterval(-1))))
	require.Equal(t, 40, r.heartbeatStates[2].timeout, "-1 means none computed yet")

	require.NoError(t, r.Step(hbResp(2, withSendTime(clk.Now()))))
	require.Equal(t, 40, r.heartbeatStates[2].timeout, "absent interval leaves it unchanged")

	require.NoError(t, r.Step(hbResp(2, withSendTime(clk.Now()), withInterval(0))))
	require.Equal(t, 0, r.heartbeatStates[2].timeout)
	r.readMessages()
	require.NotPanics(t, func() {
		for i, tick := range tickHeartbeats(r, 3) {
			require.Len(t, filterTarget(tick, 2), 1, "interval 0 means every tick (tick %d)", i+1)
		}
	})
}

func filterTarget(msgs []*pb.Message, to uint64) []*pb.Message {
	var out []*pb.Message
	for _, m := range msgs {
		if m.GetTo() == to {
			out = append(out, m)
		}
	}
	return out
}

func TestDynatune_ResponseWithoutDynatuneFieldsIsHarmless(t *testing.T) {
	// e.g. a follower that does not run Dynatune (mixed-version cluster).
	r := newDynatuneLeader(newFakeClock())
	r.trk.Progress[2].RecentActive = false
	require.NotPanics(t, func() { require.NoError(t, r.Step(hbResp(2))) })
	_, ok := r.leaderMetrics.getRTT(2)
	require.False(t, ok, "no SendTime, no RTT")
	require.Equal(t, dynHeartbeat, r.heartbeatStates[2].timeout)
	require.True(t, r.trk.Progress[2].RecentActive)
}

func TestDynatune_ResponseFromUnknownPeerIsIgnored(t *testing.T) {
	clk := newFakeClock()
	r := newDynatuneLeader(clk)
	require.NotPanics(t, func() {
		require.NoError(t, r.Step(hbResp(9, withSendTime(clk.Now()), withInterval(5))))
	})
	require.Len(t, r.heartbeatStates, 2)
	_, ok := r.leaderMetrics.getRTT(9)
	require.False(t, ok)
}

func TestDynatune_LegacyResponseHandlingStillRuns(t *testing.T) {
	clk := newFakeClock()
	r := newDynatuneLeader(clk)
	require.Len(t, r.heartbeatStates, 2, "precondition: Dynatune is active")
	r.trk.Progress[2].RecentActive = false
	require.Less(t, r.trk.Progress[2].Match, r.raftLog.lastIndex(), "precondition: follower is behind")
	require.NoError(t, r.Step(hbResp(2, withSendTime(clk.Now()))))
	require.True(t, r.trk.Progress[2].RecentActive)
	apps := filterType(r.readMessages(), pb.MsgApp)
	require.NotEmpty(t, filterTarget(apps, 2), "a lagging follower still gets an append after its heartbeat response")
}

func TestDynatune_LeaderWithCheckQuorumSurvivesWorstCaseIntervals(t *testing.T) {
	// Followers always answer, and ask for the largest interval the formula can
	// produce inside the stock window (T/2 with T < 2*ElectionTick => 999).
	// CheckQuorum needs one response per election window, so the leader must
	// keep its job over many windows.
	clk := newFakeClock()
	r := newDynatuneLeader(clk, func(c *Config) { c.CheckQuorum = true })
	require.Len(t, r.heartbeatStates, 2, "precondition: Dynatune is active")
	defer func() {
		require.Equal(t, 999, r.heartbeatStates[2].timeout, "the requested interval was really in force")
	}()
	for i := 0; i < 5*dynElection; i++ {
		clk.Advance(time.Millisecond)
		r.tick()
		for _, hb := range heartbeats(r.readMessages()) {
			require.NoError(t, r.Step(hbResp(hb.GetTo(), withSendTime(time.Unix(0, hb.GetSendTime())), withInterval(999))))
		}
		r.readMessages()
		require.Equal(t, StateLeader, r.state, "stepped down at tick %d", i+1)
	}
}

// ============================================================================
// C. Follower side
// ============================================================================

func TestDynatune_FollowerRecordsRTTAndSequenceId(t *testing.T) {
	r := newDynatuneFollower(newFakeClock())
	sendTime := t0.Add(3 * time.Second)
	require.NoError(t, r.Step(hbMsg(7, ms(5), sendTime, 0)))
	r.readMessages()
	require.Equal(t, []time.Duration{ms(5)}, r.followerMetrics.rttQueue)
	require.Equal(t, []sequenceIdInfo{{id: 7, timestamp: sendTime}}, r.followerMetrics.sequenceIdQueue)
}

func TestDynatune_NoTuningUntilMoreThanMinSamples(t *testing.T) {
	r := newDynatuneFollower(newFakeClock()) // min = 3
	for i := int64(1); i <= 3; i++ {
		resp := feed(t, r, ms(54.2), i)[0]
		require.NotNil(t, resp.HeartbeatInterval)
		require.Equal(t, int64(-1), resp.GetHeartbeatInterval(), "heartbeat #%d: not enough samples", i)
		requireStockTimeoutRange(t, r)
	}
	resp := feed(t, r, ms(54.2), 4)[0]
	require.NotEqual(t, int64(-1), resp.GetHeartbeatInterval(), "4th sample (> min 3) starts tuning")
	require.Less(t, r.randomizedElectionTimeout, dynElection)
}

func TestDynatune_TunesTimeoutAndIntervalFromMeasuredRTT(t *testing.T) {
	r := newDynatuneFollower(newFakeClock())
	resps := feed(t, r, ms(54.2), 1, 2, 3, 4, 5)
	last := resps[len(resps)-1]
	// base = int(54.2) + 3*int(0) = 54  =>  timeout in [54, 107]
	require.GreaterOrEqual(t, r.randomizedElectionTimeout, 54)
	require.LessOrEqual(t, r.randomizedElectionTimeout, 107)
	// no gaps => loss 0 => interval = floor(timeout/2)
	require.Equal(t, int64(r.randomizedElectionTimeout/2), last.GetHeartbeatInterval())
}

func TestDynatune_SubMillisecondRTTFallsBackToTheStockDraw(t *testing.T) {
	r := newDynatuneFollower(newFakeClock())
	resps := feed(t, r, ms(0.7), 1, 2, 3, 4, 5)
	requireStockTimeoutRange(t, r) // base 0 => stock draw
	require.Equal(t, int64(r.randomizedElectionTimeout/2), resps[len(resps)-1].GetHeartbeatInterval())
}

func TestDynatune_ResponseEchoesRttSendTimeAndContext(t *testing.T) {
	r := newDynatuneFollower(newFakeClock())
	sendTime := t0.Add(9 * time.Second)
	require.NoError(t, r.Step(hbMsg(1, ms(5), sendTime, 0)))
	resp := onlyResp(t, r.readMessages())
	require.Equal(t, uint64(1), resp.GetTo())
	require.Equal(t, int64(ms(5)), resp.GetRtt())
	require.Equal(t, sendTime.UnixNano(), resp.GetSendTime(), "the leader needs SendTime back to compute the RTT")
	require.Equal(t, []byte("ctx"), resp.GetContext())
}

func TestDynatune_LossFromSequenceGapsFeedsTheInterval(t *testing.T) {
	r := newDynatuneFollower(newFakeClock(), func(c *Config) { c.MinElectionMetricsCapacity = 2 })
	resps := feed(t, r, ms(54.2), 1, 2, 4, 5, 7, 10)
	// ids 1..10 with 6 received => loss 0.4 => n = ceil(ln(0.01)/ln(0.4)) = 6
	require.Equal(t, int64(r.randomizedElectionTimeout/7), resps[len(resps)-1].GetHeartbeatInterval())
}

func TestDynatune_HeartbeatWithoutDynatuneFieldsChangesNothing(t *testing.T) {
	r := newDynatuneFollower(newFakeClock())
	require.NoError(t, r.Step(plainHb(0)))
	resp := onlyResp(t, r.readMessages())
	require.Nil(t, resp.SendTime, "nothing to echo")
	require.Empty(t, r.followerMetrics.rttQueue)
	require.Empty(t, r.followerMetrics.sequenceIdQueue)
	requireStockTimeoutRange(t, r)
}

func TestDynatune_MetricsResetOnRoleChange(t *testing.T) {
	changes := map[string]func(r *raft){
		"becomeFollower":     func(r *raft) { r.becomeFollower(2, None) },
		"becomeCandidate":    func(r *raft) { r.becomeCandidate() },
		"becomePreCandidate": func(r *raft) { r.becomePreCandidate() },
	}
	for name, change := range changes {
		t.Run(name, func(t *testing.T) {
			r := newDynatuneFollower(newFakeClock())
			feed(t, r, ms(5), 1, 2, 3)
			require.Len(t, r.followerMetrics.rttQueue, 3)
			change(r)
			require.Empty(t, r.followerMetrics.rttQueue)
			require.Empty(t, r.followerMetrics.sequenceIdQueue)
		})
	}
}

func TestDynatune_CommitStillAdvancesFromHeartbeat(t *testing.T) {
	r := newDynatuneFollower(newFakeClock())
	require.NotNil(t, r.followerMetrics, "precondition: Dynatune is active")
	require.NoError(t, r.Step(&pb.Message{
		Type: pb.MsgApp.Enum(), From: new(uint64(1)), To: new(uint64(2)), Term: new(uint64(1)),
		Index: new(uint64(0)), LogTerm: new(uint64(0)), Commit: new(uint64(0)),
		Entries: []*pb.Entry{{Index: new(uint64(1)), Term: new(uint64(1))}, {Index: new(uint64(2)), Term: new(uint64(1))}},
	}))
	r.readMessages()
	require.NoError(t, r.Step(hbMsg(1, ms(5), t0, 2)))
	r.readMessages()
	require.Equal(t, uint64(2), r.raftLog.committed)
}

// ============================================================================
// D. Elections
// ============================================================================

func TestDynatune_PreCandidateRedrawsTimeoutAndResetsMetrics(t *testing.T) {
	r := newDynatuneFollower(newFakeClock())
	feed(t, r, ms(54.2), 1, 2, 3, 4)
	setRandomizedElectionTimeout(r, 9999) // a stale draw
	r.becomePreCandidate()
	require.Empty(t, r.followerMetrics.rttQueue)
	require.NotEqual(t, 9999, r.randomizedElectionTimeout)
	requireStockTimeoutRange(t, r)
}

func preVoteReject(r *raft, from uint64) *pb.Message {
	return &pb.Message{Type: pb.MsgPreVoteResp.Enum(), From: new(from), To: new(r.id), Term: new(r.Term), Reject: new(true)}
}

func TestDynatune_LostPreVoteWithNoEarlierWinFallsBackToStockRange(t *testing.T) {
	r := newDynatuneFollower(newFakeClock(), func(c *Config) { c.PreVote = true })
	require.NotNil(t, r.followerMetrics, "precondition: Dynatune is active")
	require.NoError(t, r.Step(&pb.Message{From: new(uint64(2)), Type: pb.MsgHup.Enum()}))
	require.Equal(t, StatePreCandidate, r.state)
	r.readMessages()
	for _, from := range []uint64{1, 3} {
		require.NoError(t, r.Step(preVoteReject(r, from)))
	}
	require.Equal(t, StateFollower, r.state)
	requireStockTimeoutRange(t, r)
}

func TestDynatune_LostPreVoteUsesTheLastWonPreVoteDurationAsBase(t *testing.T) {
	// Reference behaviour: the duration kept is that of the last pre-vote round
	// that was WON (lastCampaignTimeTaken is set only on VoteWon), and it is
	// consumed after a later round is LOST. (DYNATUNE.md describes it as the
	// failed attempt's own duration; the code does not do that.)
	clk := newFakeClock()
	r := newDynatuneFollower(clk, func(c *Config) { c.PreVote = true })

	require.NoError(t, r.Step(&pb.Message{From: new(uint64(2)), Type: pb.MsgHup.Enum()}))
	r.readMessages()
	clk.Advance(60 * time.Millisecond)
	require.NoError(t, r.Step(&pb.Message{Type: pb.MsgPreVoteResp.Enum(), From: new(uint64(1)), To: new(uint64(2)), Term: new(r.Term + 1)}))
	require.Equal(t, StateCandidate, r.state, "pre-vote won after 60 ms")
	r.readMessages()

	r.becomeFollower(2, None) // the real election did not conclude
	clk.Advance(time.Second)
	require.NoError(t, r.Step(&pb.Message{From: new(uint64(2)), Type: pb.MsgHup.Enum()}))
	require.Equal(t, StatePreCandidate, r.state)
	r.readMessages()
	for _, from := range []uint64{1, 3} {
		require.NoError(t, r.Step(preVoteReject(r, from)))
	}
	require.Equal(t, StateFollower, r.state)
	require.GreaterOrEqual(t, r.randomizedElectionTimeout, 60)
	require.LessOrEqual(t, r.randomizedElectionTimeout, 119)
}

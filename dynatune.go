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
	"errors"
	"math"
	"time"

	pb "go.etcd.io/raft/v3/raftpb"
	"go.etcd.io/raft/v3/tracker"
)

// Defaults filled into Config when Dynatune is on and a tunable is left at its
// zero value. PROVISIONAL (decision Q6 is open): they follow the reference
// implementation's etcd-layer defaults (factor 3, goal 0.99, K 5, window
// 1000), except the minimum window, which is 10 as in the paper/README -- the
// reference code's own default is 100, which at ~1 heartbeat per second would
// delay tuning by ~100 s. The etcd layer passes explicit values anyway.
const (
	defaultMaxElectionMetricsCapacity = 1000
	defaultMinElectionMetricsCapacity = 10
	defaultElectionSafetyFactor       = 3
	defaultHeartbeatReachabilityGoal  = 0.99
	defaultK                          = 5
)

// validateDynatune checks and defaults the Dynatune Config surface. With
// Dynatune off it does nothing at all, so no existing configuration can be
// broken by fields it never set (stock behaviour stays byte-identical).
//
// Zero means "use the default" for every tunable. The reachability goal is
// only range-checked when OptimizeHeartbeatInterval is set (it is unused
// otherwise), and K only when it is not (likewise).
func (c *Config) validateDynatune() error {
	if !c.Dynatune {
		return nil
	}
	if c.MaxElectionMetricsCapacity < 0 || c.MinElectionMetricsCapacity < 0 {
		return errors.New("election metrics capacities must not be negative")
	}
	if c.ElectionSafetyFactor < 0 {
		return errors.New("election safety factor must not be negative")
	}

	if c.MaxElectionMetricsCapacity == 0 {
		c.MaxElectionMetricsCapacity = defaultMaxElectionMetricsCapacity
	}
	if c.MinElectionMetricsCapacity == 0 {
		c.MinElectionMetricsCapacity = defaultMinElectionMetricsCapacity
	}
	if c.MinElectionMetricsCapacity >= c.MaxElectionMetricsCapacity {
		return errors.New("min election metrics capacity must be less than the max")
	}
	if c.ElectionSafetyFactor == 0 {
		c.ElectionSafetyFactor = defaultElectionSafetyFactor
	}

	if c.HeartbeatReachabilityGoal == 0 {
		c.HeartbeatReachabilityGoal = defaultHeartbeatReachabilityGoal
	}
	if c.OptimizeHeartbeatInterval {
		g := c.HeartbeatReachabilityGoal
		if math.IsNaN(g) || g <= 0 || g >= 1 {
			return errors.New("heartbeat reachability goal must be in (0,1)")
		}
	}

	if c.K == 0 {
		c.K = defaultK
	}
	if !c.OptimizeHeartbeatInterval && c.K < 0 {
		return errors.New("K must not be negative")
	}
	return nil
}

// initDynatune sets up the clock (always: it is a function value, nothing is
// allocated) and, only when Dynatune is on, the tuning state. Called from
// newRaft after validation, so the Config values are already defaulted.
func (r *raft) initDynatune(c *Config) {
	r.clock = c.Clock
	if r.clock == nil {
		r.clock = time.Now
	}
	if !c.Dynatune {
		return
	}
	r.dynatune = true
	r.electionSafetyFactor = c.ElectionSafetyFactor
	r.heartbeatReachabilityGoal = c.HeartbeatReachabilityGoal
	r.k = c.K
	r.optimizeHeartbeatInterval = c.OptimizeHeartbeatInterval
	r.followerMetrics = newFollowerMetrics(c.MaxElectionMetricsCapacity, c.MinElectionMetricsCapacity)
	r.leaderMetrics = &leaderMetrics{}
	r.heartbeatStates = make(map[uint64]*heartbeatState)
}

// Dynatune (Shiozaki, "Dynatune: Dynamic Tuning of Raft Election Parameters
// Using Network Measurement", IPDPSW 2025): election-timeout and
// heartbeat-interval tuning from measured RTT and packet loss.
//
// This file holds the PURE parts -- data structures and formulas -- so they
// can be tested in isolation (dynatune_test.go). Wiring into raft.go is gated
// by a master switch (off by default => byte-identical to stock) and lives
// elsewhere.
//
// Behaviour follows the reference implementation (distsys-lab/raft @ cdc18d1)
// deliberately, quirks included (decision Q11, DYNATUNE_ARM_TASKS.md), so the
// port reproduces Dynatune rather than a "corrected" variant:
//
//  1. The RTT spread uses a running sum of squares, m2 += (x - mean_at_insert)^2,
//     that is never revisited when the mean moves. It is NOT textbook Welford
//     and underestimates the true sample standard deviation.
//  2. Sequence ids repeated by re-sent heartbeats are counted as received
//     packets, so the loss estimate can be negative (treated as zero loss).
//  3. The base election timeout truncates the mean and the deviation to whole
//     milliseconds separately before combining them.
//  4. With zero loss the heartbeat interval is half the election timeout.
//
// Deliberate differences, all robustness guards (no change for sane inputs):
// total loss (>= 1) yields interval 1 instead of an implementation-defined
// float-to-int conversion of an infinity; a negative RTT (clock stepping
// backwards) is clamped to 0; window capacities <= 0 mean "unbounded" instead
// of panicking; a non-positive variance (float residue after evicting samples
// from a near-constant window) reads as zero spread instead of NaN, which the
// reference turns into the minimum int64 and so silently disables tuning
// (found by the simulated cluster, TestDynatuneStdDevIsNeverNegative...).

// sequenceIdInfo records one received heartbeat sequence id and its send time.
type sequenceIdInfo struct {
	id        uint64
	timestamp time.Time
}

// followerMetrics is a follower's bounded sliding window of RTT samples and
// heartbeat sequence ids. Not safe for concurrent use: raft is single-threaded.
type followerMetrics struct {
	rttQueue        []time.Duration
	deviationSqs    []float64
	sum             time.Duration
	mean            time.Duration
	m2              float64
	count           int
	sequenceIdQueue []sequenceIdInfo
	maxQueueSize    int
	minQueueSize    int
}

func newFollowerMetrics(maxQueueSize, minQueueSize int) *followerMetrics {
	return &followerMetrics{
		rttQueue:        make([]time.Duration, 0),
		deviationSqs:    make([]float64, 0),
		sequenceIdQueue: make([]sequenceIdInfo, 0),
		maxQueueSize:    maxQueueSize,
		minQueueSize:    minQueueSize,
	}
}

// isReady reports whether both windows hold strictly more than the minimum
// number of samples, i.e. whether tuning may start.
func (fm *followerMetrics) isReady() bool {
	return len(fm.rttQueue) > fm.minQueueSize && len(fm.sequenceIdQueue) > fm.minQueueSize
}

// reset clears all samples but keeps the configured capacities.
func (fm *followerMetrics) reset() {
	fm.rttQueue = make([]time.Duration, 0)
	fm.deviationSqs = make([]float64, 0)
	fm.sum = 0
	fm.mean = 0
	fm.m2 = 0
	fm.count = 0
	fm.sequenceIdQueue = make([]sequenceIdInfo, 0)
}

// addRTT records one RTT sample, evicting the oldest first when the window is
// full (together with the deviation square stored for it).
func (fm *followerMetrics) addRTT(rtt time.Duration) {
	if fm.maxQueueSize > 0 && len(fm.rttQueue) >= fm.maxQueueSize {
		fm.sum -= fm.rttQueue[0]
		fm.rttQueue = fm.rttQueue[1:]
		fm.count--
		fm.m2 -= fm.deviationSqs[0]
		fm.deviationSqs = fm.deviationSqs[1:]
	}

	fm.rttQueue = append(fm.rttQueue, rtt)
	fm.sum += rtt
	fm.count++
	newMean := float64(fm.sum) / float64(fm.count)
	fm.mean = time.Duration(newMean)
	deviation := float64(rtt) - newMean
	deviationSq := deviation * deviation
	fm.m2 += deviationSq
	fm.deviationSqs = append(fm.deviationSqs, deviationSq)
}

func (fm *followerMetrics) getMean() time.Duration {
	return fm.mean
}

// getStdDev is sqrt(m2/(count-1)) -- see quirk 1 in the file comment.
func (fm *followerMetrics) getStdDev() time.Duration {
	if fm.count < 2 {
		return 0
	}
	variance := fm.m2 / float64(fm.count-1)
	if !(variance > 0) {
		// Evicting old samples leaves floating-point residue in m2 that can be
		// slightly negative when the window is (near) constant. sqrt of that is
		// NaN and time.Duration(NaN) is implementation-defined (the minimum
		// int64 on amd64), which turned the base timeout hugely negative and
		// silently disabled tuning in the reference. A non-positive variance
		// means no measurable spread.
		return 0
	}
	return time.Duration(math.Sqrt(variance))
}

// addSequenceIdInfo inserts a received sequence id keeping the queue ordered
// by send time (not by id). A sample whose timestamp equals an existing one is
// ignored. The oldest entry is dropped when the queue exceeds its capacity.
func (fm *followerMetrics) addSequenceIdInfo(id uint64, timestamp time.Time) {
	info := sequenceIdInfo{id: id, timestamp: timestamp}

	inserted := false
	for i := len(fm.sequenceIdQueue) - 1; i >= 0; i-- {
		if info.timestamp.After(fm.sequenceIdQueue[i].timestamp) {
			fm.sequenceIdQueue = append(fm.sequenceIdQueue[:i+1], append([]sequenceIdInfo{info}, fm.sequenceIdQueue[i+1:]...)...)
			inserted = true
			break
		} else if info.timestamp.Equal(fm.sequenceIdQueue[i].timestamp) {
			inserted = true
			break
		}
	}
	if !inserted {
		fm.sequenceIdQueue = append([]sequenceIdInfo{info}, fm.sequenceIdQueue...)
	}

	if fm.maxQueueSize > 0 && len(fm.sequenceIdQueue) > fm.maxQueueSize {
		fm.sequenceIdQueue = fm.sequenceIdQueue[1:]
	}
}

// calculatePacketCount is the number of sequence ids in [firstID, lastID],
// inclusive, allowing for uint64 wrap-around.
func calculatePacketCount(firstID, lastID uint64) uint64 {
	if lastID >= firstID {
		return lastID - firstID + 1
	}
	return (math.MaxUint64 - firstID) + lastID + 2
}

// calculatePacketLossRate is 1 - received/expected over the sequence window;
// 0 with fewer than two samples. May be negative -- see quirk 2.
func (fm *followerMetrics) calculatePacketLossRate() float64 {
	if len(fm.sequenceIdQueue) < 2 {
		return 0.0
	}
	first := fm.sequenceIdQueue[0]
	last := fm.sequenceIdQueue[len(fm.sequenceIdQueue)-1]
	expected := calculatePacketCount(first.id, last.id)
	received := uint64(len(fm.sequenceIdQueue))
	return 1.0 - (float64(received) / float64(expected))
}

// electionBaseTimeoutMs is int(mean_ms + factor*stddev_ms), each term
// truncated to whole milliseconds first -- see quirk 3. Zero (sub-millisecond
// links) tells the caller to fall back to the configured election timeout.
func electionBaseTimeoutMs(mean, stddev time.Duration, factor int) int {
	return int(mean.Milliseconds() + int64(factor)*stddev.Milliseconds())
}

// drawElectionTimeout draws a randomized election timeout in [base, 2*base)
// using intn(n) for randomness. ok is false when base <= 0: the caller must
// then fall back to the stock draw.
func drawElectionTimeout(base int, intn func(int) int) (timeout int, ok bool) {
	if base <= 0 {
		return 0, false
	}
	return base + intn(base), true
}

// heartbeatIntervalMs is the heartbeat interval a follower asks its leader to
// use, derived from its randomized election timeout and the measured loss.
//
// Without optimisation it is the fixed ratio timeout/(K+1). With it, the
// follower asks for enough heartbeats per election window that at least one
// arrives with probability reachabilityGoal:
//
//	n = ceil(ln(1-goal) / ln(loss));  interval = floor(timeout / (n+1))
//
// and n is 1 when no loss was measured (quirk 4). Total loss (>= 1) returns 1,
// the fastest valid interval (a guard, see the file comment).
func heartbeatIntervalMs(randomizedTimeout int, packetLossRate, reachabilityGoal float64, k int64, optimize bool) int64 {
	if !optimize {
		denom := float64(k) + 1
		if denom < 1 {
			denom = 1
		}
		return int64(math.Floor(float64(randomizedTimeout) / denom))
	}

	n := 1.0
	switch {
	case packetLossRate <= 0:
		// no loss measured: n stays 1
	case packetLossRate >= 1:
		return 1
	default:
		n = math.Ceil(math.Log(1-reachabilityGoal) / math.Log(packetLossRate))
		if !(n >= 1) { // also catches NaN from an out-of-range goal
			n = 1
		}
	}
	return int64(math.Floor(float64(randomizedTimeout) / (n + 1)))
}

// rttSince is now minus the heartbeat's send time (unix ns), never negative.
func rttSince(now time.Time, sendTimeNanos int64) time.Duration {
	d := now.Sub(time.Unix(0, sendTimeNanos))
	if d < 0 {
		return 0
	}
	return d
}

// leaderMetrics holds the leader's latest RTT measurement per follower. The
// zero value is usable.
type leaderMetrics struct {
	rtts map[uint64]time.Duration
}

func (ls *leaderMetrics) updateRTT(followerID uint64, rtt time.Duration) {
	if ls.rtts == nil {
		ls.rtts = make(map[uint64]time.Duration)
	}
	ls.rtts[followerID] = rtt
}

func (ls *leaderMetrics) getRTT(followerID uint64) (time.Duration, bool) {
	rtt, ok := ls.rtts[followerID]
	return rtt, ok
}

// heartbeatState is the leader's per-follower heartbeat schedule: ticks since
// the last heartbeat, ticks to wait before the next (the follower's requested
// interval), and the next sequence id.
type heartbeatState struct {
	elapsed    int
	timeout    int
	sequenceId uint64
}

// initHeartbeatStates returns a fresh schedule for the given followers. The
// result is never nil, so peers added later can be inserted safely.
func initHeartbeatStates(followerIDs []uint64, defaultTimeout int) map[uint64]*heartbeatState {
	states := make(map[uint64]*heartbeatState, len(followerIDs))
	for _, id := range followerIDs {
		states[id] = &heartbeatState{timeout: defaultTimeout}
	}
	return states
}

// ---------------------------------------------------------------------------
// Leader side (task C5b). Every method below is called only when r.dynatune.
// ---------------------------------------------------------------------------

// followerIDs lists the tracker's members other than this node, in id order
// (deterministic, so messages are always emitted in the same order).
func (r *raft) followerIDs() []uint64 {
	var ids []uint64
	r.trk.Visit(func(id uint64, _ *tracker.Progress) {
		if id != r.id {
			ids = append(ids, id)
		}
	})
	return ids
}

// heartbeatStateFor returns the schedule entry for a follower, creating it on
// first use. Creating lazily (instead of hooking every membership change)
// means a peer added after the election can never hit a missing entry -- the
// reference implementation dereferences it unchecked and would crash.
func (r *raft) heartbeatStateFor(id uint64) *heartbeatState {
	st := r.heartbeatStates[id]
	if st == nil {
		if r.heartbeatStates == nil {
			r.heartbeatStates = make(map[uint64]*heartbeatState)
		}
		st = &heartbeatState{timeout: r.heartbeatTimeout}
		r.heartbeatStates[id] = st
	}
	return st
}

// resetHeartbeatSchedule starts a fresh schedule for every follower. Called
// when this node becomes leader.
func (r *raft) resetHeartbeatSchedule() {
	r.heartbeatStates = initHeartbeatStates(r.followerIDs(), r.heartbeatTimeout)
}

// syncHeartbeatSchedule makes the schedule match the current membership:
// entries for new peers are added and entries for removed peers dropped.
func (r *raft) syncHeartbeatSchedule() {
	for _, id := range r.followerIDs() {
		r.heartbeatStateFor(id)
	}
	for id := range r.heartbeatStates {
		if _, ok := r.trk.Progress[id]; !ok || id == r.id {
			delete(r.heartbeatStates, id)
		}
	}
}

// stampHeartbeat adds the Dynatune fields to an outgoing heartbeat: the send
// time (so the follower can echo it back and the leader can measure the
// round trip), the follower's current sequence id (so it can detect lost
// heartbeats), and the last RTT measured to that follower (0 if none yet).
// Heartbeats that are not tick-driven (ReadIndex, MsgBeat) re-use the current
// sequence id, as in the reference implementation.
func (r *raft) stampHeartbeat(m *pb.Message, to uint64) {
	st := r.heartbeatStateFor(to)
	rtt, _ := r.leaderMetrics.getRTT(to)
	m.Rtt = new(int64(rtt))
	m.SendTime = new(r.clock().UnixNano())
	m.SequenceId = new(int64(st.sequenceId))
}

// tickHeartbeatSchedule replaces the single shared heartbeat timer: each
// follower is heartbeated on its own cadence (the interval it requested).
func (r *raft) tickHeartbeatSchedule() {
	r.syncHeartbeatSchedule()
	for _, id := range r.followerIDs() {
		st := r.heartbeatStates[id]
		st.elapsed++
		if st.elapsed >= st.timeout {
			st.elapsed = 0
			st.sequenceId++
			// readOnly.heartbeatCtx keeps pending ReadOnlySafe requests
			// moving, as the stock broadcast does.
			r.sendHeartbeat(id, r.readOnly.heartbeatCtx())
		}
	}
	// HeirRaft's selection gate in tickHeartbeat keys on heartbeatElapsed.
	// With the global MsgBeat timer gone nothing else would ever reset it, so
	// keep it cycling at HeartbeatTick like the stock path does.
	if r.heartbeatElapsed >= r.heartbeatTimeout {
		r.heartbeatElapsed = 0
	}
}

// ---------------------------------------------------------------------------
// Follower side (task C5c). Called only when r.dynatune.
// ---------------------------------------------------------------------------

// tuneFromHeartbeat is the follower's half of Dynatune, run for every
// heartbeat after the stock handling (commit, heir). It
//
//   - records the RTT the leader reports and the heartbeat's (sequence id,
//     send time) in the sliding windows;
//   - once both windows hold more than the minimum number of samples, redraws
//     this node's election timeout from the measured RTT (mean + factor*stddev,
//     in whole ms; the stock draw when that is 0) and works out the heartbeat
//     interval to ask the leader for (-1 until then);
//   - fills resp: it echoes the leader's RTT and send time (the leader needs
//     SendTime back to measure the round trip) and carries the interval.
//
// A heartbeat from a leader that does not run Dynatune carries none of the
// fields: nothing is recorded and nothing is echoed.
func (r *raft) tuneFromHeartbeat(m, resp *pb.Message) {
	fm := r.followerMetrics
	if m.Rtt != nil {
		fm.addRTT(time.Duration(m.GetRtt()))
		resp.Rtt = new(m.GetRtt())
	}
	if m.SendTime != nil {
		resp.SendTime = new(m.GetSendTime())
	}
	if m.SequenceId != nil && m.SendTime != nil {
		fm.addSequenceIdInfo(uint64(m.GetSequenceId()), time.Unix(0, m.GetSendTime()))
	}

	interval := int64(-1)
	if fm.isReady() {
		base := electionBaseTimeoutMs(fm.getMean(), fm.getStdDev(), r.electionSafetyFactor)
		if t, ok := drawElectionTimeout(base, globalRand.Intn); ok {
			r.randomizedElectionTimeout = t
		} else {
			// Sub-millisecond link: nothing to tune from, so use the stock draw.
			r.resetRandomizedElectionTimeout()
		}
		interval = heartbeatIntervalMs(r.randomizedElectionTimeout, fm.calculatePacketLossRate(),
			r.heartbeatReachabilityGoal, r.k, r.optimizeHeartbeatInterval)
	}
	resp.HeartbeatInterval = new(interval)
}

// ---------------------------------------------------------------------------
// Elections (task C5d). Called only when r.dynatune.
//
// Reference behaviour, reproduced faithfully (it differs from how DYNATUNE.md
// describes it): a PreVote round is timestamped when it starts; when a round
// is WON, how long it took is remembered (lastCampaignTimeTaken); when a later
// round is LOST, the follower it falls back to draws its next election
// timeout from that remembered duration (in whole ms, [d, 2d)), or from the
// stock draw when it is 0 -- i.e. a node whose pre-vote just failed retries
// after about one quorum round trip instead of a full election timeout. The
// duration therefore belongs to the last round that was won, not to the one
// that failed. Only PreVote rounds are timestamped, so with PreVote disabled
// the duration stays 0 and the stock draw is always used.
// ---------------------------------------------------------------------------

// recordPreVoteAttempt timestamps the start of a PreVote round.
func (r *raft) recordPreVoteAttempt() {
	r.campaignAttemptTimestamp = r.clock()
}

// recordPreVoteWon remembers how long the PreVote round that was just won took.
func (r *raft) recordPreVoteWon() {
	d := r.clock().Sub(r.campaignAttemptTimestamp)
	if d < 0 {
		d = 0
	}
	r.lastCampaignTimeTaken = d
}

// recordPreVoteLost flags that the follower state we are about to enter
// follows a lost PreVote round (consumed by applyPostLossTimeout).
func (r *raft) recordPreVoteLost() {
	r.lastElectionFailed = true
}

// applyPostLossTimeout, called from becomeFollower after reset() has drawn the
// stock timeout, replaces that draw when the previous round was lost and a
// pre-vote duration is known; otherwise the stock draw stays.
func (r *raft) applyPostLossTimeout() {
	if !r.lastElectionFailed {
		return
	}
	r.lastElectionFailed = false
	base := int(r.lastCampaignTimeTaken / time.Millisecond)
	if t, ok := drawElectionTimeout(base, globalRand.Intn); ok {
		r.randomizedElectionTimeout = t
	}
}

// resetHeartbeatElapsed restarts every follower's heartbeat timer. A client
// proposal's MsgApp already proves the leader is alive, so a heartbeat right
// after it would be wasted traffic.
func (r *raft) resetHeartbeatElapsed() {
	for _, st := range r.heartbeatStates {
		st.elapsed = 0
	}
}

// handleHeartbeatRespDynatune consumes the Dynatune fields of a follower's
// heartbeat response: the measured round trip and the interval the follower
// asks for. The sender is already known to be a member (stepLeader returns
// early for unknown senders). Responses without the fields -- a follower that
// does not run Dynatune -- are simply ignored here; the reference
// implementation dereferences SendTime unchecked and would crash on them.
func (r *raft) handleHeartbeatRespDynatune(m *pb.Message) {
	from := m.GetFrom()
	if m.SendTime != nil {
		r.leaderMetrics.updateRTT(from, rttSince(r.clock(), m.GetSendTime()))
	}
	if m.HeartbeatInterval != nil {
		// -1 (or any negative value) means "none computed yet". The cap keeps
		// a misbehaving follower from starving itself of heartbeats for longer
		// than an election window, which the formula itself never exceeds.
		if iv := m.GetHeartbeatInterval(); iv >= 0 {
			if iv > int64(r.electionTimeout) {
				iv = int64(r.electionTimeout)
			}
			r.heartbeatStateFor(from).timeout = int(iv)
		}
	}
}

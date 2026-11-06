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

	"github.com/stretchr/testify/require"
)

// Scenario tests on the simulated 3-node cluster (dynatune_sim_test.go).
//
// They pin what the design GUARANTEES and deliberately do not claim more. In
// particular they do NOT assert that failover completes quickly: the
// simulation showed that with tuned (round-trip-scale) timeouts both followers
// usually time out within a few milliseconds of each other, both win their
// PreVote and then split the real vote; the retry then uses a stock timeout
// (1-2 s), because becomeCandidate redraws via reset() -- as in the reference.
// So detection is fast but completion is bimodal (~0.2 s or ~1.5 s). Whether
// the reference behaves the same is to be checked in the B4/F-phase runs.

// failoverTimes cuts the leader off and returns the simulated ms until the
// first follower stops being a follower (detection) and until a new leader
// exists (completion).
func (c *simCluster) failoverTimes(lead uint64) (detection, completion int64) {
	c.t.Helper()
	c.isolated[lead] = true
	start := c.now
	detection = -1
	for i := 0; i < 20000; i++ {
		c.step()
		if detection < 0 {
			for _, id := range c.followers(lead) {
				if c.nodes[id].state != StateFollower {
					detection = c.now - start
				}
			}
		}
		if l := c.leader(); l != 0 && l != lead {
			return detection, c.now - start
		}
	}
	c.t.Fatalf("no new leader within 20 simulated seconds after isolating %d", lead)
	return 0, 0
}

// warmedUp returns a simulation whose leader has been in office for warmupMs.
func warmedUp(t *testing.T, o simOpts, warmupMs int64) (*simCluster, uint64) {
	t.Helper()
	c := newSim(t, o)
	lead := c.electLeader()
	c.run(warmupMs)
	require.Equal(t, lead, c.leader(), "leadership must be stable during warm-up")
	return c, lead
}

func TestDynatuneSim_TunedTimeoutsTrackTheRTTAndLeadershipStaysStable(t *testing.T) {
	// 25 ms each way => RTT 50 ms. 20 s of warm-up fills and wraps the
	// 100-sample window, which is what exposed the negative-sigma defect.
	c, lead := warmedUp(t, simOpts{dynatune: true, oneWayMs: 25, seed: 1}, 20_000)
	term := c.nodes[lead].Term
	for i := 0; i < 10_000; i++ {
		c.step()
		for _, id := range c.followers(lead) {
			n := c.nodes[id]
			// base = int(mean 50 ms) + 3*int(sigma 0) = 50  =>  timeout in [50, 99]
			require.GreaterOrEqual(t, n.randomizedElectionTimeout, 50, "t=%d node %d", c.now, id)
			require.LessOrEqual(t, n.randomizedElectionTimeout, 99, "t=%d node %d", c.now, id)
			// the leader asks for at most half of that, never zero loss assumed wrongly
			require.LessOrEqual(t, c.nodes[lead].heartbeatStates[id].timeout, 49, "t=%d node %d", c.now, id)
			require.Greater(t, c.nodes[lead].heartbeatStates[id].timeout, 0)
		}
	}
	require.Equal(t, lead, c.leader(), "no spurious election while tuned")
	require.Equal(t, term, c.nodes[lead].Term)
}

func TestDynatuneSim_FailureDetectionIsFastAndCompletionIsBounded(t *testing.T) {
	for run := 0; run < 8; run++ {
		c, lead := warmedUp(t, simOpts{dynatune: true, oneWayMs: 25, seed: int64(run + 1)}, 20_000)
		detection, completion := c.failoverTimes(lead)
		// A follower's election timeout is at most 99 ms and its timer was
		// already running when the leader was cut off.
		require.GreaterOrEqual(t, detection, int64(0))
		require.LessOrEqual(t, detection, int64(150), "run %d: detection", run)
		require.Greater(t, completion, detection)
		// Liveness only: each split vote costs one stock timeout (1-2 s) and
		// several in a row are possible, so no tight bound is claimed.
		require.LessOrEqual(t, completion, int64(15000), "run %d: a leader must emerge", run)
	}
}

func TestDynatuneSim_StockControlDetectsSlowly(t *testing.T) {
	// Same topology and workload with Dynatune off: the control for the test
	// above. Election timeout 1000-1999 ms, heartbeats every 100 ms.
	for run := 0; run < 8; run++ {
		c, lead := warmedUp(t, simOpts{dynatune: false, oneWayMs: 25, seed: int64(run + 1)}, 20_000)
		detection, completion := c.failoverTimes(lead)
		require.GreaterOrEqual(t, detection, int64(800), "run %d: stock detection", run)
		require.Greater(t, completion, detection)
	}
}

func TestDynatuneSim_NoGainOnALANWhereTheRTTRoundsToZero(t *testing.T) {
	// Zero delay => measured RTT 0 => base timeout 0 => the stock draw is used
	// (reference behaviour, integer milliseconds). Dynatune must then behave
	// like stock: stock-range timeouts and slow detection, but stable.
	c, lead := warmedUp(t, simOpts{dynatune: true, oneWayMs: 0, seed: 1}, 20_000)
	for _, id := range c.followers(lead) {
		n := c.nodes[id]
		require.GreaterOrEqual(t, n.randomizedElectionTimeout, dynElection)
		require.Less(t, n.randomizedElectionTimeout, 2*dynElection)
	}
	detection, _ := c.failoverTimes(lead)
	// Still on the stock scale (hundreds of ms and up), far from the tuned
	// scale (~100 ms). Not >= 1000 as for stock: with zero loss the leader
	// heartbeats only every T/2, so the follower's timer is already up to T/2
	// into its window when the leader dies, i.e. detection >= T_min/2 = 500 ms
	// (a property of the algorithm: sparse heartbeats shorten detection on
	// average even without tuning, at the price of fewer liveness probes).
	require.GreaterOrEqual(t, detection, int64(400), "no tuning on a link too fast to measure")
}

// settle runs until the current leader's two followers each hold at least
// minSamples sequence samples (every election resets a follower's windows, so
// a fixed run time cannot guarantee a populated window), then returns the
// leader. Fails if that does not happen within capMs.
func (c *simCluster) settle(minSamples int, capMs int) uint64 {
	c.t.Helper()
	for i := 0; i < capMs; i += 100 {
		c.run(100)
		lead := c.leader()
		if lead == 0 {
			continue
		}
		ok := true
		for _, id := range c.followers(lead) {
			if len(c.nodes[id].followerMetrics.sequenceIdQueue) < minSamples {
				ok = false
			}
		}
		if ok {
			return lead
		}
	}
	c.t.Fatalf("windows never reached %d samples within %d simulated ms", minSamples, capMs)
	return 0
}

// meanRequestedInterval samples, every ms for ms ms, the heartbeat interval
// the leader is currently using for each follower, and returns the mean.
// ok is false if leadership changed while sampling (the sample is then void).
func (c *simCluster) meanRequestedInterval(lead uint64, ms int) (mean float64, ok bool) {
	var sum, n float64
	for i := 0; i < ms; i++ {
		c.step()
		if c.leader() != lead {
			return 0, false
		}
		for _, id := range c.followers(lead) {
			sum += float64(c.nodes[lead].heartbeatStates[id].timeout)
			n++
		}
	}
	return sum / n, true
}

func TestDynatuneSim_MeasuredLossShortensTheRequestedInterval(t *testing.T) {
	// 30% of the leader's heartbeats are lost (UDP-like); acknowledgements and
	// log replication are unaffected. With goal 0.99 and loss ~0.3 the follower
	// asks for timeout/5 instead of the zero-loss timeout/2, so the leader's
	// mean interval must be far shorter than in a lossless control run.
	// Spurious elections are expected here (a 1% per-window miss goal over
	// thousands of windows), so wait for populated windows, then compare means
	// over a stable stretch instead of asserting a single instant.
	lossy := newSim(t, simOpts{dynatune: true, oneWayMs: 25, hbLossProb: 0.3, seed: 1})
	lossy.electLeader()
	var leadL uint64
	var meanLossy float64
	for attempt := 0; ; attempt++ {
		require.Less(t, attempt, 50, "no stable 3 s stretch in 50 attempts")
		leadL = lossy.settle(80, 120_000)
		// all followers' measured loss is near 0.3 once the window is populated
		for _, id := range lossy.followers(leadL) {
			loss := lossy.nodes[id].followerMetrics.calculatePacketLossRate()
			require.Greater(t, loss, 0.1, "node %d measured loss", id)
			require.Less(t, loss, 0.55, "node %d measured loss", id)
		}
		var stable bool
		if meanLossy, stable = lossy.meanRequestedInterval(leadL, 3000); stable {
			break
		}
	}

	control, leadC := warmedUp(t, simOpts{dynatune: true, oneWayMs: 25, seed: 1}, 20_000)
	meanControl, ok := control.meanRequestedInterval(leadC, 3000)
	require.True(t, ok, "the lossless control must keep its leader")

	require.Greater(t, meanControl, 25.0, "control: ~timeout/2")
	require.Less(t, meanLossy, 0.7*meanControl, "lossy mean %.1f ms vs control %.1f ms", meanLossy, meanControl)
}

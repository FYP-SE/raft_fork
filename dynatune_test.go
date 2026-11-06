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
	"math"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// Tests for the pure parts of Dynatune (task C2). Expected values are derived
// by hand from the REFERENCE implementation (distsys-lab/raft @ cdc18d1), so
// the port reproduces the authors' behaviour. Where that behaviour is a quirk
// rather than textbook maths, the test name says so (…Quirk) and the
// decision to keep it is recorded in DYNATUNE_ARM_TASKS.md (Q11); flip the
// expectation here if the decision changes.

func ms(f float64) time.Duration { return time.Duration(f * float64(time.Millisecond)) }

var t0 = time.Unix(1_700_000_000, 0)

// ---------------------------------------------------------------- RTT window

func TestDynatuneStdDevMatchesReferenceRunningM2(t *testing.T) {
	// Reference keeps m2 += (x - mean_at_insert)^2 (NOT textbook Welford):
	// samples 10,20,30 ms -> m2 = 0 + 25 + 100 = 125 ms^2, sigma = sqrt(125/2)
	// = 7.905694 ms. (The textbook sample sigma would be exactly 10 ms.)
	fm := newFollowerMetrics(10, 3)
	for _, v := range []float64{10, 20, 30} {
		fm.addRTT(ms(v))
	}
	require.Equal(t, ms(20), fm.getMean())
	require.InDelta(t, 7905694, float64(fm.getStdDev()), 1000) // ns, 1 µs tolerance
}

func TestDynatuneStdDevQuirkDiffersFromTextbookSigma(t *testing.T) {
	fm := newFollowerMetrics(10, 3)
	for _, v := range []float64{10, 20, 30} {
		fm.addRTT(ms(v))
	}
	textbook := ms(10)
	require.Greater(t, fm.getStdDev(), ms(7), "sigma must be computed, not zero")
	require.Less(t, fm.getStdDev(), textbook, "reference sigma underestimates the textbook sigma")
}

func TestDynatuneStdDevIsNeverNegativeWhenRTTIsConstantAfterEviction(t *testing.T) {
	// Found by the simulated cluster (C5e). The first heartbeat of a term
	// carries RTT 0 (nothing measured yet) and the rest are a constant 50 ms.
	// Once the window is full, evicting old samples subtracts their stored
	// squares from m2 and leaves floating-point residue that can be slightly
	// NEGATIVE; sqrt(negative) is NaN and time.Duration(NaN) is
	// implementation-defined (the minimum int64 on amd64), which made the base
	// timeout hugely negative and silently disabled tuning. The reference has
	// this flaw; a negative spread must read as 0.
	fm := newFollowerMetrics(100, 10)
	fm.addRTT(0)
	for i := 0; i < 400; i++ {
		fm.addRTT(ms(50))
		require.GreaterOrEqual(t, int64(fm.getStdDev()), int64(0), "after sample %d", i+2)
	}
	require.Equal(t, time.Duration(0), fm.getStdDev(), "the window is constant by now")
	require.Equal(t, 50, electionBaseTimeoutMs(fm.getMean(), fm.getStdDev(), 3), "50 ms RTT must tune to a 50 ms base")
}

func TestDynatuneEmptyAndSingleSampleHaveZeroSpread(t *testing.T) {
	fm := newFollowerMetrics(10, 3)
	require.Equal(t, time.Duration(0), fm.getMean())
	require.Equal(t, time.Duration(0), fm.getStdDev())
	fm.addRTT(ms(5))
	require.Equal(t, ms(5), fm.getMean())
	require.Equal(t, time.Duration(0), fm.getStdDev())
}

func TestDynatuneConstantRTTHasZeroStdDev(t *testing.T) {
	fm := newFollowerMetrics(10, 3)
	for i := 0; i < 6; i++ {
		fm.addRTT(ms(5))
	}
	require.Equal(t, ms(5), fm.getMean())
	require.Equal(t, time.Duration(0), fm.getStdDev())
}

func TestDynatuneRTTWindowEvictsOldestWhenFull(t *testing.T) {
	// max=3, add 10,20,30,40: 10 is evicted together with its stored
	// deviation square (0): m2 = 125 - 0 + 100 = 225 ms^2 over count 3,
	// sigma = sqrt(225/2) = 10.606602 ms, mean = 30 ms.
	fm := newFollowerMetrics(3, 1)
	for _, v := range []float64{10, 20, 30, 40} {
		fm.addRTT(ms(v))
	}
	require.Equal(t, []time.Duration{ms(20), ms(30), ms(40)}, fm.rttQueue)
	require.Equal(t, ms(30), fm.getMean())
	require.InDelta(t, 10606601, float64(fm.getStdDev()), 1000)
}

// ------------------------------------------------------- sequence-id window

func seqAt(fm *followerMetrics, id uint64, secs int) {
	fm.addSequenceIdInfo(id, t0.Add(time.Duration(secs)*time.Second))
}

func seqIDs(fm *followerMetrics) []uint64 {
	ids := make([]uint64, 0, len(fm.sequenceIdQueue))
	for _, s := range fm.sequenceIdQueue {
		ids = append(ids, s.id)
	}
	return ids
}

func TestDynatuneSequenceQueueIsOrderedByTimestampNotId(t *testing.T) {
	fm := newFollowerMetrics(10, 1)
	seqAt(fm, 1, 3)
	seqAt(fm, 2, 1)
	seqAt(fm, 3, 2)
	require.Equal(t, []uint64{2, 3, 1}, seqIDs(fm))
}

func TestDynatuneSequenceQueueIgnoresEqualTimestamp(t *testing.T) {
	fm := newFollowerMetrics(10, 1)
	seqAt(fm, 1, 1)
	seqAt(fm, 9, 1)
	require.Equal(t, []uint64{1}, seqIDs(fm))
}

func TestDynatuneSequenceQueueEvictsOldestWhenOverMax(t *testing.T) {
	fm := newFollowerMetrics(3, 1)
	for i := 1; i <= 4; i++ {
		seqAt(fm, uint64(i), i)
	}
	require.Equal(t, []uint64{2, 3, 4}, seqIDs(fm))
}

func TestDynatuneReadyNeedsBothQueuesStrictlyAboveMin(t *testing.T) {
	fm := newFollowerMetrics(10, 3)
	for i := 1; i <= 3; i++ {
		fm.addRTT(ms(1))
		seqAt(fm, uint64(i), i)
	}
	require.False(t, fm.isReady(), "exactly min samples is not enough")
	fm.addRTT(ms(1))
	require.False(t, fm.isReady(), "RTT queue alone is not enough")
	seqAt(fm, 4, 4)
	require.True(t, fm.isReady())
}

func TestDynatuneResetClearsSamplesKeepsCapacities(t *testing.T) {
	fm := newFollowerMetrics(10, 3)
	fm.addRTT(ms(7))
	seqAt(fm, 1, 1)
	require.NotEmpty(t, fm.rttQueue, "precondition: samples were recorded")
	require.NotEmpty(t, fm.sequenceIdQueue, "precondition: sequence ids were recorded")
	fm.reset()
	require.Empty(t, fm.rttQueue)
	require.Empty(t, fm.sequenceIdQueue)
	require.Equal(t, time.Duration(0), fm.getMean())
	require.Equal(t, 0, fm.count)
	require.Equal(t, 10, fm.maxQueueSize)
	require.Equal(t, 3, fm.minQueueSize)
}

// ------------------------------------------------------------ packet loss

func TestDynatunePacketLossNoGapsIsZero(t *testing.T) {
	fm := newFollowerMetrics(20, 1)
	for i := 1; i <= 10; i++ {
		seqAt(fm, uint64(i), i)
	}
	require.Len(t, fm.sequenceIdQueue, 10, "precondition: all ids recorded")
	require.InDelta(t, 0.0, fm.calculatePacketLossRate(), 1e-12)
}

func TestDynatunePacketLossFromGaps(t *testing.T) {
	// ids 1,2,4,5,7,10 -> expected 10 packets, received 6 -> loss 0.4
	fm := newFollowerMetrics(20, 1)
	for i, id := range []uint64{1, 2, 4, 5, 7, 10} {
		seqAt(fm, id, i+1)
	}
	require.InDelta(t, 0.4, fm.calculatePacketLossRate(), 1e-9)
}

func TestDynatunePacketLossNeedsTwoSamples(t *testing.T) {
	fm := newFollowerMetrics(20, 1)
	require.Equal(t, 0.0, fm.calculatePacketLossRate())
	seqAt(fm, 5, 1)
	require.Len(t, fm.sequenceIdQueue, 1, "precondition: the single id was recorded")
	require.Equal(t, 0.0, fm.calculatePacketLossRate(), "one sample: no estimate")
	seqAt(fm, 9, 2) // ids 5 and 9: expected 5 packets, received 2 -> loss 0.6
	require.InDelta(t, 0.6, fm.calculatePacketLossRate(), 1e-9, "two samples: estimate starts")
}

func TestDynatunePacketCount(t *testing.T) {
	require.Equal(t, uint64(5), calculatePacketCount(5, 9))
	require.Equal(t, uint64(1), calculatePacketCount(7, 7))
	// wrap-around: Max-1, Max, 0, 1
	require.Equal(t, uint64(4), calculatePacketCount(math.MaxUint64-1, 1))
	require.Equal(t, uint64(2), calculatePacketCount(math.MaxUint64, 0))
}

func TestDynatunePacketLossDuplicateIdsGiveNegativeRateQuirk(t *testing.T) {
	// The leader reuses a sequence id when a heartbeat is re-sent without a
	// tick (e.g. ReadIndex), so duplicates with distinct timestamps inflate
	// "received": ids 1,2,2,3 -> expected 3, received 4 -> 1 - 4/3 = -1/3.
	// The reference does not clamp this (seen live as -0.0625 in B3).
	fm := newFollowerMetrics(20, 1)
	for i, id := range []uint64{1, 2, 2, 3} {
		seqAt(fm, id, i+1)
	}
	require.InDelta(t, -1.0/3.0, fm.calculatePacketLossRate(), 1e-9)
}

// ------------------------------------------------------ election timeout

func TestDynatuneElectionBaseTimeoutTruncatesEachTermToWholeMs(t *testing.T) {
	cases := []struct {
		name         string
		mean, stddev time.Duration
		factor       int
		want         int
	}{
		{"sigma term truncated to 0", ms(54.2), ms(0.4), 3, 54},
		{"mean and sigma truncated separately", ms(20.9), ms(2.9), 3, 20 + 3*2},
		{"sub-millisecond LAN gives 0 (caller falls back)", ms(0.7), 0, 3, 0},
		{"~1.3 ms RTT gives 1 ms", ms(1.3), ms(0.2), 3, 1},
		{"factor 0 uses the mean only", ms(10), ms(2), 0, 10},
		{"factor 2", ms(100), ms(5), 2, 110},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			require.Equal(t, c.want, electionBaseTimeoutMs(c.mean, c.stddev, c.factor))
		})
	}
}

func TestDynatuneDrawElectionTimeoutRange(t *testing.T) {
	var gotN int
	lo, ok := drawElectionTimeout(54, func(n int) int { gotN = n; return 0 })
	require.True(t, ok)
	require.Equal(t, 54, gotN, "randomness is drawn over [0, base)")
	require.Equal(t, 54, lo)
	hi, ok := drawElectionTimeout(54, func(n int) int { return n - 1 })
	require.True(t, ok)
	require.Equal(t, 107, hi) // 2*base - 1
}

func TestDynatuneDrawElectionTimeoutRejectsNonPositiveBase(t *testing.T) {
	_, ok := drawElectionTimeout(1, func(n int) int { return 0 })
	require.True(t, ok, "base 1 is the smallest valid base")
	for _, base := range []int{0, -1, -100} {
		_, ok := drawElectionTimeout(base, func(n int) int { return 0 })
		require.False(t, ok, "base %d must make the caller fall back to the stock draw", base)
	}
}

// ------------------------------------------------------ heartbeat interval

func TestDynatuneHeartbeatIntervalWithoutOptimizationUsesK(t *testing.T) {
	require.Equal(t, int64(166), heartbeatIntervalMs(1000, 0.3, 0.99, 5, false)) // floor(1000/6)
	require.Equal(t, int64(100), heartbeatIntervalMs(1000, 0, 0.99, 9, false))
	require.Equal(t, int64(1000), heartbeatIntervalMs(1000, 0, 0.99, 0, false))
}

func TestDynatuneHeartbeatIntervalZeroOrNegativeLossIsHalfTheWindow(t *testing.T) {
	// ceilLogTerm is 1 when loss <= 0, so interval = floor(T/2). Matches the
	// B3 live observation: randomized timeout ~1870 ms -> interval 935 ms.
	require.Equal(t, int64(935), heartbeatIntervalMs(1870, 0, 0.99, 5, true))
	require.Equal(t, int64(935), heartbeatIntervalMs(1870, -0.0625, 0.99, 5, true))
	require.Equal(t, int64(500), heartbeatIntervalMs(1001, 0, 0.99, 5, true))
}

func TestDynatuneHeartbeatIntervalFromMeasuredLoss(t *testing.T) {
	// interval = floor(T / (ceil(ln(1-goal)/ln(loss)) + 1)), T = 900.
	// Ratios chosen away from integers to avoid pinning floating-point noise.
	cases := []struct {
		goal, loss float64
		want       int64
	}{
		{0.99, 0.05, 300}, // ceil(1.537)=2  -> 900/3
		{0.99, 0.2, 225},  // ceil(2.861)=3  -> 900/4
		{0.99, 0.3, 180},  // ceil(3.825)=4  -> 900/5
		{0.99, 0.5, 112},  // ceil(6.644)=7  -> 900/8
		{0.99, 0.9, 20},   // ceil(43.71)=44 -> 900/45
		{0.999, 0.2, 150}, // ceil(4.292)=5  -> 900/6
		{0.999, 0.5, 81},  // ceil(9.966)=10 -> 900/11
	}
	for _, c := range cases {
		require.Equal(t, c.want, heartbeatIntervalMs(900, c.loss, c.goal, 5, true), "goal=%v loss=%v", c.goal, c.loss)
	}
}

func TestDynatuneHeartbeatIntervalTotalLossIsDefined(t *testing.T) {
	// Deviation from the reference, which computes log(1)=0 -> -Inf and relies
	// on an implementation-defined float->int conversion. We return the
	// fastest valid interval (1) instead. At runtime this is equivalent: the
	// leader increments elapsed before comparing, so 0 and 1 both mean "every
	// tick".
	require.Equal(t, int64(1), heartbeatIntervalMs(900, 1.0, 0.99, 5, true))
	require.Equal(t, int64(1), heartbeatIntervalMs(900, 1.5, 0.99, 5, true))
}

func TestDynatuneHeartbeatIntervalIsNeverNegativeNorAboveTheWindow(t *testing.T) {
	for _, T := range []int{0, 1, 2, 100, 1870} {
		for _, loss := range []float64{-1, 0, 0.01, 0.2, 0.99, 1, 2} {
			for _, opt := range []bool{true, false} {
				got := heartbeatIntervalMs(T, loss, 0.99, 5, opt)
				require.GreaterOrEqual(t, got, int64(0), "T=%d loss=%v opt=%v", T, loss, opt)
				require.LessOrEqual(t, got, int64(T)+1, "T=%d loss=%v opt=%v", T, loss, opt)
			}
		}
	}
}

// ----------------------------------------------------- leader-side helpers

func TestDynatuneRttSince(t *testing.T) {
	now := t0.Add(7 * time.Millisecond)
	require.Equal(t, ms(7), rttSince(now, t0.UnixNano()))
	require.Equal(t, time.Duration(0), rttSince(t0, t0.UnixNano()))
}

func TestDynatuneRttSinceNeverNegativeWhenClockStepsBack(t *testing.T) {
	// A negative sample would corrupt the mean; clamp to 0.
	require.Equal(t, time.Duration(0), rttSince(t0, t0.Add(5*time.Millisecond).UnixNano()))
}

func TestDynatuneLeaderMetricsKeepsLatestRTTPerFollower(t *testing.T) {
	ls := &leaderMetrics{} // zero value must be usable (nil map)
	_, ok := ls.getRTT(2)
	require.False(t, ok)
	ls.updateRTT(2, ms(5))
	ls.updateRTT(2, ms(7))
	ls.updateRTT(3, ms(1))
	got, ok := ls.getRTT(2)
	require.True(t, ok)
	require.Equal(t, ms(7), got)
	got, ok = ls.getRTT(3)
	require.True(t, ok)
	require.Equal(t, ms(1), got)
	_, ok = ls.getRTT(4)
	require.False(t, ok)
}

func TestDynatuneInitHeartbeatStates(t *testing.T) {
	st := initHeartbeatStates([]uint64{2, 3}, 100)
	require.Len(t, st, 2)
	for _, id := range []uint64{2, 3} {
		require.Equal(t, &heartbeatState{elapsed: 0, timeout: 100, sequenceId: 0}, st[id])
	}
	empty := initHeartbeatStates(nil, 100)
	require.NotNil(t, empty, "must be usable as a map (safe to add peers later)")
	require.Empty(t, empty)
}

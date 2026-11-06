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
	"math/rand"
	"testing"
	"time"

	pb "go.etcd.io/raft/v3/raftpb"
)

// A deterministic discrete-event simulation of a 3-node cluster (task C5e):
// a shared fake clock advances one millisecond per step (one raft tick = 1 ms,
// as Dynatune requires), messages are delivered after a configurable per-link
// delay, heartbeats can be dropped (UDP-like loss) and a node can be isolated.
// It exercises the leader, follower and election wiring end to end, without
// touching the interaction-test harness. Only raft's own election draws
// (crypto/rand) are not seeded, so assertions use margins that hold for any
// draw; the simulator's loss decisions use a fixed seed.

type simMsg struct {
	at  int64 // simulated ms
	seq int   // tie-break: FIFO among messages due at the same time
	to  uint64
	m   *pb.Message
}

type simCluster struct {
	t        *testing.T
	clk      *fakeClock
	nodes    map[uint64]*raft
	ids      []uint64
	now      int64 // ms since start
	queue    []simMsg
	seq      int
	isolated map[uint64]bool
	// link decides, per message, the one-way delay in ms and whether to drop it.
	link func(m *pb.Message) (delayMs int64, drop bool)
}

type simOpts struct {
	dynatune   bool
	oneWayMs   int64   // one-way link delay (RTT = 2x)
	hbLossProb float64 // probability of dropping a leader->follower heartbeat
	seed       int64   // seed for the loss decisions
}

func newSim(t *testing.T, o simOpts) *simCluster {
	t.Helper()
	clk := newFakeClock()
	c := &simCluster{t: t, clk: clk, nodes: map[uint64]*raft{}, ids: []uint64{1, 2, 3}, isolated: map[uint64]bool{}}
	rng := rand.New(rand.NewSource(o.seed))
	c.link = func(m *pb.Message) (int64, bool) {
		if o.hbLossProb > 0 && m.GetType() == pb.MsgHeartbeat && rng.Float64() < o.hbLossProb {
			return 0, true
		}
		return o.oneWayMs, false
	}
	for _, id := range c.ids {
		cfg := newTestConfig(id, dynElection, dynHeartbeat, newTestMemoryStorage(withPeers(1, 2, 3)))
		cfg.PreVote = true
		cfg.CheckQuorum = true
		cfg.Clock = clk.Now
		if o.dynatune {
			cfg.Dynatune = true
			cfg.MaxElectionMetricsCapacity = 100
			cfg.MinElectionMetricsCapacity = 10
			cfg.ElectionSafetyFactor = 3
			cfg.HeartbeatReachabilityGoal = 0.99
			cfg.K = 5
			cfg.OptimizeHeartbeatInterval = true
		}
		r := newRaft(cfg)
		r.becomeFollower(0, None)
		c.nodes[id] = r
	}
	return c
}

func (c *simCluster) enqueue(at int64, to uint64, m *pb.Message) {
	c.seq++
	c.queue = append(c.queue, simMsg{at: at, seq: c.seq, to: to, m: m})
	for i := len(c.queue) - 1; i > 0; i-- {
		a, b := c.queue[i], c.queue[i-1]
		if a.at < b.at || (a.at == b.at && a.seq < b.seq) {
			c.queue[i], c.queue[i-1] = b, a
		} else {
			break
		}
	}
}

// flush moves node id's outgoing messages into the delivery queue.
func (c *simCluster) flush(id uint64) {
	for _, m := range c.nodes[id].readMessages() {
		to := m.GetTo()
		if c.isolated[id] || c.isolated[to] {
			continue
		}
		d, drop := c.link(m)
		if drop {
			continue
		}
		c.enqueue(c.now+d, to, m)
	}
}

func (c *simCluster) deliverDue() {
	for len(c.queue) > 0 && c.queue[0].at <= c.now {
		e := c.queue[0]
		c.queue = c.queue[1:]
		if c.isolated[e.to] {
			continue
		}
		_ = c.nodes[e.to].Step(e.m) // errors such as dropped proposals are irrelevant here
		c.flush(e.to)
	}
}

// step advances simulated time by one millisecond.
func (c *simCluster) step() {
	c.now++
	c.clk.Advance(time.Millisecond)
	c.deliverDue()
	for _, id := range c.ids {
		if c.isolated[id] {
			continue
		}
		c.nodes[id].tick()
		c.flush(id)
	}
	c.deliverDue()
}

func (c *simCluster) run(ms int64) {
	for i := int64(0); i < ms; i++ {
		c.step()
	}
}

// leader returns the non-isolated node that is currently leader, or 0.
func (c *simCluster) leader() uint64 {
	var lead uint64
	for _, id := range c.ids {
		if !c.isolated[id] && c.nodes[id].state == StateLeader {
			lead = id
		}
	}
	return lead
}

// electLeader runs until a leader exists (bounded) and returns it.
func (c *simCluster) electLeader() uint64 {
	c.t.Helper()
	for i := 0; i < 10000; i++ {
		c.step()
		if l := c.leader(); l != 0 {
			return l
		}
	}
	c.t.Fatalf("no leader elected within 10 simulated seconds")
	return 0
}

// isolateLeaderAndMeasure cuts the leader off and returns how many simulated
// ms pass until one of the other two nodes is leader (bounded at 20 s).
func (c *simCluster) isolateLeaderAndMeasure(lead uint64) int64 {
	c.t.Helper()
	c.isolated[lead] = true
	start := c.now
	for i := 0; i < 20000; i++ {
		c.step()
		if l := c.leader(); l != 0 && l != lead {
			return c.now - start
		}
	}
	c.t.Fatalf("no new leader within 20 simulated seconds after isolating %d", lead)
	return 0
}

func (c *simCluster) followers(lead uint64) []uint64 {
	var out []uint64
	for _, id := range c.ids {
		if id != lead {
			out = append(out, id)
		}
	}
	return out
}

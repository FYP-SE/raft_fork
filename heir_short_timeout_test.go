package raft

import (
	"testing"

	pb "go.etcd.io/raft/v3/raftpb"
	"go.etcd.io/raft/v3/stability"
)

// DESIGN_UPDATE.md D1 -- the heir gets a short timeout.
//
// v1's heir fired at exactly electionTimeout (HeirJitter 0.1 x ET 10 gave
// Intn(1) = 0: no jitter at all), so it had almost no head start over stock.
// D1: the heir draws HeirTimeout (default 3 ticks) plus at least one tick of
// real jitter, retries a failed PreVote round after another heir draw, and
// stops acting as heir once electionTimeout ticks of leader silence have
// passed -- from then on it fires at the same total silence a stock draw
// from [ET, 2ET) would give, so its worst case is stock's.

func newShortTimeoutRaft(t *testing.T, mut func(*Config)) *raft {
	t.Helper()
	cfg := newTestConfig(1, 10, 1, newTestMemoryStorage(withPeers(1, 2, 3)))
	cfg.StabilityScorer = stability.ConstScorer(0)
	cfg.HeirElection = true
	cfg.PreVote = true
	if mut != nil {
		mut(cfg)
	}
	r := newRaft(cfg)
	r.becomeFollower(1, 3)
	r.recordHeir(newHeirMsg(1))        // I am the heir
	r.resetRandomizedElectionTimeout() // as stepFollower does after recordHeir
	return r
}

func TestHeirTimeout_DefaultDrawIsThreeOrFourTicks(t *testing.T) {
	r := newShortTimeoutRaft(t, nil)
	if r.heirTimeout != 3 {
		t.Fatalf("heirTimeout = %d, want default 3", r.heirTimeout)
	}
	seen := map[int]int{}
	for i := 0; i < timeoutSamples; i++ {
		r.resetRandomizedElectionTimeout()
		seen[r.randomizedElectionTimeout]++
	}
	if len(seen) != 2 || seen[3] == 0 || seen[4] == 0 {
		t.Fatalf("heir draws = %v, want both 3 and 4 ticks and nothing else", seen)
	}
}

func TestHeirTimeout_Validation(t *testing.T) {
	base := func() *Config {
		cfg := newTestConfig(1, 10, 1, newTestMemoryStorage(withPeers(1, 2, 3)))
		cfg.StabilityScorer = stability.ConstScorer(0)
		cfg.HeirElection = true
		return cfg
	}
	for name, mut := range map[string]func(*Config){
		"below 2":                 func(c *Config) { c.HeirTimeout = 1 },
		"not below election":      func(c *Config) { c.HeirTimeout = 10 },
		"not above heartbeat":     func(c *Config) { c.HeartbeatTick = 3; c.HeirTimeout = 3 },
		"negative":                func(c *Config) { c.HeirTimeout = -1 },
		"default unusable (ET=2)": func(c *Config) { c.ElectionTick = 2 },
	} {
		cfg := base()
		mut(cfg)
		if err := cfg.validate(); err == nil {
			t.Errorf("%s: validate() accepted HeirTimeout=%d ET=%d HB=%d", name, cfg.HeirTimeout, cfg.ElectionTick, cfg.HeartbeatTick)
		}
	}
	// With a small election timeout the default clamps to ET-1.
	cfg := base()
	cfg.ElectionTick = 3
	if err := cfg.validate(); err != nil {
		t.Fatalf("ET=3: validate(): %v", err)
	}
	if cfg.HeirTimeout != 2 {
		t.Fatalf("ET=3: HeirTimeout = %d, want default clamped to 2", cfg.HeirTimeout)
	}
	// HeirElection off: HeirTimeout is unused and never rejected.
	cfg = newTestConfig(1, 2, 1, newTestMemoryStorage(withPeers(1, 2, 3)))
	cfg.HeirElection = false
	cfg.HeirLogPriority = false  // explicit, regardless of RAFT_HEIRRAFT_FORCE_ON
	cfg.GracefulHandover = false // (heir_conformance_test.go)
	cfg.StabilityScorer = nil
	if err := cfg.validate(); err != nil {
		t.Fatalf("HeirElection off, ET=2: validate(): %v", err)
	}
}

// The heir draw never reaches electionTimeout, even with a large jitter.
func TestHeirTimeout_DrawStaysBelowElectionTimeout(t *testing.T) {
	r := newShortTimeoutRaft(t, func(c *Config) { c.HeirTimeout = 8; c.HeirJitter = 0.5 })
	for i := 0; i < timeoutSamples; i++ {
		r.resetRandomizedElectionTimeout()
		if got := r.randomizedElectionTimeout; got < 8 || got >= r.electionTimeout {
			t.Fatalf("heir draw = %d, want in [8, %d)", got, r.electionTimeout)
		}
	}
}

// preVoteRoundTicks ticks r (no peer ever answers) and returns the ticks,
// counted from the last leader contact, at which it started a PreVote round.
func preVoteRoundTicks(r *raft, ticks int) []int {
	var rounds []int
	for i := 1; i <= ticks; i++ {
		r.tick()
		for _, m := range r.msgs {
			if m.GetType() == pb.MsgPreVote {
				rounds = append(rounds, i)
				break
			}
		}
		r.msgs = nil
	}
	return rounds
}

// A failed PreVote round is retried after another heir draw, not after a
// full election timeout.
func TestHeirTimeout_FastRetriesWithinElectionTimeout(t *testing.T) {
	r := newShortTimeoutRaft(t, nil)
	rounds := preVoteRoundTicks(r, r.electionTimeout-1)
	if len(rounds) < 2 {
		t.Fatalf("PreVote rounds before ET = %v, want >= 2 (fast retry)", rounds)
	}
	if rounds[0] < 3 || rounds[0] > 4 {
		t.Fatalf("first PreVote round at tick %d, want 3 or 4", rounds[0])
	}
	for i := 1; i < len(rounds); i++ {
		if gap := rounds[i] - rounds[i-1]; gap < 3 || gap > 4 {
			t.Fatalf("retry gap %d ticks (rounds %v), want 3 or 4", gap, rounds)
		}
	}
}

// After ET ticks of silence the heir is an ordinary node: its next round
// comes at the total silence a stock draw from [ET, 2ET) gives, and the
// distribution covers that whole range.
func TestHeirTimeout_FallsBackToStockAfterElectionTimeout(t *testing.T) {
	et := 10
	seen := map[int]bool{}
	for trial := 0; trial < 2000; trial++ {
		r := newShortTimeoutRaft(t, nil)
		rounds := preVoteRoundTicks(r, 3*et)
		var after []int
		for _, x := range rounds {
			if x >= et {
				after = append(after, x)
			}
		}
		if len(after) == 0 {
			t.Fatalf("no PreVote round at or after ET: %v", rounds)
		}
		if first := after[0]; first < et || first >= 2*et {
			t.Fatalf("first round after the heir window at silence %d (rounds %v), want in [%d,%d) like stock", first, rounds, et, 2*et)
		}
		seen[after[0]] = true
	}
	for x := et + 1; x < 2*et; x++ {
		if !seen[x] {
			t.Errorf("fallback round never landed at silence %d: not a stock-like spread", x)
		}
	}
}

// Hearing from the leader restarts the heir window.
func TestHeirTimeout_LeaderContactResetsSilence(t *testing.T) {
	r := newShortTimeoutRaft(t, nil)
	for i := 0; i < 2*r.electionTimeout; i++ {
		r.tick()
	}
	r.msgs = nil
	if r.heirSilence < r.electionTimeout {
		t.Fatalf("heirSilence = %d, want >= ET after 2 ET of silence", r.heirSilence)
	}
	if err := r.Step(&pb.Message{From: new(uint64(3)), To: new(uint64(1)), Term: new(r.Term),
		Type: pb.MsgHeartbeat.Enum(), Heir: new(uint64(1))}); err != nil {
		t.Fatal(err)
	}
	if r.heirSilence != 0 {
		t.Fatalf("heirSilence = %d after a leader heartbeat, want 0", r.heirSilence)
	}
	if got := r.randomizedElectionTimeout; got < 3 || got > 4 {
		t.Fatalf("randomizedElectionTimeout = %d after leader contact, want a heir draw (3-4)", got)
	}
}

// Non-heirs are untouched by D1: stock range.
func TestHeirTimeout_NonHeirStillStock(t *testing.T) {
	r := newShortTimeoutRaft(t, nil)
	r.recordHeir(newHeirMsg(2))
	assertVanillaDistribution(t, r)
}

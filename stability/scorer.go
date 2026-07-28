// Package stability implements HeirRaft's local node-stability scoring
// (DESIGN.md §2.1). It has no OS/etcd dependency: the host application
// samples its own signals in and reads a score out.
package stability

// Scorer reports a node's current stability in [0,255], 255 = most stable.
// Implementations must be non-blocking and cheap -- Score is called at
// heartbeat-tick frequency by the raft library.
//
// A nil Scorer disables all HeirRaft behaviour in raft.Config; the mechanism
// treats it as if every node reported a constant score, i.e. inert.
type Scorer interface {
	Score() uint8
}

// ConstScorer is a fixed-value Scorer for tests: it always reports the same
// score regardless of any sampling.
type ConstScorer uint8

// Score implements Scorer.
func (c ConstScorer) Score() uint8 { return uint8(c) }

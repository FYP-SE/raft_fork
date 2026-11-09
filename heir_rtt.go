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
	"fmt"
	"math"
	"slices"
	"strings"
	"time"
)

// PeerRTTProvider gives the local node's current round-trip-time estimate
// to a peer (DESIGN_UPDATE.md E8). ok is false while no estimate exists
// (peer not probed yet, or unreachable). Called from the raft goroutine,
// so implementations must be cheap and must not call back into raft.
type PeerRTTProvider interface {
	PeerRTT(id uint64) (rtt time.Duration, ok bool)
}

// heirCandidate is a follower that passed selectHeir's eligibility check
// (in sync in heirEntryNeeded of the last heirEntryWindow samples).
type heirCandidate struct {
	id    uint64
	score uint8
	rtt   time.Duration // unknownRTT when the provider has no estimate
}

// unknownRTT ranks a follower without an RTT estimate after every follower
// with one.
const unknownRTT = time.Duration(math.MaxInt64)

// selectHeirByRTT is selectHeir's final step under HeirRTTRank
// (DESIGN_UPDATE.md E8). The in-sync bookkeeping and eligibility are
// unchanged; only the choice among eligible followers differs:
//
//  1. Health gate: a candidate is healthy if its score is at least
//     bestScore - HysteresisMargin, bestScore being the highest eligible
//     score. A follower with one fully bad signal (>= 51 below a healthy
//     one, D7) is therefore never chosen while a healthy one exists.
//  2. Rank: lowest RTT first; unknown RTT last; ties by higher score, then
//     lower ID.
//  3. Churn control: a heir kept by the sync grace (keep) is replaced only
//     after minHeirTenure, and only if it is no longer healthy or the top
//     candidate's RTT is lower by more than heirRTTMargin.
func (r *raft) selectHeirByRTT(cands []heirCandidate, bestScore uint8, keep bool) {
	if len(cands) == 0 {
		if !keep {
			r.changeHeir(None)
		}
		return
	}
	curEligible := false
	for _, c := range cands {
		if c.id == r.heir {
			curEligible = true
		}
	}
	floor := int(bestScore) - int(r.hysteresisMargin)
	healthy := make([]heirCandidate, 0, len(cands))
	for _, c := range cands {
		if int(c.score) >= floor {
			if rtt, ok := r.peerRTT.PeerRTT(c.id); ok && rtt > 0 {
				c.rtt = rtt
			} else {
				c.rtt = unknownRTT
			}
			healthy = append(healthy, c)
		}
	}
	slices.SortStableFunc(healthy, func(a, b heirCandidate) int {
		switch {
		case a.rtt != b.rtt:
			if a.rtt < b.rtt {
				return -1
			}
			return 1
		case a.score != b.score:
			return int(b.score) - int(a.score)
		case a.id < b.id:
			return -1
		case a.id > b.id:
			return 1
		}
		return 0
	})
	top := healthy[0]

	if !keep {
		r.logHeirRanking(healthy)
		r.changeHeir(top.id)
		return
	}
	if top.id == r.heir {
		return
	}
	cur, curHealthy := heirCandidate{}, false
	for _, c := range healthy {
		if c.id == r.heir {
			cur, curHealthy = c, true
			break
		}
	}
	// A kept heir that is not eligible right now (a few slow acks) is left
	// to the sync grace: RTT never replaces it. Only an eligible heir below
	// the health floor is replaced on health grounds.
	if !curEligible {
		r.heirTenure++
		return
	}
	better := !curHealthy ||
		(top.rtt != unknownRTT && (cur.rtt == unknownRTT || top.rtt+r.heirRTTMargin < cur.rtt))
	if better && r.heirTenure >= r.minHeirTenure {
		r.logHeirRanking(healthy)
		r.changeHeir(top.id)
		return
	}
	// As in the score ranking, tenure only accumulates while a challenger
	// exists.
	r.heirTenure++
}

// logHeirRanking logs the ranked healthy candidates when the heir changes,
// so the run logs show which RTTs the choice was based on.
func (r *raft) logHeirRanking(ranked []heirCandidate) {
	parts := make([]string, 0, len(ranked))
	for _, c := range ranked {
		rtt := "unknown"
		if c.rtt != unknownRTT {
			rtt = c.rtt.String()
		}
		parts = append(parts, fmt.Sprintf("%x(rtt=%s,score=%d)", c.id, rtt, c.score))
	}
	r.logger.Infof("%x heir ranking by RTT: %s", r.id, strings.Join(parts, " "))
}

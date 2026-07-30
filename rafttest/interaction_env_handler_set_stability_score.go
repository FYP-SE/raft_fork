// Copyright 2019 The etcd Authors
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

package rafttest

import (
	"testing"

	"github.com/cockroachdb/datadriven"

	"go.etcd.io/raft/v3/stability"
)

// T4.6 (TASKS.md): changes a node's stability score at runtime, so
// datadriven tests can simulate a leader degrading (or recovering) over
// time -- something the fixed-value stability.ConstScorer used by earlier
// tasks' tests can't do. Only works for a node added with the
// `mutable-stability-score` add-nodes arg (which wires in a *stability.Var
// instead of a ConstScorer); using it on any other node is a test-authoring
// mistake, so it fails the test rather than silently no-op-ing.
func (env *InteractionEnv) handleSetStabilityScore(t *testing.T, d datadriven.TestData) error {
	idx := firstAsNodeIdx(t, d)
	var score int
	d.ScanArgs(t, "score", &score)

	v, ok := env.Nodes[idx].Config.StabilityScorer.(*stability.Var)
	if !ok {
		t.Fatalf("node %d's StabilityScorer is a %T, not a *stability.Var -- add it with mutable-stability-score=N", idx+1, env.Nodes[idx].Config.StabilityScorer)
	}
	v.Set(uint8(score))
	return nil
}

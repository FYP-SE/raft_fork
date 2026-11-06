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

// Config surface of the Dynatune port (task C5, group E, plus the zero-value
// guard of group A). Deliberately NOT newTestConfig for the "off" cases: like
// heir_config_test.go, they assert Config's own zero-value behaviour.
//
// The default VALUES of the tunables are not pinned here: which values define
// "the Dynatune arm" is still an open decision (Q6). Tests assert only that
// defaults are valid, and that explicit values are never overwritten.

func dynCfgBase() *Config {
	return &Config{
		ID:              1,
		ElectionTick:    1000,
		HeartbeatTick:   100,
		Storage:         newTestMemoryStorage(withPeers(1, 2, 3)),
		MaxSizePerMsg:   noLimit,
		MaxInflightMsgs: 256,
	}
}

func TestDynatuneConfig_ZeroValueIsOffAndValid(t *testing.T) {
	cfg := dynCfgBase()
	require.NoError(t, cfg.validate())
	require.False(t, cfg.Dynatune)
	require.False(t, cfg.OptimizeHeartbeatInterval)
	require.Nil(t, cfg.Clock)
}

func TestDynatuneConfig_OffIgnoresInvalidTunables(t *testing.T) {
	cfg := dynCfgBase()
	cfg.MaxElectionMetricsCapacity = 5
	cfg.MinElectionMetricsCapacity = 10 // min > max
	cfg.ElectionSafetyFactor = -1
	cfg.HeartbeatReachabilityGoal = 5
	cfg.K = -3
	cfg.OptimizeHeartbeatInterval = true
	require.NoError(t, cfg.validate(), "stock configs must never be broken by unused Dynatune fields")
}

func TestDynatuneConfig_OnFillsValidDefaults(t *testing.T) {
	cfg := dynCfgBase()
	cfg.Dynatune = true
	require.NoError(t, cfg.validate())
	require.Greater(t, cfg.MinElectionMetricsCapacity, 0)
	require.Greater(t, cfg.MaxElectionMetricsCapacity, cfg.MinElectionMetricsCapacity)
	require.Greater(t, cfg.ElectionSafetyFactor, 0)
	require.Greater(t, cfg.HeartbeatReachabilityGoal, 0.0)
	require.Less(t, cfg.HeartbeatReachabilityGoal, 1.0)
	require.Greater(t, cfg.K, int64(0))
}

func TestDynatuneConfig_OnKeepsExplicitValues(t *testing.T) {
	cfg := dynCfgBase()
	cfg.Dynatune = true
	cfg.MaxElectionMetricsCapacity = 321
	cfg.MinElectionMetricsCapacity = 7
	cfg.ElectionSafetyFactor = 2
	cfg.HeartbeatReachabilityGoal = 0.999
	cfg.K = 9
	cfg.OptimizeHeartbeatInterval = true
	require.NoError(t, cfg.validate())
	require.Equal(t, 321, cfg.MaxElectionMetricsCapacity)
	require.Equal(t, 7, cfg.MinElectionMetricsCapacity)
	require.Equal(t, 2, cfg.ElectionSafetyFactor)
	require.Equal(t, 0.999, cfg.HeartbeatReachabilityGoal)
	require.Equal(t, int64(9), cfg.K)
	require.True(t, cfg.OptimizeHeartbeatInterval)
}

func TestDynatuneConfig_OnRejectsInvalidValues(t *testing.T) {
	cases := []struct {
		name string
		mod  func(*Config)
	}{
		{"min >= max window", func(c *Config) { c.MaxElectionMetricsCapacity = 5; c.MinElectionMetricsCapacity = 5 }},
		{"min > max window", func(c *Config) { c.MaxElectionMetricsCapacity = 5; c.MinElectionMetricsCapacity = 9 }},
		{"negative min window", func(c *Config) { c.MinElectionMetricsCapacity = -1 }},
		{"negative max window", func(c *Config) { c.MaxElectionMetricsCapacity = -1 }},
		{"negative safety factor", func(c *Config) { c.ElectionSafetyFactor = -1 }},
		{"goal == 1 when optimising", func(c *Config) { c.OptimizeHeartbeatInterval = true; c.HeartbeatReachabilityGoal = 1 }},
		{"goal > 1 when optimising", func(c *Config) { c.OptimizeHeartbeatInterval = true; c.HeartbeatReachabilityGoal = 1.5 }},
		{"negative goal when optimising", func(c *Config) { c.OptimizeHeartbeatInterval = true; c.HeartbeatReachabilityGoal = -0.1 }},
		{"NaN goal when optimising", func(c *Config) { c.OptimizeHeartbeatInterval = true; c.HeartbeatReachabilityGoal = math.NaN() }},
		{"negative K when not optimising", func(c *Config) { c.OptimizeHeartbeatInterval = false; c.K = -1 }},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			cfg := dynCfgBase()
			cfg.Dynatune = true
			c.mod(cfg)
			require.Error(t, cfg.validate())
		})
	}
}

func TestDynatuneConfig_OnlyChecksTheTunableThatIsUsed(t *testing.T) {
	// goal is irrelevant when not optimising; K is irrelevant when optimising.
	cfg := dynCfgBase()
	cfg.Dynatune = true
	cfg.OptimizeHeartbeatInterval = false
	cfg.HeartbeatReachabilityGoal = 1.5
	require.NoError(t, cfg.validate())

	cfg = dynCfgBase()
	cfg.Dynatune = true
	cfg.OptimizeHeartbeatInterval = true
	cfg.K = -1
	require.NoError(t, cfg.validate())
}

func TestDynatuneConfig_NilClockDefaultsToWallClock(t *testing.T) {
	for _, on := range []bool{false, true} {
		cfg := dynCfgBase()
		cfg.Dynatune = on
		r := newRaft(cfg)
		require.NotNil(t, r.clock, "dynatune=%v", on)
		require.WithinDuration(t, time.Now(), r.clock(), time.Minute, "dynatune=%v", on)
	}
}

func TestDynatuneConfig_InjectedClockIsUsed(t *testing.T) {
	cfg := dynCfgBase()
	cfg.Dynatune = true
	cfg.Clock = func() time.Time { return t0 }
	r := newRaft(cfg)
	require.Equal(t, t0, r.clock())
}

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

	"google.golang.org/protobuf/proto"

	pb "go.etcd.io/raft/v3/raftpb"
)

// TestHeirFieldsMessageSize measures the exact protobuf size HeirRaft's two
// Message fields add (stability = field 15, heir = field 16), for the final
// evaluation's per-message overhead table. Run with -v to print the table.
//
// IDs are etcd-style random 64-bit member IDs; the worst case (top bit set)
// needs a 10-byte varint. Field 16 needs a 2-byte tag, field 15 a 1-byte tag.
func TestHeirFieldsMessageSize(t *testing.T) {
	const (
		leaderID = uint64(0x8e9e05c52164694d) // top bit set: 10-byte varint, worst case
		heirID   = uint64(0x91bc3c398fb3c146) // top bit set
	)
	entry := &pb.Entry{Term: new(uint64(7)), Index: new(uint64(123456)), Type: pb.EntryNormal.Enum(),
		Data: make([]byte, 290)} // ~ one put: 16 B key + 256 B value + request framing
	base := func(typ pb.MessageType) *pb.Message {
		m := &pb.Message{Type: typ.Enum(), To: new(heirID), From: new(leaderID), Term: new(uint64(7))}
		switch typ {
		case pb.MsgApp:
			m.LogTerm, m.Index, m.Commit = new(uint64(7)), new(uint64(123455)), new(uint64(123450))
			m.Entries = []*pb.Entry{entry}
		case pb.MsgHeartbeat:
			m.Commit = new(uint64(123450))
		case pb.MsgAppResp:
			m.Index = new(uint64(123456))
		}
		return m
	}
	cases := []struct {
		typ           pb.MessageType
		stability     bool // followers stamp their score on responses
		heir          bool // the leader stamps the heir on MsgApp/MsgHeartbeat
		maxExtraBytes int
	}{
		{pb.MsgApp, false, true, 12},
		{pb.MsgHeartbeat, false, true, 12},
		{pb.MsgAppResp, true, false, 3},
		{pb.MsgHeartbeatResp, true, false, 3},
		// Upper bound if both fields were present on one message.
		{pb.MsgApp, true, true, 15},
	}
	for _, c := range cases {
		stock := base(c.typ)
		heirraft := base(c.typ)
		if c.stability {
			heirraft.Stability = new(uint32(255)) // 255 needs a 2-byte varint: worst case
		}
		if c.heir {
			heirraft.Heir = new(heirID)
		}
		s, h := proto.Size(stock), proto.Size(heirraft)
		t.Logf("%-17s stability=%-5v heir=%-5v stock=%4d B heirraft=%4d B extra=%2d B (%.1f %%)",
			c.typ, c.stability, c.heir, s, h, h-s, 100*float64(h-s)/float64(s))
		if h-s > c.maxExtraBytes {
			t.Errorf("%s: extra %d B, want <= %d B", c.typ, h-s, c.maxExtraBytes)
		}
	}
	// Explicit "no heir" stamp (heir = 0 after a drop, DESIGN_UPDATE D4).
	m0, m1 := base(pb.MsgHeartbeat), base(pb.MsgHeartbeat)
	m1.Heir = new(uint64(0))
	t.Logf("MsgHeartbeat heir=0 stamp: extra=%d B", proto.Size(m1)-proto.Size(m0))
	if d := proto.Size(m1) - proto.Size(m0); d != 3 {
		t.Errorf("heir=0 stamp: extra %d B, want 3 B", d)
	}
}

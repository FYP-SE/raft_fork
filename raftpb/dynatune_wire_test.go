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

package raftpb

import (
	"testing"

	"google.golang.org/protobuf/encoding/protowire"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
)

// Task C3 (Dynatune port) -- wire tests for the four new Message fields:
//
//	rtt = 17, send_time = 18, sequence_id = 19, heartbeat_interval = 20
//
// The reference implementation (distsys-lab/raft @ cdc18d1) uses 15-18, which
// would collide with HeirRaft's stability = 15 and heir = 16, so the port
// uses 17-20. Same contract as the HeirRaft fields (heirraft_wire_test.go):
// proto2 optional, nil by default, and no wire tag at all when unset -- so
// with Dynatune off, traffic is byte-identical to stock.

func fieldNumber(t *testing.T, name protoreflect.Name) protoreflect.FieldNumber {
	t.Helper()
	fd := (&Message{}).ProtoReflect().Descriptor().Fields().ByName(name)
	if fd == nil {
		t.Fatalf("Message has no field %q", name)
	}
	return fd.Number()
}

// TestMessage_DynatuneFieldNumbers pins the numbers (a renumbering would
// silently break wire compatibility between builds) and that HeirRaft's
// fields did not move.
func TestMessage_DynatuneFieldNumbers(t *testing.T) {
	for name, want := range map[protoreflect.Name]protoreflect.FieldNumber{
		"rtt":                17,
		"send_time":          18,
		"sequence_id":        19,
		"heartbeat_interval": 20,
		"stability":          15,
		"heir":               16,
	} {
		if got := fieldNumber(t, name); got != want {
			t.Errorf("field %q has number %d, want %d", name, got, want)
		}
	}
}

func TestMessage_DynatuneFieldsRoundtrip(t *testing.T) {
	want := &Message{
		Type:              MsgHeartbeat.Enum(),
		To:                proto.Uint64(2),
		From:              proto.Uint64(1),
		Term:              proto.Uint64(5),
		Stability:         proto.Uint32(200), // HeirRaft fields coexist
		Heir:              proto.Uint64(3),
		Rtt:               proto.Int64(55_262_688),
		SendTime:          proto.Int64(1_700_000_000_123_456_789),
		SequenceId:        proto.Int64(42),
		HeartbeatInterval: proto.Int64(935),
	}
	data, err := proto.Marshal(want)
	if err != nil {
		t.Fatalf("Marshal: %v", err)
	}
	got := &Message{}
	if err := proto.Unmarshal(data, got); err != nil {
		t.Fatalf("Unmarshal: %v", err)
	}
	if !proto.Equal(want, got) {
		t.Errorf("roundtrip mismatch: got %+v, want %+v", got, want)
	}
	if got.GetRtt() != 55_262_688 || got.GetSendTime() != 1_700_000_000_123_456_789 ||
		got.GetSequenceId() != 42 || got.GetHeartbeatInterval() != 935 {
		t.Errorf("getters wrong: rtt=%d send=%d seq=%d hb=%d",
			got.GetRtt(), got.GetSendTime(), got.GetSequenceId(), got.GetHeartbeatInterval())
	}
	if got.GetStability() != 200 || got.GetHeir() != 3 {
		t.Errorf("HeirRaft fields disturbed: stability=%d heir=%d", got.GetStability(), got.GetHeir())
	}
}

// The reference uses -1 as "no interval computed yet" in heartbeat responses;
// a negative int64 must survive the wire.
func TestMessage_DynatuneNegativeHeartbeatIntervalRoundtrip(t *testing.T) {
	want := &Message{Type: MsgHeartbeatResp.Enum(), HeartbeatInterval: proto.Int64(-1)}
	data, err := proto.Marshal(want)
	if err != nil {
		t.Fatalf("Marshal: %v", err)
	}
	got := &Message{}
	if err := proto.Unmarshal(data, got); err != nil {
		t.Fatalf("Unmarshal: %v", err)
	}
	if got.HeartbeatInterval == nil || got.GetHeartbeatInterval() != -1 {
		t.Errorf("HeartbeatInterval = %v, want -1 (present)", got.HeartbeatInterval)
	}
}

func TestMessage_DynatuneFieldsUnsetByDefault(t *testing.T) {
	m := &Message{Type: MsgHeartbeat.Enum(), To: proto.Uint64(2), From: proto.Uint64(1), Term: proto.Uint64(5)}
	data, err := proto.Marshal(m)
	if err != nil {
		t.Fatalf("Marshal: %v", err)
	}
	got := &Message{}
	if err := proto.Unmarshal(data, got); err != nil {
		t.Fatalf("Unmarshal: %v", err)
	}
	if got.Rtt != nil || got.SendTime != nil || got.SequenceId != nil || got.HeartbeatInterval != nil {
		t.Errorf("Dynatune fields must be nil when never set: %+v", got)
	}
	if got.GetRtt() != 0 || got.GetSendTime() != 0 || got.GetSequenceId() != 0 || got.GetHeartbeatInterval() != 0 {
		t.Errorf("getters must return the proto2 zero value when unset")
	}
}

// TestMessage_DynatuneDisabledModeWireIdentical is the byte-identity check:
// a heartbeat that sets only pre-existing fields (1-16, including HeirRaft's)
// must emit no wire tag for 17-20 -- a stock binary decoding these bytes
// could not tell Dynatune exists. Scans raw tags, as heirraft_wire_test.go does.
func TestMessage_DynatuneDisabledModeWireIdentical(t *testing.T) {
	m := &Message{
		Type:      MsgHeartbeat.Enum(),
		To:        proto.Uint64(2),
		From:      proto.Uint64(1),
		Term:      proto.Uint64(5),
		Commit:    proto.Uint64(9),
		Context:   []byte("ctx"),
		Stability: proto.Uint32(200),
		Heir:      proto.Uint64(3),
	}
	data, err := proto.Marshal(m)
	if err != nil {
		t.Fatalf("Marshal: %v", err)
	}
	for len(data) > 0 {
		num, typ, n := protowire.ConsumeTag(data)
		if n < 0 {
			t.Fatalf("ConsumeTag: %v", protowire.ParseError(n))
		}
		data = data[n:]
		if num >= 17 && num <= 20 {
			t.Fatalf("found wire tag for field %d, want none when Dynatune fields are unset", num)
		}
		fn := protowire.ConsumeFieldValue(num, typ, data)
		if fn < 0 {
			t.Fatalf("ConsumeFieldValue: %v", protowire.ParseError(fn))
		}
		data = data[fn:]
	}
}

// A zero value that is SET must still be sent (proto2 presence): sequence id 0
// is the first heartbeat of a term and must not vanish from the wire.
func TestMessage_DynatuneExplicitZeroIsPreservedOnTheWire(t *testing.T) {
	m := &Message{Type: MsgHeartbeat.Enum(), SequenceId: proto.Int64(0), Rtt: proto.Int64(0)}
	data, err := proto.Marshal(m)
	if err != nil {
		t.Fatalf("Marshal: %v", err)
	}
	got := &Message{}
	if err := proto.Unmarshal(data, got); err != nil {
		t.Fatalf("Unmarshal: %v", err)
	}
	if got.SequenceId == nil || got.Rtt == nil {
		t.Errorf("explicit zero lost: SequenceId=%v Rtt=%v", got.SequenceId, got.Rtt)
	}
}

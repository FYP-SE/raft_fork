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
)

// T4.2 (TASKS.md) -- wire compatibility tests for the new Message.stability
// (field 15) and Message.heir (field 16) additions. DESIGN.md requires that
// with HeirElection/HeirLogPriority/GracefulHandover all off, wire traffic is
// byte-identical to stock; these tests pin that, plus roundtrip correctness
// for when the fields are in use.

// TestMessage_HeirRaftFieldsRoundtrip checks basic marshal/unmarshal
// correctness for the new fields -- the "new sender, new receiver" case.
func TestMessage_HeirRaftFieldsRoundtrip(t *testing.T) {
	want := &Message{
		Type:      MsgHeartbeat.Enum(),
		To:        proto.Uint64(2),
		From:      proto.Uint64(1),
		Term:      proto.Uint64(5),
		Stability: proto.Uint32(200),
		Heir:      proto.Uint64(3),
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
	if got.GetStability() != 200 {
		t.Errorf("GetStability() = %d, want 200", got.GetStability())
	}
	if got.GetHeir() != 3 {
		t.Errorf("GetHeir() = %d, want 3", got.GetHeir())
	}
}

// TestMessage_HeirRaftFieldsUnsetByDefault checks that a Message which never
// touches Stability/Heir (the case for every message when the HeirRaft flags
// are off) marshals/unmarshals with both fields nil and their getters
// returning the proto2 zero value -- i.e. behaviourally invisible to code
// that doesn't know about them.
func TestMessage_HeirRaftFieldsUnsetByDefault(t *testing.T) {
	m := &Message{Type: MsgApp.Enum(), To: proto.Uint64(2), From: proto.Uint64(1), Term: proto.Uint64(5)}
	data, err := proto.Marshal(m)
	if err != nil {
		t.Fatalf("Marshal: %v", err)
	}
	got := &Message{}
	if err := proto.Unmarshal(data, got); err != nil {
		t.Fatalf("Unmarshal: %v", err)
	}
	if got.Stability != nil {
		t.Errorf("Stability = %v, want nil", got.Stability)
	}
	if got.Heir != nil {
		t.Errorf("Heir = %v, want nil", got.Heir)
	}
	if got.GetStability() != 0 || got.GetHeir() != 0 {
		t.Errorf("GetStability()/GetHeir() = %d/%d, want 0/0", got.GetStability(), got.GetHeir())
	}
}

// TestMessage_DisabledModeWireIdentical is the byte-identity check: a Message
// using only pre-existing fields (1-14), with Stability/Heir left unset, must
// serialize with no wire tag for field 15 or 16 at all -- meaning a stock
// (pre-HeirRaft) binary decoding these bytes would not observe any
// difference from before this change. We scan the raw wire tags rather than
// just checking Go-struct nil-ness, since that's what actually crosses the
// network.
func TestMessage_DisabledModeWireIdentical(t *testing.T) {
	m := &Message{
		Type:       MsgApp.Enum(),
		To:         proto.Uint64(2),
		From:       proto.Uint64(1),
		Term:       proto.Uint64(5),
		LogTerm:    proto.Uint64(4),
		Index:      proto.Uint64(10),
		Commit:     proto.Uint64(9),
		Vote:       proto.Uint64(1),
		Reject:     proto.Bool(true),
		RejectHint: proto.Uint64(3),
		Context:    []byte("ctx"),
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
		if num == 15 || num == 16 {
			t.Fatalf("found wire tag for field %d, want no tag emitted when Stability/Heir are unset", num)
		}
		fn := protowire.ConsumeFieldValue(num, typ, data)
		if fn < 0 {
			t.Fatalf("ConsumeFieldValue: %v", protowire.ParseError(fn))
		}
		data = data[fn:]
	}
}

// TestMessage_UnknownFieldForwardCompat documents proto2's unknown-field
// behaviour, which is what actually protects the old<->new binary case in
// this repo (there's no second Message schema to unmarshal-with-an-old-type
// in a single binary, so we simulate a "future" field the way this code will
// itself one day be the "old" side of a similar addition): bytes carrying an
// unrecognised field number decode without error, the value is preserved in
// the message's unknown-fields set, and it survives a remarshal unchanged.
// A real old (pre-T4.2) etcd/raft binary receiving fields 15/16 gets exactly
// this treatment -- decodes the rest of the message fine, silently drops
// stability/heir on the floor, and requires no protocol-level negotiation.
func TestMessage_UnknownFieldForwardCompat(t *testing.T) {
	known := &Message{Type: MsgApp.Enum(), To: proto.Uint64(2), From: proto.Uint64(1), Term: proto.Uint64(5)}
	data, err := proto.Marshal(known)
	if err != nil {
		t.Fatalf("Marshal: %v", err)
	}
	// Append a field number no current or planned Message field uses, to
	// stand in for "a future addition this binary doesn't know about yet".
	const futureField = 99
	data = protowire.AppendTag(data, futureField, protowire.VarintType)
	data = protowire.AppendVarint(data, 42)

	got := &Message{}
	if err := proto.Unmarshal(data, got); err != nil {
		t.Fatalf("Unmarshal with unknown field %d: %v", futureField, err)
	}
	if got.GetType() != MsgApp || got.GetTo() != 2 || got.GetFrom() != 1 || got.GetTerm() != 5 {
		t.Errorf("known fields corrupted by presence of unknown field: %+v", got)
	}

	remarshaled, err := proto.Marshal(got)
	if err != nil {
		t.Fatalf("remarshal: %v", err)
	}
	found := false
	rest := remarshaled
	for len(rest) > 0 {
		num, typ, n := protowire.ConsumeTag(rest)
		if n < 0 {
			t.Fatalf("ConsumeTag: %v", protowire.ParseError(n))
		}
		rest = rest[n:]
		if num == futureField {
			found = true
		}
		fn := protowire.ConsumeFieldValue(num, typ, rest)
		if fn < 0 {
			t.Fatalf("ConsumeFieldValue: %v", protowire.ParseError(fn))
		}
		rest = rest[fn:]
	}
	if !found {
		t.Errorf("unknown field %d dropped on remarshal, want preserved", futureField)
	}
}

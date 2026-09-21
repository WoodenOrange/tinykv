// Copyright 2015 The etcd Authors
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
	"bytes"
	"reflect"
	"testing"

	pb "github.com/pingcap-incubator/tinykv/proto/pkg/eraftpb"
)

type ignoreSizeHintMemStorage struct {
	*MemoryStorage
}

func (s *ignoreSizeHintMemStorage) Entries(lo, hi uint64, maxSize uint64) ([]pb.Entry, error) {
	return s.MemoryStorage.Entries(lo, hi)
}

// TestRawNodeProposeAndConfChange ensures that RawNode.Propose and RawNode.ProposeConfChange
// send the given proposal and ConfChange to the underlying raft.
func TestRawNodeProposeAndConfChange3A(t *testing.T) {
	s := NewMemoryStorage()
	var err error
	rawNode, err := NewRawNode(newTestConfig(1, []uint64{1}, 10, 1, s))
	if err != nil {
		t.Fatal(err)
	}
	rd := rawNode.Ready()
	s.Append(rd.Entries)
	rawNode.Advance(rd)

	if d := rawNode.Ready(); !IsEmptyHardState(d.HardState) || len(d.Entries) > 0 {
		t.Fatalf("expected empty hard state: %#v", d)
	}

	rawNode.Campaign()
	rd = rawNode.Ready()
	if rd.SoftState.Lead != rawNode.Raft.id {
		t.Fatalf("expected become leader")
	}

	// propose a command and a ConfChange.
	rawNode.Propose([]byte("somedata"))
	cc := pb.ConfChange{ChangeType: pb.ConfChangeType_AddNode, NodeId: 1}
	ccdata, err := cc.Marshal()
	if err != nil {
		t.Fatal(err)
	}
	rawNode.ProposeConfChange(cc)

	entries := rawNode.Raft.RaftLog.entries
	if l := len(entries); l < 2 {
		t.Fatalf("len(entries) = %d, want >= 2", l)
	} else {
		entries = entries[l-2:]
	}
	if !bytes.Equal(entries[0].Data, []byte("somedata")) {
		t.Errorf("entries[0].Data = %v, want %v", entries[0].Data, []byte("somedata"))
	}
	if entries[1].EntryType != pb.EntryType_EntryConfChange {
		t.Fatalf("type = %v, want %v", entries[1].EntryType, pb.EntryType_EntryConfChange)
	}
	if !bytes.Equal(entries[1].Data, ccdata) {
		t.Errorf("data = %v, want %v", entries[1].Data, ccdata)
	}
}

// +++
func TestRawNodeRequestReadIndex2B(t *testing.T) {
	storage := NewMemoryStorage()
	raftNode := newTestRaft(1, []uint64{1, 2, 3}, 10, 1, storage)
	raftNode.becomeCandidate()
	raftNode.becomeLeader()
	raftNode.RaftLog.committed = raftNode.RaftLog.LastIndex()
	raftNode.msgs = nil
	rawNode := &RawNode{
		Raft:          raftNode,
		readIndexAcks: make(map[uint64]*readIndexState),
	}

	if err := rawNode.RequestReadIndex(1); err != nil {
		t.Fatalf("RequestReadIndex returned error: %v", err)
	}
	ready := rawNode.Ready()
	if len(ready.Messages) != 2 {
		t.Fatalf("heartbeat count = %d, want 2", len(ready.Messages))
	}
	for _, message := range ready.Messages {
		if message.MsgType != pb.MessageType_MsgHeartbeat || message.Commit != readIndexContextBit|1 {
			t.Fatalf("unexpected read-index heartbeat: %+v", message)
		}
	}

	if err := rawNode.Step(pb.Message{
		MsgType: pb.MessageType_MsgHeartbeatResponse,
		From:    2,
		Term:    raftNode.Term,
		Index:   raftNode.RaftLog.LastIndex(),
		LogTerm: raftNode.Term,
		Commit:  readIndexContextBit | 1,
	}); err != nil {
		t.Fatalf("Step returned error: %v", err)
	}
	ready = rawNode.Ready()
	if len(ready.ReadStates) != 1 {
		t.Fatalf("read states count = %d, want 1", len(ready.ReadStates))
	}
	readState := ready.ReadStates[0]
	if readState.RequestCtx != 1 || readState.Index != raftNode.RaftLog.committed {
		t.Fatalf("unexpected read state: %+v", readState)
	}
}

// TestRawNodeProposeAddDuplicateNode ensures that two proposes to add the same node should
// not affect the later propose to add new node.
func TestRawNodeProposeAddDuplicateNode3A(t *testing.T) {
	s := NewMemoryStorage()
	rawNode, err := NewRawNode(newTestConfig(1, []uint64{1}, 10, 1, s))
	if err != nil {
		t.Fatal(err)
	}
	rd := rawNode.Ready()
	s.Append(rd.Entries)
	rawNode.Advance(rd)

	rawNode.Campaign()
	for {
		rd = rawNode.Ready()
		s.Append(rd.Entries)
		if rd.SoftState.Lead == rawNode.Raft.id {
			rawNode.Advance(rd)
			break
		}
		rawNode.Advance(rd)
	}

	proposeConfChangeAndApply := func(cc pb.ConfChange) {
		rawNode.ProposeConfChange(cc)
		rd = rawNode.Ready()
		s.Append(rd.Entries)
		for _, entry := range rd.CommittedEntries {
			if entry.EntryType == pb.EntryType_EntryConfChange {
				var cc pb.ConfChange
				cc.Unmarshal(entry.Data)
				rawNode.ApplyConfChange(cc)
			}
		}
		rawNode.Advance(rd)
	}

	cc1 := pb.ConfChange{ChangeType: pb.ConfChangeType_AddNode, NodeId: 1}
	ccdata1, err := cc1.Marshal()
	if err != nil {
		t.Fatal(err)
	}
	proposeConfChangeAndApply(cc1)

	// try to add the same node again
	proposeConfChangeAndApply(cc1)

	// the new node join should be ok
	cc2 := pb.ConfChange{ChangeType: pb.ConfChangeType_AddNode, NodeId: 2}
	ccdata2, err := cc2.Marshal()
	if err != nil {
		t.Fatal(err)
	}
	proposeConfChangeAndApply(cc2)

	lastIndex, err := s.LastIndex()
	if err != nil {
		t.Fatal(err)
	}

	// the last three entries should be: ConfChange cc1, cc1, cc2
	entries, err := s.Entries(lastIndex-2, lastIndex+1)
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 3 {
		t.Fatalf("len(entries) = %d, want %d", len(entries), 3)
	}
	if !bytes.Equal(entries[0].Data, ccdata1) {
		t.Errorf("entries[0].Data = %v, want %v", entries[0].Data, ccdata1)
	}
	if !bytes.Equal(entries[2].Data, ccdata2) {
		t.Errorf("entries[2].Data = %v, want %v", entries[2].Data, ccdata2)
	}
}

// TestRawNodeStart ensures that a node can be started correctly, and can accept and commit
// proposals.
func TestRawNodeStart2AC(t *testing.T) {
	storage := NewMemoryStorage()
	rawNode, err := NewRawNode(newTestConfig(1, []uint64{1}, 10, 1, storage))
	if err != nil {
		t.Fatal(err)
	}
	rawNode.Campaign()
	rd := rawNode.Ready()
	storage.Append(rd.Entries)
	rawNode.Advance(rd)

	rawNode.Propose([]byte("foo"))
	rd = rawNode.Ready()
	if el := len(rd.Entries); el != len(rd.CommittedEntries) || el != 1 {
		t.Errorf("got len(Entries): %+v, len(CommittedEntries): %+v, want %+v", el, len(rd.CommittedEntries), 1)
	}
	if !reflect.DeepEqual(rd.Entries[0].Data, rd.CommittedEntries[0].Data) || !reflect.DeepEqual(rd.Entries[0].Data, []byte("foo")) {
		t.Errorf("got %+v %+v , want %+v", rd.Entries[0].Data, rd.CommittedEntries[0].Data, []byte("foo"))
	}
	storage.Append(rd.Entries)
	rawNode.Advance(rd)

	if rawNode.HasReady() {
		t.Errorf("unexpected Ready: %+v", rawNode.Ready())
	}
}

func TestRawNodeRestart2AC(t *testing.T) {
	entries := []pb.Entry{
		{Term: 1, Index: 1},
		{Term: 1, Index: 2, Data: []byte("foo")},
	}
	st := pb.HardState{Term: 1, Commit: 1}

	want := Ready{
		Entries: []pb.Entry{},
		// commit up to commit index in st
		CommittedEntries: entries[:st.Commit],
	}

	storage := NewMemoryStorage()
	storage.SetHardState(st)
	storage.Append(entries)
	rawNode, err := NewRawNode(newTestConfig(1, nil, 10, 1, storage))
	if err != nil {
		t.Fatal(err)
	}
	rd := rawNode.Ready()
	if !reflect.DeepEqual(rd, want) {
		t.Errorf("g = %#v,\n             w   %#v", rd, want)
	}
	rawNode.Advance(rd)
	if rawNode.HasReady() {
		t.Errorf("unexpected Ready: %+v", rawNode.Ready())
	}
}

func TestRawNodeRestartFromSnapshot2C(t *testing.T) {
	snap := pb.Snapshot{
		Metadata: &pb.SnapshotMetadata{
			ConfState: &pb.ConfState{Nodes: []uint64{1, 2}},
			Index:     2,
			Term:      1,
		},
	}
	entries := []pb.Entry{
		{Term: 1, Index: 3, Data: []byte("foo")},
	}
	st := pb.HardState{Term: 1, Commit: 3}

	want := Ready{
		Entries: []pb.Entry{},
		// commit up to commit index in st
		CommittedEntries: entries,
	}

	s := NewMemoryStorage()
	s.SetHardState(st)
	s.ApplySnapshot(snap)
	s.Append(entries)
	rawNode, err := NewRawNode(newTestConfig(1, nil, 10, 1, s))
	if err != nil {
		t.Fatal(err)
	}
	if rd := rawNode.Ready(); !reflect.DeepEqual(rd, want) {
		t.Errorf("g = %#v,\n             w   %#v", rd, want)
	} else {
		rawNode.Advance(rd)
	}
	if rawNode.HasReady() {
		t.Errorf("unexpected Ready: %+v", rawNode.HasReady())
	}
}

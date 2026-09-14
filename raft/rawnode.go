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
	"errors"

	pb "github.com/pingcap-incubator/tinykv/proto/pkg/eraftpb"
)

// ErrStepLocalMsg is returned when try to step a local raft message
var ErrStepLocalMsg = errors.New("raft: cannot step raft local message")

// ErrStepPeerNotFound is returned when try to step a response message
// but there is no peer found in raft.Prs for that node.
var ErrStepPeerNotFound = errors.New("raft: cannot step as peer not found")

// SoftState provides state that is volatile and does not need to be persisted to the WAL.
type SoftState struct {
	Lead      uint64
	RaftState StateType
}

// Ready encapsulates the entries and messages that are ready to read,
// be saved to stable storage, committed or sent to other peers.
// All fields in Ready are read-only.
type Ready struct {
	// The current volatile state of a Node.
	// SoftState will be nil if there is no update.
	// It is not required to consume or store SoftState.
	*SoftState

	// The current state of a Node to be saved to stable storage BEFORE
	// Messages are sent.
	// HardState will be equal to empty state if there is no update.
	pb.HardState

	// Entries specifies entries to be saved to stable storage BEFORE
	// Messages are sent.
	Entries []pb.Entry

	// Snapshot specifies the snapshot to be saved to stable storage.
	Snapshot pb.Snapshot

	// CommittedEntries specifies entries to be committed to a
	// store/state-machine. These have previously been committed to stable
	// store.
	CommittedEntries []pb.Entry

	// Messages specifies outbound messages to be sent AFTER Entries are
	// committed to stable storage.
	// If it contains a MessageType_MsgSnapshot message, the application MUST report back to raft
	// when the snapshot has been received or has failed by calling ReportSnapshot.
	Messages []pb.Message

	// ReadStates contains read indexes confirmed by a quorum.
	ReadStates []ReadState
}

// +++
// ReadState：返回给上层的读确认凭证
type ReadState struct {
	Index      uint64 //已committed，表示该索引之前的所有日志都已被多数派确认,状态机应该应用到该索引的日志
	RequestCtx uint64 //存readIndex，只读请求分配的序号
}

// RawNode is a wrapper of Raft.
type RawNode struct {
	Raft *Raft
	// Your Data Here (2A).
	//上次返回的状态，用于检测变化
	prevHardState pb.HardState
	prevLead      uint64
	prevState     StateType
	//只读请求的信息
	readIndexAcks map[uint64]map[uint64]struct{} //readIndex->已确认节点->struct{}，用于统计多数派确认
	readStates    []ReadState                    //待上传的已确认读状态
}

const readIndexContextBit uint64 = 1 << 63 //最高位标记ReadIndex心跳

// NewRawNode returns a new RawNode given configuration and a list of raft peers.
func NewRawNode(config *Config) (*RawNode, error) {
	// Your Code Here (2A).
	if err := config.validate(); err != nil {
		return nil, err
	}
	r := newRaft(config)
	hs, _, err := config.Storage.InitialState()
	if err != nil {
		return nil, err
	}
	return &RawNode{
		Raft:          r,
		prevHardState: hs,
		prevLead:      None,
		prevState:     StateFollower,
		readIndexAcks: make(map[uint64]map[uint64]struct{}),
	}, nil
}

// +++
// RequestReadIndex发起只读索引确认
func (rn *RawNode) RequestReadIndex(readIndex uint64) error {
	//必须为 Leader，且readIndex有效
	if readIndex == 0 || readIndex&readIndexContextBit != 0 || rn.Raft.State != StateLeader {
		return ErrProposalDropped
	}
	//当前commit索引的term要等于当前term，避免旧日志
	term, err := rn.Raft.RaftLog.Term(rn.Raft.RaftLog.committed)
	if err != nil || term != rn.Raft.Term {
		return ErrProposalDropped
	}

	//初始化确认集合，自己确认
	acks := map[uint64]struct{}{rn.Raft.id: {}}
	rn.readIndexAcks[readIndex] = acks
	//若自己已满足多数派，立即完成
	if len(acks)*2 > len(rn.Raft.Prs) {
		rn.completeReadIndex(readIndex)
		return nil
	}
	//向所有其他节点发送带readIndex的心跳
	for id := range rn.Raft.Prs {
		if id != rn.Raft.id {
			rn.Raft.sendHeartbeatWithCommit(id, readIndexContextBit|readIndex)
		}
	}
	return nil
}

// +++
// 完成读确认：生成ReadState并加入待上抛队列
func (rn *RawNode) completeReadIndex(readIndex uint64) {
	delete(rn.readIndexAcks, readIndex)
	rn.readStates = append(rn.readStates, ReadState{Index: rn.Raft.RaftLog.committed, RequestCtx: readIndex})
}

// +++
// 处理心跳响应中的ReadIndex确认
func (rn *RawNode) handleReadIndexResponse(m pb.Message) {
	//是Leader，是心跳响应，且Commit高位被置位（是readIndex）
	if m.MsgType != pb.MessageType_MsgHeartbeatResponse || m.Commit&readIndexContextBit == 0 || rn.Raft.State != StateLeader {
		return
	}
	//处理并统计对应readIndex的回复
	readIndex := m.Commit &^ readIndexContextBit
	acks, ok := rn.readIndexAcks[readIndex]
	if !ok {
		return
	}
	acks[m.From] = struct{}{}           //记录该节点已确认
	if len(acks)*2 > len(rn.Raft.Prs) { //多数派已确认
		rn.completeReadIndex(readIndex)
	}
}

// Tick advances the internal logical clock by a single tick.
func (rn *RawNode) Tick() {
	rn.Raft.tick()
}

// Campaign causes this RawNode to transition to candidate state.
func (rn *RawNode) Campaign() error {
	return rn.Raft.Step(pb.Message{
		MsgType: pb.MessageType_MsgHup,
	})
}

// Propose proposes data be appended to the raft log.
func (rn *RawNode) Propose(data []byte) error {
	ent := pb.Entry{Data: data}
	return rn.Raft.Step(pb.Message{
		MsgType: pb.MessageType_MsgPropose,
		From:    rn.Raft.id,
		Entries: []*pb.Entry{&ent}})
}

// ProposeConfChange proposes a config change.
func (rn *RawNode) ProposeConfChange(cc pb.ConfChange) error {
	data, err := cc.Marshal()
	if err != nil {
		return err
	}
	ent := pb.Entry{EntryType: pb.EntryType_EntryConfChange, Data: data}
	return rn.Raft.Step(pb.Message{
		MsgType: pb.MessageType_MsgPropose,
		Entries: []*pb.Entry{&ent},
	})
}

// ApplyConfChange applies a config change to the local node.
func (rn *RawNode) ApplyConfChange(cc pb.ConfChange) *pb.ConfState {
	if cc.NodeId == None {
		return &pb.ConfState{Nodes: nodes(rn.Raft)}
	}
	switch cc.ChangeType {
	case pb.ConfChangeType_AddNode:
		rn.Raft.addNode(cc.NodeId)
	case pb.ConfChangeType_RemoveNode:
		rn.Raft.removeNode(cc.NodeId)
	default:
		panic("unexpected conf type")
	}
	return &pb.ConfState{Nodes: nodes(rn.Raft)}
}

// Step advances the state machine using the given message.
func (rn *RawNode) Step(m pb.Message) error {
	// ignore unexpected local messages receiving over network
	if IsLocalMsg(m.MsgType) {
		return ErrStepLocalMsg
	}
	if pr := rn.Raft.Prs[m.From]; pr != nil || !IsResponseMsg(m.MsgType) {
		err := rn.Raft.Step(m)
		if err == nil {
			rn.handleReadIndexResponse(m) //对心跳响应额外处理，尝试走只读请求的确认逻辑
		}
		return err
	}
	return ErrStepPeerNotFound
}

// Ready returns the current point-in-time state of this RawNode.
func (rn *RawNode) Ready() Ready {
	// Your Code Here (2A).
	rd := Ready{}

	//SoftState切换才填
	if rn.Raft.Lead != rn.prevLead || rn.Raft.State != rn.prevState {
		rd.SoftState = &SoftState{
			Lead:      rn.Raft.Lead,
			RaftState: rn.Raft.State,
		}
	}

	//HardState
	hs := pb.HardState{
		Term:   rn.Raft.Term,
		Vote:   rn.Raft.Vote,
		Commit: rn.Raft.RaftLog.committed,
	}
	if !isHardStateEqual(hs, rn.prevHardState) {
		rd.HardState = hs
	}

	//Entries
	rd.Entries = rn.Raft.RaftLog.unstableEntries()

	//Snapshot
	if rn.Raft.RaftLog.pendingSnapshot != nil {
		rd.Snapshot = *rn.Raft.RaftLog.pendingSnapshot
	}

	//CommittedEntries
	rd.CommittedEntries = rn.Raft.RaftLog.nextEnts()

	//Messages
	if len(rn.Raft.msgs) > 0 {
		rd.Messages = rn.Raft.msgs
	} else {
		rd.Messages = nil
	}
	rn.Raft.msgs = make([]pb.Message, 0) //清空

	//ReadStates
	if len(rn.readStates) > 0 {
		rd.ReadStates = rn.readStates
		rn.readStates = nil
	}

	return rd
}

// HasReady called when RawNode user need to check if any Ready pending.
func (rn *RawNode) HasReady() bool {
	// Your Code Here (2A).
	//SoftState
	if rn.Raft.Lead != rn.prevLead || rn.Raft.State != rn.prevState {
		return true
	}
	//HardState
	hs := pb.HardState{
		Term:   rn.Raft.Term,
		Vote:   rn.Raft.Vote,
		Commit: rn.Raft.RaftLog.committed,
	}
	if !isHardStateEqual(hs, rn.prevHardState) {
		return true
	}
	//未持久化条目
	if len(rn.Raft.RaftLog.unstableEntries()) > 0 {
		return true
	}
	//待应用条目
	if len(rn.Raft.RaftLog.nextEnts()) > 0 {
		return true
	}
	//快照
	if rn.Raft.RaftLog.pendingSnapshot != nil {
		return true
	}
	//待发送消息
	if len(rn.Raft.msgs) > 0 {
		return true
	}

	//待处理只读请求
	if len(rn.readStates) > 0 {
		return true
	}
	return false
}

// Advance notifies the RawNode that the application has applied and saved progress in the
// last Ready results.
func (rn *RawNode) Advance(rd Ready) {
	// Your Code Here (2A).
	//更新preSoftState
	if rd.SoftState != nil {
		rn.prevLead = rd.SoftState.Lead
		rn.prevState = rd.SoftState.RaftState
	}

	//更新preHardState
	if !IsEmptyHardState(rd.HardState) {
		rn.prevHardState = rd.HardState
	}

	//更新stabled
	if len(rd.Entries) > 0 {
		last := rd.Entries[len(rd.Entries)-1].Index
		if last > rn.Raft.RaftLog.stabled {
			rn.Raft.RaftLog.stabled = last
		}
	}

	//更新applied
	if len(rd.CommittedEntries) > 0 {
		last := rd.CommittedEntries[len(rd.CommittedEntries)-1].Index
		if last > rn.Raft.RaftLog.applied {
			rn.Raft.RaftLog.applied = last
		}
	}

	//快照处理（2C）
	if !IsEmptySnap(&rd.Snapshot) {
		rn.Raft.RaftLog.pendingSnapshot = nil
		rn.Raft.RaftLog.stabled = rd.Snapshot.Metadata.Index
		if rd.Snapshot.Metadata.Index > rn.Raft.RaftLog.applied {
			rn.Raft.RaftLog.applied = rd.Snapshot.Metadata.Index
		}
	}

	//尝试压缩内存中已被storage compact的日志
	rn.Raft.RaftLog.maybeCompact()

}

// GetProgress return the Progress of this node and its peers, if this
// node is leader.
func (rn *RawNode) GetProgress() map[uint64]Progress {
	prs := make(map[uint64]Progress)
	if rn.Raft.State == StateLeader {
		for id, p := range rn.Raft.Prs {
			prs[id] = *p
		}
	}
	return prs
}

// TransferLeader tries to transfer leadership to the given transferee.
func (rn *RawNode) TransferLeader(transferee uint64) {
	_ = rn.Raft.Step(pb.Message{MsgType: pb.MessageType_MsgTransferLeader, From: transferee})
}

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
	"math/rand"

	pb "github.com/pingcap-incubator/tinykv/proto/pkg/eraftpb"
)

// None is a placeholder node ID used when there is no leader.
const None uint64 = 0

// StateType represents the role of a node in a cluster.
type StateType uint64

const (
	StateFollower StateType = iota
	StateCandidate
	StateLeader
)

var stmap = [...]string{
	"StateFollower",
	"StateCandidate",
	"StateLeader",
}

func (st StateType) String() string {
	return stmap[uint64(st)]
}

// ErrProposalDropped is returned when the proposal is ignored by some cases,
// so that the proposer can be notified and fail fast.
var ErrProposalDropped = errors.New("raft proposal dropped")

// Config contains the parameters to start a raft.
type Config struct {
	// ID is the identity of the local raft. ID cannot be 0.
	ID uint64

	// peers contains the IDs of all nodes (including self) in the raft cluster. It
	// should only be set when starting a new raft cluster. Restarting raft from
	// previous configuration will panic if peers is set. peer is private and only
	// used for testing right now.
	peers []uint64

	// ElectionTick is the number of Node.Tick invocations that must pass between
	// elections. That is, if a follower does not receive any message from the
	// leader of current term before ElectionTick has elapsed, it will become
	// candidate and start an election. ElectionTick must be greater than
	// HeartbeatTick. We suggest ElectionTick = 10 * HeartbeatTick to avoid
	// unnecessary leader switching.
	ElectionTick int
	// HeartbeatTick is the number of Node.Tick invocations that must pass between
	// heartbeats. That is, a leader sends heartbeat messages to maintain its
	// leadership every HeartbeatTick ticks.
	HeartbeatTick int

	// Storage is the storage for raft. raft generates entries and states to be
	// stored in storage. raft reads the persisted entries and states out of
	// Storage when it needs. raft reads out the previous state and configuration
	// out of storage when restarting.
	Storage Storage
	// Applied is the last applied index. It should only be set when restarting
	// raft. raft will not return entries to the application smaller or equal to
	// Applied. If Applied is unset when restarting, raft might return previous
	// applied entries. This is a very application dependent configuration.
	Applied uint64
}

func (c *Config) validate() error {
	if c.ID == None {
		return errors.New("cannot use none as id")
	}

	if c.HeartbeatTick <= 0 {
		return errors.New("heartbeat tick must be greater than 0")
	}

	if c.ElectionTick <= c.HeartbeatTick {
		return errors.New("election tick must be greater than heartbeat tick")
	}

	if c.Storage == nil {
		return errors.New("storage cannot be nil")
	}

	return nil
}

// Progress represents a follower’s progress in the view of the leader. Leader maintains
// progresses of all followers, and sends entries to the follower based on its progress.
type Progress struct {
	Match, Next uint64
	//+++
	LastSent uint64 // 最后一次发送的日志索引，用于成功响应时更新 Match
}

type Raft struct {
	id uint64

	Term uint64
	Vote uint64

	// the log
	RaftLog *RaftLog

	// log replication progress of each peers
	Prs map[uint64]*Progress

	// this peer's role
	State StateType

	// votes records
	votes map[uint64]bool

	// msgs need to send
	msgs []pb.Message

	// the leader id
	Lead uint64

	// heartbeat interval, should send
	heartbeatTimeout int
	// baseline of election interval
	electionTimeout int
	// number of ticks since it reached last heartbeatTimeout.
	// only leader keeps heartbeatElapsed.
	heartbeatElapsed int
	// Ticks since it reached last electionTimeout when it is leader or candidate.
	// Number of ticks since it reached last electionTimeout or received a
	// valid message from current leader when it is a follower.
	electionElapsed int

	// leadTransferee is id of the leader transfer target when its value is not zero.
	// Follow the procedure defined in section 3.10 of Raft phd thesis.
	// (https://web.stanford.edu/~ouster/cgi-bin/papers/OngaroPhD.pdf)
	// (Used in 3A leader transfer)
	leadTransferee uint64

	// Only one conf change may be pending (in the log, but not yet
	// applied) at a time. This is enforced via PendingConfIndex, which
	// is set to a value >= the log index of the latest pending
	// configuration change (if any). Config changes are only allowed to
	// be proposed if the leader's applied index is greater than this
	// value.
	// (Used in 3A conf change)
	PendingConfIndex uint64
}

// newRaft return a raft peer with the given config
func newRaft(c *Config) *Raft {
	if err := c.validate(); err != nil {
		panic(err.Error())
	}
	// Your Code Here (2A).
	//初始化
	hardState, confState, err := c.Storage.InitialState()
	if err != nil {
		panic(err)
	}
	raftLog := newLog(c.Storage)
	raftLog.applied = c.Applied
	raftLog.committed = hardState.Commit

	//填充Raft
	r := &Raft{
		id:               c.ID,
		Term:             hardState.Term,
		Vote:             hardState.Vote,
		RaftLog:          raftLog,
		Prs:              make(map[uint64]*Progress),
		State:            StateFollower,
		votes:            make(map[uint64]bool),
		msgs:             make([]pb.Message, 0),
		Lead:             None,
		heartbeatTimeout: c.HeartbeatTick,
		electionTimeout:  c.ElectionTick,
		heartbeatElapsed: 0,
		electionElapsed:  0,
	}

	//集群节点进度
	if len(c.peers) > 0 {
		//有peers，新建集群
		for _, peer := range c.peers {
			r.Prs[peer] = &Progress{Next: raftLog.LastIndex() + 1, Match: 0, LastSent: 0}
		}
	} else {
		//没有，是重启节点
		for _, node := range confState.Nodes {
			r.Prs[node] = &Progress{Next: raftLog.LastIndex() + 1, Match: 0, LastSent: 0}
		}
	}
	//确保自身节点存在于进度映射，不存在则补充创建
	if _, ok := r.Prs[r.id]; !ok {
		r.Prs[r.id] = &Progress{Next: raftLog.LastIndex() + 1, Match: 0, LastSent: 0}
	}

	//随机化选举计时
	r.electionElapsed = -(1 + rand.Intn(r.electionTimeout-1))

	return r
}

// sendAppend sends an append RPC with new entries (if any) and the
// current commit index to the given peer. Returns true if a message was sent.
func (r *Raft) sendAppend(to uint64) bool {
	// Your Code Here (2A).
	pr, ok := r.Prs[to]
	if !ok {
		return false
	}

	lastIdx := r.RaftLog.LastIndex()
	prevIdx := pr.Next - 1

	//获取prevIdx的任期
	prevTerm, err := r.RaftLog.Term(prevIdx)
	if err != nil {
		return false
	}

	//构建Append消息
	m := pb.Message{
		MsgType: pb.MessageType_MsgAppend,
		From:    r.id,
		To:      to,
		Term:    r.Term,
		Index:   prevIdx,
		LogTerm: prevTerm,
		Commit:  r.RaftLog.committed,
	}

	//若没有新条目，发送空的Append
	if pr.Next > lastIdx {
		m.Entries = nil
		r.msgs = append(r.msgs, m)
		return true
	}

	//获取从pr.Next到lastIdx的条目
	entries := r.RaftLog.entries
	if len(entries) == 0 {
		return false
	}
	offset := entries[0].Index
	lo := pr.Next
	hi := lastIdx + 1
	if lo < offset {
		//需要快照
		return false
	}
	startIdx := lo - offset
	endIdx := hi - offset
	if startIdx > uint64(len(entries)) || endIdx > uint64(len(entries)) {
		return false
	}
	//转换为指针切片
	m.Entries = entriesToPtr(entries[startIdx:endIdx])
	r.msgs = append(r.msgs, m)

	//记录发送的最后索引
	pr.LastSent = lastIdx
	return true
}

// sendHeartbeat sends a heartbeat RPC to the given peer.
func (r *Raft) sendHeartbeat(to uint64) {
	// Your Code Here (2A).
	//填充加入即可
	m := pb.Message{
		MsgType: pb.MessageType_MsgHeartbeat,
		From:    r.id,
		To:      to,
		Term:    r.Term,
		Commit:  r.RaftLog.committed,
	}
	r.msgs = append(r.msgs, m)
}

// tick advances the internal logical clock by a single tick.
func (r *Raft) tick() {
	// Your Code Here (2A).
	r.electionElapsed++
	if r.State == StateLeader {
		//是leader
		r.heartbeatElapsed++
		//广播心跳
		if r.heartbeatElapsed >= r.heartbeatTimeout {
			r.heartbeatElapsed = 0
			//发给所有
			for id := range r.Prs {
				if id != r.id {
					r.sendHeartbeat(id)
				}
			}
		}
	} else {
		//Follower或Candidate，超时选举
		if r.electionElapsed >= r.electionTimeout {
			//重置随机超时
			r.electionElapsed = -(1 + rand.Intn(r.electionTimeout-1))
			_ = r.Step(pb.Message{MsgType: pb.MessageType_MsgHup})
		}
	}
}

// becomeFollower transform this peer's state to Follower
func (r *Raft) becomeFollower(term uint64, lead uint64) {
	// Your Code Here (2A).
	if term > r.Term {
		r.Term = term
	}
	r.State = StateFollower
	r.Lead = lead
	r.Vote = None
	r.votes = make(map[uint64]bool)
	//随机选举超时[electionTimeout, 2*electionTimeout-1] ticks
	r.electionElapsed = -(1 + rand.Intn(r.electionTimeout-1))
}

// becomeCandidate transform this peer's state to candidate
func (r *Raft) becomeCandidate() {
	// Your Code Here (2A).
	r.Term++
	r.Vote = r.id
	r.State = StateCandidate
	r.Lead = None
	r.votes = make(map[uint64]bool)
	r.votes[r.id] = true
	//随机选举超时[electionTimeout, 2*electionTimeout-1] ticks
	r.electionElapsed = -(1 + rand.Intn(r.electionTimeout-1))
}

// becomeLeader transform this peer's state to leader
func (r *Raft) becomeLeader() {
	// Your Code Here (2A).
	// NOTE: Leader should propose a noop entry on its term
	r.State = StateLeader
	r.Lead = r.id
	r.votes = make(map[uint64]bool)
	//重置节点日志同步进度
	lastIdx := r.RaftLog.LastIndex()
	for id := range r.Prs {
		r.Prs[id] = &Progress{Next: lastIdx + 1, Match: 0, LastSent: 0}
	}
	//noop日志
	noop := pb.Entry{
		Term:  r.Term,
		Index: lastIdx + 1,
		Data:  nil,
	}
	r.RaftLog.entries = append(r.RaftLog.entries, noop)
	//同步空日志
	for id := range r.Prs {
		if id != r.id {
			r.sendAppend(id)
		}
	}
	//重置心跳
	r.heartbeatElapsed = 0
}

// Step the entrance of handle message, see `MessageType`
// on `eraftpb.proto` for what msgs should be handled
func (r *Raft) Step(m pb.Message) error {
	// Your Code Here (2A).
	//本地消息不检查任期
	if IsLocalMsg(m.MsgType) {
		switch r.State {
		case StateFollower:
			return r.stepFollower(m)
		case StateCandidate:
			return r.stepCandidate(m)
		case StateLeader:
			return r.stepLeader(m)
		}
		return nil
	}
	//切换任期
	if m.Term > r.Term {
		r.becomeFollower(m.Term, None)
	}
	//忽略任期更小的消息
	if m.Term < r.Term {
		return nil
	}
	switch r.State {
	case StateFollower:
		return r.stepFollower(m)
	case StateCandidate:
		return r.stepCandidate(m)
	case StateLeader:
		return r.stepLeader(m)
	}
	return nil
}

// +++
// Follower状态处理消息：选举，投票，接收心跳，日志同步，快照同步
func (r *Raft) stepFollower(m pb.Message) error {
	switch m.MsgType {
	case pb.MessageType_MsgHup:
		r.campaign()
	case pb.MessageType_MsgRequestVote:
		r.handleRequestVote(m)
	case pb.MessageType_MsgHeartbeat:
		r.handleHeartbeat(m)
	case pb.MessageType_MsgAppend:
		r.handleAppendEntries(m)
	case pb.MessageType_MsgSnapshot:
		r.handleSnapshot(m)
	}
	return nil
}

// +++
// Candidate状态处理消息：选举，投票，被投票，接收心跳，日志
func (r *Raft) stepCandidate(m pb.Message) error {
	switch m.MsgType {
	case pb.MessageType_MsgHup:
		r.campaign()
	case pb.MessageType_MsgRequestVote:
		r.handleRequestVote(m)
	case pb.MessageType_MsgRequestVoteResponse:
		r.handleRequestVoteResponse(m)
	case pb.MessageType_MsgHeartbeat:
		r.handleHeartbeat(m)
	case pb.MessageType_MsgAppend:
		r.handleAppendEntries(m)
	}
	return nil
}

// +++
// Leader状态处理消息：强制发送心跳，拒绝投票，接收心跳，接收心跳回执
func (r *Raft) stepLeader(m pb.Message) error {
	switch m.MsgType {
	case pb.MessageType_MsgBeat:
		//手动触发心跳，向所有其他节点发送心跳
		for id := range r.Prs {
			if id != r.id {
				r.sendHeartbeat(id)
			}
		}
	case pb.MessageType_MsgRequestVote:
		resp := pb.Message{
			MsgType: pb.MessageType_MsgRequestVoteResponse,
			To:      m.From,
			From:    r.id,
			Term:    r.Term,
			Reject:  true,
		}
		r.msgs = append(r.msgs, resp)
	case pb.MessageType_MsgHeartbeat:
		//
	case pb.MessageType_MsgHeartbeatResponse:
		//2A不处理
	case pb.MessageType_MsgAppendResponse:
		r.handleAppendResponse(m)
	case pb.MessageType_MsgPropose:
		r.handlePropose(m)
	}
	return nil
}

// +++
// 发起一轮选举
func (r *Raft) campaign() {
	//切换状态
	r.becomeCandidate()
	//获取本地最新日志
	lastIdx := r.RaftLog.LastIndex()
	lastTerm, _ := r.RaftLog.Term(lastIdx)
	//发起投票请求
	for id := range r.Prs {
		if id == r.id {
			continue
		}
		m := pb.Message{
			MsgType: pb.MessageType_MsgRequestVote,
			From:    r.id,
			To:      id,
			Term:    r.Term,
			LogTerm: lastTerm,
			Index:   lastIdx,
		}
		r.msgs = append(r.msgs, m)
	}
	r.checkVotes() // 立即检查自身票数
}

// +++
// 检查是否当选
func (r *Raft) checkVotes() {
	if r.State != StateCandidate {
		return
	}
	//统计票
	granted := 0
	rejected := 0
	for _, v := range r.votes {
		if v {
			granted++
		} else {
			rejected++
		}
	}
	quorum := len(r.Prs)/2 + 1
	if granted >= quorum {
		r.becomeLeader()
	} else if rejected >= quorum {
		//拒绝票过半，直接退回Follower
		r.becomeFollower(r.Term, None)
	}
}

// +++
// 处理其他节点发起的投票请求
func (r *Raft) handleRequestVote(m pb.Message) {
	//已投，直接拒绝
	if r.Vote != None && r.Vote != m.From {
		resp := pb.Message{
			MsgType: pb.MessageType_MsgRequestVoteResponse,
			To:      m.From,
			From:    r.id,
			Term:    r.Term,
			Reject:  true,
		}
		r.msgs = append(r.msgs, resp)
		return
	}
	//安全性选举限制：比较日志
	lastIdx := r.RaftLog.LastIndex()
	lastTerm, _ := r.RaftLog.Term(lastIdx)
	if m.LogTerm < lastTerm || (m.LogTerm == lastTerm && m.Index < lastIdx) {
		//发起者任期更小或任期相等但日志编号更小，拒绝
		resp := pb.Message{
			MsgType: pb.MessageType_MsgRequestVoteResponse,
			To:      m.From,
			From:    r.id,
			Term:    r.Term,
			Reject:  true,
		}
		r.msgs = append(r.msgs, resp)
		return
	}
	//赞成
	r.Vote = m.From
	r.electionElapsed = 0 //选举超时时间重置
	resp := pb.Message{
		MsgType: pb.MessageType_MsgRequestVoteResponse,
		To:      m.From,
		From:    r.id,
		Term:    r.Term,
		Reject:  false,
	}
	r.msgs = append(r.msgs, resp)
}

// +++
// 处理其他节点返回的投票结果
func (r *Raft) handleRequestVoteResponse(m pb.Message) {
	//已不是candidate，忽略
	if r.State != StateCandidate {
		return
	}
	// 记录赞成或反对，反对也要记录
	r.votes[m.From] = !m.Reject
	r.checkVotes() //验票
}

// +++
func (r *Raft) handleAppendResponse(m pb.Message) {
	// 仅当状态为 Leader 且收到的是对等节点的响应
	if r.State != StateLeader {
		return
	}
	pr, ok := r.Prs[m.From]
	if !ok {
		return
	}

	if m.Reject {
		// 日志不一致，回退 Next（简单递减）
		if pr.Next > 1 {
			pr.Next--
		} else {
			pr.Next = 1
		}
		// 尝试重新发送
		r.sendAppend(m.From)
		return
	}

	// 成功响应
	// 更新 Match 为之前发送的最后索引
	if pr.LastSent > pr.Match {
		pr.Match = pr.LastSent
	}
	pr.Next = pr.Match + 1

	// 尝试更新 commit 索引
	r.maybeUpdateCommit()
}

// +++
// maybeUpdateCommit尝试推进commit索引(仅leader)
func (r *Raft) maybeUpdateCommit() {
	if r.State != StateLeader {
		return
	}
	oldCommit := r.RaftLog.committed // 更新前的commit，用于比较是否有新的commit
	lastIdx := r.RaftLog.LastIndex()
	//从当前committed+1开始，向上寻找能获得多数同意的最大索引
	for idx := r.RaftLog.committed + 1; idx <= lastIdx; idx++ {
		//检查该索引的任期是否等于当前任期（Raft 规则）
		term, err := r.RaftLog.Term(idx)
		if err != nil || term != r.Term {
			continue
		}
		//统计Match>=idx的节点数（包括自己）
		count := 1
		for id, pr := range r.Prs {
			if id == r.id {
				continue
			}
			if pr.Match >= idx {
				count++
			}
		}
		if count*2 > len(r.Prs) {
			//多数同意，推进commit
			r.RaftLog.committed = idx
		} else {
			//更大的必不满足
			break
		}
	}
	//如果有新commit，立刻广播同步
	if r.RaftLog.committed > oldCommit {
		for id := range r.Prs {
			if id != r.id {
				r.sendAppend(id)
			}
		}
	}
}

// handleAppendEntries handle AppendEntries RPC request
func (r *Raft) handleAppendEntries(m pb.Message) {
	// Your Code Here (2A).
	//若消息任期小于当前任期，拒绝
	if m.Term < r.Term {
		resp := pb.Message{
			MsgType: pb.MessageType_MsgAppendResponse,
			To:      m.From,
			From:    r.id,
			Term:    r.Term,
			Reject:  true,
		}
		r.msgs = append(r.msgs, resp)
		return
	}

	//消息任期 >= 当前任期，转为 Follower 并更新任期和 Leader
	r.becomeFollower(m.Term, m.From)
	r.electionElapsed = 0 //重置选举超时计时器

	//检查prevLogIndex和prevLogTerm
	prevIdx := m.Index
	prevTerm, err := r.RaftLog.Term(prevIdx)
	if err != nil || prevTerm != m.LogTerm {
		//日志不一致，拒绝
		resp := pb.Message{
			MsgType: pb.MessageType_MsgAppendResponse,
			To:      m.From,
			From:    r.id,
			Term:    r.Term,
			Reject:  true,
		}
		r.msgs = append(r.msgs, resp)
		return
	}

	//处理追加条目
	if len(m.Entries) > 0 {
		entries := ptrsToEntries(m.Entries)
		//追加到日志，处理冲突和截断
		r.RaftLog.entries = appendEntriesToLog(r.RaftLog.entries, entries)
	}

	//更新commit索引
	if m.Commit > r.RaftLog.committed {
		lastIdx := r.RaftLog.LastIndex()
		if m.Commit < lastIdx {
			r.RaftLog.committed = m.Commit
		} else {
			r.RaftLog.committed = lastIdx
		}
	}

	//回复成功
	resp := pb.Message{
		MsgType: pb.MessageType_MsgAppendResponse,
		To:      m.From,
		From:    r.id,
		Term:    r.Term,
		Reject:  false,
	}
	r.msgs = append(r.msgs, resp)
}

// handleHeartbeat handle Heartbeat RPC request
func (r *Raft) handleHeartbeat(m pb.Message) {
	// Your Code Here (2A).
	//心跳消息任期更小，拒绝
	if m.Term < r.Term {
		resp := pb.Message{
			MsgType: pb.MessageType_MsgHeartbeatResponse,
			To:      m.From,
			From:    r.id,
			Term:    r.Term,
			Reject:  true,
		}
		r.msgs = append(r.msgs, resp)
		return
	}

	//消息任期>=当前任期，转为Follower并更新任期和Leader
	r.becomeFollower(m.Term, m.From)
	r.electionElapsed = 0 //重置选举超时计时器

	//更新已提交日志ID
	if m.Commit > r.RaftLog.committed {
		r.RaftLog.committed = min(m.Commit, r.RaftLog.LastIndex())
	}
	//回复
	resp := pb.Message{
		MsgType: pb.MessageType_MsgHeartbeatResponse,
		To:      m.From,
		From:    r.id,
		Term:    r.Term,
		Reject:  false,
	}
	r.msgs = append(r.msgs, resp)
}

// +++
// 处理客户端提案MsgPropose（仅Leader）
func (r *Raft) handlePropose(m pb.Message) {
	if len(m.Entries) == 0 {
		return
	}
	//为每个条目设置的Index和Term
	lastIdx := r.RaftLog.LastIndex()
	for i, ent := range m.Entries {
		ent.Index = lastIdx + uint64(i) + 1
		ent.Term = r.Term
		if ent.EntryType != pb.EntryType_EntryConfChange {
			ent.EntryType = pb.EntryType_EntryNormal
		}
	}
	//指针切片转值切片写入内存日志
	newEntries := ptrsToEntries(m.Entries)
	r.RaftLog.entries = append(r.RaftLog.entries, newEntries...)

	//给所有Follower同步日志
	for id := range r.Prs {
		if id != r.id {
			r.sendAppend(id)
		}
	}
}

// handleSnapshot handle Snapshot RPC request
func (r *Raft) handleSnapshot(m pb.Message) {
	// Your Code Here (2C).
}

// addNode add a new node to raft group
func (r *Raft) addNode(id uint64) {
	// Your Code Here (3A).
}

// removeNode remove a node from raft group
func (r *Raft) removeNode(id uint64) {
	// Your Code Here (3A).
}

// +++
// 将值切片转换为指针切片
func entriesToPtr(entries []pb.Entry) []*pb.Entry {
	ptrs := make([]*pb.Entry, len(entries))
	for i := range entries {
		ptrs[i] = &entries[i]
	}
	return ptrs
}

// +++
// 将指针切片转换为值切片
func ptrsToEntries(ptrs []*pb.Entry) []pb.Entry {
	vals := make([]pb.Entry, len(ptrs))
	for i, e := range ptrs {
		vals[i] = *e
	}
	return vals
}

// +++
// appendEntriesToLog 将新条目追加到现有日志中，处理冲突，任期不同则截断
func appendEntriesToLog(existing []pb.Entry, newEntries []pb.Entry) []pb.Entry {
	if len(newEntries) == 0 {
		return existing
	}
	if len(existing) == 0 {
		return newEntries
	}

	offset := existing[0].Index
	lastIdx := existing[len(existing)-1].Index

	for i, e := range newEntries {
		idx := e.Index
		if idx <= lastIdx {
			localIdx := int(idx - offset)
			if localIdx >= len(existing) {
				//索引超出范围，追加后续所有条目
				return append(existing, newEntries[i:]...)
			}
			if existing[localIdx].Term != e.Term {
				//冲突：从该索引处截断，然后追加剩余新条目
				existing = existing[:localIdx]
				return append(existing, newEntries[i:]...)
			}
			//任期相同，继续检查下一个
		} else {
			//idx > lastIdx，直接追加剩余条目
			return append(existing, newEntries[i:]...)
		}
	}
	//所有新条目均已存在且任期相同，无需更改
	return existing
}

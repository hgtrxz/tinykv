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
	"github.com/pingcap-incubator/tinykv/log"
	pb "github.com/pingcap-incubator/tinykv/proto/pkg/eraftpb"
	"math/rand"
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
	Match, Next uint64 // Next: 当节点晋升为 Leader 时，初始化为 LastIndex+1
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
	// random num in [electionTimeout, 2*electionTimeout-1]
	randomElectionTimeout int
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

	tickerFunc func()

	stepFunc func(pb.Message)
}

// newRaft return a raft peer with the given config
func newRaft(c *Config) *Raft {
	if err := c.validate(); err != nil {
		panic(err.Error())
	}

	// init Raft
	raft := &Raft{
		id:               c.ID,
		RaftLog:          newLog(c.Storage),
		heartbeatTimeout: c.HeartbeatTick,
		electionTimeout:  c.ElectionTick,
		Prs:              make(map[uint64]*Progress),
	}
	li := raft.RaftLog.LastIndex()
	hardState, confState, _ := raft.RaftLog.storage.InitialState()

	// prs
	var nodes []uint64
	if c.peers != nil { // init state
		nodes = c.peers
	} else { // node restart
		nodes = confState.Nodes
	}
	for _, p := range nodes {
		if p == raft.id {
			continue
		}
		raft.Prs[p] = &Progress{Match: li + 1, Next: 0}
	}
	// raft log
	raft.Term, raft.Vote, raft.RaftLog.committed = hardState.Term, hardState.Vote, hardState.Commit

	// follower init...
	raft.becomeFollower(raft.Term, raft.Lead)
	// Your Code Here (2A).
	return raft
}

// sendAppend sends an append RPC with new entries (if any) and the
// current commit index to the given peer. Returns true if a message was sent.
func (r *Raft) sendAppend(to uint64) bool {
	// Your Code Here (2A).
	return false
}

// sendHeartbeat sends a heartbeat RPC to the given peer.
func (r *Raft) sendHeartbeat(to uint64) {
	// Your Code Here (2A).
	// lastIndex & lastTerm
	li := r.RaftLog.LastIndex()
	lt, _ := r.RaftLog.Term(li)

	req := pb.Message{
		MsgType: pb.MessageType_MsgHeartbeat,
		To:      to,
		From:    r.id,
		Term:    r.Term,
		LogTerm: lt,
		Index:   li,
	}
	r.sendMsg(req)
}

// tick advances the internal logical clock by a single tick.
func (r *Raft) tick() {
	// Your Code Here (2A).
	r.tickerFunc()
}

// becomeFollower transform this peer's state to Follower
func (r *Raft) becomeFollower(term uint64, lead uint64) {
	// Your Code Here (2A).
	if term > r.Term {
		r.Vote, r.votes = None, make(map[uint64]bool)
	}
	r.randomElectionTimeout = r.electionTimeout + rand.Intn(r.electionTimeout)
	r.State, r.Term, r.Lead, r.electionElapsed = StateFollower, term, lead, 0
	r.tickerFunc, r.stepFunc = r.tickElection, r.stepFollower
	log.Infof("Node[%d] become Follower Term[%d]", r.id, r.Term)
}

// Follower: handle heartbeat rpc  & append log & snapshot
func (r *Raft) stepFollower(msg pb.Message) {
	resp := r.initRespMsg(msg)
	switch msg.MsgType {
	case pb.MessageType_MsgHup:
		r.startElection()
	case pb.MessageType_MsgHeartbeat:
		// todo
		r.handleHeartbeat(msg)
		r.electionElapsed = 0
		resp.MsgType = pb.MessageType_MsgHeartbeatResponse
		r.sendMsg(resp)
	case pb.MessageType_MsgAppend:
		// todo
	case pb.MessageType_MsgSnapshot:
		// todo
	}
	log.Infof("[%s] Node[%d] handle message[from:%d msg_type:%s]", r.State.String(), r.id, msg.From, msg.MsgType.String())
}

// becomeCandidate transform this peer's state to candidate
func (r *Raft) becomeCandidate() {
	// Your Code Here (2A).
	r.randomElectionTimeout = r.electionTimeout + rand.Intn(r.electionTimeout)
	r.State, r.Term, r.Lead = StateCandidate, r.Term+1, None
	r.tickerFunc, r.stepFunc = r.tickElection, r.stepCandidate
	log.Infof("Node[%d] become Candidate Term[%d]", r.id, r.Term)
}

// Candidate: heartbeat rpc & vote resp
func (r *Raft) stepCandidate(msg pb.Message) {
	resp := r.initRespMsg(msg)
	switch msg.MsgType {
	case pb.MessageType_MsgHup:
		r.startElection()
	case pb.MessageType_MsgHeartbeat:
		resp.MsgType = pb.MessageType_MsgHeartbeatResponse
		r.sendMsg(resp)
		r.becomeFollower(msg.Term, msg.From)
	case pb.MessageType_MsgRequestVoteResponse:
		if !msg.Reject {
			r.Vote++
		}
		if r.Vote > uint64(len(r.Prs)/2) {
			r.becomeLeader()
		}
	}
}

func (r *Raft) tickElection() {
	r.electionElapsed++
	if r.electionElapsed < r.randomElectionTimeout {
		return
	}
	// pass a local message 'MessageType_MsgHup' to its Step method and start a new election.
	_ = r.Step(pb.Message{MsgType: pb.MessageType_MsgHup, From: r.id, Term: None})
}

// becomeLeader transform this peer's state to leader
func (r *Raft) becomeLeader() {
	// Your Code Here (2A).
	// NOTE: Leader should propose a noop entry on its term
	r.State, r.Lead = StateLeader, r.id
	r.tickerFunc, r.stepFunc = r.tickHeartbeat, r.stepLeader
	log.Infof("Node[%d] become Leader Term[%d]", r.id, r.Term)
}

func (r *Raft) stepLeader(msg pb.Message) {
	switch msg.MsgType {
	case pb.MessageType_MsgBeat:
		r.broadcastHeartbeat() // todo 能不能改成异步的
	case pb.MessageType_MsgHeartbeatResponse:
		//
	default:
		//
	}
}

func (r *Raft) broadcastHeartbeat() {
	go func() {
		if r.State != StateLeader {
			return
		}
		for k, _ := range r.Prs {
			if k == r.id {
				continue
			}
			r.sendHeartbeat(k)
		}
	}()
}

func (r *Raft) tickHeartbeat() {
	r.heartbeatElapsed++
	if r.heartbeatElapsed < r.heartbeatTimeout {
		return
	}
	// signals the leader to send a heartbeat of the 'MessageType_MsgHeartbeat' type to its followers.
	_ = r.Step(pb.Message{MsgType: pb.MessageType_MsgBeat, From: r.id, Term: None})
}

// Step the entrance of handle message, see `MessageType`
// on `eraftpb.proto` for what msgs should be handled
func (r *Raft) Step(m pb.Message) error {
	// Your Code Here (2A).
	// handle Term
	switch {
	case m.Term == None:
		// local message
	case m.Term < r.Term:
		// ignore
		resp := pb.Message{
			MsgType: m.MsgType + 1, // todo req => resp 可以这样吗
			To:      m.From,
			From:    r.id,
			Term:    r.Term,
		}
		if m.MsgType == pb.MessageType_MsgRequestVote { // reject vote req
			resp.Reject = true
		}
		r.sendMsg(resp)
		return nil
	case m.Term > r.Term:
		// become Follower
		lead := None
		if m.MsgType == pb.MessageType_MsgSnapshot || m.MsgType == pb.MessageType_MsgHeartbeat ||
			m.MsgType == pb.MessageType_MsgAppend { // 其他消息不能确定谁是Leader
			lead = m.From
		}
		r.becomeFollower(m.Term, lead)
	}
	// handle Msg
	switch m.MsgType {
	case pb.MessageType_MsgRequestVote: // 处理投票请求(任何状态的节点处理投票请求的handler都一样)
		r.handleVoteReq(m)
	default:
		r.stepFunc(m)
	}
	return nil
}

func (r *Raft) sendMsg(msg pb.Message) {
	r.msgs = append(r.msgs, msg)
}

func (r *Raft) handleVoteReq(m pb.Message) {
	resp := r.initRespMsg(m)
	resp.MsgType = pb.MessageType_MsgRequestVoteResponse

	// election restriction
	canVote := (r.Vote == None && r.Lead == None) || r.Vote == m.From
	isUpToDate := m.Term > r.Term || (m.Term == r.Term && m.LogTerm > r.RaftLog.LastIndex())
	reject := canVote && isUpToDate

	if !reject {
		r.Vote, r.votes[m.From] = m.From, true
	}
	resp.Reject = reject

	r.sendMsg(resp)
}

// zero election timeout => term++ => become Candidate => vote itself => vote req rpc
func (r *Raft) startElection() {
	r.electionReset()

	li := r.RaftLog.LastIndex()
	lt, _ := r.RaftLog.Term(r.RaftLog.LastIndex())
	msg := pb.Message{
		MsgType: pb.MessageType_MsgRequestVote,
		From:    r.id,
		Term:    r.Term,
		LogTerm: lt,
		Index:   li,
	}
	for p, _ := range r.Prs {
		if p == r.id {
			continue
		}
		msg.To = p
		r.sendMsg(msg)
	}
}

// 发起选举前，重新状态
func (r *Raft) electionReset() {
	r.electionElapsed = 0
	r.Vote, r.votes, r.Lead = 1, make(map[uint64]bool), None
}

// handleAppendEntries handle AppendEntries RPC request
func (r *Raft) handleAppendEntries(m pb.Message) {
	// Your Code Here (2A).
}

// handleHeartbeat handle Heartbeat RPC request
func (r *Raft) handleHeartbeat(m pb.Message) {
	// Your Code Here (2A).
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

func (r *Raft) initRespMsg(msg pb.Message) pb.Message {
	return pb.Message{
		To:   msg.From,
		From: r.id,
		Term: r.Term,
	}
}

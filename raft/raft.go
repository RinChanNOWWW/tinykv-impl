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
	votes       map[uint64]bool
	voteCount   int
	denialCount int

	// msgs need to send
	msgs []pb.Message

	// the leader id
	Lead uint64

	// heartbeat interval, should send
	heartbeatTimeout int
	// baseline of election interval
	electionTimeout int
	// real election timeout time.
	// random between et and 2*et
	realElectionTimeout int
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
	hardState, _, err := c.Storage.InitialState()
	if err != nil {
		panic(err)
	}
	r := &Raft{
		id:               c.ID,
		Lead:             None,
		Vote:             hardState.Vote,
		Term:             hardState.Term,
		RaftLog:          newLog(c.Storage),
		Prs:              make(map[uint64]*Progress),
		State:            StateFollower,
		votes:            make(map[uint64]bool),
		voteCount:        1,
		denialCount:      0,
		msgs:             make([]pb.Message, 0),
		heartbeatTimeout: c.HeartbeatTick,
		electionTimeout:  c.ElectionTick,
	}
	lastIndex := r.RaftLog.LastIndex()
	for _, id := range c.peers {
		if id == r.id {
			r.Prs[id] = &Progress{Match: lastIndex, Next: lastIndex + 1}
		} else {
			r.Prs[id] = &Progress{Match: 0, Next: lastIndex + 1}
		}
	}
	return r
}

func (r *Raft) resetRealElectionTimeout() {
	// 随机生成
	r.realElectionTimeout = r.electionTimeout + rand.Intn(r.electionTimeout)
}

// sendAppend sends an append RPC with new entries (if any) and the
// current commit index to the given peer. Returns true if a message was sent.
func (r *Raft) sendAppend(to uint64) bool {
	// Your Code Here (2A).
	lastIndex := r.RaftLog.LastIndex()
	preLogIndex := r.Prs[to].Next - 1
	if lastIndex < preLogIndex {
		return true
	}
	preLogTerm, err := r.RaftLog.Term(preLogIndex)
	if err != nil {
		return false
	}
	entries := r.RaftLog.Entries(preLogIndex+1, lastIndex+1)
	if err != nil {
		return false
	}
	sendEntreis := make([]*pb.Entry, 0)
	for _, en := range entries {
		sendEntreis = append(sendEntreis, &pb.Entry{
			EntryType: en.EntryType,
			Term:      en.Term,
			Index:     en.Index,
			Data:      en.Data,
		})
	}
	msg := pb.Message{
		MsgType: pb.MessageType_MsgAppend,
		To:      to,
		From:    r.id,
		Term:    r.Term,
		LogTerm: preLogTerm,
		Index:   preLogIndex,
		Entries: sendEntreis,
		Commit:  r.RaftLog.committed,
	}
	r.msgs = append(r.msgs, msg)
	return true
}

// sendHeartbeat sends a heartbeat RPC to the given peer.
func (r *Raft) sendHeartbeat(to uint64) {
	// Your Code Here (2A).
	msg := pb.Message{
		MsgType: pb.MessageType_MsgHeartbeat,
		From:    r.id,
		To:      to,
		Term:    r.Term,
	}
	r.msgs = append(r.msgs, msg)
}

// tick advances the internal logical clock by a single tick.
func (r *Raft) tick() {
	// Your Code Here (2A).
	if r.isLeader() {
		r.tickHeartbeat()
	} else {
		r.tickElection()
	}
}

func (r *Raft) tickHeartbeat() {
	r.heartbeatElapsed += 1
	if r.heartbeatElapsed == r.heartbeatTimeout {
		r.heartbeatElapsed = 0
		r.bcastHeartbeat()
	}
}

func (r *Raft) tickElection() {
	r.electionElapsed += 1
	if r.electionElapsed == r.realElectionTimeout {
		r.electionElapsed = 0
		r.campaign()
	}
}

// becomeFollower transform this peer's state to Follower
func (r *Raft) becomeFollower(term uint64, lead uint64) {
	// Your Code Here (2A).
	r.State = StateFollower
	r.Term = term
	r.Lead = lead
	r.votes = nil
	r.voteCount = 0
	r.denialCount = 0
	r.resetTick()
}

// becomeCandidate transform this peer's state to candidate
func (r *Raft) becomeCandidate() {
	// Your Code Here (2A).
	r.State = StateCandidate
	r.Term += 1
	r.Vote = r.id
	r.votes = make(map[uint64]bool)
	r.votes[r.id] = true
	r.voteCount = 1
	r.resetTick()
	if r.voteCount > len(r.Prs)/2 {
		r.becomeLeader()
	}
}

// becomeLeader transform this peer's state to leader
func (r *Raft) becomeLeader() {
	// Your Code Here (2A).
	r.State = StateLeader
	for _, v := range r.Prs {
		v.Match = 0
		v.Next = r.RaftLog.LastIndex() + 1
	}
	// NOTE: Leader should propose a noop entry on its term
	r.resetTick()
	r.appendEntries(&pb.Entry{
		EntryType: pb.EntryType_EntryNormal,
		Term:      r.Term,
		Index:     r.RaftLog.LastIndex() + 1,
		Data:      nil,
	})
	r.bcastAppend()
}

// Step the entrance of handle message, see `MessageType`
// on `eraftpb.proto` for what msgs should be handled
func (r *Raft) Step(m pb.Message) error {
	// Your Code Here (2A).
	switch r.State {
	case StateFollower:
		r.stepFollower(m)
	case StateCandidate:
		r.stepCandidate(m)
	case StateLeader:
		r.stepLeader(m)
	}
	return nil
}

func (r *Raft) stepFollower(m pb.Message) {
	switch m.MsgType {
	case pb.MessageType_MsgHup:
		r.campaign()
	case pb.MessageType_MsgRequestVote:
		r.handleVoteRequest(m)
	case pb.MessageType_MsgAppend:
		r.handleAppendEntries(m)
	}
}

func (r *Raft) stepCandidate(m pb.Message) {
	switch m.MsgType {
	case pb.MessageType_MsgHup:
		r.campaign()
	case pb.MessageType_MsgRequestVoteResponse:
		r.handleVoteResponse(m)
	case pb.MessageType_MsgRequestVote:
		r.handleVoteRequest(m)
	case pb.MessageType_MsgAppend:
		r.handleAppendEntries(m)
	}
}

func (r *Raft) stepLeader(m pb.Message) {
	switch m.MsgType {
	case pb.MessageType_MsgPropose:
		lastIndex := r.RaftLog.LastIndex()
		ents := make([]*pb.Entry, 0)
		for _, e := range m.Entries {
			ents = append(ents, &pb.Entry{
				EntryType: e.EntryType,
				Term:      r.Term,
				Index:     lastIndex + 1,
				Data:      e.Data,
			})
			lastIndex += 1
		}
		r.appendEntries(ents...)
		r.bcastAppend()
		r.updateCommit()
	case pb.MessageType_MsgRequestVote:
		r.handleVoteRequest(m)
	case pb.MessageType_MsgAppend:
		r.becomeFollower(m.Term, m.From)
	case pb.MessageType_MsgBeat:
		r.bcastHeartbeat()
	case pb.MessageType_MsgAppendResponse:
		r.handleAppendResponse(m)
	}
}

// campaign becomes a candidate and start to request vote
func (r *Raft) campaign() {
	r.becomeCandidate()
	r.bcastVoteRequest()
}

// handleAppendEntries handle AppendEntries RPC request
func (r *Raft) handleAppendEntries(m pb.Message) {
	// Your Code Here (2A).
	// Reply false if term < currentTerm (§5.1)
	if m.Term < r.Term {
		r.sendAppendResponse(m.From, true)
		return
	}
	r.becomeFollower(m.Term, m.From)
	// Reply false if log doesn’t contain an entry at prevLogIndex
	// whose term matches prevLogTerm (§5.3)
	term, err := r.RaftLog.Term(m.Index)
	if err != nil || term != m.LogTerm {
		r.sendAppendResponse(m.From, true)
		return
	}
	// If an existing entry conflicts with a new one (same index
	// but different terms), delete the existing entry and all that
	// follow it (§5.3)
	if len(m.Entries) > 0 {
		appendStart := 0
		for i, ent := range m.Entries {
			if ent.Index > r.RaftLog.LastIndex() {
				appendStart = i
				break
			}
			validTerm, _ := r.RaftLog.Term(ent.Index)
			if validTerm != ent.Term {
				r.RaftLog.RemoveEntriesAfter(ent.Index)
				break
			}
			appendStart = i
			r.RaftLog.stabled = max(ent.Index, r.RaftLog.stabled)
		}
		if m.Entries[appendStart].Index > r.RaftLog.LastIndex() {
			r.appendEntries(m.Entries[appendStart:]...)
		}
	}
	if m.Commit > r.RaftLog.committed {
		r.RaftLog.committed = min(m.Commit, r.RaftLog.LastIndex())
	}
	r.sendAppendResponse(m.From, false)
}

// handleHeartbeat handle Heartbeat RPC request
func (r *Raft) handleHeartbeat(m pb.Message) {
	// Your Code Here (2A).
}

// handleSnapshot handle Snapshot RPC request
func (r *Raft) handleSnapshot(m pb.Message) {
	// Your Code Here (2C).
}

// handleVoteRequest handle vote request
func (r *Raft) handleVoteRequest(m pb.Message) {
	lastTerm, _ := r.RaftLog.Term(r.RaftLog.LastIndex())
	if r.Term > m.Term || r.isMoreUpToDateThan(m.LogTerm, m.Index) {
		r.sendVoteResponse(m.From, true)
		return
	}
	if r.Term < m.Term {
		r.becomeFollower(m.Term, r.Lead)
		r.Vote = m.From
		r.sendVoteResponse(m.From, false)
		return
	}
	if r.Vote == m.From {
		r.sendVoteResponse(m.From, false)
		return
	}
	if r.isFollower() &&
		r.Vote == None &&
		(lastTerm < m.LogTerm || (lastTerm == m.LogTerm && m.Index >= r.RaftLog.LastIndex())) {
		r.sendVoteResponse(m.From, false)
		return
	}
	r.sendVoteResponse(m.From, true)
}

func (r *Raft) isMoreUpToDateThan(term, index uint64) bool {
	lastTerm, _ := r.RaftLog.Term(r.RaftLog.LastIndex())
	if lastTerm > term || (lastTerm == term && r.RaftLog.LastIndex() > index) {
		return true
	}
	return false
}

// addNode add a new node to raft group
func (r *Raft) addNode(id uint64) {
	// Your Code Here (3A).
}

// removeNode remove a node from raft group
func (r *Raft) removeNode(id uint64) {
	// Your Code Here (3A).
}

// resetTick reset elapsed time to 0
func (r *Raft) resetTick() {
	r.heartbeatElapsed = 0
	r.electionElapsed = 0
	r.resetRealElectionTimeout()
}

// appendEntries append entry to raft log entries
func (r *Raft) appendEntries(entries ...*pb.Entry) {
	ents := make([]pb.Entry, 0)
	for _, e := range entries {
		ents = append(ents, pb.Entry{
			EntryType: e.EntryType,
			Term:      e.Term,
			Index:     e.Index,
			Data:      e.Data,
		})
	}
	r.RaftLog.appendEntries(ents...)
	r.Prs[r.id].Match = r.RaftLog.LastIndex()
	r.Prs[r.id].Next = r.RaftLog.LastIndex() + 1
}

// isLeader return if is leader
func (r *Raft) isLeader() bool {
	return r.State == StateLeader
}

// isFollower return if is follower
func (r *Raft) isFollower() bool {
	return r.State == StateFollower
}

// ------------------ follower methods ------------------

// sendVoteResponse send vote response
func (r *Raft) sendVoteResponse(nvote uint64, reject bool) {
	msg := pb.Message{
		MsgType: pb.MessageType_MsgRequestVoteResponse,
		From:    r.id,
		To:      nvote,
		Term:    r.Term,
		Reject:  reject,
	}
	r.msgs = append(r.msgs, msg)
}

// sendAppendResponse send append response
func (r *Raft) sendAppendResponse(to uint64, reject bool) {
	msg := pb.Message{
		MsgType: pb.MessageType_MsgAppendResponse,
		From:    r.id,
		To:      to,
		Term:    r.Term,
		Reject:  reject,
		Index:   r.RaftLog.LastIndex(),
	}
	r.msgs = append(r.msgs, msg)
}

// ------------------ candidate methods ------------------

// bcastVoteRequest is used by candidate to send vote request
func (r *Raft) bcastVoteRequest() {
	lastIndex := r.RaftLog.LastIndex()
	lastTerm, _ := r.RaftLog.Term(lastIndex)
	for peer := range r.Prs {
		if peer != r.id {
			msg := pb.Message{
				MsgType: pb.MessageType_MsgRequestVote,
				From:    r.id,
				To:      peer,
				Term:    r.Term,
				LogTerm: lastTerm,
				Index:   lastIndex,
			}
			r.msgs = append(r.msgs, msg)
		}
	}
}

// handleVoteResponse handle vote response
func (r *Raft) handleVoteResponse(m pb.Message) {
	if m.Term > r.Term {
		r.becomeFollower(m.Term, r.Lead)
		r.Vote = m.From
		return
	}
	if !m.Reject {
		r.votes[m.From] = true
		r.voteCount += 1
	} else {
		r.votes[m.From] = false
		r.denialCount += 1
	}
	if r.voteCount > len(r.Prs)/2 {
		r.becomeLeader()
	} else if r.denialCount > len(r.Prs)/2 {
		r.becomeFollower(r.Term, r.Lead)
	}
}

// ------------------- leader methods -------------------

// bcastAppend is used by leader to bcast append request to followers
func (r *Raft) bcastAppend() {
	for peer := range r.Prs {
		if peer != r.id {
			r.sendAppend(peer)
		}
	}
}

// bcastHeartbeat is used by leader to bcast append request to followers
func (r *Raft) bcastHeartbeat() {
	for peer := range r.Prs {
		if peer != r.id {
			r.sendHeartbeat(peer)
		}
	}
}

func (r *Raft) handleAppendResponse(m pb.Message) {
	if m.Term > r.Term {
		r.becomeFollower(m.Term, None)
		return
	}
	if !m.Reject {
		r.Prs[m.From].Next = m.Index + 1
		r.Prs[m.From].Match = m.Index
	} else if r.Prs[m.From].Next > 0 {
		r.Prs[m.From].Next -= 1
		r.sendAppend(m.From)
	}
	r.updateCommit()
}

// updateCommit ...
func (r *Raft) updateCommit() {
	minMatch := r.RaftLog.LastIndex()
	for peer, p := range r.Prs {
		if peer == r.id {
			continue
		}
		minMatch = min(minMatch, p.Match)
	}
	if minMatch < r.RaftLog.committed {
		return
	}
	commitUpdated := false
	for i := minMatch; i <= r.RaftLog.LastIndex(); i += 1 {
		if i <= r.RaftLog.committed {
			continue
		}
		matchCnt := 0
		for _, p := range r.Prs {
			if p.Match >= i {
				matchCnt += 1
			}
		}
		// leader only commit on it's current term (§5.4.2)
		term, _ := r.RaftLog.Term(i)
		if matchCnt > len(r.Prs)/2 && term == r.Term && r.RaftLog.committed != i {
			r.RaftLog.committed = i
			commitUpdated = true
		}
	}
	if commitUpdated {
		r.bcastAppend()
	}
}

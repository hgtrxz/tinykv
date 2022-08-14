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
	pb "github.com/pingcap-incubator/tinykv/proto/pkg/eraftpb"
	"github.com/pingcap/errors"
)

const (
	SentinelLogIndex = 0
)

// RaftLog manage the log entries, its struct look like:
//
//  snapshot/first.....applied....committed....stabled.....last
//  --------|------------------------------------------------|
//                            log entries
//
// for simplify the RaftLog implement should manage all log entries
// that not truncated
type RaftLog struct {
	// storage contains all stable entries since the last snapshot.
	// stabled logs
	storage Storage

	// committed is the highest log position that is known to be in
	// stable storage on a quorum of nodes.
	committed uint64

	// applied is the highest log position that the application has
	// been instructed to apply to its state machine.
	// Invariant: applied <= committed
	// applied but unstable log
	applied uint64

	// log entries with index <= stabled are persisted to storage.
	// It is used to record the logs that are not persisted by storage yet.
	// Everytime handling `Ready`, the unstabled logs will be included.
	stabled uint64

	// all entries that have not yet compact.
	entries []pb.Entry

	// the incoming unstable snapshot, if any.
	// (Used in 2C)
	pendingSnapshot *pb.Snapshot

	// Your Data Here (2A).
}

// newLog returns log using the given storage. It recovers the log
// to the state that it just commits and applies the latest snapshot.
func newLog(storage Storage) *RaftLog {
	// Your Code Here (2A).
	lastIdx, _ := storage.LastIndex()
	// entries[0] as sentinel
	sentinel := pb.Entry{EntryType: pb.EntryType_EntryNormal, Index: SentinelLogIndex}
	entries := make([]pb.Entry, 0)
	return &RaftLog{
		storage:         storage,
		committed:       0,
		applied:         0,
		stabled:         lastIdx,
		entries:         append(entries, sentinel),
		pendingSnapshot: nil,
	}
}

// We need to compact the log entries in some point of time like
// storage compact stabled log entries prevent the log entries
// grow unlimitedly in memory
func (l *RaftLog) maybeCompact() {
	// Your Code Here (2C).
}

// unstableEntries return all the unstable entries
func (l *RaftLog) unstableEntries() []pb.Entry {
	// Your Code Here (2A).
	return nil
}

// nextEnts returns all the committed but not applied entries
func (l *RaftLog) nextEnts() (ents []pb.Entry) {
	// Your Code Here (2A).
	return nil
}

// LastIndex return the last index of the log entries
func (l *RaftLog) LastIndex() uint64 {
	length := len(l.entries)
	if li, err := l.storage.LastIndex(); err == nil && li != 0 && length == 1 { // 所有的 log 都已经持久化，entries 只有哨兵日志
		return li
	}
	// 节点只有一个 sentinel log OR 部分保存为快照
	return l.entries[length-1].Index
}

// Term return the term of the entry in the given index
// [1 2 3 4] [?] 5 6 => log_idx:5->idx:1  stabled=4 todo 搞清楚 log index 是怎么存储的
// [?] 1 2 => log_idx:2->idx:2 stabled=0
func (l *RaftLog) Term(i uint64) (uint64, error) {
	// 1.没有快照 => return l.entries[i].Term
	// 2.有快照，且 i 日志在快照里面 => l.storage.Entries(i,i+1)
	// 3.有快照，但 i 日志在内存里面 => l.entries[i - l.storage.LastIndex()]

	if i <= l.stabled { // 日志在外存
		return l.storage.Term(i)
	}
	// 日志在内存
	arrIdx := i - l.stabled
	if arrIdx < 0 || arrIdx >= uint64(len(l.entries)) {
		return None, errors.Errorf("illegal log index:%d", i)
	}
	return l.entries[arrIdx].Term, nil
}

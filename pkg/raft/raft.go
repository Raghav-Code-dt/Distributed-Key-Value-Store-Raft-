package raft

import (
	"bytes"
	"encoding/gob"
	"sync"
	"sync/atomic"
	"time"
)

// Raft represents a single node in a Raft consensus cluster.
type Raft struct {
	mu        sync.Mutex    // Lock to protect shared access to this peer's state
	peers     []interface{} // Mock RPC client endpoints (to be replaced with actual RPC interface)
	me        int           // This peer's index into peers[]
	dead      int32         // set by Kill()
	applyCh   chan ApplyMsg // Channel to send committed log entries to the service
	persister Storage       // Object to hold this peer's persisted state

	// Persistent state on all servers
	currentTerm int
	votedFor    int
	log         *RaftLog

	// Volatile state on all servers
	commitIndex int
	lastApplied int
	state       NodeRole

	// Volatile state on leaders
	nextIndex  []int
	matchIndex []int

	// Election state
	lastHeartbeat time.Time
}

// GetState returns the current term and whether this node believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.currentTerm, rf.state == Leader
}

// Start proposes a command to the Raft cluster.
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.state != Leader {
		return -1, -1, false
	}

	index := rf.log.LastLogIndex() + 1
	term := rf.currentTerm

	rf.log.Append(LogEntry{
		Term:    term,
		Command: command,
	})
	rf.persist()

	rf.nextIndex[rf.me] = index + 1
	rf.matchIndex[rf.me] = index

	// Immediately trigger a round of AppendEntries to replicate
	go rf.broadcastHeartbeats()

	return index, term, true
}

// applier runs in the background and applies committed logs to the state machine.
func (rf *Raft) applier() {
	for !rf.killed() {
		rf.mu.Lock()

		if rf.commitIndex > rf.lastApplied {
			rf.lastApplied++
			entry := rf.log.Entries[rf.lastApplied]

			msg := ApplyMsg{
				CommandValid: true,
				Command:      entry.Command,
				CommandIndex: rf.lastApplied,
			}

			rf.mu.Unlock()
			// Send outside the lock to avoid deadlocks with the KV service
			rf.applyCh <- msg
		} else {
			rf.mu.Unlock()
			time.Sleep(10 * time.Millisecond)
		}
	}
}

// Kill sets the node to a dead state, used mainly for testing.
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
}

// killed checks if the node has been killed.
func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

// Make creates a new Raft node.
func Make(peers []interface{}, me int, persister Storage, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me
	rf.applyCh = applyCh

	rf.currentTerm = 0
	rf.votedFor = -1
	rf.log = NewRaftLog()
	rf.state = Follower
	rf.lastHeartbeat = time.Now()

	// Initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// Start the background election ticker and applier
	go rf.ticker()
	go rf.applier()

	return rf
}

// persist saves the Raft node's persistent state to stable storage.
// Note: It assumes the caller holds the node's lock.
func (rf *Raft) persist() {
	w := new(bytes.Buffer)
	e := gob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.log.Entries)
	data := w.Bytes()
	rf.persister.SaveRaftState(data)
}

// readPersist restores previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 {
		return // Bootstrap without state
	}
	r := bytes.NewBuffer(data)
	d := gob.NewDecoder(r)

	var currentTerm int
	var votedFor int
	var entries []LogEntry

	if d.Decode(&currentTerm) == nil &&
		d.Decode(&votedFor) == nil &&
		d.Decode(&entries) == nil {
		rf.currentTerm = currentTerm
		rf.votedFor = votedFor
		rf.log.Entries = entries
	}
}

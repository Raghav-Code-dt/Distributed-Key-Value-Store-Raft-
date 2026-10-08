package raft

import (
	"sync"
	"sync/atomic"
	"time"
)
// Raft represents a single node in a Raft consensus cluster.
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []interface{}       // Mock RPC client endpoints (to be replaced with actual RPC interface)
	me        int                 // This peer's index into peers[]
	dead      int32               // set by Kill()
	applyCh   chan ApplyMsg       // Channel to send committed log entries to the service

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
	// TODO: return (index, term, isLeader)
	return -1, -1, false
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
func Make(peers []interface{}, me int, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.me = me
	rf.applyCh = applyCh
	
	rf.currentTerm = 0
	rf.votedFor = -1
	rf.log = NewRaftLog()
	rf.state = Follower
	rf.lastHeartbeat = time.Now()

	// Start the background election ticker
	go rf.ticker()

	return rf
}

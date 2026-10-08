package raft

import "sync"

// Persister simulates non-volatile storage for testing node crashes and reboots.
type Persister struct {
	mu        sync.Mutex
	raftstate []byte
}

// MakePersister creates a new mock Persister.
func MakePersister() *Persister {
	return &Persister{}
}

// SaveRaftState saves the Raft node's persistent state.
func (ps *Persister) SaveRaftState(state []byte) {
	ps.mu.Lock()
	defer ps.mu.Unlock()
	ps.raftstate = clone(state)
}

// ReadRaftState returns the saved Raft node state.
func (ps *Persister) ReadRaftState() []byte {
	ps.mu.Lock()
	defer ps.mu.Unlock()
	return clone(ps.raftstate)
}

// clone creates a deep copy of a byte slice to prevent race conditions.
func clone(orig []byte) []byte {
	x := make([]byte, len(orig))
	copy(x, orig)
	return x
}

package raft

// becomeFollower transitions the node to the Follower state.
// Note: It assumes the caller holds the node's lock.
func (rf *Raft) becomeFollower(term int) {
	// TODO: implement state transition
}

// becomeCandidate transitions the node to the Candidate state and increments the term.
// Note: It assumes the caller holds the node's lock.
func (rf *Raft) becomeCandidate() {
	// TODO: implement state transition
}

// becomeLeader transitions the node to the Leader state.
// Note: It assumes the caller holds the node's lock.
func (rf *Raft) becomeLeader() {
	// TODO: implement state transition
}

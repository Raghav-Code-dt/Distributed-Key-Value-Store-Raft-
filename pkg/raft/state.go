package raft

// becomeFollower transitions the node to the Follower state.
// Note: It assumes the caller holds the node's lock.
func (rf *Raft) becomeFollower(term int) {
	rf.state = Follower
	rf.currentTerm = term
	rf.votedFor = -1
	rf.persist()
}

// becomeCandidate transitions the node to the Candidate state and increments the term.
// Note: It assumes the caller holds the node's lock.
func (rf *Raft) becomeCandidate() {
	rf.state = Candidate
	rf.currentTerm++
	rf.votedFor = rf.me
	rf.persist()
}

// becomeLeader transitions the node to the Leader state.
// Note: It assumes the caller holds the node's lock.
func (rf *Raft) becomeLeader() {
	rf.state = Leader
	rf.nextIndex = make([]int, len(rf.peers))
	rf.matchIndex = make([]int, len(rf.peers))
	lastLogIndex := rf.log.LastLogIndex()

	for i := range rf.peers {
		rf.nextIndex[i] = lastLogIndex + 1
		rf.matchIndex[i] = 0
	}
}

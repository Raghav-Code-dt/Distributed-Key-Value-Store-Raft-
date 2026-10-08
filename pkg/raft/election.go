package raft

// ticker triggers leader elections periodically by observing election timeouts.
func (rf *Raft) ticker() {
	// TODO: loop and sleep for randomized election timeout
}

// resetElectionTimer resets the election timeout to a new random duration (250-500ms).
func (rf *Raft) resetElectionTimer() {
	// TODO: implement randomized timer reset
}

// startElection begins a new leader election round.
func (rf *Raft) startElection() {
	// TODO: broadcast RequestVote RPCs to all peers
}

package raft

import (
	"math/rand"
	"time"
)

// ticker triggers leader elections periodically by observing election timeouts.
func (rf *Raft) ticker() {
	for !rf.killed() {
		rf.mu.Lock()
		state := rf.state
		lastHeartbeat := rf.lastHeartbeat
		rf.mu.Unlock()

		if state != Leader {
			// Randomized timeout between 250ms and 500ms
			timeout := time.Duration(250+rand.Intn(250)) * time.Millisecond
			if time.Since(lastHeartbeat) > timeout {
				rf.mu.Lock()
				rf.startElection()
				rf.mu.Unlock()
			}
		}
		
		// Sleep for a short interval before checking again
		time.Sleep(10 * time.Millisecond)
	}
}

// resetElectionTimer updates the last heartbeat time.
func (rf *Raft) resetElectionTimer() {
	rf.lastHeartbeat = time.Now()
}

// startElection begins a new leader election round.
// Note: It assumes the caller holds the node's lock.
func (rf *Raft) startElection() {
	rf.becomeCandidate()
	rf.resetElectionTimer()
	// TODO: broadcast RequestVote RPCs to all peers
}

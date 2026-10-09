package raft

import (
	"math/rand"
	"time"
)

// ticker triggers leader elections periodically by observing e/lection timeouts.
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
		} else {
			// Send periodic heartbeats to maintain leadership and push updates
			if time.Since(lastHeartbeat) > 100*time.Millisecond {
				rf.broadcastHeartbeats()
				rf.mu.Lock()
				rf.resetElectionTimer()
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

	term := rf.currentTerm
	candidateId := rf.me
	lastLogIndex := rf.log.LastLogIndex()
	lastLogTerm := rf.log.LastLogTerm()

	args := RequestVoteArgs{
		Term:         term,
		CandidateId:  candidateId,
		LastLogIndex: lastLogIndex,
		LastLogTerm:  lastLogTerm,
	}

	votesReceived := 1 // Vote for self

	for i := range rf.peers {
		if i == rf.me {
			continue
		}
		go func(peer int) {
			reply := RequestVoteReply{}
			if rf.sendRequestVote(peer, &args, &reply) {
				rf.mu.Lock()
				defer rf.mu.Unlock()

				// Discard responses if state or term changed while waiting
				if rf.state != Candidate || rf.currentTerm != term {
					return
				}
				if reply.Term > rf.currentTerm {
					rf.becomeFollower(reply.Term)
					return
				}
				if reply.VoteGranted {
					votesReceived++
					if votesReceived > len(rf.peers)/2 {
						rf.becomeLeader()
						go rf.broadcastHeartbeats() // Suppress other elections
					}
				}
			}
		}(i)
	}
}

// sendRequestVote is a wrapper to send the RPC to a peer.
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	client, ok := rf.peers[server].(interface {
		Call(string, interface{}, interface{}) bool
	})
	if ok {
		return client.Call("Raft.RequestVote", args, reply)
	}
	return false
}

// RequestVote is the RPC handler invoked by candidates to gather votes.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) error {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	// 1. Reply false if term < currentTerm
	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
		return nil
	}

	// Step down if the candidate has a higher term
	if args.Term > rf.currentTerm {
		rf.becomeFollower(args.Term)
	}

	reply.Term = rf.currentTerm

	// 2. If votedFor is null or candidateId, grant vote
	if rf.votedFor == -1 || rf.votedFor == args.CandidateId {
		rf.votedFor = args.CandidateId
		rf.persist()
		rf.resetElectionTimer()
		reply.VoteGranted = true
	} else {
		reply.VoteGranted = false
	}
	return nil
}

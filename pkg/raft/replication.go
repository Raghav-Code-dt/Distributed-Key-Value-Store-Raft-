package raft

// broadcastHeartbeats sends empty AppendEntries RPCs to all peers to maintain leadership.
func (rf *Raft) broadcastHeartbeats() {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.state != Leader {
		return
	}

	term := rf.currentTerm
	leaderId := rf.me
	leaderCommit := rf.commitIndex

	for i := range rf.peers {
		if i == rf.me {
			continue
		}

		args := AppendEntriesArgs{
			Term:         term,
			LeaderId:     leaderId,
			PrevLogIndex: rf.log.LastLogIndex(),
			PrevLogTerm:  rf.log.LastLogTerm(),
			Entries:      nil, // Empty for heartbeat
			LeaderCommit: leaderCommit,
		}

		go func(peer int) {
			reply := AppendEntriesReply{}
			if rf.sendAppendEntries(peer, &args, &reply) {
				rf.mu.Lock()
				defer rf.mu.Unlock()

				if rf.state != Leader || rf.currentTerm != term {
					return
				}

				if reply.Term > rf.currentTerm {
					rf.becomeFollower(reply.Term)
					return
				}
			}
		}(i)
	}
}

// sendAppendEntries is a wrapper to send the RPC to a peer.
func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	// In a complete implementation, this would use rf.peers[server].Call(...)
	// We'll leave it as a mock return for now.
	return false
}

// AppendEntries is the RPC handler invoked by the leader to replicate log entries and send heartbeats.
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.Success = false
		return
	}

	// If we receive a heartbeat from a valid leader, step down and reset timer
	rf.becomeFollower(args.Term)
	rf.resetElectionTimer()

	reply.Term = rf.currentTerm
	reply.Success = true
	// TODO: implement actual log replication and conflict resolution in Lab 2B
}

// advanceCommitIndex checks if entries can be committed based on matchIndex from a majority of peers.
func (rf *Raft) advanceCommitIndex() {
	// TODO: update commitIndex and send committed entries to applyCh
}

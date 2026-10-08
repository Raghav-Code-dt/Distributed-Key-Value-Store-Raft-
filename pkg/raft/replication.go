package raft

// broadcastHeartbeats sends AppendEntries RPCs to all peers to maintain leadership and replicate logs.
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

		nextIdx := rf.nextIndex[i]
		prevLogIndex := nextIdx - 1
		prevLogTerm := rf.log.Entries[prevLogIndex].Term

		// Copy entries to send to avoid race conditions
		entries := make([]LogEntry, len(rf.log.Entries[nextIdx:]))
		copy(entries, rf.log.Entries[nextIdx:])

		args := AppendEntriesArgs{
			Term:         term,
			LeaderId:     leaderId,
			PrevLogIndex: prevLogIndex,
			PrevLogTerm:  prevLogTerm,
			Entries:      entries,
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

				if reply.Success {
					matchIdx := args.PrevLogIndex + len(args.Entries)
					if matchIdx > rf.matchIndex[peer] {
						rf.matchIndex[peer] = matchIdx
						rf.nextIndex[peer] = matchIdx + 1
						rf.advanceCommitIndex()
					}
				} else {
					// Log inconsistency: decrement nextIndex and retry (relies on periodic heartbeats or immediate retry)
					rf.nextIndex[peer]--
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

	// Consistency Check: Log must contain an entry at PrevLogIndex matching PrevLogTerm
	if args.PrevLogIndex > rf.log.LastLogIndex() {
		reply.Success = false
		return
	}
	if rf.log.Entries[args.PrevLogIndex].Term != args.PrevLogTerm {
		reply.Success = false
		return
	}

	// Conflict Resolution & Truncation
	insertIndex := args.PrevLogIndex + 1
	for i, entry := range args.Entries {
		if insertIndex <= rf.log.LastLogIndex() && rf.log.Entries[insertIndex].Term != entry.Term {
			rf.log.Truncate(insertIndex)
		}
		
		if insertIndex > rf.log.LastLogIndex() {
			// Append the rest of the new entries
			rf.log.Append(args.Entries[i:]...)
			break
		}
		insertIndex++
	}

	// Advance commitIndex if leader has moved forward
	if args.LeaderCommit > rf.commitIndex {
		lastNewIndex := args.PrevLogIndex + len(args.Entries)
		if args.LeaderCommit < lastNewIndex {
			rf.commitIndex = args.LeaderCommit
		} else {
			rf.commitIndex = lastNewIndex
		}
	}

	reply.Success = true
}

// advanceCommitIndex checks if entries can be committed based on matchIndex from a majority of peers.
// Note: It assumes the caller holds the node's lock.
func (rf *Raft) advanceCommitIndex() {
	for n := rf.log.LastLogIndex(); n > rf.commitIndex; n-- {
		// Rule: A leader can only commit log entries from its current term by counting replicas
		if rf.log.Entries[n].Term != rf.currentTerm {
			continue
		}

		matchCount := 1 // Leader already has the entry
		for i := range rf.peers {
			if i == rf.me {
				continue
			}
			if rf.matchIndex[i] >= n {
				matchCount++
			}
		}

		if matchCount > len(rf.peers)/2 {
			rf.commitIndex = n
			// The applier goroutine will notice commitIndex > lastApplied and apply entries.
			break
		}
	}
}

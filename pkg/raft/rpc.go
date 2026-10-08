package raft

// NodeRole represents the current role of the Raft node (Follower, Candidate, or Leader).
type NodeRole int

const (
	Follower NodeRole = iota
	Candidate
	Leader
)

// ApplyMsg is sent on the applyCh when a log entry is committed.
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int
}

// LogEntry represents a single entry in the Raft log.
type LogEntry struct {
	Term    int
	Command interface{}
}

// RequestVoteArgs contains arguments for the RequestVote RPC.
type RequestVoteArgs struct {
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

// RequestVoteReply contains the reply for the RequestVote RPC.
type RequestVoteReply struct {
	Term        int
	VoteGranted bool
}

// AppendEntriesArgs contains arguments for the AppendEntries RPC (and heartbeats).
type AppendEntriesArgs struct {
	Term         int
	LeaderId     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []LogEntry
	LeaderCommit int
}

// AppendEntriesReply contains the reply for the AppendEntries RPC.
type AppendEntriesReply struct {
	Term    int
	Success bool
}

package raft

// RaftLog encapsulates the 1-based log indexing, truncation, and persistence.
type RaftLog struct {
	Entries []LogEntry
}

// NewRaftLog creates a new empty log.
func NewRaftLog() *RaftLog {
	// Raft logs are 1-based. We append a dummy entry at index 0.
	return &RaftLog{
		Entries: make([]LogEntry, 1),
	}
}

// LastLogIndex returns the index of the last entry in the log.
func (l *RaftLog) LastLogIndex() int {
	return len(l.Entries) - 1
}

// LastLogTerm returns the term of the last entry in the log.
func (l *RaftLog) LastLogTerm() int {
	return l.Entries[l.LastLogIndex()].Term
}

// Truncate truncates the log from the specified index.
func (l *RaftLog) Truncate(index int) {
	if index <= len(l.Entries) {
		l.Entries = l.Entries[:index]
	}
}

// Append adds new entries to the log.
func (l *RaftLog) Append(entries ...LogEntry) {
	l.Entries = append(l.Entries, entries...)
}

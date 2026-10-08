package raft

import "testing"

// TestInitialElection (Lab 2A)
func TestInitialElection(t *testing.T) {
	// TODO: 3 nodes start; exactly 1 leader is elected; remaining 2 stay followers.
}

// TestReElection (Lab 2A)
func TestReElection(t *testing.T) {
	// TODO: Disconnect the leader; remaining 2 nodes must elect a new leader.
	// Reconnect old leader; it must step down to follower.
}

// TestSplitVote (Lab 2A)
func TestSplitVote(t *testing.T) {
	// TODO: When simultaneous candidates split votes, election timers back off.
}

// TestBasicAgree (Lab 2B)
func TestBasicAgree(t *testing.T) {
	// TODO: Propose 1 command to leader; all 3 nodes commit and push it to applyCh.
}

// TestFailAgree (Lab 2B)
func TestFailAgree(t *testing.T) {
	// TODO: Propose command with 1 follower disconnected; remaining 2 nodes still commit.
}

// TestFailNoAgree (Lab 2B)
func TestFailNoAgree(t *testing.T) {
	// TODO: Disconnect 2 followers in a 3-node cluster; leader fails to commit.
}

// TestRejoin (Lab 2B)
func TestRejoin(t *testing.T) {
	// TODO: Partitioned leader rejoins; discards uncommitted entries for majority log.
}

// TestConcurrentStarts (Lab 2C)
func TestConcurrentStarts(t *testing.T) {
	// TODO: Multiple concurrent calls to rf.Start() maintain monotonic indices.
}

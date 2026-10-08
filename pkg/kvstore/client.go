package kvstore

import (
	"crypto/rand"
	"math/big"
)

// Clerk acts as a client wrapper for the KV service.
type Clerk struct {
	servers  []interface{} // Mock RPC endpoints for the servers
	leaderID int           // The node we believe is the current leader
	clientID int64         // Unique ID for this client
	seqNum   int64         // Monotonically increasing sequence number
}

// nrand generates a random 64-bit integer.
func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	return bigx.Int64()
}

// MakeClerk initializes a new Clerk.
func MakeClerk(servers []interface{}) *Clerk {
	return &Clerk{
		servers:  servers,
		clientID: nrand(),
		seqNum:   0,
		leaderID: 0,
	}
}

// Get fetches the current value for a key.
func (ck *Clerk) Get(key string) string {
	ck.seqNum++
	// TODO: loop over servers until successful reply from the leader
	return ""
}

// PutAppend handles setting or appending values to a key.
func (ck *Clerk) PutAppend(key string, value string, op string) {
	ck.seqNum++
	// TODO: loop over servers until successful reply from the leader
}

// Put sets the value for a key.
func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, OpPut)
}

// Append appends to the value for a key.
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, OpAppend)
}

package kvstore

const (
	OpPut    = "Put"
	OpAppend = "Append"
	OpGet    = "Get"

	OK             = "OK"
	ErrNoKey       = "ErrNoKey"
	ErrWrongLeader = "ErrWrongLeader"
)

// Op represents a single client operation submitted to the Raft log.
type Op struct {
	Type  string // "Put", "Append", or "Get"
	Key   string
	Value string // Empty for "Get"

	ClientID int64 // Unique ID of the client issuing the request
	SeqNum   int64 // Monotonically increasing sequence number for this client's operations
}

// OpResult represents the result of applying an operation to the state machine.
type OpResult struct {
	Value    string
	Err      string
	ClientID int64
	SeqNum   int64
}

// PutAppendArgs holds the RPC arguments for Put and Append requests.
type PutAppendArgs struct {
	Key   string
	Value string
	Op    string // "Put" or "Append"

	ClientID int64
	SeqNum   int64
}

// PutAppendReply holds the RPC reply for Put and Append requests.
type PutAppendReply struct {
	Err string
}

// GetArgs holds the RPC arguments for Get requests.
type GetArgs struct {
	Key string

	ClientID int64
	SeqNum   int64
}

// GetReply holds the RPC reply for Get requests.
type GetReply struct {
	Err   string
	Value string
}

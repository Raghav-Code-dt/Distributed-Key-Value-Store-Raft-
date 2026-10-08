package kvstore

import (
	"sync"
	"distributed-kv/pkg/raft"
)

// KVServer represents a single node providing the KV service.
type KVServer struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32

	store       *KVStore
	notifyChans map[int]chan OpResult // index -> notify channel
}

// Get handles the Get RPC from clients.
func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// TODO: Check duplicate, submit Op to Raft, wait on notify channel
}

// PutAppend handles the Put or Append RPC from clients.
func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// TODO: Check duplicate, submit Op to Raft, wait on notify channel
}

// applier runs in the background and processes committed entries from Raft.
func (kv *KVServer) applier() {
	// TODO: Read from kv.applyCh, apply to KVStore, notify waiting clients
}

// Kill sets the server to a dead state.
func (kv *KVServer) Kill() {
	// TODO: stop server and associated Raft node
}

// StartKVServer initializes a new KVServer.
func StartKVServer(servers []interface{}, me int, persister interface{}, maxraftstate int) *KVServer {
	kv := &KVServer{
		me:          me,
		store:       NewKVStore(),
		notifyChans: make(map[int]chan OpResult),
		applyCh:     make(chan raft.ApplyMsg),
	}
	kv.rf = raft.Make(servers, me, kv.applyCh)
	// TODO: start applier goroutine
	return kv
}

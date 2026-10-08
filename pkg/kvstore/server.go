package kvstore

import (
	"distributed-kv/pkg/raft"
	"sync"
	"time"
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

func (kv *KVServer) getNotifyChan(index int) chan OpResult {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	ch, ok := kv.notifyChans[index]
	if !ok {
		ch = make(chan OpResult, 1)
		kv.notifyChans[index] = ch
	}
	return ch
}

func (kv *KVServer) removeNotifyChan(index int) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	delete(kv.notifyChans, index)
}

// Get handles the Get RPC from clients.
func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	op := Op{
		Type:     OpGet,
		Key:      args.Key,
		ClientID: args.ClientID,
		SeqNum:   args.SeqNum,
	}

	index, _, isLeader := kv.rf.Start(op)
	if !isLeader {
		reply.Err = ErrWrongLeader
		return
	}

	ch := kv.getNotifyChan(index)
	defer kv.removeNotifyChan(index)

	select {
	case result := <-ch:
		reply.Err = result.Err
		reply.Value = result.Value
	case <-time.After(500 * time.Millisecond):
		reply.Err = ErrWrongLeader
	}
}

// PutAppend handles the Put or Append RPC from clients.
func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	op := Op{
		Type:     args.Op,
		Key:      args.Key,
		Value:    args.Value,
		ClientID: args.ClientID,
		SeqNum:   args.SeqNum,
	}

	index, _, isLeader := kv.rf.Start(op)
	if !isLeader {
		reply.Err = ErrWrongLeader
		return
	}

	ch := kv.getNotifyChan(index)
	defer kv.removeNotifyChan(index)

	select {
	case result := <-ch:
		reply.Err = result.Err
	case <-time.After(500 * time.Millisecond):
		reply.Err = ErrWrongLeader
	}
}

// applier runs in the background and processes committed entries from Raft.
func (kv *KVServer) applier() {
	for msg := range kv.applyCh {
		if msg.CommandValid {
			op := msg.Command.(Op)
			result := OpResult{Err: OK}

			// Check idempotency for modifying operations
			if op.Type != OpGet {
				if _, isDup := kv.store.CheckDuplicate(op.ClientID, op.SeqNum); !isDup {
					if op.Type == OpPut {
						kv.store.Put(op.Key, op.Value)
					} else if op.Type == OpAppend {
						kv.store.Append(op.Key, op.Value)
					}
					kv.store.RecordOperation(op.ClientID, op.SeqNum, "")
				}
			} else {
				// Get operations simply read
				val, ok := kv.store.Get(op.Key)
				if ok {
					result.Value = val
				} else {
					result.Err = ErrNoKey
				}
			}

			// Notify the waiting RPC handler if this node is the leader who submitted it
			kv.mu.Lock()
			if ch, ok := kv.notifyChans[msg.CommandIndex]; ok {
				// Channel has buffer 1, so this won't block
				select {
				case ch <- result:
				default:
				}
			}
			kv.mu.Unlock()
		}
	}
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
	kv.rf = raft.Make(servers, me, persister.(*raft.Persister), kv.applyCh)

	// Start the background applier goroutine
	go kv.applier()

	return kv
}

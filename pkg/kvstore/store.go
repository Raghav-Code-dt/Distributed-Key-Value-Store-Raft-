package kvstore

import "sync"

// KVStore represents the core in-memory key-value database.
type KVStore struct {
	mu        sync.Mutex
	data      map[string]string // The actual key-value data map
	
	// Deduplication table for client retries: ClientID -> Last SeqNum processed
	lastSeq   map[int64]int64
	// Caches the reply for the last processed SeqNum per ClientID
	lastReply map[int64]string
}

// NewKVStore initializes a new KVStore.
func NewKVStore() *KVStore {
	return &KVStore{
		data:      make(map[string]string),
		lastSeq:   make(map[int64]int64),
		lastReply: make(map[int64]string),
	}
}

// Get retrieves the value for a given key.
func (kv *KVStore) Get(key string) (string, bool) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	val, ok := kv.data[key]
	return val, ok
}

// Put sets the value for a given key.
func (kv *KVStore) Put(key, value string) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	kv.data[key] = value
}

// Append appends the value to the existing value for a given key.
func (kv *KVStore) Append(key, value string) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	kv.data[key] += value
}

// CheckDuplicate verifies if an operation has already been processed to ensure idempotency.
func (kv *KVStore) CheckDuplicate(clientID int64, seqNum int64) (string, bool) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	
	last, exists := kv.lastSeq[clientID]
	if exists && seqNum <= last {
		return kv.lastReply[clientID], true
	}
	return "", false
}

// RecordOperation marks an operation as processed for a client.
func (kv *KVStore) RecordOperation(clientID int64, seqNum int64, reply string) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	
	kv.lastSeq[clientID] = seqNum
	kv.lastReply[clientID] = reply
}

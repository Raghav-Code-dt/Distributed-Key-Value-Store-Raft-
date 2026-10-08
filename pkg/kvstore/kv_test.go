package kvstore

import "testing"

// TestBasicStore (Lab 3A)
func TestBasicStore(t *testing.T) {
	// TODO: Client Put("k1", "v1"), Append("k1", "v2"), Get("k1") == "v1v2".
}

// TestConcurrentClients (Lab 3A)
func TestConcurrentClients(t *testing.T) {
	// TODO: Multiple client threads issuing random Put/Append/Get concurrently.
}

// TestPartitionKV (Lab 3A)
func TestPartitionKV(t *testing.T) {
	// TODO: Partition 3-node cluster. Clients writing to majority succeed; 
	// writes to minority fail/hang. Repair partition; cluster converges.
}

// TestClientRetries (Lab 3A)
func TestClientRetries(t *testing.T) {
	// TODO: Drop client RPC responses; client retries the same operation; 
	// verify store applies it exactly once.
}

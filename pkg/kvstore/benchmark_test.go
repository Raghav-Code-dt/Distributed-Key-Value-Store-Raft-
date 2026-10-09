package kvstore

import (
	"distributed-kv/pkg/labnet"
	"distributed-kv/pkg/raft"
	"fmt"
	"testing"
)

// setupCluster creates an in-memory 3-node Raft cluster for benchmarking.
func setupCluster() (*labnet.Network, []*KVServer, *Clerk) {
	net := labnet.NewNetwork()
	net.SetReliable(true)
	net.SetLongDelays(false)

	numServers := 3
	servers := make([]*KVServer, numServers)
	endpoints := make([]interface{}, numServers)

	// Create client endpoints for the servers to talk to each other
	for i := 0; i < numServers; i++ {
		endName := fmt.Sprintf("server-%d", i)
		endpoints[i] = labnet.MakeClientEnd(endName, net)
		net.ConnectEndpoint(endName, endName)
	}

	// Boot the servers
	for i := 0; i < numServers; i++ {
		persister := raft.MakePersister()

		// In a real setup, endpoints would be a copy with specific configurations,
		// but for a quick benchmark this is sufficient.
		servers[i] = StartKVServer(endpoints, i, persister, -1)

		// Register the server with the network hub
		srv := labnet.MakeServer()
		srv.AddService(servers[i])
		srv.AddService(servers[i].rf) // Expose Raft RPCs too
		net.AddServer(fmt.Sprintf("server-%d", i), srv)
		net.Connect(fmt.Sprintf("server-%d", i))
	}

	clerk := MakeClerk(endpoints)
	return net, servers, clerk
}

// BenchmarkPut measures the throughput of concurrent Put operations.
func BenchmarkPut(b *testing.B) {
	_, _, clerk := setupCluster()

	b.ResetTimer() // Don't time the cluster bootup

	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			clerk.Put("benchmark_key", "benchmark_value")
		}
	})
}

// BenchmarkGet measures the throughput of concurrent Get operations.
func BenchmarkGet(b *testing.B) {
	_, _, clerk := setupCluster()
	clerk.Put("benchmark_key", "initial_value")

	b.ResetTimer()

	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			clerk.Get("benchmark_key")
		}
	})
}

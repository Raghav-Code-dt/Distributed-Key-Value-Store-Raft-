package main

import (
	"flag"
	"fmt"
	"log"
	"net"
	"net/rpc"
	"strings"

	"distributed-kv/pkg/kvstore"
	"distributed-kv/pkg/raft"
	"distributed-kv/pkg/tcpnet"
)

func main() {
	// Parse CLI arguments
	port := flag.String("port", "8001", "The TCP port this server should listen on")
	peersArg := flag.String("peers", "", "Comma-separated list of peer addresses (e.g., localhost:8002,localhost:8003)")
	me := flag.Int("id", 0, "The ID of this server (0, 1, or 2)")
	inMemory := flag.Bool("inmemory", false, "Use in-memory persistence instead of disk")
	fastApply := flag.Bool("fastapply", false, "Use eager applyCond wakeup to skip 10ms polling")
	flag.Parse()

	peerAddrs := strings.Split(*peersArg, ",")
	if *peersArg == "" {
		peerAddrs = []string{}
	}

	var persister raft.Storage
	if *inMemory {
		persister = raft.MakePersister()
	} else {
		persister = raft.MakeDiskPersister(fmt.Sprintf("raft-state-%d.dat", *me))
	}

	// 2. Set up TCP Clients for Peers
	peers := make([]interface{}, len(peerAddrs))
	for i, addr := range peerAddrs {
		peers[i] = tcpnet.MakeTCPClientEnd(addr)
	}

	// 3. Boot the KVServer (and underlying Raft node)
	kvServer := kvstore.StartKVServer(peers, *me, persister, -1)
	if *fastApply {
		kvServer.GetRaft().FastApply = true
	}

	// 4. Start the TCP RPC Listener
	server := rpc.NewServer()
	// Expose the KVServer methods (Get, PutAppend)
	server.RegisterName("KVServer", kvServer)
	// Expose the Raft methods (RequestVote, AppendEntries)
	server.RegisterName("Raft", kvServer.GetRaft())

	listener, err := net.Listen("tcp", ":"+*port)
	if err != nil {
		log.Fatalf("KVServer %d failed to listen on port %s: %v", *me, *port, err)
	}

	fmt.Printf("🚀 KVServer Node %d is booting up on port %s...\n", *me, *port)
	fmt.Printf("🔗 Connected to peers: %v\n", peerAddrs)

	// Block forever, accepting incoming TCP connections
	server.Accept(listener)
}

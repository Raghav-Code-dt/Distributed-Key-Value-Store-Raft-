package labnet

import (
	"log"
	"math/rand"
	"sync"
	"time"
)

// Network simulates an in-memory network for testing distributed systems.
// It supports simulating delays, drops, disconnects, and network partitions.
type Network struct {
	mu         sync.Mutex
	reliable   bool
	longDelays bool

	// Map of server name to the actual Server object
	servers map[string]*Server
	// Map to track if a specific server is currently connected to the network
	connected map[string]bool

	// Track connections between clients and servers: map[clientName]serverName
	endpoints map[string]string
}

// NewNetwork creates a new mock network.
func NewNetwork() *Network {
	return &Network{
		reliable:   true,
		longDelays: false,
		servers:    make(map[string]*Server),
		connected:  make(map[string]bool),
		endpoints:  make(map[string]string),
	}
}

// AddServer registers a Server with the network under a specific name.
func (n *Network) AddServer(serverName string, server *Server) {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.servers[serverName] = server
}

// ConnectEndpoint links a client endpoint name to a target server name.
func (n *Network) ConnectEndpoint(endname string, serverName string) {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.endpoints[endname] = serverName
}

// SetReliable controls whether the network drops or reorders packets.
func (n *Network) SetReliable(reliable bool) {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.reliable = reliable
}

// SetLongDelays controls whether the network simulates long message delays.
func (n *Network) SetLongDelays(longDelays bool) {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.longDelays = longDelays
}

// Disconnect simulates disconnecting a node from the network.
func (n *Network) Disconnect(serverName string) {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.connected[serverName] = false
}

// Connect simulates reconnecting a node to the network.
func (n *Network) Connect(serverName string) {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.connected[serverName] = true
}

// Partition simulates partitioning the network into two or more groups.
// This is typically done by disconnecting nodes in the minority group.
func (n *Network) Partition(groupA []string, groupB []string) {
	// Custom partition logic can be built on top of Connect/Disconnect.
	for _, s := range groupB {
		n.Disconnect(s)
	}
}

// ProcessReq is called by ClientEnd to deliver an RPC.
func (n *Network) ProcessReq(endname string, svcMeth string, args interface{}, reply interface{}) bool {
	n.mu.Lock()
	serverName, ok := n.endpoints[endname]
	if !ok {
		n.mu.Unlock()
		log.Printf("Network: unknown endpoint %v", endname)
		return false
	}

	server, serverExists := n.servers[serverName]
	serverConnected := n.connected[serverName]
	reliable := n.reliable
	longDelays := n.longDelays
	n.mu.Unlock()

	if !serverExists || !serverConnected {
		// Simulate network partition/disconnect
		return false
	}

	// Simulate unreliable network: 10% chance to drop the request completely
	if !reliable && rand.Intn(1000) < 100 {
		return false
	}

	// Simulate network delays
	if longDelays {
		// Delay up to 200ms
		time.Sleep(time.Duration(rand.Intn(200)) * time.Millisecond)
	} else if !reliable {
		// Short delays (up to 27ms) to cause reordering
		time.Sleep(time.Duration(rand.Intn(27)) * time.Millisecond)
	}

	// Dispatch the request to the server
	err := server.dispatch(svcMeth, args, reply)
	if err != nil {
		log.Printf("Network: dispatch error: %v", err)
		return false
	}

	// Simulate unreliable network: 10% chance to drop the reply
	if !reliable && rand.Intn(1000) < 100 {
		return false
	}

	// Simulate reply delay
	if longDelays {
		time.Sleep(time.Duration(rand.Intn(200)) * time.Millisecond)
	}

	return true
}

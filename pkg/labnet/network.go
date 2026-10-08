package labnet

// Network simulates an in-memory network for testing distributed systems.
// It supports simulating delays, drops, disconnects, and network partitions.
type Network struct {
	// Implementation to follow
}

// Disconnect simulates disconnecting a node from the network.
func (n *Network) Disconnect(nodeID int) {
	// TODO
}

// Connect simulates connecting a node to the network.
func (n *Network) Connect(nodeID int) {
	// TODO
}

// Partition simulates partitioning the network into two or more groups.
func (n *Network) Partition(groupA []int, groupB []int) {
	// TODO
}

// SetReliable controls whether the network drops or reorders packets.
func (n *Network) SetReliable(reliable bool) {
	// TODO
}

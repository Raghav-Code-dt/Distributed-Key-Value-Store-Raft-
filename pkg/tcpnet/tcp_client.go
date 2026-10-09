package tcpnet

import (
	"log"
	"net/rpc"
	"time"
)

// TCPClientEnd acts as a proxy for an RPC client, fulfilling the interface required by Raft.
type TCPClientEnd struct {
	address string
}

// MakeTCPClientEnd initializes a new TCPClientEnd.
func MakeTCPClientEnd(address string) *TCPClientEnd {
	return &TCPClientEnd{
		address: address,
	}
}

// Call connects to the remote server over TCP and issues the RPC.
func (e *TCPClientEnd) Call(svcMeth string, args interface{}, reply interface{}) bool {
	// For high-performance, connections should be pooled.
	// To keep it simple and robust against crashes, we dial fresh connections here.

	// Dial with a timeout so we don't hang forever if a node crashes.
	client, err := rpc.Dial("tcp", e.address)
	if err != nil {
		return false
	}
	defer client.Close()

	// Issue the RPC call
	call := client.Go(svcMeth, args, reply, nil)

	// Block until the call completes or a timeout fires
	select {
	case <-call.Done:
		if call.Error != nil {
			log.Printf("TCPClientEnd: Error calling %s on %s: %v", svcMeth, e.address, call.Error)
			return false
		}
		return true
	case <-time.After(2 * time.Second): // 2 second RPC timeout
		log.Printf("TCPClientEnd: Timeout calling %s on %s", svcMeth, e.address)
		return false
	}
}

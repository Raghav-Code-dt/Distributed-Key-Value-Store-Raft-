package labnet

// ClientRPC defines the interface a client uses to send an RPC.
type ClientRPC interface {
	Call(svcMeth string, args interface{}, reply interface{}) bool
}

// Server defines the interface for an RPC server.
type Server interface {
	AddService(svc interface{})
}

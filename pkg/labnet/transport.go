package labnet

import (
	"fmt"
	"reflect"
	"strings"
	"sync"
)

// ClientRPC defines the interface a client uses to send an RPC.
type ClientRPC interface {
	Call(svcMeth string, args interface{}, reply interface{}) bool
}

// Server acts as a mock RPC server that holds registered services.
type Server struct {
	mu       sync.Mutex
	services map[string]*Service
}

// Service represents a registered object and its exported methods.
type Service struct {
	rcvr    reflect.Value
	methods map[string]reflect.Method
}

// MakeServer creates a new mock RPC server.
func MakeServer() *Server {
	return &Server{
		services: make(map[string]*Service),
	}
}

// AddService registers an object so its exported methods can be called via RPC.
func (rs *Server) AddService(svc interface{}) {
	rs.mu.Lock()
	defer rs.mu.Unlock()

	typ := reflect.TypeOf(svc)
	val := reflect.ValueOf(svc)
	
	// Extract the name of the struct (e.g., "Raft")
	name := reflect.Indirect(val).Type().Name()

	methods := make(map[string]reflect.Method)
	for i := 0; i < typ.NumMethod(); i++ {
		method := typ.Method(i)
		methods[method.Name] = method
	}

	rs.services[name] = &Service{
		rcvr:    val,
		methods: methods,
	}
}

// dispatch dynamically invokes the requested service method.
func (rs *Server) dispatch(svcMeth string, args interface{}, reply interface{}) error {
	parts := strings.Split(svcMeth, ".")
	if len(parts) != 2 {
		return fmt.Errorf("ill-formed service method %v", svcMeth)
	}
	serviceName, methodName := parts[0], parts[1]

	rs.mu.Lock()
	service, ok := rs.services[serviceName]
	rs.mu.Unlock()

	if !ok {
		return fmt.Errorf("service %v not found", serviceName)
	}

	method, ok := service.methods[methodName]
	if !ok {
		return fmt.Errorf("method %v not found in service %v", methodName, serviceName)
	}

	// Invoke the method via reflection
	in := []reflect.Value{service.rcvr, reflect.ValueOf(args), reflect.ValueOf(reply)}
	method.Func.Call(in)

	return nil
}

// ClientEnd represents a client's view of a connection to a specific server.
type ClientEnd struct {
	endname string
	net     *Network
}

// MakeClientEnd creates a new client stub.
func MakeClientEnd(endname string, net *Network) *ClientEnd {
	return &ClientEnd{
		endname: endname,
		net:     net,
	}
}

// Call asks the Network to deliver the RPC to the destination server.
func (e *ClientEnd) Call(svcMeth string, args interface{}, reply interface{}) bool {
	// Defer to the central network dispatcher, which simulates unreliability.
	return e.net.ProcessReq(e.endname, svcMeth, args, reply)
}

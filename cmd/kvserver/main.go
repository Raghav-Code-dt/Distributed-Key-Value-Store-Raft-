package main

import (
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"
	// "distributed-kv/pkg/kvstore"
)

func main() {
	fmt.Println("Starting Distributed Key-Value Store Node...")

	// TODO: Parse command-line flags for node ID and peer addresses
	// TODO: Initialize TCP transport and establish RPC connections to peers
	// TODO: kvstore.StartKVServer(...)

	// Wait for interrupt signal to gracefully shutdown the server
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)

	<-sigCh
	log.Println("Shutting down KV node...")
	// TODO: kvServer.Kill()
}

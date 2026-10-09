package main

import (
	"flag"
	"fmt"
	"os"
	"strings"
	"time"

	"distributed-kv/pkg/kvstore"
	"distributed-kv/pkg/tcpnet"
)

func main() {
	serversArg := flag.String("servers", "localhost:8001,localhost:8002,localhost:8003", "Comma-separated list of server addresses")
	flag.Parse()

	args := flag.Args()
	if len(args) < 1 {
		fmt.Println("Usage: kvclient [-servers ...] <command> [args...]")
		fmt.Println("Commands:")
		fmt.Println("  put <key> <value>   - Store a value")
		fmt.Println("  get <key>           - Retrieve a value")
		fmt.Println("  benchmark           - Run an aggressive load test")
		os.Exit(1)
	}

	command := args[0]

	// 1. Set up TCP Clients for the Clerk
	serverAddrs := strings.Split(*serversArg, ",")
	servers := make([]interface{}, len(serverAddrs))
	for i, addr := range serverAddrs {
		servers[i] = tcpnet.MakeTCPClientEnd(addr)
	}

	// 2. Boot the Idempotent Clerk
	clerk := kvstore.MakeClerk(servers)

	// 3. Execute the command
	switch command {
	case "put":
		if len(args) != 3 {
			fmt.Println("Usage: kvclient put <key> <value>")
			os.Exit(1)
		}
		key, value := args[1], args[2]
		fmt.Printf("🚀 Sending PUT %s=%s to cluster...\n", key, value)
		
		start := time.Now()
		clerk.Put(key, value)
		fmt.Printf("✅ Success! (Took %v)\n", time.Since(start))

	case "get":
		if len(args) != 2 {
			fmt.Println("Usage: kvclient get <key>")
			os.Exit(1)
		}
		key := args[1]
		fmt.Printf("🚀 Sending GET %s to cluster...\n", key)
		
		start := time.Now()
		val := clerk.Get(key)
		if val == "" {
			fmt.Printf("✅ Success! Value: <empty> (Took %v)\n", time.Since(start))
		} else {
			fmt.Printf("✅ Success! Value: %s (Took %v)\n", val, time.Since(start))
		}

	case "benchmark":
		fmt.Println("🔥 Starting aggressive Load Test...")
		start := time.Now()
		numOps := 1000

		// Send 1000 requests as fast as possible synchronously (for simplicity)
		for i := 0; i < numOps; i++ {
			clerk.Put(fmt.Sprintf("benchKey%d", i), "benchValue")
			if i%100 == 0 && i > 0 {
				fmt.Printf("Processed %d ops...\n", i)
			}
		}

		duration := time.Since(start)
		opsPerSec := float64(numOps) / duration.Seconds()
		fmt.Printf("🏁 Benchmark Complete!\n")
		fmt.Printf("Total Time: %v\n", duration)
		fmt.Printf("Throughput: %.2f ops/sec\n", opsPerSec)

	default:
		fmt.Printf("Unknown command: %s\n", command)
		os.Exit(1)
	}
}

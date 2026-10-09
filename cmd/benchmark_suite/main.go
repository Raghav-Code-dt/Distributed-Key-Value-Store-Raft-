package main

import (
	"context"
	"fmt"
	"log"
	"math/rand"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"distributed-kv/pkg/kvstore"
	"distributed-kv/pkg/tcpnet"
)

type Result struct {
	Ops        int
	OpsPerSec  float64
	P50        time.Duration
	P95        time.Duration
	P99        time.Duration
}

func buildServer() string {
	binPath := filepath.Join(os.TempDir(), "kvserver-bench")
	cmd := exec.Command("go", "build", "-o", binPath, "./cmd/kvserver")
	if err := cmd.Run(); err != nil {
		log.Fatalf("Failed to build: %v", err)
	}
	return binPath
}

func runBenchmark(binPath string, clients int, inMemory bool, fastApply bool) Result {
	testDir, _ := os.MkdirTemp("", "bench-*")
	defer os.RemoveAll(testDir)

	peersArg := "localhost:9001,localhost:9002,localhost:9003"
	
	cmds := make([]*exec.Cmd, 3)
	for i := 0; i < 3; i++ {
		args := []string{"-id", fmt.Sprintf("%d", i), "-port", fmt.Sprintf("%d", 9001+i), "-peers", peersArg}
		if inMemory {
			args = append(args, "-inmemory")
		}
		if fastApply {
			args = append(args, "-fastapply")
		}
		cmd := exec.Command(binPath, args...)
		cmd.Dir = testDir
		f, _ := os.Create(filepath.Join(testDir, fmt.Sprintf("server-%d.log", i)))
		cmd.Stdout = f
		cmd.Stderr = f
		if err := cmd.Start(); err != nil {
			log.Fatalf("Start failed: %v", err)
		}
		cmds[i] = cmd
	}

	defer func() {
		for _, cmd := range cmds {
			if cmd != nil && cmd.Process != nil {
				cmd.Process.Kill()
				cmd.Wait()
			}
		}
	}()

	time.Sleep(1 * time.Second) // wait for leader

	peers := []interface{}{
		tcpnet.MakeTCPClientEnd("localhost:9001"),
		tcpnet.MakeTCPClientEnd("localhost:9002"),
		tcpnet.MakeTCPClientEnd("localhost:9003"),
	}

	var totalOps int64
	latencies := make(chan time.Duration, clients*100000)
	
	ctx, cancel := context.WithTimeout(context.Background(), 35*time.Second)
	defer cancel()

	var wg sync.WaitGroup
	for i := 0; i < clients; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			ck := kvstore.MakeClerk(peers)
			for {
				select {
				case <-ctx.Done():
					return
				default:
				}
				
				start := time.Now()
				if rand.Intn(2) == 0 {
					ck.Get("bench_key")
				} else {
					ck.Put("bench_key", "val")
				}
				elapsed := time.Since(start)
				
				atomic.AddInt64(&totalOps, 1)
				latencies <- elapsed
			}
		}()
	}

	// Warmup: wait 2 seconds while clients fire
	time.Sleep(2 * time.Second)
	atomic.StoreInt64(&totalOps, 0)
	
	// Drain warmup latencies
	for len(latencies) > 0 {
		<-latencies
	}

	// Wait 30 seconds for real measurement
	time.Sleep(30 * time.Second)
	cancel() // stop clients

	wg.Wait()
	close(latencies)

	var lats []time.Duration
	for l := range latencies {
		lats = append(lats, l)
	}

	sort.Slice(lats, func(i, j int) bool { return lats[i] < lats[j] })

	var p50, p95, p99 time.Duration
	if len(lats) > 0 {
		p50 = lats[int(float64(len(lats))*0.50)]
		p95 = lats[int(float64(len(lats))*0.95)]
		p99 = lats[int(float64(len(lats))*0.99)]
	}

	for i := 0; i < 3; i++ {
		exec.Command("cp", filepath.Join(testDir, fmt.Sprintf("server-%d.log", i)), fmt.Sprintf("/tmp/server-%d.log", i)).Run()
	}
	return Result{
		Ops:       int(totalOps),
		OpsPerSec: float64(totalOps) / 30.0,
		P50:       p50,
		P95:       p95,
		P99:       p99,
	}
}

func main() {
	if len(os.Args) > 1 && os.Args[1] == "worker" {
		clients := 1
		inMemory := false
		fastApply := false
		fmt.Sscanf(os.Args[2], "%d", &clients)
		if os.Args[3] == "true" {
			inMemory = true
		}
		if os.Args[4] == "true" {
			fastApply = true
		}
		res := runBenchmark(os.Args[5], clients, inMemory, fastApply)
		fmt.Printf("%d %v %v %v %d\n", int(res.OpsPerSec), res.P50, res.P95, res.P99, res.Ops)
		os.Exit(0)
	}

	fmt.Println("==================================================")
	fmt.Println("🚀 Distributed KV Store - TCP Raft Benchmark")
	fmt.Printf("CPU: %s (%d Cores)\n", runtime.GOARCH, runtime.NumCPU())
	fmt.Printf("OS: %s\n", runtime.GOOS)
	fmt.Printf("Go Version: %s\n", runtime.Version())
	fmt.Println("Topology: 3-Node Cluster (localhost TCP), 3 OS Processes")
	fmt.Println("Workload: 50% Reads / 50% Writes, 30s duration")
	fmt.Println("==================================================")

	binPath := buildServer()
	defer os.Remove(binPath)

	clientCounts := []int{1, 4, 16, 64}
	modes := []struct {
		Name     string
		InMemory bool
	}{
		{"In-Memory", true},
		{"Durable (fsync)", false},
	}

	for _, mode := range modes {
		fmt.Printf("\n--- Mode: %s ---\n", mode.Name)
		for _, clients := range clientCounts {
			fmt.Printf("Clients: %d\n", clients)
			for run := 1; run <= 3; run++ {
				fmt.Printf("  Run %d: ", run)
				
				ctx, cancel := context.WithTimeout(context.Background(), 40*time.Second)
				cmd := exec.CommandContext(ctx, os.Args[0], "worker", fmt.Sprintf("%d", clients), fmt.Sprintf("%t", mode.InMemory), "true", binPath)
				out, err := cmd.CombinedOutput()
				cancel()
				if err != nil {
					if ctx.Err() == context.DeadlineExceeded {
						fmt.Printf("FAILED (Timeout - Deadlock/Port Exhaustion)\n")
					} else {
						fmt.Printf("FAILED (Crash/Error)\n")
					}
				} else {
					fmt.Print(string(out))
				}
			}
		}
	}
}

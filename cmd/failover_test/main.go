package main

import (
	"fmt"
	"log"
	"net/rpc"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"time"

	"distributed-kv/pkg/kvstore"
	"distributed-kv/pkg/tcpnet"
)

func buildServer() string {
	binPath := filepath.Join(os.TempDir(), "kvserver-failover")
	cmd := exec.Command("go", "build", "-o", binPath, "./cmd/kvserver")
	if err := cmd.Run(); err != nil {
		log.Fatalf("Failed to build kvserver: %v", err)
	}
	return binPath
}

func startServer(binPath string, id int, port int, peers string, dir string) *exec.Cmd {
	cmd := exec.Command(binPath, "-id", fmt.Sprintf("%d", id), "-port", fmt.Sprintf("%d", port), "-peers", peers, "-inmemory")
	cmd.Dir = dir
	cmd.Stdout = nil
	cmd.Stderr = nil
	if err := cmd.Start(); err != nil {
		log.Fatalf("Failed to start server %d: %v", id, err)
	}
	return cmd
}

func findLeader(addrs []string) (int, error) {
	for i, addr := range addrs {
		client, err := rpc.Dial("tcp", addr)
		if err != nil {
			continue
		}
		args := kvstore.PutAppendArgs{
			Key:      "leader_probe",
			Value:    "probe",
			Op:       kvstore.OpPut,
			ClientID: 999999,
			SeqNum:   time.Now().UnixNano(),
		}
		reply := kvstore.PutAppendReply{}
		err = client.Call("KVServer.PutAppend", &args, &reply)
		client.Close()
		if err == nil && reply.Err == kvstore.OK {
			return i, nil
		}
	}
	return -1, fmt.Errorf("no leader found")
}

func main() {
	trials := 50
	binPath := buildServer()
	defer os.Remove(binPath)

	testDir, err := os.MkdirTemp("", "failover-*")
	if err != nil {
		log.Fatal(err)
	}
	defer os.RemoveAll(testDir)

	peersArg := "localhost:9001,localhost:9002,localhost:9003"
	ports := []int{9001, 9002, 9003}
	addrs := []string{"localhost:9001", "localhost:9002", "localhost:9003"}

	cmds := make([]*exec.Cmd, 3)
	for i := 0; i < 3; i++ {
		cmds[i] = startServer(binPath, i, ports[i], peersArg, testDir)
	}
	defer func() {
		for _, cmd := range cmds {
			if cmd != nil && cmd.Process != nil {
				cmd.Process.Kill()
				cmd.Wait()
			}
		}
	}()

	fmt.Printf("Cluster booted. Running %d leader-kill failover trials...\n", trials)
	time.Sleep(1500 * time.Millisecond) // Let initial election settle

	clientEnds := []interface{}{
		tcpnet.MakeTCPClientEnd(addrs[0]),
		tcpnet.MakeTCPClientEnd(addrs[1]),
		tcpnet.MakeTCPClientEnd(addrs[2]),
	}
	ck := kvstore.MakeClerk(clientEnds)

	var failoverDurations []time.Duration

	for t := 1; t <= trials; t++ {
		// Verify cluster is responsive
		ck.Put("health_key", fmt.Sprintf("val-%d", t))

		// Find current leader
		leaderIdx := -1
		for retry := 0; retry < 10; retry++ {
			idx, err := findLeader(addrs)
			if err == nil {
				leaderIdx = idx
				break
			}
			time.Sleep(50 * time.Millisecond)
		}

		if leaderIdx == -1 {
			log.Fatalf("Trial %d: Unable to locate active leader before kill", t)
		}

		// Kill the leader
		cmds[leaderIdx].Process.Kill()
		cmds[leaderIdx].Wait()
		killTime := time.Now()

		// Probe cluster concurrently until write succeeds with new leader
		// Issue synchronous write to measure exact end-to-end recovery time
		var duration time.Duration
		recovered := false

		putDone := make(chan bool, 1)
		go func() {
			ck.Put("probe_key", fmt.Sprintf("probe-%d", t))
			putDone <- true
		}()

		select {
		case <-putDone:
			duration = time.Since(killTime)
			recovered = true
		case <-time.After(5 * time.Second):
			recovered = false
		}

		if !recovered {
			fmt.Printf("Trial %d: FAILED (timed out after 5s)\n", t)
			continue
		}

		failoverDurations = append(failoverDurations, duration)
		fmt.Printf("Trial %2d: Leader %d killed -> failover in %v\n", t, leaderIdx, duration)

		// Resurrect the dead node to restore 3-node quorum
		cmds[leaderIdx] = startServer(binPath, leaderIdx, ports[leaderIdx], peersArg, testDir)
		time.Sleep(1 * time.Second) // Let cluster stabilize before next kill
	}

	if len(failoverDurations) == 0 {
		log.Fatal("All trials failed!")
	}

	sort.Slice(failoverDurations, func(i, j int) bool {
		return failoverDurations[i] < failoverDurations[j]
	})

	min := failoverDurations[0]
	max := failoverDurations[len(failoverDurations)-1]
	p50 := failoverDurations[len(failoverDurations)*50/100]
	p95 := failoverDurations[len(failoverDurations)*95/100]
	p99 := failoverDurations[len(failoverDurations)*99/100]

	fmt.Println("\n==================================================")
	fmt.Printf("Failover Test Results (%d successful trials / %d total)\n", len(failoverDurations), trials)
	fmt.Printf("  Min:    %v\n", min)
	fmt.Printf("  Median (p50): %v\n", p50)
	fmt.Printf("  p95:    %v\n", p95)
	fmt.Printf("  p99:    %v\n", p99)
	fmt.Printf("  Max:    %v\n", max)
	fmt.Println("==================================================")
}

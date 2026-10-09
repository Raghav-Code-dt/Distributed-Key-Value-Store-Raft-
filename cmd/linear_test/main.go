package main

import (
	"context"
	"fmt"
	"log"
	"math/rand"
	"os"
	"os/exec"
	"path/filepath"
	"sync"
	"time"

	"distributed-kv/pkg/kvstore"
	"distributed-kv/pkg/tcpnet"
	"github.com/anishathalye/porcupine"
)

// KVInput represents an input to the KV store
type KVInput struct {
	Op    uint8 // 0: Get, 1: Put, 2: Append
	Key   string
	Value string
}

// KVOutput represents an output from the KV store
type KVOutput struct {
	Value string
}

var kvModel = porcupine.Model{
	Init: func() interface{} {
		return ""
	},
	Step: func(state, input, output interface{}) (bool, interface{}) {
		s := state.(string)
		inp := input.(KVInput)
		out := output.(KVOutput)

		if inp.Op == 0 { // Get
			return s == out.Value, s
		} else if inp.Op == 1 { // Put
			return true, inp.Value
		} else if inp.Op == 2 { // Append
			return true, s + inp.Value
		}
		return false, s
	},
	DescribeOperation: func(input, output interface{}) string {
		inp := input.(KVInput)
		out := output.(KVOutput)
		switch inp.Op {
		case 0:
			return fmt.Sprintf("Get('%s') -> '%s'", inp.Key, out.Value)
		case 1:
			return fmt.Sprintf("Put('%s', '%s')", inp.Key, inp.Value)
		case 2:
			return fmt.Sprintf("Append('%s', '%s')", inp.Key, inp.Value)
		}
		return "<unknown>"
	},
}

func buildServer() string {
	binPath := filepath.Join(os.TempDir(), "kvserver")
	cmd := exec.Command("go", "build", "-o", binPath, "./cmd/kvserver")
	if err := cmd.Run(); err != nil {
		log.Fatalf("Failed to build kvserver: %v", err)
	}
	return binPath
}

func startServer(binPath string, id int, port int, peers string) *exec.Cmd {
	cmd := exec.Command(binPath, "-id", fmt.Sprintf("%d", id), "-port", fmt.Sprintf("%d", port), "-peers", peers)
	cmd.Stdout = nil // discard logs to keep it clean
	cmd.Stderr = nil
	if err := cmd.Start(); err != nil {
		log.Fatalf("Failed to start server %d: %v", id, err)
	}
	return cmd
}

func cleanData() {
	files, _ := filepath.Glob("raft-state-*.dat")
	for _, f := range files {
		os.Remove(f)
	}
}

func runTest(seed int64) bool {
	r := rand.New(rand.NewSource(seed))
	cleanData()
	defer cleanData()

	binPath := buildServer()
	defer os.Remove(binPath)

	peersArg := "localhost:8001,localhost:8002,localhost:8003"
	
	cmds := make([]*exec.Cmd, 3)
	for i := 0; i < 3; i++ {
		cmds[i] = startServer(binPath, i, 8001+i, peersArg)
	}

	defer func() {
		for _, cmd := range cmds {
			if cmd != nil && cmd.Process != nil {
				cmd.Process.Kill()
				cmd.Wait()
			}
		}
	}()

	// Wait for leader election
	time.Sleep(800 * time.Millisecond)

	peers := []interface{}{
		tcpnet.MakeTCPClientEnd("localhost:8001"),
		tcpnet.MakeTCPClientEnd("localhost:8002"),
		tcpnet.MakeTCPClientEnd("localhost:8003"),
	}

	var events []porcupine.Event
	var mu sync.Mutex
	var wg sync.WaitGroup

	numClients := 3
	opsPerClient := 10
	
	clientIdBase := r.Intn(1000)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	for c := 0; c < numClients; c++ {
		wg.Add(1)
		go func(clientID int) {
			defer wg.Done()
			clerk := kvstore.MakeClerk(peers)
			for i := 0; i < opsPerClient; i++ {
				opType := r.Intn(3)
				key := "x"
				val := fmt.Sprintf("%d", r.Intn(10))

				inp := KVInput{Op: uint8(opType), Key: key, Value: val}
				
				// invokeTime := time.Now().UnixNano()
				mu.Lock()
				events = append(events, porcupine.Event{
					Kind: porcupine.CallEvent,
					Value: inp,
					Id: int(clientID*1000 + i),
				})
				mu.Unlock()

				var out KVOutput
				
				// Run with timeout to prevent hanging on partition/crash
				done := make(chan bool, 1)
				go func() {
					if opType == 0 {
						out.Value = clerk.Get(key)
					} else if opType == 1 {
						clerk.Put(key, val)
					} else {
						clerk.Append(key, val)
					}
					done <- true
				}()
				
				select {
				case <-done:
				case <-ctx.Done():
					return // Timeout
				}

				// returnTime := time.Now().UnixNano()

				mu.Lock()
				events = append(events, porcupine.Event{
					Kind: porcupine.ReturnEvent,
					Value: out,
					Id: int(clientID*1000 + i),
				})
				mu.Unlock()
			}
		}(clientIdBase + c)
	}

	// Fault injection
	go func() {
		time.Sleep(time.Duration(r.Intn(1000)) * time.Millisecond)
		killTarget := r.Intn(3)
		if cmds[killTarget] != nil && cmds[killTarget].Process != nil {
			cmds[killTarget].Process.Kill()
			cmds[killTarget].Wait()
			cmds[killTarget] = nil
		}
	}()

	wg.Wait()
	
	// Convert events to operations using porcupine
	res := porcupine.CheckEvents(kvModel, events)
	if !res {
		fmt.Printf("Linearizability violation detected. History:\n")
		for _, e := range events {
			fmt.Printf("Event: Kind=%v, Id=%v, Value=%+v\n", e.Kind, e.Id, e.Value)
		}
	}
	return res
}

func main() {
	seed := int64(1791541684054185553)
	fmt.Printf("Reproducing failing seed: %d\n", seed)
	if runTest(seed) {
		fmt.Println("PASS (wait, it was supposed to fail!)")
	} else {
		fmt.Println("FAIL (successfully reproduced)")
	}
}

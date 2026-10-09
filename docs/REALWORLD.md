# Transitioning to a Real-World Deployment

This document outlines the architecture and technical steps required to port our Key-Value Store from the `labnet` in-memory simulator to a real-world, OS-level deployment using physical TCP Sockets and Disk I/O.

## 1. Physical Disk Persistence
**Current State:** 
The `Persister` struct in `pkg/raft/persister.go` simulates non-volatile storage by copying the Raft state (Term, VotedFor, Log entries) into an in-memory byte slice. If the OS process crashes, the memory is cleared, and the data is lost.

**Target State:**
We will implement a `DiskPersister` struct. 
*   It will take a physical file path (e.g., `raft-state-8001.dat`).
*   When `rf.persist()` is called, it will use `os.WriteFile` to synchronously write the `gob`-encoded byte array directly to the physical disk.
*   Upon booting, `Make()` will use `os.ReadFile` to hydrate the Raft instance from disk.

## 2. TCP RPC Networking
**Current State:**
The `labnet` package uses Go's `reflect` package to dynamically execute methods on registered structs. Clients send messages to a central `Network` struct, which routes them to other structs.

**Target State:**
We will use Go's standard library `net/rpc`.
*   **TCPClientEnd:** We will write a struct with a `.Call()` method that connects to a target port (e.g., `localhost:8002`) over TCP, fires an `rpc.Call`, and gracefully handles network timeout errors.
*   **RPC Server:** We will register the `KVServer` and its underlying `Raft` module with Go's `rpc.Server`, bind it to a `net.Listener`, and accept incoming TCP socket connections concurrently.

## 3. The Server Daemon (`cmd/kvserver/main.go`)
We will create a compiled binary that acts as a standalone server node.
*   **Configuration:** The binary will accept its own port and a list of peer ports via command-line arguments.
*   **Boot Sequence:**
    1. Parse CLI arguments.
    2. Instantiate the `DiskPersister`.
    3. Instantiate `TCPClientEnd` structs pointing to the peer ports.
    4. Start the `KVServer` and `Raft` modules.
    5. Boot the `net/rpc` TCP server and block forever.

## 4. The CLI Client (`cmd/kvclient/main.go`)
We will build a simple command-line interface for users to manually send requests.
*   The CLI will accept commands like `put key value` and `get key`.
*   It will instantiate the idempotent `Clerk` and pass it `TCPClientEnd` structs.
*   The `Clerk` will sweep across the real TCP ports, retrying until it hits the current Leader.

## 5. Resolved Bugs & Architecture Gotchas

### The `encoding/gob` Interface Panic
When transitioning from in-memory mock storage to physical `DiskPersister`, the server would panic and crash with an `unexpected EOF` error on the TCP socket. 
*   **Root Cause:** The `KVServer` passes generic operations (`kvstore.Op`) into the Raft log as an empty `interface{}`. When Go's `encoding/gob` attempts to serialize this to the hard drive, it crashes because it does not inherently know how to encode unregistered concrete types hidden inside generic interfaces.
*   **Resolution:** Explicitly called `gob.Register(Op{})` during server boot to preemptively map the memory layout for the disk encoder.

### Quorum Sizing & Array Index Bounds
If a node was booted with a `-peers` array that only included *other* nodes (length 2), it would panic with `index out of range` during leader election.
*   **Root Cause:** The Raft algorithm fundamentally relies on `len(peers)` to calculate the total cluster size and Quorum (`len(peers)/2`). Furthermore, it uses the server's own ID (`rf.me`) as a direct index into arrays like `nextIndex` and `matchIndex`.
*   **Resolution:** The `-peers` flag must always contain the **entire cluster's addresses**, including the server's own address, in the exact same order on every node. The internal Raft loops automatically `continue` and skip over their own `rf.me` index when broadcasting over TCP.

## 6. Benchmarking & Performance Profiling
When running the `kvclient benchmark` command against the standalone TCP daemons, the observed throughput sits at roughly **~100 ops/sec**, representing a severe drop from the **15,000 ops/sec** observed in the simulated `testing.B` harness. 

This drop perfectly isolates the fundamental latency bottlenecks of physical hardware in distributed systems:
1. **Physical Disk I/O:** The simulated `Persister` executes zero-cost memory allocations. The real `DiskPersister` executes a synchronous, blocking `os.WriteFile` flush to a physical `.dat` file on the hard drive for every single Raft state change.
2. **Synchronous TCP Handshakes:** The `TCPClientEnd` executes a fresh `rpc.Dial` for every individual RPC, resulting in a full 3-way TCP handshake over the OS loopback interface per request, compared to the simulator's zero-cost pointer passing.
3. **Serial Client Execution:** The `benchmark` loop executes sequentially, waiting for full cluster consensus and disk I/O before issuing the next command, whereas the `testing.B` suite was configured to blast thousands of concurrent requests across parallel goroutines.

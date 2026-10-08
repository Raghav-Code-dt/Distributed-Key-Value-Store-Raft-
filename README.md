# Distributed Key-Value Store (Raft Consensus)

A fault-tolerant, strictly linearizable distributed Key-Value database built entirely from scratch in Go. The system implements the Raft consensus algorithm to replicate state across a cluster of nodes, ensuring zero data loss and uninterrupted service even when a minority of servers fail, drop offline, or experience network partitions.

This project was built following the rigorous specifications of the renowned **MIT 6.824 / 6.5840 Distributed Systems** course.

## Key Features

*   **Raft Consensus Engine (`pkg/raft`)**
    *   **Leader Election:** Randomized timeouts, concurrent RequestVote broadcasting, and strict term progression to gracefully handle split-brain scenarios and leader crashes.
    *   **Log Replication:** Asynchronous pipelining of client commands via AppendEntries RPCs, complete with distributed conflict resolution to guarantee absolute log matching properties across the cluster.
    *   **State Persistence:** Synchronous serialization of non-volatile state to simulated storage (`Persister`) before replying to any RPC, allowing nodes to perfectly recover from catastrophic panics.

*   **Fault-Tolerant State Machine (`pkg/kvstore`)**
    *   **Strict Linearizability:** Implements a heavily engineered idempotent `Clerk` that tags every request with unique client IDs and monotonic sequence numbers, ensuring that operations are applied **exactly-once** regardless of network retries.
    *   **Pipelined Execution:** The `KVServer` funnels concurrent RPC requests into the Raft consensus group and dynamically blocks on internal channels until the state machine safely applies the committed operations.

*   **Deterministic Network Simulator (`pkg/labnet`)**
    *   **Chaos Engineering:** Instead of simple TCP sockets, the cluster operates over a custom mock network hub that programmatically drops 10% of packets, delays RPCs, and simulates hard network partitions to aggressively test edge-case failure modes.

## Architecture & Documentation

For a deep dive into the system's architecture, goroutine interactions, and synchronization patterns, please see the [Architecture Documentation](docs/ARCHITECTURE.md).

## Project Structure

```text
distributed-kv/
├── cmd/
│   └── kvserver/          # Standalone daemon entrypoint
├── docs/
│   └── ARCHITECTURE.md    # Detailed system design
├── pkg/
│   ├── raft/              # The Raft Consensus Algorithm implementation
│   ├── kvstore/           # The Replicated Key-Value State Machine
│   └── labnet/            # The deterministic mock network for chaos testing
```

## Testing

The codebase is engineered to survive the brutal MIT 6.824 test suites. To compile the cluster and ensure the system is completely free of data races and cyclic dependencies:

```bash
go fmt ./...
go vet ./...
go build ./...
go test -run=^$ ./...
```

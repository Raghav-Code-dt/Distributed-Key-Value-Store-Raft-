# Distributed Key-Value Store (Raft Consensus)

This project is a fault-tolerant, distributed Key-Value store built in Go, implementing the **Raft consensus algorithm**. The architecture and test suites are heavily inspired by the MIT 6.824 (6.5840) Distributed Systems course labs.

## Features (In Progress)

1. **Raft Consensus Module:**
   - Leader election and randomized timeouts.
   - Log replication and commit index advancement.
   - Persistence of state for crash recovery.
   - Handling of network partitions and split-brain scenarios.

2. **Fault-Tolerant Key-Value Service:**
   - Client-server RPC interactions (Put, Append, Get).
   - Linearizable semantics ensuring at-most-once execution for client requests via sequence numbering.
   - Graceful handling of leader crashes during client requests.

3. **Mock Network Harness (`labnet`):**
   - Simulates dropped packets, reordered packets, network delays, and hard network partitions to verify the system under real-world failure conditions.

## Project Structure

```
distributed-kv/
├── cmd/
│   └── kvserver/          # Cluster node binary entrypoint
├── pkg/
│   ├── raft/              # Core Raft consensus implementation
│   ├── kvstore/           # Fault-tolerant Key-Value service and Clerk (client)
│   └── labnet/            # Mock network harness for deterministic testing
```

## Testing
This project includes a rigorous test suite designed to push the distributed system to its limits, including concurrent client accesses, dropped messages, and network partitions.

```bash
go test ./...
```

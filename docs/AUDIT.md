# Codebase Audit & Architecture Review

This document provides an objective audit of the system's implemented features, execution environments, and known architectural constraints as of October 2026.

## 1. Implemented Features
*   **Leader Election:** Implemented via randomized timers (250-500ms) and `RequestVote` RPCs.
*   **Log Replication:** Implemented via `AppendEntries` RPCs with strict term/index consistency checks.
*   **Commit/Apply:** `commitIndex` advances upon majority replication; a background `applier` goroutine pushes committed operations to the KV state machine.
*   **Client Deduplication:** Strict Exactly-Once semantics using a unique 64-bit `ClientID` and a monotonically increasing `SeqNum`. The state machine tracks the highest `SeqNum` applied per client.
*   **Persistence:** **Partially durable.** The `DiskPersister` uses `os.WriteFile` to overwrite the entire `raft-state-X.dat` file on every state change. *Crucially, it does not explicitly call `fsync()`.* It relies on the OS page cache to flush, meaning a hard kernel panic (power loss) could lose recently "persisted" data. Additionally, rewriting the entire log on every append is an $O(N)$ operation that degrades throughput over time.
*   **Snapshots / Log Compaction:** **Not implemented.** The Raft log grows indefinitely in memory and on disk.
*   **Read Path:** **Sub-optimal but strictly correct.** `Get` requests are treated identically to `Put` requests—they are appended to the Raft log and must achieve consensus to prevent stale reads. (Optimized `ReadIndex` or Leader Leases are absent).
*   **Membership Changes:** **Not implemented.** The cluster topology (3 nodes) is statically configured at boot time.

## 2. Execution Environments
*   **Simulated Tests (`pkg/labnet`):** Runs as a single OS process with nodes as parallel goroutines communicating over a deterministic mock network that injects packet loss, delays, and partitions.
*   **Real-World (`cmd/kvserver`):** Runs as three distinct OS processes communicating over physical loopback TCP sockets (`net/rpc`), with a separate process (`cmd/kvclient`) acting as the client.

## 3. Known Bugs, TODOs, and Correctness Risks
*   **Missing Fsync (Correctness Risk):** Because we use `os.WriteFile` without a dedicated `f.Sync()`, a physical power loss immediately after a write could violate the Raft persistence guarantee.
*   **Unbounded Log Growth (OOM Risk):** Without Snapshots/Log Compaction, the server will eventually exhaust system memory and disk I/O throughput.
*   **Zombie Goroutines (TODO):** In `pkg/kvstore/server.go`, the `Kill()` method contains a `// TODO` to gracefully stop the server and its Raft node. It currently sets an atomic `dead` flag but does not close TCP listeners, leaving ports temporarily hanging.

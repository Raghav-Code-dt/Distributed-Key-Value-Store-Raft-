# System Architecture

The Distributed Key-Value Store is designed in a highly modular, deeply asynchronous fashion. The system is split into three completely decoupled layers: the Client, the Key-Value Server, and the Raft Consensus Module.

This document outlines how data flows through the system, how goroutines interact, and how concurrency is safely managed.

---

## 1. High-Level Data Flow

When a user wants to set a key (`Put("foo", "bar")`), the following pipeline executes:

1.  **Client Request:** The `Clerk` (Client) sends an RPC to the server it believes is the leader.
2.  **Server Ingestion:** The `KVServer` receives the RPC, packages it into an `Op` struct, and submits it to the local `Raft` module via `rf.Start(op)`.
3.  **Consensus:** The `Raft` leader appends the `Op` to its local log and fires off concurrent `AppendEntries` RPCs to all followers.
4.  **Commitment:** Once a majority of followers acknowledge they have written the log, the `Raft` leader updates its `commitIndex`.
5.  **Execution:** A background `applier` goroutine inside the `Raft` module notices the new commit and pushes the `Op` up a Go channel (`applyCh`).
6.  **State Modification:** A background `applier` goroutine inside the `KVServer` reads from `applyCh`, verifies the operation isn't a duplicate, modifies the local `KVStore` hash map, and sends the result down a local notification channel.
7.  **Client Reply:** The waiting RPC handler in the `KVServer` receives the result from the notification channel and replies to the `Clerk`.

---

## 2. Concurrency & Goroutine Model

The system utilizes heavy multi-threading. Every node runs several long-lived background goroutines that communicate via channels and memory-safe mutexes.

### Raft Module Goroutines
*   **Election Ticker (`ticker`):** Wakes up every 10ms to check if the `lastHeartbeat` has exceeded a randomized timeout (250-500ms). If so, it triggers an election.
*   **Heartbeat Broadcaster:** (Only runs on the Leader). Routinely fires off `AppendEntries` RPCs to maintain authority and replicate new log slices.
*   **Raft Applier (`applier`):** Continuously monitors the `commitIndex`. When the index advances, it securely packages the logs into `ApplyMsg` structs and channels them to the KV Service.

### KV Service Goroutines
*   **RPC Handlers (`Get`, `PutAppend`):** One goroutine is spawned for *every* incoming client request. They block dynamically on specific notification channels.
*   **KV Applier (`applier`):** Reads from the `applyCh` channel asynchronously. This ensures that the Raft consensus layer is strictly decoupled from the KV execution layer.

---

## 3. Strict Linearizability & Idempotency

Network unreliability forces clients to aggressively retry requests. If a Leader commits an operation but crashes before replying, the client will retry the operation on a new server. Without idempotency, this results in double-execution (violating linearizability).

To solve this:
1.  **Unique Identifiers:** The `Clerk` assigns a randomly generated, globally unique `ClientID` to itself upon initialization.
2.  **Monotonic Sequence Numbers:** Every new operation is assigned an incrementing `SeqNum`.
3.  **Deduplication Cache:** The `KVStore` maintains a table mapping `ClientID -> lastSeqNum`. Before applying an operation, the `applier` verifies that the `SeqNum` is strictly greater than the cached value. If not, the operation is completely ignored, but the cached result of the previous execution is safely returned to the client.

---

## 4. Crash Recovery (Persistence)

Nodes can violently crash at any time. To guarantee safety, specific Raft state variables (`currentTerm`, `votedFor`, and the entire `Log`) must be strictly persisted to non-volatile storage *before* the node replies to any RPC.

We utilize a mock `Persister` to simulate disk I/O. The Raft module uses Go's `encoding/gob` to securely serialize the state array. Upon booting (`Make()`), the node attempts to deserialize the bytes to gracefully resume from where it crashed.

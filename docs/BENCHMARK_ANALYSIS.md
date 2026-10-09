# Performance Bottleneck & Root-Cause Analysis

## 1. The ~10ms Latency Floor
The minimum bound of ~10.5ms per operation observed in the 1-client benchmark is caused by a **hardcoded polling loop in the background `applier()` goroutine**.

* **Root Cause & Evidence:** In [`pkg/raft/raft.go`](file:///home/raghav/Desktop/Work/Distributed/distributed-kv/pkg/raft/raft.go), the `applier()` loop checks if `commitIndex > lastApplied`. When false, instead of using an event-driven mechanism (e.g. `sync.Cond`), it sleeps for a fixed duration:
  ```go
  // pkg/raft/raft.go : applier()
  } else {
      rf.mu.Unlock()
      time.Sleep(10 * time.Millisecond) // <--- The ~10ms latency floor
  }
  ```
* **Impact:** Even if replication and consensus commit across peers in <1ms over TCP, the committed entry cannot be applied or notified to the client until the next 10ms sleep expires.

---

## 2. The ~4-5ms Serialization (~210-230 ops/s Plateau)
From 4 to 64 clients, throughput plateaus at ~210–230 ops/sec regardless of concurrency level (~4.5ms per operation).

* **Root Cause & Evidence (pprof CPU Profile):**
  A 10-second CPU profile collected via `net/http/pprof` (`go tool pprof`) under benchmark load revealed:
  - **49.85% of total CPU time (6.73s of 13.5s sampled)** is consumed by `encoding/gob.(*Encoder).encodeStruct`.
  - In [`pkg/raft/raft.go`](file:///home/raghav/Desktop/Work/Distributed/distributed-kv/pkg/raft/raft.go), `persist()` is called inside `Start()` and `AppendEntries()` while **holding `rf.mu.Lock()`**:
    ```go
    func (rf *Raft) persist() {
        w := new(bytes.Buffer)
        e := gob.NewEncoder(w)
        e.Encode(rf.currentTerm)
        e.Encode(rf.votedFor)
        e.Encode(rf.log.Entries) // <--- O(N) full log serialization under lock!
        data := w.Bytes()
        rf.persister.SaveRaftState(data)
    }
    ```
  - Without log compaction/snapshotting, `rf.log.Entries` grows monotonically with every write. Creating a fresh reflection-based `gob.NewEncoder` and re-encoding the entire growing log slice on every operation while holding `rf.mu` blocks all concurrent client proposals and heartbeats, strictly serializing throughput to ~4.5ms per op.

---

## 3. The 1.3s - 1.5s p99 Latency Tails (at 64 Clients)
At 64 clients, p99 latency spikes past 1 second (1.328s – 1.497s).

* **Root Cause & Evidence:**
  - In [`pkg/tcpnet/tcp_client.go`](file:///home/raghav/Desktop/Work/Distributed/distributed-kv/pkg/tcpnet/tcp_client.go):
    ```go
    func (e *TCPClientEnd) Call(svcMeth string, args interface{}, reply interface{}) bool {
        client, err := rpc.Dial("tcp", e.address)
        if err != nil { return false }
        defer client.Close()
        ...
    }
    ```
  - Fresh TCP handshakes (`rpc.Dial`) and teardowns (`defer client.Close()`) occur for **every RPC**, including client requests and peer-to-peer `AppendEntries` heartbeats.
  - At 64 concurrent clients plus node heartbeats, hundreds of concurrent `connect()` attempts storm the loopback interface, exceeding the kernel TCP listen backlog queue (`net.Listen` backlog).
  - When the TCP backlog overflows, Linux silently drops incoming SYN packets. The client TCP stack enters Retransmission Timeout (RTO), which defaults to **1,000ms** before retransmitting SYN. This directly introduces the ~1.3–1.5s tail latency.
  - In addition, client RPC handlers time out at 500ms (`time.After(500 * time.Millisecond)` in [`pkg/kvstore/server.go`](file:///home/raghav/Desktop/Work/Distributed/distributed-kv/pkg/kvstore/server.go)), triggering retries across non-leader nodes.

---

## 4. Verification of Durable Mode (fsync)
* **Execution & Timing:**
  Instrumented [`pkg/raft/disk_persister.go`](file:///home/raghav/Desktop/Work/Distributed/distributed-kv/pkg/raft/disk_persister.go) to track `f.Sync()` invocation count and per-call execution latency during durable mode runs.
* **Findings:**
  - `f.Sync()` was verified to be actively called on every persistence operation (5,109 calls across 2,212 operations in a 30s benchmark window).
  - Measured latency: **3.19µs – 5.47µs** (~0.004ms).
* **Conclusion on why In-Memory and Durable modes are indistinguishable:**
  Because the disk file is opened via standard OS buffering (`os.OpenFile(..., O_WRONLY|O_CREATE|O_TRUNC, 0644)`), the underlying NVMe/storage controller and OS write cache complete `f.Sync()` in single-digit microseconds. Relative to the **10ms applier sleep floor** and the **4.5ms lock-held `gob.Encode` bottleneck**, a 0.005ms `fsync` overhead is statistically undetectable in macro benchmarks.

---

## 5. Origin of the "15K ops/s" Figure
* The previously cited 15,000 ops/s was measured using the in-memory simulator harness (`pkg/raft/benchmark_test.go` via `labnet`).
* `labnet` bypasses the OS TCP network stack entirely, passing Go pointer references directly over in-process Go channels. It incurs zero TCP handshake/teardown overhead, zero serialization/deserialization cost, and operates on mock memory slices without physical file descriptor interactions.

---

## 6. Proposed Remediation Plan (Behind `-fast` Flag)
1. **Event-Driven Applier (`sync.Cond`):** Replace `time.Sleep(10 * time.Millisecond)` in `applier()` with condition variable wakeups triggered on `commitIndex` advancement.
2. **Asynchronous / Out-of-Lock Persistence:** Decouple state persistence from `rf.mu`. Snapshot/copy the Raft log slice in memory, release `rf.mu`, and encode/persist asynchronously or outside the critical lock path to unblock concurrent proposals.
3. **Persistent TCP Connection Pooling:** Update [`pkg/tcpnet/tcp_client.go`](file:///home/raghav/Desktop/Work/Distributed/distributed-kv/pkg/tcpnet/tcp_client.go) to maintain persistent `*rpc.Client` instances across RPC calls, eliminating ephemeral port churn, connection teardown overhead, and SYN drop RTO penalties.

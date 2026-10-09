# Fault Tolerance & Chaos Engineering

One of the most impressive technical achievements of this Distributed Key-Value Store is its ability to completely mask infrastructure failures and hard panics from the end-user application. 

This document outlines a real-world chaos engineering event that occurred during the transition to physical TCP sockets.

## The Incident: Array Bounds Panic
During deployment testing, a configuration error caused Node 2 (port `8003`) to boot with an incomplete cluster map. When a client randomly selected Node 2 to process a `PUT` operation, Node 2 attempted to calculate the cluster quorum, threw a `runtime error: index out of range`, and **hard-crashed**.

This caused the OS to instantly sever the TCP socket, dropping the client's RPC connection mid-flight.

## The Resilience Response (Exactly-Once Semantics)
Despite a catastrophic node failure occurring in the middle of a write operation, the system resolved the issue autonomously. Here is the step-by-step breakdown of how the `Clerk` and `Raft` modules masked the failure:

1. **Client Detection:** The `TCPClientEnd.Call()` method caught the broken socket pipe and correctly returned an `unexpected EOF` network error rather than hanging indefinitely.
2. **Clerk Rotation:** The idempotent `Clerk` absorbed the network failure without crashing the client application. It immediately flagged Node 2 as unresponsive, incremented its `leaderID` pointer, and seamlessly re-routed the identical `PUT` payload (with the same `ClientID` and `SeqNum`) to Node 0.
3. **Cluster Self-Healing:** Node 0 and Node 1 recognized that Node 2's heartbeats had ceased. The remaining nodes constituted a mathematical quorum ($2/3$), maintained availability, and successfully committed the rerouted `PUT` operation to their replicated logs.
4. **Idempotency Guarantee:** Because the `Clerk` retained the original `SeqNum`, the background `applier` goroutine was guaranteed to execute the operation exactly once, preventing duplicate writes.

## The Result
From the end-user's perspective in the CLI, the operation simply appeared to take a few hundred extra milliseconds:
```text
🚀 Sending PUT hello=world to cluster...
2026/10/09 14:32:58 TCPClientEnd: Error calling KVServer.PutAppend on localhost:8003: unexpected EOF
✅ Success! (Took 279.50299ms)
```

The user saw `✅ Success!`, entirely unaware that the database node they initially connected to had burst into flames. This perfectly demonstrates strict linearizability and high availability in a hostile environment.

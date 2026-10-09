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

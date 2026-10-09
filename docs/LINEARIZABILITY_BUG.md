# Linearizability Bug Fix Post-Mortem

During rigorous fault-injection testing with Porcupine (`cmd/linear_test`), a severe linearizability violation was discovered when leader crashes were injected at high frequencies.

## The Bug
When a client sends a `Put` or `Append` to the Leader:
1. The Leader calls `rf.Start(op)` and receives an `index`.
2. It waits on a local Go channel mapped to that `index`: `<-kv.notifyChans[index]`.
3. **The Race Condition:** If this Leader is suddenly killed (`kill -9`) or network partitioned *before* replicating the operation, a new Leader is elected. The new Leader may commit a different client's operation at that exact same `index`.
4. When the old Leader recovers (or the partition heals), its background `applier` goroutine sees the new commit at `index` and blindly pushes a generic `OpResult{Err: OK}` down `kv.notifyChans[index]`.
5. The pending RPC handler wakes up, assumes *its own* operation succeeded, and incorrectly replies `OK` to the original client.
6. The client assumes its write succeeded, but it was actually silently dropped.

## The Fix
We updated `OpResult` to carry the `ClientID` and `SeqNum` of the actually committed operation.
When the RPC handler (`Get` or `PutAppend`) wakes up, it now verifies:
```go
if result.ClientID != op.ClientID || result.SeqNum != op.SeqNum {
    reply.Err = ErrWrongLeader // Operation was overwritten
}
```
This forces the client to safely retry the operation with the new leader.

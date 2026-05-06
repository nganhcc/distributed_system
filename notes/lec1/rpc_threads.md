# RPC and Threads

## Threads

Threads are independent execution paths inside one process.

In distributed systems, threads matter because one server usually needs to:

- accept multiple network requests at the same time
- keep serving while one request is blocked on disk or network I/O
- run background work such as timeouts, retries, or lease checks

### Why Threads Exist

If a server handled every request serially, one slow client or one slow disk read would block everyone else.

Threads let the server overlap work:

- one thread can wait for RPC input
- another thread can process a client request
- another thread can scan for expired tasks or dead peers

### Main Risks

Threads make concurrency easier to express, but harder to reason about.

The common bugs are:

- race conditions
- deadlocks
- lost wakeups
- inconsistent shared state

If two threads touch the same memory, you must define who owns the data and when it may be modified.

### Shared Memory

Threads in the same process share:

- heap memory
- global variables
- file descriptors
- address space

That makes communication fast, but also dangerous.

Example:

- thread 1 increments `n`
- thread 2 increments `n`
- both read the old value
- one update gets lost

The fix is synchronization:

- mutexes
- condition variables
- channels
- atomics

### Synchronization Tools

The most common tools are:

- `mutex`: protects critical sections
- `condition variable`: waits for a state change
- `channel`: passes data or signals ownership in Go
- `wait group`: waits for a set of goroutines to finish

The rule is simple:

- shared mutable state must be protected
- data passed by message is easier to reason about than shared memory

### Threads in Distributed Systems

In distributed systems, threads are used for:

- RPC handlers
- background monitors
- retry timers
- log replication
- leader election timeouts
- garbage collection or cleanup

Typical pattern:

1. one thread listens on a socket
2. one thread handles each incoming request
3. background threads check timeouts or progress

This is exactly the kind of structure used in the MapReduce coordinator and later labs.

## RPC

RPC means Remote Procedure Call.

It lets a program call a function on another machine as if it were a local function call.

The important idea is convenience:

- local call syntax
- remote execution semantics

That convenience hides a lot of network complexity.

### RPC In Practice

An RPC system usually has:

- a client stub
- a server stub
- serialization and deserialization
- transport over TCP or Unix sockets
- timeout and retry logic

The client sends a request message.
The server receives it, runs the handler, and sends a reply.

### What RPC Hides

RPC looks like a function call, but it is not a normal function call.

It is different because:

- it can fail due to network errors
- it can arrive late
- it can be duplicated
- the server may crash mid-request
- the reply may never come back

So the caller must think about failure, not only return values.

### Failure Model

In distributed systems, RPC is usually treated as unreliable.

Common failure cases:

- request lost
- reply lost
- server crashed
- server restarted
- client timed out
- network partition

Because of that, code that uses RPC must be designed with retries and idempotence in mind.

### At-Least-Once And At-Most-Once

If the client retries, the server may see the same request more than once.

That gives you the classic choices:

- at-least-once: retry until success, but duplicates may happen
- at-most-once: prevent duplicates, harder to implement

Most lab systems use a simple at-least-once style and make handlers tolerant of duplicates.

### RPC In Go

In the 6.824 labs, RPC is used heavily with Go's `net/rpc`.

The structure is usually:

- define request and reply structs
- register a server object
- expose methods with exported names
- dial the server from the client
- call the method by name

This is the exact pattern used in MapReduce:

- workers call `Coordinator.GetTask`
- workers call `Coordinator.FinishTask`

## RPC + Threads Together

These two ideas are connected.

RPC handlers usually run in their own goroutines or threads, so multiple requests can be processed concurrently.

That creates a concurrency problem:

- many RPC handlers may touch the same shared server state
- therefore the server must use locks or another safe design

In a distributed system, the coordinator is often a shared object accessed by many concurrent RPCs.

Example:

- one worker asks for a task
- another worker reports completion
- a monitor thread requeues timed-out tasks

Without synchronization, those operations can corrupt task state.

## Distributed Systems View

RPC and threads are not just programming tools.
They are part of the system model.

Distributed systems must assume:

- processes can crash
- messages can be delayed
- messages can be duplicated
- state may be observed concurrently
- progress depends on timeouts and retries

So the design usually splits into two parts:

- communication protocol through RPC
- local concurrency control through threads and locks

## How This Appears In MapReduce

MapReduce is the clearest example in this repo.

### Coordinator

The coordinator:

- receives RPCs from workers
- stores task state
- hands out tasks
- uses a monitor thread to detect slow workers

Because multiple workers can call it at once, it must protect task maps with a mutex.

### Worker

Each worker:

- repeatedly asks for tasks over RPC
- runs map or reduce code locally
- writes intermediate files
- reports completion back to the coordinator

Workers do not share memory with each other.

That is important:

- workers communicate through files and RPC, not shared memory
- this makes the system more realistic for distributed computing

### Timeout Logic

The coordinator's timeout monitor is a background thread.

Its job is to:

- check in-progress tasks
- move stuck tasks back to pending
- let another worker retry them

This is a common distributed-systems pattern:

- assume a worker may vanish
- use timeouts to recover

### Duplicate Completion

A slow worker may finish after the coordinator already requeued the task.

That means the system may see the same task completion more than once.

The coordinator must treat that carefully:

- only the first valid completion should change state
- late completions should not corrupt the job

This is one reason task state is tracked explicitly.

## Why Threads Matter In Later Labs

The same ideas show up later in GFS, Raft, and key/value storage.

Examples:

- multiple client RPCs arrive at the same time
- a background thread applies committed log entries
- a leader thread sends heartbeats
- a lease monitor checks expiration
- a cleanup thread removes stale state

If you understand RPC and threads well, later labs become much easier to reason about.

## Practical Rules

These are the rules I want to remember:

- never trust RPC to succeed
- always think about retries and duplicate requests
- protect shared state with mutexes or another consistent design
- keep handlers short when possible
- use background threads for monitoring and retries
- make operations idempotent when duplicate execution is possible

## Summary

Threads give a server concurrency.

RPC gives a distributed system a clean communication model.

In distributed systems, they always come together:

- threads handle concurrent local execution
- RPC moves requests across machines
- locks protect server state
- timeouts and retries recover from failures

The MapReduce lab is the first place where these ideas become concrete in this repo.

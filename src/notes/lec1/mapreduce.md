# MapReduce

## What The System Does

MapReduce is a distributed batch processing model for turning a large input set into a smaller aggregated output.

The project splits work into two phases:

1. `Map` reads input data and emits intermediate key/value pairs.
2. `Reduce` groups all values for the same key and produces the final result.

The implementation in this repo is a coordinator-worker design:

- `mrcoordinator.go` starts the coordinator.
- `mrworker.go` starts a worker and loads the application plugin.
- `mr/coordinator.go` assigns tasks, tracks state, and handles timeouts.
- `mr/worker.go` executes map and reduce tasks.
- `mr/rpc.go` defines the RPC messages used between coordinator and workers.

## Core Idea

The coordinator never does the map or reduce work itself. It only:

- creates tasks
- hands tasks to workers
- tracks which tasks are pending, in progress, or finished
- reassigns tasks if a worker disappears or stalls

Workers are stateless executors. They repeatedly ask for work, process one task, report completion, and ask again.

## File Layout

Important files in this lecture:

- `main/mrcoordinator.go`: starts the coordinator with the input files and number of reduce partitions.
- `main/mrworker.go`: loads the plugin and calls the worker loop.
- `mr/coordinator.go`: task scheduler and RPC server.
- `mr/worker.go`: map/reduce execution and intermediate file handling.
- `mr/rpc.go`: shared RPC structs and the Unix socket name.
- `mrapps/wc.go`: example application plugin for word count.

## RPC Design

The coordinator and workers communicate over RPC.

The RPC layer defines two message types:

- `Args`: sent from worker to coordinator
- `Reply`: sent from coordinator to worker

The worker uses two RPC calls:

- `Coordinator.GetTask`
- `Coordinator.FinishTask`

This keeps the protocol simple:

1. worker asks for a task
2. coordinator replies with `map`, `reduce`, `wait`, or `done`
3. worker executes the task
4. worker reports completion

## Coordinator State

The coordinator keeps task state in three buckets:

- pending
- in progress
- done

It maintains these for both map tasks and reduce tasks.

This makes scheduling straightforward:

- if there is a pending map task, hand out map work
- if all maps are done, hand out reduce work
- if no work is ready but tasks are in progress, tell workers to wait
- when nothing remains, tell workers to exit

## Task Assignment

When a worker asks for work, the coordinator does this:

1. check whether the whole job is already done
2. if there is a pending map task, give it out
3. if all map tasks are finished and reduce tasks are pending, give out a reduce task
4. otherwise tell the worker to wait

Map tasks carry the input filename.
Reduce tasks carry the reduce partition index and the total number of map tasks.

## Map Worker Logic

The map worker:

1. opens the input file
2. reads all contents
3. calls the plugin’s `Map` function
4. partitions emitted key/value pairs by `ihash(key) % NReduce`
5. writes one intermediate file per reduce bucket
6. reports completion back to the coordinator

The intermediate files are named:

- `mr-mapIndex-reduceIndex`

That layout lets each reduce worker find exactly the shard it needs.

## Reduce Worker Logic

The reduce worker:

1. gets its reduce index from the coordinator
2. opens every `mr-mapIndex-reduceIndex` file for that partition
3. decodes all key/value pairs
4. sorts by key
5. groups identical keys
6. calls the plugin’s `Reduce` function
7. writes the final output file
8. reports completion back to the coordinator

The final output files are named:

- `mr-out-0`
- `mr-out-1`
- and so on

## Fault Tolerance

The coordinator uses a timeout-based retry scheme.

If a task stays in the in-progress bucket for too long, the monitor moves it back to pending. That lets another worker retry it.

Important detail:

- a slow worker may still finish after the timeout
- the coordinator accepts that late completion if the task has already been requeued. 
(toi da comment tinh nang nay, vi toi muon nhung slow worker nay du hoan thanh van khong duoc tinh trong coordinator)
This is necessary so one delayed worker does not block the whole job.

## Atomic Output

The worker writes to a temporary file first and renames it into place afterward.

That avoids partial output files being visible to other workers.

This matters because:

- reduce workers only read final intermediate files after the map phase
- final output should not appear half-written

Using temp file + rename makes the write effectively atomic.

## Plugin Loading

`mrworker.go` loads the application logic from a plugin, for example `wc.so`.

The plugin exports:

- `Map(filename, contents) []mr.KeyValue`
- `Reduce(key, values) string`

This separates the framework from the application logic:

- the framework handles scheduling and data movement
- the plugin decides how to process the data

## What I Implemented

The main implementation work in this lecture was:

- RPC message definitions for coordinator/worker communication
- task scheduling state machine in the coordinator
- timeout and retry logic
- worker-side map execution
- worker-side reduce execution
- intermediate file generation and cleanup
- final output generation
- plugin loading for different MapReduce applications

## Testing Notes

The test script runs several scenarios:

- word count correctness
- indexer correctness
- map parallelism
- reduce parallelism
- job count
- early exit
- crash recovery

On macOS, the original script expected GNU utilities like `timeout`, so I made the test harness portable instead of changing the core algorithm.

## Short Summary

The MapReduce lecture is about splitting a large data job into independent map and reduce stages, then using a coordinator to schedule those stages safely across multiple workers.

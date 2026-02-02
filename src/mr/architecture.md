# Distributed MapReduce Architecture

## Overview

```
                        +---------------+
                        |  Coordinator  |
                        |               |
                        | - tracks      |
                        |   task state  |
                        | - assigns     |
                        |   tasks       |
                        +-------+-------+
                            RPC |
               +------------+--+--+------------+
               v            v     v            v
          +--------+  +--------+ +--------+ +--------+
          |Worker 1|  |Worker 2| |Worker 3| |Worker 4|
          +--------+  +--------+ +--------+ +--------+
```

- **One coordinator** (server) listens on a Unix socket for RPC requests.
- **Many workers** (clients) connect to the coordinator, ask for tasks, execute them, and report back.

## Phases

### Phase 1 — Map

Each Map task:
1. Reads one input file (e.g. `pg-1.txt`)
2. Calls `Map(filename, contents)` -> emits key-value pairs
3. Partitions pairs into `nReduce` buckets using `ihash(key) % nReduce`
4. Writes each bucket to intermediate file `mr-X-Y` (X = map task number, Y = reduce bucket)

```
Input files:       pg-0.txt    pg-1.txt    pg-2.txt
                      |           |           |
                   Worker 1    Worker 2    Worker 3
                   Map()       Map()       Map()
                      |           |           |
                      v           v           v
Intermediate:    mr-0-0       mr-1-0       mr-2-0
                 mr-0-1       mr-1-1       mr-2-1
                 mr-0-2       mr-1-2       mr-2-2
```

### Phase 2 — Reduce (starts only after ALL maps finish)

Each Reduce task:
1. Reads all intermediate files for its partition: `mr-*-Y`
2. Sorts all key-value pairs by key
3. Groups by key, calls `Reduce(key, values)` for each group
4. Writes output to `mr-out-Y`

```
Intermediate:    mr-0-0  mr-1-0  mr-2-0     mr-0-1  mr-1-1  mr-2-1
                    \       |       /           \       |       /
                     Worker 1                    Worker 2
                     Reduce()                    Reduce()
                        |                           |
                        v                           v
Output:            mr-out-0                    mr-out-1
```

## Worker Loop

1. Ask coordinator for a task (RPC)
2. Coordinator assigns a Map or Reduce task (or says "wait" / "done")
3. Worker executes task, writes output
4. Tell coordinator the task is done (RPC)
5. Repeat

## Fault Tolerance

- If a worker doesn't finish a task within **10 seconds**, the coordinator re-assigns that task to another worker.

## File Naming

- Intermediate files: `mr-X-Y` (X = map task number, Y = reduce bucket number)
- Output files: `mr-out-Y` (Y = reduce task number)

## Key Partitioning

```
ihash("apple") % nReduce  = 0  -> bucket 0
ihash("banana") % nReduce = 1  -> bucket 1
ihash("cherry") % nReduce = 0  -> bucket 0
ihash("dog") % nReduce    = 2  -> bucket 2
```

This ensures the same key always goes to the same Reduce task, regardless of which Map task produced it.

## RPC Structure

- **Shared structs** defined in `rpc.go` — both coordinator and worker import the same package
- **Server** (coordinator): `rpc.Register(c)` exposes methods on the Coordinator struct
- **Client** (worker): `call("Coordinator.MethodName", &args, &reply)` invokes them by name
- Communication over Unix socket at `/var/tmp/5840-mr-<uid>`

## Files to Implement

| File | Role |
|------|------|
| `mr/coordinator.go` | Coordinator struct, RPC handlers, task tracking, `Done()` |
| `mr/worker.go` | Worker loop, Map/Reduce execution, `Worker()` |
| `mr/rpc.go` | Request/reply struct definitions for your RPCs |

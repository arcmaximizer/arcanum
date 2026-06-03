# Event Loop

## Overview

Arcanum processes events sequentially using an event-sourced model. Every state change is captured as a chunk in a durable log, enabling crash recovery and replay.

## Invariants

The scheduler enforces the following invariants to ensure correct state transitions:

1. **Proposal match**: The proposal being satisfied must match the first proposal in the schedule
2. **Chunk alignment (log)**: `in_log_seq` must equal the current process chunk history length
3. **Chunk alignment (event)**: `in_event_seq` must equal the current event chunk length
4. **Event continuity**: If `in_event_seq > 0`, the previous chunk must end with a `Call` syscall (events cannot end mid-chunk)
5. **Process consistency**: If `in_event_seq > 0`, the chunk's process must match the previous chunk's process
6. **Single call**: Only one `Call` syscall is allowed per receipt
7. **Call position**: `Call` must be the last syscall in the list

## Atomicity

When multiple steps are grouped together, they are atomic - either all succeed or none do. The system never has half-completed state, even if it crashes mid-operation.

## Event Processing Flow

```
loop:
    1. proposal = dequeue from schedule
    
    2. event = ensure_event_running(proposal.event)
       - if event doesn't exist, create it with status = Running
       - this step is atomic with step 1
    
    3. while chunk = execute_to_yield(event):
       - execution occurs in the executor until a yield point
       - yields happen at async boundaries (network I/O, message passing)
       
       a. add chunk to log
       
       b. if chunk is a Receipt:
          - remove proposal from schedule (atomic)
          - add receipt to history (atomic)
          - read receipt's effects vector
          
          i. if effects contain a promise:
             - route the return value to promise target
             - create new proposal for target process
             - pass return value as input to new event
             - continue from step 1 (loop)
          
          ii. if no promise:
               - event is complete
               - break (loop exits)
```

## Steps Detail

### 1. Dequeue

Get the next proposal from the process's schedule. If the schedule is empty, the process has no work to do.

### 2. Ensure Event Running

The event associated with the proposal must be in `Running` state. If it's `Pending`, transition it to `Running`. This step is atomic with dequeue to prevent race conditions.

### 3. Execute to Yield

The executor runs the event's handler until hitting a yield point. Yield points occur at:
- Network I/O (fetch, HTTP requests)
- IPC (call, notify to other processes)
- Any async operation

### 3a. Handle Receipt (Promise Case)

When a receipt contains a promise:
1. The promise identifies which process should receive the result
2. A new proposal is created for that process
3. The receipt's return value becomes the new event's input
4. Loop continues with the new proposal

### 3b. Handle Receipt (No Promise)

When a receipt has no promise, the event is complete. The event status is set to `Completed` and the loop exits.

## Durability

All state changes are persisted to the log before being considered complete. On crash recovery:
1. Read the log from disk
2. Reconstruct process state by replaying chunks
3. Resume from where execution left off

This ensures exactly-once semantics - no events are lost and no events are processed twice.

## State Transitions

```
Pending -> Running (on dequeue)
Running -> Completed (on receipt without promise)
Running -> Running (on receipt with promise -> new event)
Running -> Failed (on error)
```

### Error Handling

When execution fails, the event is marked as `Failed`. This is a userspace-level signal indicating "don't do more" - it's not the runtime's place to retry. The error can be observed and handled by the application.

### Promise Creation

A promise is created when a process makes a syscall that requires a response
- specifically a `call` to another process. The scheduler appends the promise to
a global promises list when the call occurs.

### Syscall Types

- **Proposal-completing syscalls** (`Call`) trigger the end of the current
  chunk and the creation of a receipt, completing the current proposal. The Lua
  thread is suspended until the callee returns.
- **Non-completing syscalls** (all other syscalls) yield within the current
  proposal — the Lua thread resumes immediately after the operation is handled.

## Schedule Ordering

Proposals are dequeued FIFO. The loop operates per-process, meaning each process has its own event loop.

## Process Lifecycle

Processes are like Cloudflare Durable Objects - they exist Platonically regardless of whether they've been instantiated in memory before. Any process ID can be referenced and will be created on demand. Every app has an entrypoint process addressable by the app's name.

## Promise Resolution

Promises link cause and effect across processes:

- When event A calls event B (via `call`), a promise is created
- When event B completes, its result routes back to event A
- The result becomes input to a new event in A's process
- This continues A's execution after the await point
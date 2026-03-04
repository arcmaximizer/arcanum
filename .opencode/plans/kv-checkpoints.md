# KV Database with Checkpoint-based Event Tree

## Overview

Implement a KV database on top of an event-sourced DAG with periodic checkpoints
for efficient reads. Two parallel trees (events + checkpoints) maintain
transitive relationship.

## Data Model

### Tables

```typescript
// Events - the main event tree
interface EventNode {
  id: string;
  parent: string | null; // parent event
  checkpoint_id: string | null; // covering checkpoint (most recent <= this event)
  is_head: boolean; // true if this is the current head
}

// Checkpoints - parallel tree, same branch structure
interface CheckpointNode {
  id: string;
  parent: string | null; // parent checkpoint
  event_id: string; // the event this checkpoint covers
  is_full: boolean; // true if this is a full snapshot (not incremental)
}

// KV writes - immutable log of all state changes
interface KVWrites {
  key: string;
  event_id: string;
  value: string | null; // null = delete
  checkpoint_id: string; // which checkpoint this write belongs to
}

// Head tracking
interface Head {
  id: string; // always "main" or similar
  event_id: string; // current latest event
}
```

### Invariants

1. **Transitive property**: If checkpoint A is ancestor of checkpoint B, then
   event A (checkpoint A's event_id) is ancestor of event B.
2. **Covering checkpoint**: For any event E, checkpoint_id points to the most
   recent checkpoint that is an ancestor of E.
3. **Checkpoint creation**: Explicit by caller (not automatic).

## Core Functions

### addEvent

```typescript
async function addEvent(
  trx: Kysely<Db>,
  id: string,
  parentId?: string,
  kvDiffs?: Map<string, string | null>, // state changes at this event
): Promise<void>;
```

Flow:

1. Determine parent event and parent checkpoint (inherit from parent)
2. Insert event node (checkpoint_id inherited from parent)
3. Insert KV writes for this event (associated with covering checkpoint)
4. Update head to point to new event

### createCheckpoint (explicit by caller)

```typescript
async function createCheckpoint(
  trx: Kysely<Db>,
  eventId: string,
): Promise<string>;
```

- Find parent checkpoint (from event's current checkpoint_id)
- Find all KV writes from parent checkpoint's event_id to current event_id
- Store as incremental diffs (or full if first checkpoint)
- Update event's checkpoint_id to point to new checkpoint

### Read at Event

```typescript
async function get(
  trx: Kysely<Db>,
  key: string,
  eventId?: string, // defaults to head if not specified
): Promise<string | null>;
```

Flow:

1. If eventId not provided, use head's event_id
2. Find covering checkpoint for eventId (via event.checkpoint_id)
3. If checkpoint.is_full:
   - Find kv value from checkpoint directly
4. If not full:
   - Get value from parent checkpoint
   - Apply all kv writes from checkpoint.event_id to target eventId
5. Return value (or null if deleted/not found)

### Read Multiple Keys

```typescript
async function getMany(
  trx: Kysely<Db>,
  keys: string[],
  eventId?: string,
): Promise<Map<string, string | null>>;
```

Similar to get() but batched for efficiency.

### Traversal

```typescript
async function* traverse(
  trx: Kysely<Db>,
  eventId: string,
  visitor: (key: string, value: string | null) => void,
): Promise<void>;
```

Walk from checkpoint to event, applying diffs, yielding all key-value pairs.

## Implementation Phases

### Phase 1: Schema Changes

1. Add `checkpoint_id` column to nodes table
2. Create `checkpoints` table
3. Create `kv_writes` table
4. Create `heads` table (single row)
5. Update TreeDatabase interface

### Phase 2: Basic Operations

1. Modify addNode to accept kvDiffs parameter
2. Implement getHead() / setHead()
3. Implement getNode() to return checkpoint_id

### Phase 3: Checkpoint Logic

1. Implement createCheckpoint(eventId) with incremental diffs
2. Implement updateEventCheckpoint(eventId, checkpointId)
3. Ensure new events inherit checkpoint_id from parent

### Phase 4: Read Logic

1. Implement get(key, eventId?) with checkpoint traversal
2. Implement getMany() for batch reads
3. Implement traverse() for full state iteration

## Open Questions

1. **Checkpoint storage format**: Store diffs as separate rows in kv_writes, or
   compress into a blob?
2. **Full checkpoint threshold**: When does a checkpoint become "full" instead
   of incremental?
3. **Pruning**: How to delete old checkpoints safely?

## Future Enhancements (Out of Scope)

- Read caching layer (LRU cache for frequently accessed keys)
- Checkpoint compaction (merge old incremental checkpoints into full)
- Multiple heads support (if needed later)

import { type Kysely } from "kysely";

export interface TreeDatabase {
  nodes: {
    id: string;
    parent: string | undefined;
    base: string | undefined;
    checkpoint_id: string | undefined;
  };
  checkpoints: {
    id: string;
    parent: string | undefined;
    event_id: string;
  };
  kv_writes: {
    key: string;
    event_id: string;
    value: string | null;
    checkpoint_id: string;
  };
  heads: {
    id: string;
    event_id: string;
  };
}

type State = Map<string, string | null>;

const computedStateCache = new Map<string, State>();
const refCountCache = new Map<string, number>();
const contentionCache = new Map<string, number>();

export interface TraversalState {
  readonly parent: string | null;
  readonly depth: number;
  readonly index: number;
  readonly total: number;
}

export type TreeVisitor<C> = (
  id: string,
  state: TraversalState,
  context: C,
) => void;

export async function up(db: Kysely<any>): Promise<void> {
  await db.schema
    .createTable("nodes")
    .addColumn("id", "text", (col) => col.notNull())
    .addColumn("parent", "text")
    .addColumn("base", "text")
    .addColumn("checkpoint_id", "text")
    .addPrimaryKeyConstraint("pk_nodes", ["id"])
    .execute();

  await db.schema
    .createTable("checkpoints")
    .addColumn("id", "text", (col) => col.notNull())
    .addColumn("parent", "text")
    .addColumn("event_id", "text", (col) => col.notNull())
    .addPrimaryKeyConstraint("pk_checkpoints", ["id"])
    .execute();

  await db.schema
    .createTable("kv_writes")
    .addColumn("key", "text", (col) => col.notNull())
    .addColumn("event_id", "text", (col) => col.notNull())
    .addColumn("value", "text")
    .addColumn("checkpoint_id", "text", (col) => col.notNull())
    .addPrimaryKeyConstraint("pk_kv_writes", ["key", "event_id"])
    .execute();

  await db.schema
    .createTable("heads")
    .addColumn("id", "text", (col) => col.notNull())
    .addColumn("event_id", "text", (col) => col.notNull())
    .addPrimaryKeyConstraint("pk_heads", ["id"])
    .execute();
}

export async function down(db: Kysely<any>): Promise<void> {
  await db.schema.dropTable("nodes").execute();
  await db.schema.dropTable("checkpoints").execute();
  await db.schema.dropTable("kv_writes").execute();
  await db.schema.dropTable("heads").execute();
}

export const createTreeTables = up;
export const dropTreeTables = down;

export function addContention(eventId: string): void {
  const current = contentionCache.get(eventId) ?? 0;
  contentionCache.set(eventId, current + 1);
}

export function removeContention(eventId: string): void {
  const current = contentionCache.get(eventId) ?? 0;
  if (current <= 1) {
    contentionCache.delete(eventId);
  } else {
    contentionCache.set(eventId, current - 1);
  }
  tryEvict(eventId);
}

export function cacheState(eventId: string, state: State): void {
  computedStateCache.set(eventId, state);
}

export function getCachedState(eventId: string): State | undefined {
  return computedStateCache.get(eventId);
}

function tryEvict(eventId: string): void {
  const refCount = refCountCache.get(eventId) ?? 0;
  const contention = contentionCache.get(eventId) ?? 0;

  if (refCount === 0 && contention === 0) {
    computedStateCache.delete(eventId);
    refCountCache.delete(eventId);
    contentionCache.delete(eventId);
  }
}

export function incrementRefCount(eventId: string): void {
  const current = refCountCache.get(eventId) ?? 0;
  refCountCache.set(eventId, current + 1);
}

export function decrementRefCount(eventId: string): void {
  const current = refCountCache.get(eventId) ?? 0;
  if (current <= 1) {
    refCountCache.delete(eventId);
  } else {
    refCountCache.set(eventId, current - 1);
  }
  tryEvict(eventId);
}

export async function addNode(
  trx: Kysely<TreeDatabase>,
  id: string,
  parent?: string,
  kvDiffs?: Map<string, string | null>,
  base?: string,
): Promise<void> {
  const parentNode = parent
    ? await trx
      .selectFrom("nodes")
      .select("checkpoint_id")
      .where("id", "=", parent)
      .executeTakeFirst()
    : null;

  let checkpointId: string | undefined = parentNode?.checkpoint_id ?? undefined;

  await trx
    .insertInto("nodes")
    .values({ id, parent, base: base ?? parent, checkpoint_id: checkpointId })
    .onConflict((oc) => oc.column("id").doNothing())
    .execute();

  if (kvDiffs && kvDiffs.size > 0) {
    if (!checkpointId) {
      checkpointId = await createCheckpoint(trx, id);
    }

    const writes = Array.from(kvDiffs.entries()).map(([key, value]) => ({
      key,
      event_id: id,
      value,
      checkpoint_id: checkpointId!,
    }));
    await trx.insertInto("kv_writes").values(writes).execute();
  }

  await setHead(trx, id);
}

export async function getNode(
  trx: Kysely<TreeDatabase>,
  id: string,
): Promise<boolean> {
  const row = await trx
    .selectFrom("nodes")
    .select("id")
    .where("id", "=", id)
    .executeTakeFirst();
  return row !== undefined;
}

export async function addChild(
  trx: Kysely<TreeDatabase>,
  parent: string,
  child: string,
): Promise<void> {
  const existingChild = await trx
    .selectFrom("nodes")
    .select("parent")
    .where("id", "=", child)
    .executeTakeFirst();

  if (existingChild?.parent !== undefined && existingChild.parent !== null) {
    if (existingChild.parent === parent) {
      return;
    }
    throw new Error(
      `Node [${child}] already has a parent [${existingChild.parent}]. Trees require single parent.`,
    );
  }

  await trx
    .insertInto("nodes")
    .values({ id: child, parent, checkpoint_id: undefined })
    .onConflict((oc) => oc.column("id").doUpdateSet({ parent }))
    .execute();
}

export async function getParent(
  trx: Kysely<TreeDatabase>,
  childId: string,
): Promise<string | null> {
  const row = await trx
    .selectFrom("nodes")
    .select("parent")
    .where("id", "=", childId)
    .executeTakeFirst();
  return row?.parent ?? null;
}

export async function getChildren(
  trx: Kysely<TreeDatabase>,
  parentId: string,
): Promise<string[]> {
  const rows = await trx
    .selectFrom("nodes")
    .select("id")
    .where("parent", "=", parentId)
    .execute();
  return rows.map((r) => r.id);
}

export async function getHead(
  trx: Kysely<TreeDatabase>,
  treeId?: string,
): Promise<string | null> {
  const row = await trx
    .selectFrom("heads")
    .select("event_id")
    .where("id", "=", treeId ?? "main")
    .executeTakeFirst();
  return row?.event_id ?? null;
}

export async function getHeads(
  trx: Kysely<TreeDatabase>,
): Promise<Map<string, string>> {
  const rows = await trx
    .selectFrom("heads")
    .selectAll()
    .execute();
  const result = new Map<string, string>();
  for (const row of rows) {
    result.set(row.id, row.event_id);
  }
  return result;
}

export async function setHead(
  trx: Kysely<TreeDatabase>,
  eventId: string,
  treeId?: string,
): Promise<void> {
  await trx
    .insertInto("heads")
    .values({ id: treeId ?? "main", event_id: eventId })
    .onConflict((oc) => oc.column("id").doUpdateSet({ event_id: eventId }))
    .execute();
}

export async function nodes(
  trx: Kysely<TreeDatabase>,
): Promise<string[]> {
  const rows = await trx
    .selectFrom("nodes")
    .select("id")
    .execute();
  return rows.map((r) => r.id);
}

export async function createCheckpoint(
  trx: Kysely<TreeDatabase>,
  eventId: string,
): Promise<string> {
  const event = await trx
    .selectFrom("nodes")
    .selectAll()
    .where("id", "=", eventId)
    .executeTakeFirst();

  if (!event) {
    throw new Error(`Event ${eventId} not found`);
  }

  if (event.checkpoint_id) {
    return event.checkpoint_id;
  }

  const parentCheckpointId = event.checkpoint_id ?? null;

  const parentCheckpoint = parentCheckpointId
    ? await trx
      .selectFrom("checkpoints")
      .select("event_id")
      .where("id", "=", parentCheckpointId)
      .executeTakeFirst()
    : null;

  const parentEventId = parentCheckpoint?.event_id ?? null;

  const checkpointId = `checkpoint_${eventId}_${Date.now()}`;

  await trx
    .insertInto("checkpoints")
    .values({
      id: checkpointId,
      parent: parentCheckpointId ?? undefined,
      event_id: eventId,
    })
    .execute();

  await trx
    .updateTable("nodes")
    .set({ checkpoint_id: checkpointId })
    .where("id", "=", eventId)
    .execute();

  return checkpointId;
}

export async function get(
  trx: Kysely<TreeDatabase>,
  key: string,
  eventId?: string,
): Promise<string | null> {
  const targetEventId = eventId ?? (await getHead(trx));
  if (!targetEventId) return null;

  const cachedState = computedStateCache.get(targetEventId);
  if (cachedState) {
    return cachedState.get(key) ?? null;
  }

  const event = await trx
    .selectFrom("nodes")
    .select("checkpoint_id")
    .where("id", "=", targetEventId)
    .executeTakeFirst();

  if (!event) return null;

  const checkpointId = event.checkpoint_id;
  if (!checkpointId) return null;

  const checkpoint = await trx
    .selectFrom("checkpoints")
    .selectAll()
    .where("id", "=", checkpointId)
    .executeTakeFirst();

  if (!checkpoint) return null;

  const checkpointEventId = checkpoint.event_id;

  const writes = await trx
    .selectFrom("kv_writes")
    .selectAll()
    .where("key", "=", key)
    .where("event_id", ">", checkpointEventId)
    .where("event_id", "<=", targetEventId)
    .orderBy("event_id", "asc")
    .execute();

  if (writes.length === 0) {
    const priorWrite = await trx
      .selectFrom("kv_writes")
      .selectAll()
      .where("key", "=", key)
      .where("event_id", "<=", checkpointEventId)
      .orderBy("event_id", "desc")
      .executeTakeFirst();
    return priorWrite?.value ?? null;
  }

  const latestWrite = writes[writes.length - 1];
  return latestWrite?.value ?? null;
}

export async function getMany(
  trx: Kysely<TreeDatabase>,
  keys: string[],
  eventId?: string,
): Promise<Map<string, string | null>> {
  const result = new Map<string, string | null>();

  for (const key of keys) {
    result.set(key, await get(trx, key, eventId));
  }

  return result;
}

export async function* traverseState(
  trx: Kysely<TreeDatabase>,
  eventId?: string,
): AsyncGenerator<[string, string | null], void, unknown> {
  const targetEventId = eventId ?? (await getHead(trx));
  if (!targetEventId) return;

  const event = await trx
    .selectFrom("nodes")
    .select("checkpoint_id")
    .where("id", "=", targetEventId)
    .executeTakeFirst();

  if (!event || !event.checkpoint_id) return;

  const checkpoint = await trx
    .selectFrom("checkpoints")
    .select("event_id")
    .where("id", "=", event.checkpoint_id)
    .executeTakeFirst();

  if (!checkpoint) return;

  const checkpointEventId = checkpoint.event_id;

  const allWrites = await trx
    .selectFrom("kv_writes")
    .selectAll()
    .where("event_id", "<=", targetEventId)
    .orderBy("event_id", "asc")
    .execute();

  const state = new Map<string, string | null>();

  for (const write of allWrites) {
    if (write.value === null) {
      state.delete(write.key);
    } else {
      state.set(write.key, write.value);
    }
  }

  for (const [key, value] of state) {
    yield [key, value];
  }
}

export async function topologicalSort(
  trx: Kysely<TreeDatabase>,
): Promise<string[]> {
  const roots = await getRoots(trx);
  if (roots.length === 0) return [];

  const sorted: string[] = [];
  const visited = new Set<string>();

  async function visit(nodeId: string): Promise<void> {
    if (visited.has(nodeId)) return;
    visited.add(nodeId);

    sorted.push(nodeId);

    const children = await getChildren(trx, nodeId);
    for (const child of children) {
      await visit(child);
    }
  }

  for (const root of roots) {
    await visit(root);
  }

  return sorted;
}

async function getRoots(
  trx: Kysely<TreeDatabase>,
): Promise<string[]> {
  const rows = await trx
    .selectFrom("nodes")
    .select("id")
    .where("parent", "is", null)
    .execute();
  return rows.map((r) => r.id);
}

export async function traverse<C>(
  trx: Kysely<TreeDatabase>,
  visitor: TreeVisitor<C>,
  context: C,
): Promise<void> {
  const roots = await getRoots(trx);
  if (roots.length === 0) return;

  const outgoing = await loadOutgoing(trx);

  function visitNode(
    id: string,
    parent: string | null,
    depth: number,
    index: number,
    total: number,
  ): void {
    visitor(id, { parent, depth, index, total }, context);

    const children = outgoing.get(id) ?? [];
    children.forEach((childId, i) => {
      visitNode(childId, id, depth + 1, i, children.length);
    });
  }

  roots.forEach((root, i) => {
    visitNode(root, null, 0, i, roots.length);
  });
}

export async function traverseFrom<C>(
  trx: Kysely<TreeDatabase>,
  nodeId: string,
  visitor: TreeVisitor<C>,
  context: C,
): Promise<void> {
  const outgoing = await loadOutgoing(trx);

  const childCount = (outgoing.get(nodeId) ?? []).length;

  function visitNode(
    id: string,
    parent: string | null,
    depth: number,
    index: number,
    total: number,
  ): void {
    visitor(id, { parent, depth, index, total }, context);

    const children = outgoing.get(id) ?? [];
    children.forEach((childId, i) => {
      visitNode(childId, id, depth + 1, i, children.length);
    });
  }

  visitNode(nodeId, null, 0, 0, childCount);
}

async function loadOutgoing(
  trx: Kysely<TreeDatabase>,
): Promise<Map<string, string[]>> {
  const allNodes = await trx.selectFrom("nodes").selectAll().execute();

  const out = new Map<string, string[]>();
  for (const { id, parent } of allNodes) {
    if (parent) {
      let arr = out.get(parent);
      if (!arr) {
        arr = [];
        out.set(parent, arr);
      }
      arr.push(id);
    }
  }
  return out;
}

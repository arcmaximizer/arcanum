import { type Kysely } from "kysely";

export interface DAGDatabase {
  nodes: {
    id: string;
  };
  edges: {
    from_id: string;
    to_id: string;
  };
}

export interface TraversalState {
  readonly parent: string | null;
  readonly depth: number;
  readonly index: number;
  readonly total: number;
}

export type DAGVisitor<C> = (
  id: string,
  state: TraversalState,
  context: C,
) => void;

export async function createDAGTables(
  db: Kysely<any>,
): Promise<void> {
  await db.schema
    .createTable("nodes")
    .addColumn("id", "text", (col) => col.notNull())
    .addPrimaryKeyConstraint("pk_nodes", ["id"])
    .execute();

  await db.schema
    .createTable("edges")
    .addColumn("from_id", "text", (col) => col.notNull())
    .addColumn("to_id", "text", (col) => col.notNull())
    .addPrimaryKeyConstraint("pk_edges", ["from_id", "to_id"])
    .addForeignKeyConstraint(
      "fk_edges_from_id",
      ["from_id"],
      "nodes",
      ["id"],
    )
    .addForeignKeyConstraint(
      "fk_edges_to_id",
      ["to_id"],
      "nodes",
      ["id"],
    )
    .execute();
}

export async function dropDAGTables(
  db: Kysely<any>,
): Promise<void> {
  await db.schema.dropTable("edges").execute();
  await db.schema.dropTable("nodes").execute();
}

export async function addNode(
  trx: Kysely<DAGDatabase>,
  id: string,
): Promise<void> {
  await trx
    .insertInto("nodes")
    .values({ id })
    .onConflict((oc) => oc.column("id").doNothing())
    .execute();
}

export async function getNode(
  trx: Kysely<DAGDatabase>,
  id: string,
): Promise<boolean> {
  const row = await trx
    .selectFrom("nodes")
    .select("id")
    .where("id", "=", id)
    .executeTakeFirst();
  return row !== undefined;
}

export async function addEdge(
  trx: Kysely<DAGDatabase>,
  from: string,
  to: string,
): Promise<void> {
  if (from === to) {
    throw new Error(`Adding edge [${from}] -> [${to}] would create a cycle`);
  }

  const [fromNode, toNode] = await Promise.all([
    trx
      .selectFrom("nodes")
      .selectAll()
      .where("id", "=", from)
      .executeTakeFirst(),
    trx
      .selectFrom("nodes")
      .selectAll()
      .where("id", "=", to)
      .executeTakeFirst(),
  ]);

  if (!fromNode) {
    await trx
      .insertInto("nodes")
      .values({ id: from })
      .execute();
  }

  if (!toNode) {
    await trx
      .insertInto("nodes")
      .values({ id: to })
      .execute();
  }

  await trx
    .insertInto("edges")
    .values({ from_id: from, to_id: to })
    .onConflict((oc) => oc.columns(["from_id", "to_id"]).doNothing())
    .execute();
}

export async function roots(
  trx: Kysely<DAGDatabase>,
): Promise<string[]> {
  const rows = await trx
    .selectFrom("nodes")
    .select("nodes.id")
    .leftJoin("edges", "nodes.id", "edges.to_id")
    .where("edges.to_id", "is", null)
    .execute();

  return rows.map((r) => r.id);
}

export async function nodes(
  trx: Kysely<DAGDatabase>,
): Promise<string[]> {
  const rows = await trx
    .selectFrom("nodes")
    .select("id")
    .execute();

  return rows.map((r) => r.id);
}

export async function topologicalSort(
  trx: Kysely<DAGDatabase>,
): Promise<string[]> {
  const rootIds = await roots(trx);

  const sorted: string[] = [];
  const visited = new Set<string>();

  async function visit(nodeId: string, depth: number): Promise<void> {
    if (visited.has(nodeId)) return;
    visited.add(nodeId);

    const children = await trx
      .selectFrom("edges")
      .select("to_id")
      .where("from_id", "=", nodeId)
      .execute();

    for (const child of children) {
      await visit(child.to_id, depth + 1);
    }

    sorted.push(nodeId);
  }

  for (const rootId of rootIds) {
    await visit(rootId, 0);
  }

  for (const row of await trx.selectFrom("nodes").select("id").execute()) {
    await visit(row.id, 0);
  }

  return sorted;
}

export async function traverse<C>(
  trx: Kysely<DAGDatabase>,
  visitor: DAGVisitor<C>,
  context: C,
): Promise<void> {
  const rootIds = await roots(trx);

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

  rootIds.forEach((rootId, i) => {
    visitNode(rootId, null, 0, i, rootIds.length);
  });
}

export async function traverseFrom<C>(
  trx: Kysely<DAGDatabase>,
  nodeId: string,
  visitor: DAGVisitor<C>,
  context: C,
): Promise<void> {
  const outgoing = await loadOutgoing(trx);

  const childCount = await trx
    .selectFrom("edges")
    .select((eb) => eb.fn.count<number>("to_id").as("count"))
    .where("from_id", "=", nodeId)
    .executeTakeFirst();

  const total = childCount?.count ?? 0;

  function visitNode(
    id: string,
    parent: string | null,
    depth: number,
    index: number,
  ): void {
    visitor(id, { parent, depth, index, total }, context);

    const children = outgoing.get(id) ?? [];
    children.forEach((childId, i) => {
      visitNode(childId, id, depth + 1, i);
    });
  }

  visitNode(nodeId, null, 0, 0);
}

async function loadOutgoing(
  trx: Kysely<DAGDatabase>,
): Promise<Map<string, string[]>> {
  const edges = await trx.selectFrom("edges").selectAll().execute();

  const out = new Map<string, string[]>();
  for (const { from_id, to_id } of edges) {
    let arr = out.get(from_id);
    if (!arr) {
      arr = [];
      out.set(from_id, arr);
    }
    arr.push(to_id);
  }
  return out;
}

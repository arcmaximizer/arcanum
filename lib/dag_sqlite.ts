import { type Kysely } from "kysely";

export interface TreeDatabase {
  nodes: {
    id: string;
  };
  edges: {
    parent_id: string;
    child_id: string;
  };
  heads: {
    id: string;
  };
}

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
    .addPrimaryKeyConstraint("pk_nodes", ["id"])
    .execute();

  await db.schema
    .createTable("edges")
    .addColumn("parent_id", "text", (col) => col.notNull())
    .addColumn("child_id", "text", (col) => col.notNull())
    .addPrimaryKeyConstraint("pk_edges", ["parent_id", "child_id"])
    .addForeignKeyConstraint(
      "fk_edges_parent_id",
      ["parent_id"],
      "nodes",
      ["id"],
    )
    .addForeignKeyConstraint(
      "fk_edges_child_id",
      ["child_id"],
      "nodes",
      ["id"],
    )
    .execute();

  await db.schema
    .createTable("heads")
    .addColumn("id", "text", (col) => col.notNull().unique())
    .addPrimaryKeyConstraint("pk_heads", ["id"])
    .addForeignKeyConstraint(
      "fk_heads_id",
      ["id"],
      "nodes",
      ["id"],
    )
    .execute();
}

export async function down(db: Kysely<any>): Promise<void> {
  await db.schema.dropTable("heads").execute();
  await db.schema.dropTable("edges").execute();
  await db.schema.dropTable("nodes").execute();
}

export async function addNode(
  trx: Kysely<TreeDatabase>,
  id: string,
): Promise<void> {
  await trx
    .insertInto("nodes")
    .values({ id })
    .onConflict((oc) => oc.column("id").doNothing())
    .execute();
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

export async function setHead(
  trx: Kysely<TreeDatabase>,
  id: string,
): Promise<void> {
  const node = await trx
    .selectFrom("nodes")
    .selectAll()
    .where("id", "=", id)
    .executeTakeFirst();

  if (!node) {
    await trx
      .insertInto("nodes")
      .values({ id })
      .execute();
  }

  await trx
    .insertInto("heads")
    .values({ id })
    .onConflict((oc) => oc.column("id").doNothing())
    .execute();
}

export async function getHead(
  trx: Kysely<TreeDatabase>,
): Promise<string | null> {
  const row = await trx
    .selectFrom("heads")
    .select("id")
    .executeTakeFirst();
  return row?.id ?? null;
}

export async function addChild(
  trx: Kysely<TreeDatabase>,
  parent: string,
  child: string,
): Promise<void> {
  const existingEdge = await trx
    .selectFrom("edges")
    .select("parent_id")
    .where("child_id", "=", child)
    .where("parent_id", "=", parent)
    .executeTakeFirst();

  if (existingEdge) {
    return;
  }

  const existingParent = await trx
    .selectFrom("edges")
    .select("parent_id")
    .where("child_id", "=", child)
    .executeTakeFirst();

  if (existingParent) {
    throw new Error(
      `Node [${child}] already has a parent [${existingParent.parent_id}]. Trees require single parent.`,
    );
  }

  const [parentNode, childNode] = await Promise.all([
    trx
      .selectFrom("nodes")
      .selectAll()
      .where("id", "=", parent)
      .executeTakeFirst(),
    trx
      .selectFrom("nodes")
      .selectAll()
      .where("id", "=", child)
      .executeTakeFirst(),
  ]);

  if (!parentNode) {
    await trx
      .insertInto("nodes")
      .values({ id: parent })
      .execute();
  }

  if (!childNode) {
    await trx
      .insertInto("nodes")
      .values({ id: child })
      .execute();
  }

  await trx
    .insertInto("edges")
    .values({ parent_id: parent, child_id: child })
    .onConflict((oc) => oc.columns(["parent_id", "child_id"]).doNothing())
    .execute();
}

export async function getParent(
  trx: Kysely<TreeDatabase>,
  childId: string,
): Promise<string | null> {
  const row = await trx
    .selectFrom("edges")
    .select("parent_id")
    .where("child_id", "=", childId)
    .executeTakeFirst();
  return row?.parent_id ?? null;
}

export async function getChildren(
  trx: Kysely<TreeDatabase>,
  parentId: string,
): Promise<string[]> {
  const rows = await trx
    .selectFrom("edges")
    .select("child_id")
    .where("parent_id", "=", parentId)
    .execute();
  return rows.map((r) => r.child_id);
}

export async function heads(
  trx: Kysely<TreeDatabase>,
): Promise<string[]> {
  const rows = await trx
    .selectFrom("heads")
    .select("id")
    .execute();
  return rows.map((r) => r.id);
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

export async function topologicalSort(
  trx: Kysely<TreeDatabase>,
): Promise<string[]> {
  const headId = await getHead(trx);
  if (!headId) return [];

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

  await visit(headId);

  return sorted;
}

export async function traverse<C>(
  trx: Kysely<TreeDatabase>,
  visitor: TreeVisitor<C>,
  context: C,
): Promise<void> {
  const headId = await getHead(trx);
  if (!headId) return;

  const outgoing = await loadOutgoing(trx);

  const childCount = (outgoing.get(headId) ?? []).length;

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

  visitNode(headId, null, 0, 0, childCount);
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
  const edges = await trx.selectFrom("edges").selectAll().execute();

  const out = new Map<string, string[]>();
  for (const { parent_id, child_id } of edges) {
    let arr = out.get(parent_id);
    if (!arr) {
      arr = [];
      out.set(parent_id, arr);
    }
    arr.push(child_id);
  }
  return out;
}

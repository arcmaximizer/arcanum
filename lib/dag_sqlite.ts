import { type Kysely } from "kysely";

export interface TreeDatabase {
  nodes: {
    id: string;
    parent: string | undefined;
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
    .addColumn("parent", "text")
    .addPrimaryKeyConstraint("pk_nodes", ["id"])
    .execute();
}

export async function down(db: Kysely<any>): Promise<void> {
  await db.schema.dropTable("nodes").execute();
}

export async function addNode(
  trx: Kysely<TreeDatabase>,
  id: string,
  parent?: string,
): Promise<void> {
  await trx
    .insertInto("nodes")
    .values({ id, parent })
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
    throw new Error(
      `Node [${child}] already has a parent [${existingChild.parent}]. Trees require single parent.`,
    );
  }

  await trx
    .insertInto("nodes")
    .values({ id: child, parent })
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

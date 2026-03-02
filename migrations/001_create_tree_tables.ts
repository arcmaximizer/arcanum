import { up, down } from "../lib/dag_sqlite.ts";
import { Kysely } from "kysely";

export { up, down };

export async function migrate(db: Kysely<any>): Promise<void> {
  await up(db);
}

export async function rollback(db: Kysely<any>): Promise<void> {
  await down(db);
}

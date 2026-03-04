// Immutable store that allows content referencing using unique IDs
// Fetch app code and the data of any given event
// Also handles querying of state at certain checkpoints

import { ProgramId, UUID } from "../lib/types.ts";
import { ArcanumDB } from "../lib/db.ts";
import { Kysely } from "kysely";

export class AppState {
  id: ProgramId;
  db: Kysely<ArcanumDB>;

  constructor(id: ProgramId, db: Kysely<ArcanumDB>) {
    this.id = id;
    this.db = db;
  }

  // lazy get
  async get(node: UUID, key: string, tx?: Kysely<ArcanumDB>): Promise<string> {
    const db = tx ?? this.db;

    const res = await db.selectFrom("state_diffs").select("value").where(
      "key",
      "=",
      key,
    ).where("checkpoint", "=", node).where("app", "=", this.id).execute();

    if (res.length != 0) return res[0].value;
    return "";
  }

  set(node: UUID, key: string, value: string) {
  }

  // set except it ignores whether this node already has the key set
  setDangerously(node: UUID, key: string, value: string) {
  }

  checkpoint(node: UUID) {
  }
}

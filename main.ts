import { Kysely } from "kysely";
import { Database } from "@db/sqlite";
import { DenoSqliteDialect } from "./lib/db_adapter.ts";
import { createTreeTables } from "./svc/store.ts";
import type { TreeDatabase } from "./svc/store.ts";

const dbPath = process.env.DB_FILE ?? "./arcanum.db";

class Arcanum {
  #db: Kysely<TreeDatabase>;

  constructor() {
    const db = new Database(dbPath);

    this.#db = new Kysely<TreeDatabase>({
      dialect: new DenoSqliteDialect({
        database: db,
        onCreateConnection: async (conn) => {
          await conn.executeQuery(
            {
              sql: "PRAGMA foreign_keys = ON",
              parameters: [],
              queryId: { queryId: "" },
              query: { kind: "RawNode" } as any,
            },
          );
        },
      }),
    });
  }

  getDb(): Kysely<TreeDatabase> {
    return this.#db;
  }

  async #migrate() {
    await createTreeTables(this.#db);
    console.log("Database initialized");
  }

  async start() {
    await this.#migrate();
    console.log("Arcanum started");
  }
}

const arcanum = new Arcanum();
await arcanum.start();

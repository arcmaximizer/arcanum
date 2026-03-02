import { Kysely, Migrator, FileMigrationProvider } from "kysely";
import { Database } from "@db/sqlite";
import { DenoSqliteDialect } from "./lib/db_adapter.ts";
import { ArcanumDB } from "./lib/db.ts";
import * as path from "node:path";

const dbPath = "./arcanum.db";

class Arcanum {
  #db: Kysely<ArcanumDB>;

  constructor() {
    const db = new Database(dbPath);

    this.#db = new Kysely<ArcanumDB>({
      dialect: new DenoSqliteDialect({
        database: db,
        onCreateConnection: async (conn) => {
          await conn.executeQuery(
            { sql: "PRAGMA foreign_keys = ON", parameters: [], queryId: { queryId: "" }, query: { kind: "RawNode" } as any },
          );
        },
      }),
    });
  }

  async #migrate() {
    const migrator = new Migrator({
      db: this.#db,
      provider: new FileMigrationProvider({
        fs: {
          async readdir(dirPath: string) {
            return Array.from(Deno.readDirSync(dirPath)).map((e) => e.name);
          },
        },
        path,
        migrationFolder: path.join(import.meta.dirname ?? ".", "migrations"),
      }),
    });

    const { error, results } = await migrator.migrateToLatest();

    if (error) {
      console.error("Migration failed:", error);
      Deno.exit(1);
    }

    results?.forEach((it) => {
      if (it.status === "Success") {
        console.log(`Migration "${it.migrationName}" was executed successfully`);
      } else if (it.status === "Error") {
        console.error(`Failed to execute migration "${it.migrationName}"`);
      }
    });
  }

  async start() {
    await this.#migrate();
    console.log("Arcanum started");
  }
}

const arcanum = new Arcanum();
await arcanum.start();

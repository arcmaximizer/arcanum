import { Kysely } from "kysely";
export { Database as Sqlite } from "@db/sqlite";
import { DenoSqlite3Dialect } from "@soapbox/kysely-deno-sqlite";

const db = new Kysely({
  dialect: new DenoSqlite3Dialect({
    database: new Sqlite("db.sqlite3"),
  }),
});
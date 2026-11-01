import { DatabaseSync } from "node:sqlite";

const [dbPath, holdMs] = process.argv.slice(2);
if (!dbPath || !holdMs) {
  process.stderr.write("usage: hold-lock.ts <db> <hold-ms>\n");
  process.exit(2);
}

const db = new DatabaseSync(dbPath);
db.exec("BEGIN IMMEDIATE;");
process.stdout.write("locked\n");
Atomics.wait(new Int32Array(new SharedArrayBuffer(4)), 0, 0, Number(holdMs));
db.exec("COMMIT;");
db.close();

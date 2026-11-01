import assert from "node:assert/strict";
import { spawn } from "node:child_process";
import { join } from "node:path";
import { describe, it } from "node:test";
import { fileURLToPath } from "node:url";
import { beginImmediate, inTransaction, openDatabase } from "../src/schema.ts";
import { tempDir } from "./helpers.ts";

const holder = fileURLToPath(new URL("./fixtures/hold-lock.ts", import.meta.url));

function holdLock(path: string, holdMs: number) {
  const child = spawn(process.execPath, ["--no-warnings=ExperimentalWarning", holder, path, String(holdMs)]);
  const locked = new Promise<void>((resolve, reject) => {
    child.stdout.on("data", (chunk) => {
      if (String(chunk).includes("locked")) resolve();
    });
    child.on("error", reject);
  });
  const exited = new Promise<number | null>((resolve) => child.on("close", resolve));
  return { locked, exited };
}

describe("a write lock held by another process", () => {
  it("waits for the lock instead of failing the transaction", async (t) => {
    const tmp = tempDir();
    t.after(tmp.cleanup);
    const path = join(tmp.dir, "outbox.sqlite");
    const db = openDatabase(path);
    db.exec("PRAGMA busy_timeout = 20;");
    const held = holdLock(path, 1_500);
    await held.locked;

    assert.throws(() => db.exec("BEGIN IMMEDIATE;"), /locked/);
    assert.throws(() => beginImmediate(db, 50), /locked/);

    inTransaction(db, () => db.exec("CREATE TABLE lock_probe (x INTEGER);"));
    const probe = db.prepare("SELECT name FROM sqlite_master WHERE name = 'lock_probe'").get();
    assert.deepEqual({ ...probe }, { name: "lock_probe" });
    assert.equal(await held.exited, 0);
    db.close();
  });

  it("still raises an error that isn't a busy lock", (t) => {
    const tmp = tempDir();
    t.after(tmp.cleanup);
    const db = openDatabase(join(tmp.dir, "outbox.sqlite"));
    db.exec("BEGIN;");
    assert.throws(() => beginImmediate(db, 1_000), /within a transaction/);
    db.exec("ROLLBACK;");
    db.close();
  });
});

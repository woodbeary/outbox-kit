import assert from "node:assert/strict";
import { join } from "node:path";
import { after, before, describe, it } from "node:test";
import { main } from "../src/cli.ts";
import { createOutbox } from "../src/outbox.ts";
import { openDatabase } from "../src/schema.ts";
import { OK, START, fakeClock, http, scripted, tempDir } from "./helpers.ts";

const tmp = tempDir();
const path = join(tmp.dir, "outbox.sqlite");

async function run(...argv: string[]) {
  const out: string[] = [];
  const err: string[] = [];
  const code = await main(argv, { out: (text) => out.push(text), err: (text) => err.push(text) });
  return { code, out: out.join("\n"), err: err.join("\n") };
}

before(async () => {
  const time = fakeClock();
  const db = openDatabase(path);
  const crm = scripted([OK, http(422), { kind: "unknown", detail: "TimeoutError" }]);
  const outbox = createOutbox({ db, clock: time.clock, destinations: { crm: crm.destination } });
  for (const key of ["checkin:001", "checkin:002", "checkin:003"]) {
    outbox.enqueue({ destination: "crm", key, payload: { key } });
    time.advance(60_000);
  }
  await outbox.drain();
  outbox.enqueue({ destination: "crm", key: "checkin:004", payload: {} });
  db.close();
});

after(() => tmp.cleanup());

describe("outbox-kit admin CLI", () => {
  it("prints jobs per state and flags the uncertain one", async () => {
    const { code, out } = await run("status", "--db", path);
    assert.equal(code, 0);
    assert.match(out, /accepted\s+1/);
    assert.match(out, /failed\s+1/);
    assert.match(out, /uncertain\s+1 {3}oldest since .*needs a person or a receiver lookup/);
  });

  it("lists by state, and prints JSON on request", async () => {
    assert.match((await run("list", "--db", path, "--state", "uncertain")).out, /#3 crm checkin:003 uncertain attempts=1 TimeoutError/);
    const json = JSON.parse((await run("list", "--db", path, "--json")).out) as Array<{ idempotencyKey: string }>;
    assert.deepEqual(json.map((row) => row.idempotencyKey), ["checkin:001", "checkin:002", "checkin:003", "checkin:004"]);
  });

  it("shows a job's attempts without its payload", async () => {
    const { code, out } = await run("attempts", "--db", path, "--destination", "crm", "--key", "checkin:002");
    assert.equal(code, 0);
    assert.match(out, /checkin:002: failed \(HTTP 422\)/);
    assert.match(out, /attempt 1 claimed .* -> failed HTTP 422/);
    assert.doesNotMatch((await run("attempts", "--db", path, "--destination", "crm", "--key", "checkin:002", "--json")).out, /"payload"/);
  });

  it("dry-runs a replay, then replays and records who and why", async () => {
    const dry = await run("replay", "--db", path, "--state", "uncertain", "--actor", "ops", "--reason", "CRM has no record", "--dry-run");
    assert.equal(dry.out, "dry run: would replay 1 job\n  #3 crm checkin:003 (was uncertain)");
    assert.match((await run("status", "--db", path)).out, /uncertain\s+1/);

    const real = await run("replay", "--db", path, "--from", new Date(START).toISOString(), "--to", new Date(START + 180_000).toISOString(), "--actor", "ops", "--reason", "CRM mapping fixed");
    assert.equal(real.code, 0);
    assert.match(real.out, /^replayed 2 jobs/);
    assert.match((await run("attempts", "--db", path, "--destination", "crm", "--key", "checkin:002")).out, /replay by ops at .*: CRM mapping fixed/);
    const byKey = await run("replay", "--db", path, "--destination", "crm", "--key", "checkin:001", "--actor", "ops", "--reason", "x");
    assert.match(byKey.out, /skipped crm checkin:001: already accepted; pass force/);
  });

  it("refuses bad input with usage and exit code 2, and a missing database with 1", async () => {
    assert.equal((await run("replay", "--db", path, "--state", "uncertain")).code, 2);
    assert.equal((await run("replay", "--db", path, "--state", "accepted", "--actor", "a", "--reason", "r")).code, 2);
    assert.equal((await run("list", "--db", path, "--limit", "0")).code, 2);
    assert.equal((await run("status")).code, 2);
    assert.equal((await run("nope", "--db", path)).code, 2);
    assert.equal((await run("status", "--db", join(tmp.dir, "missing.sqlite"))).code, 1);
  });
});

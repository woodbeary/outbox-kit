import assert from "node:assert/strict";
import { describe, it } from "node:test";
import { DatabaseSync } from "node:sqlite";
import { IdempotencyConflictError, createOutbox } from "../src/outbox.ts";
import { openDatabase } from "../src/schema.ts";
import type { Destination, LookupAnswer, SendResult } from "../src/types.ts";
import { OK, fakeClock, gate, http, memoryDb, scripted, tempDir } from "./helpers.ts";
import { join } from "node:path";

const checkin = (id: string) => ({ checkinId: id, phoneLast4: "0142", language: "es", visitorType: "client" });

describe("enqueue", () => {
  it("writes the job in the caller's transaction, so a rollback leaves neither the record nor the job", () => {
    const db = memoryDb();
    db.exec("CREATE TABLE checkins (id TEXT PRIMARY KEY, sms_consent INTEGER NOT NULL)");
    const outbox = createOutbox({ db, destinations: { crm: scripted().destination } });

    outbox.transaction(() => {
      db.prepare("INSERT INTO checkins VALUES (?, 1)").run("c-1");
      outbox.enqueue({ destination: "crm", key: "checkin:c-1", payload: checkin("c-1") });
    });
    assert.throws(
      () =>
        outbox.transaction(() => {
          db.prepare("INSERT INTO checkins VALUES (?, 1)").run("c-2");
          outbox.enqueue({ destination: "crm", key: "checkin:c-2", payload: checkin("c-2") });
          throw new Error("consent text failed validation");
        }),
      /consent text/,
    );

    assert.equal((db.prepare("SELECT COUNT(*) AS n FROM checkins").get() as { n: number }).n, 1);
    assert.equal(outbox.get("crm", "checkin:c-1")?.state, "pending");
    assert.equal(outbox.get("crm", "checkin:c-2"), null);
  });

  it("nests inside a transaction the caller already opened", () => {
    const db = memoryDb();
    const outbox = createOutbox({ db, destinations: { crm: scripted().destination } });
    db.exec("BEGIN");
    outbox.transaction(() => outbox.enqueue({ destination: "crm", key: "k1", payload: {} }));
    db.exec("ROLLBACK");
    assert.equal(outbox.get("crm", "k1"), null);
  });

  it("treats the same key and the same payload as one job, whatever the key order", () => {
    const outbox = createOutbox({ db: memoryDb(), destinations: { crm: scripted().destination } });
    const first = outbox.enqueue({ destination: "crm", key: "checkin:c-1", payload: { a: 1, b: 2 } });
    const again = outbox.enqueue({ destination: "crm", key: "checkin:c-1", payload: { b: 2, a: 1 } });
    assert.equal(first.created, true);
    assert.equal(again.created, false);
    assert.equal(again.job.id, first.job.id);
  });

  it("refuses the same key with a different payload instead of dropping the change", () => {
    const outbox = createOutbox({ db: memoryDb(), destinations: { crm: scripted().destination } });
    outbox.enqueue({ destination: "crm", key: "quote:q-7", payload: { total: 240 } });
    assert.throws(
      () => outbox.enqueue({ destination: "crm", key: "quote:q-7", payload: { total: 260 } }),
      IdempotencyConflictError,
    );
  });

  it("refuses keys that would break or inject an HTTP header, and unknown destinations", () => {
    const outbox = createOutbox({ db: memoryDb(), destinations: { crm: scripted().destination } });
    for (const key of ["", "has space", "x\r\ninjected: 1", "café", "k".repeat(256)]) {
      assert.throws(() => outbox.enqueue({ destination: "crm", key, payload: {} }), /visible ASCII/, JSON.stringify(key));
    }
    assert.throws(() => outbox.enqueue({ destination: "erp", key: "k", payload: {} }), /no destination named erp/);
  });
});

describe("delivery states", () => {
  it("marks a 2xx accepted and logs the attempt", async () => {
    const { clock } = fakeClock();
    const crm = scripted([OK]);
    const outbox = createOutbox({ db: memoryDb(), destinations: { crm: crm.destination }, clock });
    const { job } = outbox.enqueue({ destination: "crm", key: "checkin:c-1", payload: { b: 2, a: 1 } });

    const summary = await outbox.drain();
    assert.equal(summary.accepted, 1);
    assert.equal(crm.calls[0]?.idempotencyKey, "checkin:c-1");
    assert.equal(crm.calls[0]?.body, '{"a":1,"b":2}');
    const after = outbox.get("crm", "checkin:c-1");
    assert.equal(after?.state, "accepted");
    assert.equal(after?.attempts, 1);
    const [attempt] = outbox.attempts(job.id);
    assert.equal(attempt?.outcome, "accepted");
    assert.notEqual(attempt?.sendStartedAt, null);
  });

  it("waits out the ladder after a 503, and Retry-After can only make it wait longer", async () => {
    const time = fakeClock();
    const crm = scripted([http(503), http(429, "600"), OK]);
    const outbox = createOutbox({ db: memoryDb(), destinations: { crm: crm.destination }, clock: time.clock });
    outbox.enqueue({ destination: "crm", key: "k", payload: {} });

    await outbox.drain();
    assert.equal(outbox.get("crm", "k")?.nextAttemptAt, time.now + 60_000);
    time.advance(59_999);
    assert.equal((await outbox.drain()).attempted, 0);
    time.advance(1);
    await outbox.drain();
    assert.equal(outbox.get("crm", "k")?.nextAttemptAt, time.now + 600_000);
    time.advance(600_000);
    await outbox.drain();
    assert.equal(outbox.get("crm", "k")?.state, "accepted");
    assert.equal(crm.calls.length, 3);
  });

  it("stops after maxAttempts and says why", async () => {
    const time = fakeClock();
    const crm = scripted([http(503), http(503), http(503)]);
    const outbox = createOutbox({
      db: memoryDb(),
      destinations: { crm: crm.destination },
      clock: time.clock,
      maxAttempts: 3,
      ladderMs: [1_000],
    });
    outbox.enqueue({ destination: "crm", key: "k", payload: {} });
    for (let round = 0; round < 5; round++) {
      await outbox.drain();
      time.advance(1_000);
    }
    const job = outbox.get("crm", "k");
    assert.equal(job?.state, "failed");
    assert.match(job?.reason ?? "", /gave up after 3 attempts; last: HTTP 503/);
    assert.equal(crm.calls.length, 3);
  });

  it("never retries an uncertain job on its own, however long it waits", async () => {
    const time = fakeClock();
    const crm = scripted([{ kind: "unknown", detail: "TimeoutError: The operation was aborted due to timeout" }]);
    const outbox = createOutbox({ db: memoryDb(), destinations: { crm: crm.destination }, clock: time.clock });
    outbox.enqueue({ destination: "crm", key: "k", payload: {} });

    await outbox.drain();
    for (let day = 0; day < 30; day++) {
      time.advance(86_400_000);
      await outbox.drain();
    }
    assert.equal(crm.calls.length, 1);
    const status = outbox.status();
    assert.equal(status.counts.uncertain, 1);
    assert.equal(status.dueNow, 0);
  });

  it("treats a sender that throws mid-send as uncertain", async () => {
    const outbox = createOutbox({
      db: memoryDb(),
      destinations: {
        crm: {
          send: async () => {
            throw new Error("socket hang up");
          },
        },
      },
    });
    outbox.enqueue({ destination: "crm", key: "k", payload: {} });
    await outbox.drain();
    assert.equal(outbox.get("crm", "k")?.state, "uncertain");
    assert.match(outbox.get("crm", "k")?.reason ?? "", /after the send began/);
  });

  it("fails a 409 for a person to resolve, without retrying it", async () => {
    const time = fakeClock();
    const crm = scripted([http(409)]);
    const outbox = createOutbox({ db: memoryDb(), destinations: { crm: crm.destination }, clock: time.clock });
    outbox.enqueue({ destination: "crm", key: "k", payload: {} });
    await outbox.drain();
    time.advance(86_400_000);
    await outbox.drain();
    assert.equal(outbox.get("crm", "k")?.state, "failed");
    assert.equal(crm.calls.length, 1);
  });

  it("lets a destination loosen the default once it has shown it dedupes on the key", async () => {
    const crm = scripted([http(500), OK]);
    const outbox = createOutbox({
      db: memoryDb(),
      ladderMs: [0],
      destinations: {
        crm: {
          ...crm.destination,
          classify: (result: SendResult) =>
            result.kind === "response" && result.status === 500
              ? { outcome: "retry", detail: "HTTP 500; this receiver dedupes on Idempotency-Key" }
              : result.kind === "response" && result.status < 300
                ? { outcome: "accepted", detail: "ok" }
                : { outcome: "uncertain", detail: "unexpected" },
        },
      },
    });
    outbox.enqueue({ destination: "crm", key: "k", payload: {} });
    await outbox.drain();
    assert.equal(outbox.get("crm", "k")?.state, "accepted");
    assert.equal(crm.calls.length, 2);
  });
});

describe("leases and compare-and-swap claims", () => {
  it("two workers on one database file never claim the same job", async () => {
    const tmp = tempDir();
    try {
      const path = join(tmp.dir, "outbox.sqlite");
      const first = gate<SendResult>();
      const second = gate<SendResult>();
      const a = scripted([() => first.promise]);
      const b = scripted([() => second.promise]);
      const workerA = createOutbox({ db: openDatabase(path), destinations: { crm: a.destination } });
      const workerB = createOutbox({ db: openDatabase(path), destinations: { crm: b.destination } });
      workerA.enqueue({ destination: "crm", key: "k1", payload: {} });
      workerA.enqueue({ destination: "crm", key: "k2", payload: {} });

      const runA = workerA.deliverOne();
      const runB = workerB.deliverOne();
      assert.equal(await workerB.deliverOne(), null);
      first.release(OK);
      second.release(OK);
      await Promise.all([runA, runB]);

      assert.deepEqual([a.calls[0]?.idempotencyKey, b.calls[0]?.idempotencyKey].sort(), ["k1", "k2"]);
      assert.equal(workerA.status().counts.accepted, 2);
    } finally {
      tmp.cleanup();
    }
  });

  it("puts a job back when its lease runs out before sending, and the slow worker does not send", async () => {
    const time = fakeClock();
    const db = memoryDb();
    const stall = gate<{ action: "send" }>();
    const slow = scripted([OK]);
    const fast = scripted([OK]);
    const workerA = createOutbox({
      db,
      clock: time.clock,
      destinations: { crm: { ...slow.destination, revalidate: () => stall.promise } },
    });
    const workerB = createOutbox({ db, clock: time.clock, destinations: { crm: fast.destination } });
    workerA.enqueue({ destination: "crm", key: "k", payload: {} });

    const runA = workerA.deliverOne();
    time.advance(90_001);
    assert.deepEqual(workerB.expireLeases(), { requeued: 1, uncertain: 0, exhausted: 0 });
    assert.equal((await workerB.drain()).accepted, 1);
    stall.release({ action: "send" });
    assert.equal((await runA)?.outcome, "lease_lost");
    assert.equal(slow.calls.length, 0);
    assert.equal(fast.calls.length, 1);
  });

  it("does not start a send on an expired lease, even before any sweep has run", async () => {
    const time = fakeClock();
    const stall = gate<{ action: "send" }>();
    const crm = scripted([OK]);
    const outbox = createOutbox({
      db: memoryDb(),
      clock: time.clock,
      destinations: { crm: { ...crm.destination, revalidate: () => stall.promise } },
    });
    outbox.enqueue({ destination: "crm", key: "k", payload: {} });
    const run = outbox.deliverOne();
    time.advance(90_000);
    stall.release({ action: "send" });
    assert.equal((await run)?.outcome, "lease_lost");
    assert.equal(crm.calls.length, 0);
    assert.deepEqual(outbox.expireLeases(), { requeued: 1, uncertain: 0, exhausted: 0 });
  });

  it("marks a job uncertain when its lease runs out mid-send, then records the worker's late answer", async () => {
    const time = fakeClock();
    const db = memoryDb();
    const reply = gate<SendResult>();
    const slow = scripted([() => reply.promise]);
    const workerA = createOutbox({ db, clock: time.clock, destinations: { crm: slow.destination } });
    const workerB = createOutbox({ db, clock: time.clock, destinations: { crm: scripted().destination } });
    const { job } = workerA.enqueue({ destination: "crm", key: "k", payload: {} });

    const runA = workerA.deliverOne();
    await Promise.resolve();
    time.advance(90_001);
    assert.deepEqual(workerB.expireLeases(), { requeued: 0, uncertain: 1, exhausted: 0 });
    assert.equal((await workerB.drain()).attempted, 0);

    reply.release(OK);
    assert.equal((await runA)?.outcome, "accepted");
    const after = workerA.get("crm", "k");
    assert.equal(after?.state, "accepted");
    assert.match(after?.reason ?? "", /late answer after the lease ran out/);
    assert.equal(workerA.attempts(job.id)[0]?.outcome, "accepted");
  });

  it("ignores the old worker's late answer once an admin has replayed the job and a new worker holds it", async () => {
    const time = fakeClock();
    const db = memoryDb();
    const oldReply = gate<SendResult>();
    const newReply = gate<SendResult>();
    const slow = scripted([() => oldReply.promise]);
    const fresh = scripted([() => newReply.promise]);
    const workerA = createOutbox({ db, clock: time.clock, destinations: { crm: slow.destination } });
    const workerB = createOutbox({ db, clock: time.clock, destinations: { crm: fresh.destination } });
    workerA.enqueue({ destination: "crm", key: "k", payload: {} });

    const runA = workerA.deliverOne();
    await Promise.resolve();
    time.advance(90_001);
    workerB.expireLeases();
    workerB.replay({ state: "uncertain" }, { actor: "ops", reason: "CRM shows no record for k" });
    const runB = workerB.deliverOne();
    await Promise.resolve();
    assert.equal(workerB.get("crm", "k")?.state, "claimed");

    oldReply.release(http(500));
    assert.equal((await runA)?.outcome, "lease_lost");
    assert.equal(workerB.get("crm", "k")?.state, "claimed");
    newReply.release(OK);
    assert.equal((await runB)?.outcome, "accepted");
    assert.equal(workerB.get("crm", "k")?.state, "accepted");
  });

  it("refuses a destination whose timeout is not shorter than the lease", () => {
    const send = async (): Promise<SendResult> => OK;
    assert.throws(
      () => createOutbox({ db: memoryDb(), leaseMs: 90_000, destinations: { crm: { send, timeoutMs: 90_000 } } }),
      /not shorter than the 90000 ms lease/,
    );
  });

  it("gives up when every attempt runs out of lease before sending", async () => {
    const time = fakeClock();
    const db = memoryDb();
    const never = new Promise<{ action: "send" }>(() => undefined);
    const outbox = createOutbox({
      db,
      clock: time.clock,
      maxAttempts: 2,
      destinations: { crm: { ...scripted().destination, revalidate: () => never } },
    });
    outbox.enqueue({ destination: "crm", key: "k", payload: {} });
    void outbox.deliverOne();
    time.advance(90_001);
    outbox.expireLeases();
    void outbox.deliverOne();
    time.advance(90_001);
    assert.deepEqual(outbox.expireLeases(), { requeued: 0, uncertain: 0, exhausted: 1 });
    assert.equal(outbox.get("crm", "k")?.state, "failed");
  });

  it("only claims destinations this worker has registered", async () => {
    const db = memoryDb();
    const both = createOutbox({ db, destinations: { crm: scripted().destination, erp: scripted().destination } });
    both.enqueue({ destination: "crm", key: "k", payload: {} });
    both.enqueue({ destination: "erp", key: "k", payload: {} });
    const crmOnly = createOutbox({ db, destinations: { crm: scripted([OK]).destination } });
    assert.equal((await crmOnly.drain()).attempted, 1);
    assert.equal(both.get("erp", "k")?.state, "pending");
  });
});

describe("revalidation before sending", () => {
  it("fails or suppresses a job without sending, and retries when the check itself cannot run", async () => {
    const crm = scripted();
    const outbox = createOutbox({
      db: memoryDb(),
      destinations: {
        crm: {
          ...crm.destination,
          revalidate: (payload) => {
            const body = payload as { case: string };
            if (body.case === "changed") return { action: "fail", reason: "the quote changed after the job was written" };
            if (body.case === "revoked") return { action: "suppress", reason: "SMS consent was revoked" };
            throw new Error("quote store unreachable");
          },
        },
      },
    });
    outbox.enqueue({ destination: "crm", key: "changed", payload: { case: "changed" } });
    outbox.enqueue({ destination: "crm", key: "revoked", payload: { case: "revoked" } });
    outbox.enqueue({ destination: "crm", key: "down", payload: { case: "down" } });
    await outbox.drain();
    assert.equal(crm.calls.length, 0);
    assert.equal(outbox.get("crm", "changed")?.state, "failed");
    assert.equal(outbox.get("crm", "revoked")?.state, "suppressed");
    assert.equal(outbox.get("crm", "down")?.state, "pending");
    assert.match(outbox.get("crm", "down")?.reason ?? "", /nothing sent/);
  });
});

describe("reconcile: settling uncertain jobs from the receiver's own record", () => {
  it("asks only after the wait, and moves a job only on a definite answer", async () => {
    const time = fakeClock();
    const answers: Record<string, LookupAnswer | "throw"> = {
      landed: "applied",
      lost: "not_applied",
      shrug: "unknown",
      down: "throw",
    };
    const crm = scripted([
      { kind: "unknown", detail: "timeout" },
      { kind: "unknown", detail: "timeout" },
      { kind: "unknown", detail: "timeout" },
      { kind: "unknown", detail: "timeout" },
      OK,
    ]);
    const destination: Destination = {
      ...crm.destination,
      lookup: async (key) => {
        const answer = answers[key];
        if (answer === "throw" || answer === undefined) throw new Error("lookup endpoint down");
        return answer;
      },
    };
    const outbox = createOutbox({ db: memoryDb(), clock: time.clock, destinations: { crm: destination } });
    for (const key of Object.keys(answers)) outbox.enqueue({ destination: "crm", key, payload: { key } });
    await outbox.drain();
    assert.equal(outbox.status().counts.uncertain, 4);

    assert.deepEqual(await outbox.reconcile(), { confirmedApplied: 0, confirmedNotApplied: 0, stillUncertain: 0 });
    time.advance(5 * 60_000);
    assert.deepEqual(await outbox.reconcile(), { confirmedApplied: 1, confirmedNotApplied: 1, stillUncertain: 2 });
    assert.equal(outbox.get("crm", "landed")?.state, "accepted");
    assert.equal(outbox.get("crm", "lost")?.state, "pending");
    assert.equal(outbox.get("crm", "shrug")?.state, "uncertain");
    assert.equal(outbox.get("crm", "down")?.state, "uncertain");

    await outbox.drain();
    assert.equal(outbox.get("crm", "lost")?.state, "accepted");
    assert.deepEqual(
      crm.calls.map((call) => call.idempotencyKey),
      ["landed", "lost", "shrug", "down", "lost"],
    );
    assert.deepEqual(
      outbox.audit().map((row) => [row.actor, row.toState]),
      [
        ["reconcile", "accepted"],
        ["reconcile", "pending"],
      ],
    );
  });
});

describe("durability settings", () => {
  it("opens file databases in WAL with synchronous=FULL and a busy timeout", () => {
    const tmp = tempDir();
    try {
      const db = openDatabase(join(tmp.dir, "o.sqlite"));
      const pragma = (name: string) => Object.values(db.prepare(`PRAGMA ${name}`).get() as object)[0];
      assert.equal(pragma("journal_mode"), "wal");
      assert.equal(pragma("synchronous"), 2);
      assert.equal(pragma("busy_timeout"), 5000);
      db.close();
      assert.ok(new DatabaseSync(join(tmp.dir, "o.sqlite")));
    } finally {
      tmp.cleanup();
    }
  });
});

import assert from "node:assert/strict";
import { describe, it } from "node:test";
import { createOutbox } from "../src/outbox.ts";
import { OK, START, fakeClock, http, memoryDb, scripted } from "./helpers.ts";

async function seeded() {
  const time = fakeClock();
  const crm = scripted([
    OK,
    http(422),
    { kind: "unknown", detail: "timeout" },
    http(409),
  ]);
  const outbox = createOutbox({
    db: memoryDb(),
    clock: time.clock,
    destinations: {
      crm: {
        ...crm.destination,
        revalidate: (payload) =>
          (payload as { consent?: boolean }).consent === false
            ? { action: "suppress", reason: "consent revoked" }
            : { action: "send" },
      },
    },
  });
  for (const key of ["ok", "bad-request", "timed-out", "conflict"]) {
    outbox.enqueue({ destination: "crm", key, payload: { key } });
    time.advance(1_000);
  }
  outbox.enqueue({ destination: "crm", key: "revoked", payload: { key: "revoked", consent: false } });
  await outbox.drain();
  return { time, crm, outbox };
}

const who = { actor: "ops@example.test", reason: "CRM team fixed the field mapping" };

describe("admin replay", () => {
  it("starts from the expected states", async () => {
    const { outbox } = await seeded();
    assert.deepEqual(
      Object.fromEntries(outbox.list().map((job) => [job.idempotencyKey, job.state])),
      { ok: "accepted", "bad-request": "failed", "timed-out": "uncertain", conflict: "failed", revoked: "suppressed" },
    );
  });

  it("needs a named actor and a reason every time", async () => {
    const { outbox } = await seeded();
    assert.throws(() => outbox.replay({ state: "failed" }, { actor: " ", reason: "x" }), /actor is required/);
    assert.throws(() => outbox.replay({ state: "failed" }, { actor: "ops", reason: "" }), /reason is required/);
  });

  it("replays one job by key with the same idempotency key, and audits it", async () => {
    const { outbox, crm } = await seeded();
    crm.push(OK);
    const report = outbox.replay({ destination: "crm", key: "bad-request" }, who);
    assert.deepEqual(report.replayed.map((job) => [job.idempotencyKey, job.fromState]), [["bad-request", "failed"]]);
    const queued = outbox.get("crm", "bad-request");
    assert.equal(queued?.state, "pending");
    assert.equal(queued?.attempts, 0);
    await outbox.drain();
    assert.equal(outbox.get("crm", "bad-request")?.state, "accepted");
    assert.deepEqual(
      crm.calls.filter((call) => call.idempotencyKey === "bad-request").map((call) => call.body),
      ['{"key":"bad-request"}', '{"key":"bad-request"}'],
    );
    const [entry] = outbox.audit(queued?.id);
    assert.equal(entry?.actor, "ops@example.test");
    assert.equal(entry?.reason, "CRM team fixed the field mapping");
    assert.equal(entry?.fromState, "failed");
  });

  it("resends an accepted job only with force, and never a suppressed or in-flight one", async () => {
    const { outbox } = await seeded();
    const why = (key: string, force?: boolean) =>
      outbox.replay(force ? { destination: "crm", key, force } : { destination: "crm", key }, who).skipped[0]?.why;
    assert.match(why("ok") ?? "", /pass force/);
    assert.equal(outbox.replay({ destination: "crm", key: "ok", force: true }, who).replayed.length, 1);
    assert.match(why("revoked", true) ?? "", /never replayed/);
    assert.match(why("missing") ?? "", /no job/);
    assert.match(why("ok") ?? "", /already queued/);
  });

  it("dry-runs a batch first and changes nothing", async () => {
    const { outbox } = await seeded();
    const before = outbox.status();
    const report = outbox.replay({ state: "uncertain" }, { ...who, dryRun: true });
    assert.equal(report.dryRun, true);
    assert.deepEqual(report.replayed.map((job) => job.idempotencyKey), ["timed-out"]);
    assert.deepEqual(outbox.status(), before);
    assert.deepEqual(outbox.audit(), []);
  });

  it("replays a batch by state, and a created-at window across failed and uncertain only", async () => {
    const { outbox } = await seeded();
    const failed = outbox.replay({ state: "failed", limit: 1 }, who);
    assert.deepEqual(failed.replayed.map((job) => job.idempotencyKey), ["bad-request"]);

    const fresh = await seeded();
    const window = fresh.outbox.replay({ createdFrom: START, createdTo: START + 3_000 }, who);
    assert.deepEqual(window.replayed.map((job) => job.idempotencyKey), ["bad-request", "timed-out"]);
    assert.equal(fresh.outbox.get("crm", "ok")?.state, "accepted");
    assert.equal(fresh.outbox.get("crm", "conflict")?.state, "failed");
  });

  it("refuses to batch-replay accepted or suppressed jobs", async () => {
    const { outbox } = await seeded();
    assert.throws(
      () => outbox.replay({ createdFrom: 0, createdTo: Number.MAX_SAFE_INTEGER, states: ["accepted" as "failed"] }, who),
      /cannot replay jobs in state accepted/,
    );
  });

  it("prunes payloads of settled jobs after the retention window and records who did it", async () => {
    const { outbox, time } = await seeded();
    assert.equal(outbox.prune({ olderThanMs: 30 * 86_400_000, ...who }).pruned, 0);
    time.advance(30 * 86_400_000);
    assert.equal(outbox.prune({ olderThanMs: 30 * 86_400_000, ...who }).pruned, 2);
    assert.equal(outbox.get("crm", "ok")?.payload, null);
    assert.equal(outbox.get("crm", "revoked")?.payload, null);
    assert.notEqual(outbox.get("crm", "conflict")?.payload, null);
    assert.match(outbox.replay({ destination: "crm", key: "ok", force: true }, who).skipped[0]?.why ?? "", /pruned/);
    assert.deepEqual(new Set(outbox.audit().map((row) => row.action)), new Set(["prune"]));
  });
});

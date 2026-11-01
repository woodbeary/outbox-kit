import assert from "node:assert/strict";
import { describe, it } from "node:test";
import { AsyncHandlerError, createInbox } from "../src/inbox.ts";
import { mergeKnown } from "../src/merge.ts";
import { generateSecret, signHeaders } from "../src/signing.ts";
import { fakeClock, memoryDb } from "./helpers.ts";

const secret = generateSecret();

function signed(id: string, body: string, nowMs: number) {
  return { headers: signHeaders({ id, timestampSec: Math.floor(nowMs / 1000), body, secrets: [secret] }), body };
}

function setup(maxAttempts = 6) {
  const time = fakeClock();
  const db = memoryDb();
  db.exec("CREATE TABLE payments (id TEXT PRIMARY KEY, amount_cents INTEGER NOT NULL)");
  const inbox = createInbox({ db, clock: time.clock, sources: { payments: { secrets: [secret] } }, maxAttempts, ladderMs: [60_000] });
  return { time, db, inbox };
}

describe("inbox: signed webhooks in", () => {
  it("stores a verified event once and reports the duplicate", () => {
    const { inbox, time } = setup();
    const body = '{"paymentId":"p-1","amountCents":12000}';
    assert.deepEqual(inbox.receive("payments", signed("evt_1", body, time.now)), { status: "accepted", eventId: "evt_1" });
    assert.deepEqual(inbox.receive("payments", signed("evt_1", body, time.now)), {
      status: "duplicate",
      eventId: "evt_1",
      sameBody: true,
    });
    assert.deepEqual(inbox.receive("payments", signed("evt_1", '{"paymentId":"p-1","amountCents":1}', time.now)), {
      status: "duplicate",
      eventId: "evt_1",
      sameBody: false,
    });
  });

  it("rejects bad signatures, stale timestamps, unknown sources and bodies that are not JSON", () => {
    const { inbox, time } = setup();
    const body = '{"paymentId":"p-1"}';
    const good = signed("evt_1", body, time.now);
    assert.deepEqual(inbox.receive("payments", { ...good, body: '{"paymentId":"p-2"}' }), {
      status: "rejected",
      reason: "no_matching_signature",
    });
    assert.deepEqual(inbox.receive("payments", signed("evt_1", body, time.now - 301_000)), { status: "rejected", reason: "too_old" });
    assert.deepEqual(inbox.receive("erp", good), { status: "rejected", reason: "unknown_source" });
    assert.deepEqual(inbox.receive("payments", signed("evt_2", "not json", time.now)), { status: "rejected", reason: "body_not_json" });
    assert.equal(inbox.get("payments", "evt_1"), null);
  });
});

describe("inbox: processing", () => {
  it("commits the handler's writes and the processed mark together", () => {
    const { inbox, db, time } = setup();
    inbox.receive("payments", signed("evt_1", '{"paymentId":"p-1","amountCents":12000}', time.now));
    const result = inbox.process((event, tx) => {
      const body = event.body as { paymentId: string; amountCents: number };
      tx.prepare("INSERT INTO payments VALUES (?, ?)").run(body.paymentId, body.amountCents);
    });
    assert.deepEqual(result, { processed: 1, retrying: 0, failed: 0 });
    assert.equal(inbox.get("payments", "evt_1")?.state, "processed");
    assert.equal((db.prepare("SELECT amount_cents FROM payments WHERE id = 'p-1'").get() as { amount_cents: number }).amount_cents, 12000);
    assert.deepEqual(inbox.process(() => assert.fail("nothing left to process")), { processed: 0, retrying: 0, failed: 0 });
  });

  it("rolls back a handler that throws, backs off, and stops at maxAttempts", () => {
    const { inbox, db, time } = setup(2);
    inbox.receive("payments", signed("evt_1", '{"paymentId":"p-1","amountCents":12000}', time.now));
    const failing = (_event: unknown, tx: typeof db) => {
      tx.prepare("INSERT INTO payments VALUES ('p-1', 1)").run();
      throw new Error("ledger account p-1 is closed");
    };
    assert.deepEqual(inbox.process(failing), { processed: 0, retrying: 1, failed: 0 });
    assert.equal((db.prepare("SELECT COUNT(*) AS n FROM payments").get() as { n: number }).n, 0);
    const waiting = inbox.get("payments", "evt_1");
    assert.equal(waiting?.attempts, 1);
    assert.equal(waiting?.nextAttemptAt, time.now + 60_000);
    assert.equal(waiting?.lastError, "ledger account p-1 is closed");

    assert.deepEqual(inbox.process(failing), { processed: 0, retrying: 0, failed: 0 });
    time.advance(60_000);
    assert.deepEqual(inbox.process(failing), { processed: 0, retrying: 0, failed: 1 });
    assert.equal(inbox.get("payments", "evt_1")?.state, "failed");

    assert.throws(() => inbox.retryFailed({ actor: "", reason: "x" }), /required/);
    assert.deepEqual(inbox.retryFailed({ actor: "ops", reason: "reopened ledger account p-1" }), { retried: 1 });
    assert.equal(inbox.get("payments", "evt_1")?.state, "pending");
  });

  it("refuses an async handler, whose writes would land outside the transaction", () => {
    const { inbox, db, time } = setup();
    inbox.receive("payments", signed("evt_1", '{"paymentId":"p-1","amountCents":5}', time.now));
    assert.throws(
      () =>
        inbox.process((async (_event: unknown, tx: typeof db) => {
          tx.prepare("INSERT INTO payments VALUES ('p-1', 5)").run();
        }) as unknown as () => void),
      AsyncHandlerError,
    );
    assert.equal((db.prepare("SELECT COUNT(*) AS n FROM payments").get() as { n: number }).n, 0);
    assert.equal(inbox.get("payments", "evt_1")?.state, "pending");
    assert.equal(inbox.get("payments", "evt_1")?.attempts, 0);
  });
});

describe("mergeKnown: an event never erases what we already know", () => {
  type Message = Record<string, unknown> & { id: string; contactId: string | null; status: string; toNumber: string };
  const sent: Message = { id: "m-1", contactId: "c-9f2", status: "sent", toNumber: "+17145550100" };
  const deliveryReceipt = { id: "m-1", contactId: null, status: "delivered", toNumber: "7145550100" };

  it("replays the delivery-receipt upsert that wrote a null contact id over a good one", () => {
    const naive = { ...sent, ...deliveryReceipt };
    assert.equal(naive.contactId, null);

    const merged = mergeKnown(sent, deliveryReceipt);
    assert.equal(merged.contactId, "c-9f2");
    assert.equal(merged.status, "delivered");
    assert.equal(merged.toNumber, "7145550100");
  });

  it("clears a field only when the caller names it", () => {
    assert.equal(mergeKnown(sent, { contactId: null }, { clear: ["contactId"] }).contactId, null);
    assert.equal(mergeKnown(sent, { contactId: undefined }, { clear: [] }).contactId, "c-9f2");
  });
});

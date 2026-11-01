import assert from "node:assert/strict";
import { describe, it } from "node:test";
import { Webhook } from "standardwebhooks";
import { generateSecret, secretKey, signHeaders, verifyHeaders } from "../src/signing.ts";

const body = '{"checkinId":"demo-001","language":"es","phoneLast4":"0142"}';
const nowSec = 1_790_000_000;

describe("signed webhooks", () => {
  const secret = generateSecret();

  it("verifies what it signs", () => {
    const headers = signHeaders({ id: "checkin:demo-001", timestampSec: nowSec, body, secrets: [secret] });
    assert.deepEqual(verifyHeaders({ headers, body, secrets: [secret], nowSec }), {
      ok: true,
      id: "checkin:demo-001",
      timestampSec: nowSec,
    });
  });

  it("rejects a changed body, a changed id and the wrong secret", () => {
    const headers = signHeaders({ id: "checkin:demo-001", timestampSec: nowSec, body, secrets: [secret] });
    const tampered = body.replace("0142", "0143");
    assert.equal(verifyHeaders({ headers, body: tampered, secrets: [secret], nowSec }).ok, false);
    assert.equal(verifyHeaders({ headers: { ...headers, "webhook-id": "checkin:demo-002" }, body, secrets: [secret], nowSec }).ok, false);
    assert.equal(verifyHeaders({ headers, body, secrets: [generateSecret()], nowSec }).ok, false);
  });

  it("signs the timestamp, so a captured request stops working after five minutes", () => {
    const headers = signHeaders({ id: "k", timestampSec: nowSec, body, secrets: [secret] });
    assert.deepEqual(verifyHeaders({ headers, body, secrets: [secret], nowSec: nowSec + 301 }), { ok: false, reason: "too_old" });
    assert.deepEqual(verifyHeaders({ headers, body, secrets: [secret], nowSec: nowSec - 301 }), { ok: false, reason: "too_new" });
    const moved = { ...headers, "webhook-timestamp": String(nowSec + 200) };
    assert.deepEqual(verifyHeaders({ headers: moved, body, secrets: [secret], nowSec: nowSec + 200 }), {
      ok: false,
      reason: "no_matching_signature",
    });
  });

  it("must be checked against the raw body, not a re-serialized one", () => {
    const raw = '{ "b": 1, "a": 2 }';
    const headers = signHeaders({ id: "k", timestampSec: nowSec, body: raw, secrets: [secret] });
    assert.equal(verifyHeaders({ headers, body: raw, secrets: [secret], nowSec }).ok, true);
    assert.equal(verifyHeaders({ headers, body: JSON.stringify(JSON.parse(raw)), secrets: [secret], nowSec }).ok, false);
  });

  it("rotates secrets: signs with old and new, and either side can verify", () => {
    const oldSecret = generateSecret();
    const newSecret = generateSecret();
    const headers = signHeaders({ id: "k", timestampSec: nowSec, body, secrets: [newSecret, oldSecret] });
    assert.equal(headers["webhook-signature"]?.split(" ").length, 2);
    assert.equal(verifyHeaders({ headers, body, secrets: [oldSecret], nowSec }).ok, true);
    assert.equal(verifyHeaders({ headers, body, secrets: [newSecret], nowSec }).ok, true);
  });

  it("reads Fetch Headers and Node's lowercase header objects, and refuses missing headers", () => {
    const signed = signHeaders({ id: "k", timestampSec: nowSec, body, secrets: [secret] });
    assert.equal(verifyHeaders({ headers: new Headers(signed), body, secrets: [secret], nowSec }).ok, true);
    assert.deepEqual(verifyHeaders({ headers: { "webhook-id": "k" }, body, secrets: [secret], nowSec }), {
      ok: false,
      reason: "missing_headers",
    });
    assert.deepEqual(
      verifyHeaders({ headers: { ...signed, "webhook-timestamp": "12.5" }, body, secrets: [secret], nowSec }),
      { ok: false, reason: "bad_timestamp" },
    );
  });

  it("refuses a secret with a typo instead of silently signing with a different key", () => {
    assert.throws(() => secretKey("whsec_not*base64*at*all"), /base64/);
    assert.throws(() => secretKey("whsec_c2hvcnQ="), /16 bytes/);
    assert.equal(secretKey(secret).length, 32);
  });
});

describe("interop with the standardwebhooks reference library", () => {
  const secret = generateSecret();
  const reference = new Webhook(secret);

  it("the reference library verifies outbox-kit's signature", () => {
    const timestampSec = Math.floor(Date.now() / 1000);
    const headers = signHeaders({ id: "checkin:demo-001", timestampSec, body, secrets: [secret] });
    assert.deepEqual(reference.verify(body, headers), JSON.parse(body));
  });

  it("outbox-kit verifies the reference library's signature", () => {
    const timestampSec = Math.floor(Date.now() / 1000);
    const signature = reference.sign("evt_01", new Date(timestampSec * 1000), body);
    const headers = { "webhook-id": "evt_01", "webhook-timestamp": String(timestampSec), "webhook-signature": signature };
    assert.equal(verifyHeaders({ headers, body, secrets: [secret], nowSec: timestampSec }).ok, true);
  });
});

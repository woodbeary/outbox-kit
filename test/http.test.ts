import assert from "node:assert/strict";
import { type IncomingMessage, type ServerResponse, createServer } from "node:http";
import type { AddressInfo } from "node:net";
import { after, before, describe, it } from "node:test";
import { httpDestination, neverLeft } from "../src/http.ts";
import { createOutbox } from "../src/outbox.ts";
import { generateSecret, verifyHeaders } from "../src/signing.ts";
import { fakeClock, memoryDb } from "./helpers.ts";

type Seen = { key: string | undefined; webhookId: string | undefined; verified: boolean; body: string; contentType: string | undefined };

const secret = generateSecret();
const seen: Seen[] = [];
const behaviors: Record<string, (res: ServerResponse) => void> = {
  "k-ok": (res) => res.writeHead(201).end(),
  "k-busy": (res) => res.writeHead(503, { "retry-after": "120" }).end(),
  "k-hang": () => undefined,
  "k-moved": (res) => res.writeHead(302, { location: "https://example.test/elsewhere" }).end(),
};

function handle(req: IncomingMessage, res: ServerResponse) {
  const chunks: Buffer[] = [];
  req.on("data", (chunk: Buffer) => chunks.push(chunk));
  req.on("end", () => {
    const body = Buffer.concat(chunks).toString("utf8");
    const key = req.headers["idempotency-key"] as string | undefined;
    const verdict = verifyHeaders({ headers: req.headers, body, secrets: [secret], nowSec: Math.floor(Date.now() / 1000) });
    seen.push({ key, webhookId: req.headers["webhook-id"] as string | undefined, verified: verdict.ok, body, contentType: req.headers["content-type"] });
    if (!verdict.ok) {
      res.writeHead(401).end();
      return;
    }
    (behaviors[key ?? ""] ?? ((r: ServerResponse) => r.writeHead(500).end()))(res);
  });
}

const server = createServer(handle);
let url = "";

before(async () => {
  await new Promise<void>((resolve) => server.listen(0, "127.0.0.1", resolve));
  url = `http://127.0.0.1:${(server.address() as AddressInfo).port}/hooks/checkins`;
});

after(() => {
  server.closeAllConnections();
  server.close();
});

function outboxFor(target: string, timeoutMs = 1_000) {
  const time = fakeClock();
  const outbox = createOutbox({
    db: memoryDb(),
    clock: time.clock,
    destinations: { crm: httpDestination({ url: target, secrets: [secret], timeoutMs }) },
  });
  return { outbox, time };
}

describe("httpDestination against a real local receiver", () => {
  it("signs every request so the receiver can verify it, with the idempotency key as the message id", async () => {
    const { outbox } = outboxFor(url);
    outbox.enqueue({ destination: "crm", key: "k-ok", payload: { phoneLast4: "0142", checkinId: "demo-001" } });
    await outbox.drain();
    assert.equal(outbox.get("crm", "k-ok")?.state, "accepted");
    const request = seen.find((entry) => entry.key === "k-ok");
    assert.deepEqual(request, {
      key: "k-ok",
      webhookId: "k-ok",
      verified: true,
      body: '{"checkinId":"demo-001","phoneLast4":"0142"}',
      contentType: "application/json",
    });
  });

  it("waits as long as a 503's Retry-After asks", async () => {
    const { outbox, time } = outboxFor(url);
    outbox.enqueue({ destination: "crm", key: "k-busy", payload: {} });
    await outbox.drain();
    const job = outbox.get("crm", "k-busy");
    assert.equal(job?.state, "pending");
    assert.equal(job?.nextAttemptAt, time.now + 120_000);
  });

  it("calls a timeout uncertain, because the receiver did get the request", async () => {
    const { outbox } = outboxFor(url, 200);
    outbox.enqueue({ destination: "crm", key: "k-hang", payload: {} });
    await outbox.drain();
    const job = outbox.get("crm", "k-hang");
    assert.equal(job?.state, "uncertain");
    assert.match(job?.reason ?? "", /TimeoutError/);
    assert.equal(seen.filter((entry) => entry.key === "k-hang").length, 1);
  });

  it("retries a refused connection, since nothing left this machine", async () => {
    const closed = createServer();
    await new Promise<void>((resolve) => closed.listen(0, "127.0.0.1", resolve));
    const port = (closed.address() as AddressInfo).port;
    await new Promise<void>((resolve) => closed.close(() => resolve()));
    const { outbox } = outboxFor(`http://127.0.0.1:${port}/hooks`);
    outbox.enqueue({ destination: "crm", key: "k-refused", payload: {} });
    await outbox.drain();
    const job = outbox.get("crm", "k-refused");
    assert.equal(job?.state, "pending");
    assert.match(job?.reason ?? "", /never left this process.*ECONNREFUSED/);
  });

  it("does not follow a redirect with a signed body", async () => {
    const { outbox } = outboxFor(url);
    outbox.enqueue({ destination: "crm", key: "k-moved", payload: {} });
    await outbox.drain();
    assert.equal(outbox.get("crm", "k-moved")?.state, "failed");
    assert.equal(outbox.get("crm", "k-moved")?.reason, "HTTP 302");
  });
});

describe("neverLeft", () => {
  it("is true only when every error code proves the request never left", () => {
    const refused = Object.assign(new TypeError("fetch failed"), { cause: Object.assign(new Error("connect"), { code: "ECONNREFUSED" }) });
    const reset = Object.assign(new TypeError("fetch failed"), { cause: Object.assign(new Error("other side closed"), { code: "ECONNRESET" }) });
    const both = Object.assign(new TypeError("fetch failed"), {
      cause: new AggregateError([
        Object.assign(new Error("a"), { code: "ECONNREFUSED" }),
        Object.assign(new Error("b"), { code: "EHOSTUNREACH" }),
      ]),
    });
    assert.equal(neverLeft(refused), true);
    assert.equal(neverLeft(both), true);
    assert.equal(neverLeft(reset), false);
    assert.equal(neverLeft(new DOMException("The operation was aborted due to timeout", "TimeoutError")), false);
  });
});

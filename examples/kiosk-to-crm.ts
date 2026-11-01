import assert from "node:assert/strict";
import { type ServerResponse, createServer } from "node:http";
import type { AddressInfo } from "node:net";
import {
  type LookupAnswer,
  createOutbox,
  generateSecret,
  httpDestination,
  openDatabase,
  verifyHeaders,
} from "../src/index.ts";

type Step = "ok" | "busy" | "lose-response" | "conflict";

const secret = generateSecret();
const records = new Map<string, string>();
let posts = 0;
const plan: Record<string, Step[]> = {
  "checkin:demo-001": ["ok"],
  "checkin:demo-002": ["busy", "ok"],
  "checkin:demo-003": ["lose-response"],
  "checkin:demo-004": ["conflict", "ok"],
};

const crm = createServer((req, res: ServerResponse) => {
  const chunks: Buffer[] = [];
  req.on("data", (chunk: Buffer) => chunks.push(chunk));
  req.on("end", () => {
    if (req.method === "GET") {
      const key = decodeURIComponent((req.url ?? "").split("/").pop() ?? "");
      res.writeHead(records.has(key) ? 200 : 404).end();
      return;
    }
    const body = Buffer.concat(chunks).toString("utf8");
    const verdict = verifyHeaders({ headers: req.headers, body, secrets: [secret], nowSec: Math.floor(Date.now() / 1000) });
    if (!verdict.ok) {
      res.writeHead(401).end();
      return;
    }
    posts++;
    const step = plan[verdict.id]?.shift() ?? "ok";
    if (step === "busy") res.writeHead(503, { "retry-after": "30" }).end();
    else if (step === "conflict") res.writeHead(409).end();
    else {
      if (!records.has(verdict.id)) records.set(verdict.id, body);
      if (step === "ok") res.writeHead(200).end();
    }
  });
});
await new Promise<void>((resolve) => crm.listen(0, "127.0.0.1", resolve));
const base = `http://127.0.0.1:${(crm.address() as AddressInfo).port}`;

let now = Date.UTC(2026, 10, 2, 16, 0, 0);
const clock = () => now;
const db = openDatabase(":memory:");
db.exec("CREATE TABLE checkins (id TEXT PRIMARY KEY, phone_last4 TEXT NOT NULL, language TEXT NOT NULL, sms_consent INTEGER NOT NULL)");

const outbox = createOutbox({
  db,
  clock,
  destinations: {
    crm: {
      ...httpDestination({ url: `${base}/checkins`, secrets: [secret], timeoutMs: 300 }),
      lookup: async (key): Promise<LookupAnswer> => {
        const response = await fetch(`${base}/checkins/${encodeURIComponent(key)}`);
        return response.status === 200 ? "applied" : response.status === 404 ? "not_applied" : "unknown";
      },
    },
  },
});

const show = (title: string) => {
  console.log(`\n${title}`);
  for (const job of outbox.list()) {
    console.log(`  ${job.idempotencyKey.padEnd(17)} ${job.state.padEnd(10)} ${job.reason ?? ""}`);
  }
};

const visitors = [
  ["demo-001", "0142", "es"],
  ["demo-002", "0587", "en"],
  ["demo-003", "0311", "es"],
  ["demo-004", "0920", "es"],
] as const;
for (const [id, phoneLast4, language] of visitors) {
  outbox.transaction(() => {
    db.prepare("INSERT INTO checkins VALUES (?, ?, ?, 1)").run(id, phoneLast4, language);
    outbox.enqueue({ destination: "crm", key: `checkin:${id}`, payload: { checkinId: id, phoneLast4, language } });
  });
}
console.log("Four kiosk check-ins, each written in one transaction with its outbox job.");

await outbox.drain();
show("First drain:");

now += 60_000;
await outbox.drain();
show("One minute later. demo-002 went through; demo-003 is uncertain, so nothing retries it:");

now += 5 * 60_000;
const reconciled = await outbox.reconcile();
show(`Five minutes later, the outbox asks the CRM for demo-003 by key (${reconciled.confirmedApplied} found). Nothing is resent:`);

const plannedReplay = outbox.replay(
  { destination: "crm", key: "checkin:demo-004" },
  { actor: "front-desk lead", reason: "merged the duplicate contact in the CRM", dryRun: true },
);
console.log(`\nA person fixes the 409 in the CRM, dry-runs the replay (${plannedReplay.replayed.length} job), then replays it.`);
outbox.replay({ destination: "crm", key: "checkin:demo-004" }, { actor: "front-desk lead", reason: "merged the duplicate contact in the CRM" });
await outbox.drain();
show("After the replay:");

crm.closeAllConnections();
crm.close();

assert.deepEqual(outbox.status().counts, { pending: 0, claimed: 0, accepted: 4, failed: 0, uncertain: 0, suppressed: 0 });
assert.equal(records.size, 4);
assert.equal(posts, 6);
console.log(`\nThe CRM holds ${records.size} records from ${posts} signed requests. No check-in was written twice.`);

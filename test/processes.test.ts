import assert from "node:assert/strict";
import { spawn } from "node:child_process";
import { createServer } from "node:http";
import type { AddressInfo } from "node:net";
import { join } from "node:path";
import { describe, it } from "node:test";
import { fileURLToPath } from "node:url";
import { createOutbox } from "../src/outbox.ts";
import { openDatabase } from "../src/schema.ts";
import { generateSecret, verifyHeaders } from "../src/signing.ts";
import { scripted, tempDir } from "./helpers.ts";

const WORKERS = 4;
const JOBS = 200;
const workerScript = fileURLToPath(new URL("./fixtures/worker.ts", import.meta.url));

function runWorker(args: string[]): Promise<{ code: number | null; out: string; err: string }> {
  return new Promise((resolve) => {
    const child = spawn(process.execPath, ["--no-warnings=ExperimentalWarning", workerScript, ...args]);
    let out = "";
    let err = "";
    child.stdout.on("data", (chunk) => (out += chunk));
    child.stderr.on("data", (chunk) => (err += chunk));
    child.on("close", (code) => resolve({ code, out, err }));
  });
}

describe("separate OS processes draining one SQLite file", () => {
  it(`${WORKERS} workers, ${JOBS} jobs: every key is sent once and accepted`, async (t) => {
    const tmp = tempDir();
    const secret = generateSecret();
    const received = new Map<string, number>();
    let unsigned = 0;
    const server = createServer((req, res) => {
      const chunks: Buffer[] = [];
      req.on("data", (chunk: Buffer) => chunks.push(chunk));
      req.on("end", () => {
        const body = Buffer.concat(chunks).toString("utf8");
        const verdict = verifyHeaders({ headers: req.headers, body, secrets: [secret], nowSec: Math.floor(Date.now() / 1000) });
        if (!verdict.ok) unsigned++;
        const key = String(req.headers["idempotency-key"]);
        received.set(key, (received.get(key) ?? 0) + 1);
        res.writeHead(200).end();
      });
    });
    try {
      await new Promise<void>((resolve) => server.listen(0, "127.0.0.1", resolve));
      const url = `http://127.0.0.1:${(server.address() as AddressInfo).port}/hooks`;
      const path = join(tmp.dir, "outbox.sqlite");
      const db = openDatabase(path);
      const seed = createOutbox({ db, destinations: { crm: scripted().destination } });
      seed.transaction(() => {
        for (let index = 0; index < JOBS; index++) {
          seed.enqueue({ destination: "crm", key: `job-${String(index).padStart(3, "0")}`, payload: { index } });
        }
      });

      const results = await Promise.all(Array.from({ length: WORKERS }, () => runWorker([path, url, secret])));
      for (const result of results) assert.equal(result.code, 0, result.err);
      const sentByWorkers = results.map((result) => (JSON.parse(result.out) as { sent: number }).sent);
      t.diagnostic(`jobs sent per worker process: ${sentByWorkers.join(", ")}`);

      assert.equal(sentByWorkers.reduce((sum, count) => sum + count, 0), JOBS);
      assert.equal(received.size, JOBS);
      assert.deepEqual([...new Set(received.values())], [1]);
      assert.equal(unsigned, 0);
      assert.equal(seed.status().counts.accepted, JOBS);
      db.close();
    } finally {
      server.closeAllConnections();
      server.close();
      tmp.cleanup();
    }
  });
});

import { httpDestination } from "../../src/http.ts";
import { createOutbox } from "../../src/outbox.ts";
import { openDatabase } from "../../src/schema.ts";

const [dbPath, url, secret] = process.argv.slice(2);
if (!dbPath || !url || !secret) {
  process.stderr.write("usage: worker.ts <db> <url> <secret>\n");
  process.exit(2);
}

const outbox = createOutbox({
  db: openDatabase(dbPath),
  destinations: { crm: httpDestination({ url, secrets: [secret], timeoutMs: 5_000 }) },
});

let sent = 0;
for (;;) {
  const summary = await outbox.drain({ limit: 10 });
  sent += summary.attempted;
  if (summary.attempted === 0) break;
}
process.stdout.write(JSON.stringify({ pid: process.pid, sent }));

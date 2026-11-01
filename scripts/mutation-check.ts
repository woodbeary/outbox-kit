import { spawnSync } from "node:child_process";
import { cpSync, mkdtempSync, readFileSync, rmSync, symlinkSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { fileURLToPath } from "node:url";

const root = fileURLToPath(new URL("..", import.meta.url));

const mutations: Array<{ rule: string; file: string; from: string; to: string }> = [
  {
    rule: "uncertain jobs are never claimed",
    file: "src/outbox.ts",
    from:
      "WHERE state = 'pending' AND next_attempt_at <= ?\n" +
      "                           AND destination IN (SELECT value FROM json_each(?))\n" +
      "                         ORDER BY next_attempt_at, id LIMIT 1)\n" +
      "              AND state = 'pending'",
    to:
      "WHERE state IN ('pending', 'uncertain') AND COALESCE(next_attempt_at, 0) <= ?\n" +
      "                           AND destination IN (SELECT value FROM json_each(?))\n" +
      "                         ORDER BY next_attempt_at, id LIMIT 1)\n" +
      "              AND state IN ('pending', 'uncertain')",
  },
  {
    rule: "only the current claim token can settle a job",
    file: "src/outbox.ts",
    from: "WHERE id = ? AND claim_token = ? AND state IN ('claimed', 'uncertain')",
    to: "WHERE id = ? AND (claim_token = ? OR 1) AND state IN ('claimed', 'uncertain')",
  },
  {
    rule: "no send starts on an expired lease",
    file: "src/outbox.ts",
    from: "AND claim_token = ? AND lease_until > ?",
    to: "AND claim_token = ? AND (lease_until > ? OR 1)",
  },
  {
    rule: "a lease that runs out mid-send is uncertain, not pending",
    file: "src/outbox.ts",
    from: "SET state = 'uncertain', lease_until = NULL",
    to: "SET state = 'pending', lease_until = NULL",
  },
  {
    rule: "a request that may have arrived is uncertain",
    file: "src/classify.ts",
    from: 'return { outcome: "uncertain", detail: result.detail };',
    to: 'return { outcome: "retry", detail: result.detail };',
  },
  {
    rule: "500, 502 and 504 are uncertain",
    file: "src/classify.ts",
    from: "if (status >= 500 && !DEFINITE_SERVER_REFUSALS.has(status)) {",
    to: "if (false) {",
  },
  {
    rule: "same key with a different payload is an error",
    file: "src/outbox.ts",
    from: "if (existing.payloadSha256 !== hash) throw",
    to: "if (false) throw",
  },
  {
    rule: "retries stop at maxAttempts",
    file: "src/outbox.ts",
    from: "if (job.attempts >= maxAttempts) {",
    to: "if (false) {",
  },
  {
    rule: "the timestamp is inside the signature",
    file: "src/signing.ts",
    from: "update(`${id}.${timestampSec}.`)",
    to: "update(`${id}..`)",
  },
  {
    rule: "an inbound null never erases a known value",
    file: "src/merge.ts",
    from: "if (value === null || value === undefined) {",
    to: "if (value === undefined) {",
  },
  {
    rule: "suppressed jobs are never replayed",
    file: "src/outbox.ts",
    from: 'else if (job.state === "suppressed") skip',
    to: "else if (false) skip",
  },
  {
    rule: "every replay writes an audit row",
    file: "src/outbox.ts",
    from: "VALUES (?, ?, 'replay', ?, ?, ?, 'pending')",
    to: "SELECT ?, ?, 'replay', ?, ?, ?, 'pending' WHERE 0",
  },
  {
    rule: "a failing inbox handler's writes roll back",
    file: "src/inbox.ts",
    from: 'db.exec("ROLLBACK;");\n        if (error instanceof AsyncHandlerError',
    to: 'db.exec("COMMIT;");\n        if (error instanceof AsyncHandlerError',
  },
  {
    rule: "only proof that nothing left counts as not sent",
    file: "src/http.ts",
    from: "return codes.length > 0 && codes.every((code) => NEVER_LEFT_CODES.has(code));",
    to: "return codes.some((code) => NEVER_LEFT_CODES.has(code)) || codes.includes(\"ECONNRESET\");",
  },
];

function runTests(cwd: string) {
  return spawnSync(process.execPath, ["--no-warnings=ExperimentalWarning", "--test", "--test-reporter=tap", "test/**/*.test.ts"], {
    cwd,
    encoding: "utf8",
  });
}

const work = mkdtempSync(join(tmpdir(), "outbox-kit-mutants-"));
let survivors = 0;
try {
  for (const entry of ["src", "test", "package.json"]) cpSync(join(root, entry), join(work, entry), { recursive: true });
  symlinkSync(join(root, "node_modules"), join(work, "node_modules"), "dir");
  const baseline = runTests(work);
  if (baseline.status !== 0 || !/^# pass [1-9]/m.test(baseline.stdout)) {
    throw new Error(`the unmutated suite must pass before mutants mean anything:\n${baseline.stdout.slice(-2000)}`);
  }
  console.log(`baseline: ${/^# pass (\d+)/m.exec(baseline.stdout)?.[1]} tests pass unmutated\n`);
  for (const mutation of mutations) {
    const path = join(work, mutation.file);
    const original = readFileSync(path, "utf8");
    if (!original.includes(mutation.from)) {
      console.error(`stale mutation, pattern not found: ${mutation.rule}`);
      process.exitCode = 1;
      continue;
    }
    writeFileSync(path, original.replace(mutation.from, mutation.to));
    const run = runTests(work);
    writeFileSync(path, original);
    const failed = /^# fail (\d+)/m.exec(run.stdout)?.[1] ?? "?";
    const caught = run.status !== 0;
    if (!caught) survivors++;
    console.log(`${caught ? "caught " : "SURVIVED"}  ${mutation.rule} (${failed} failing)`);
  }
} finally {
  rmSync(work, { recursive: true, force: true });
}
console.log(`\n${mutations.length - survivors} of ${mutations.length} broken rules were caught by the tests.`);
if (survivors > 0) process.exitCode = 1;

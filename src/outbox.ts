import { randomUUID } from "node:crypto";
import type { DatabaseSync, SQLInputValue } from "node:sqlite";
import { DEFAULT_LADDER_MS, DEFAULT_MAX_DELAY_MS, classifyHttp, nextDelayMs } from "./classify.ts";
import { canonicalJson, clip, sha256Hex } from "./json.ts";
import { inTransaction, migrate } from "./schema.ts";
import {
  JOB_STATES,
  type Clock,
  type Decision,
  type Destination,
  type Job,
  type JobState,
  type Revalidation,
  type SendResult,
} from "./types.ts";

export interface OutboxOptions {
  db: DatabaseSync;
  destinations?: Record<string, Destination>;
  clock?: Clock;
  leaseMs?: number;
  maxAttempts?: number;
  ladderMs?: readonly number[];
  maxDelayMs?: number;
  newToken?: () => string;
}

export interface EnqueueInput {
  destination: string;
  key: string;
  payload: unknown;
}

export interface DeliveryResult {
  jobId: number;
  destination: string;
  idempotencyKey: string;
  outcome: JobState | "lease_lost";
  detail: string;
}

export interface DrainSummary {
  expired: { requeued: number; uncertain: number; exhausted: number };
  attempted: number;
  accepted: number;
  retrying: number;
  failed: number;
  uncertain: number;
  suppressed: number;
  leaseLost: number;
  results: DeliveryResult[];
}

export type ReplayState = "failed" | "uncertain";

export type ReplaySelector =
  | { destination: string; key: string; force?: boolean }
  | { state: ReplayState; destination?: string; limit?: number }
  | {
      createdFrom: number;
      createdTo: number;
      states?: readonly ReplayState[];
      destination?: string;
      limit?: number;
    };

export interface ReplayOptions {
  actor: string;
  reason: string;
  dryRun?: boolean;
}

export interface ReplayReport {
  dryRun: boolean;
  replayed: Array<{ id: number; destination: string; idempotencyKey: string; fromState: JobState }>;
  skipped: Array<{ destination: string; idempotencyKey: string; why: string }>;
}

export interface StatusReport {
  counts: Record<JobState, number>;
  dueNow: number;
  oldestUncertainAt: number | null;
}

export interface JobSummary {
  id: number;
  destination: string;
  idempotencyKey: string;
  state: JobState;
  attempts: number;
  reason: string | null;
  createdAt: number;
  updatedAt: number;
  nextAttemptAt: number | null;
}

export interface AttemptRow {
  attempt: number;
  claimedAt: number;
  sendStartedAt: number | null;
  finishedAt: number | null;
  outcome: string | null;
  detail: string | null;
}

export class IdempotencyConflictError extends Error {
  readonly destination: string;
  readonly key: string;
  constructor(destination: string, key: string) {
    super(
      `idempotency key ${key} for ${destination} was already used with a different payload; ` +
        "a key names one write, so a changed write needs a new key",
    );
    this.name = "IdempotencyConflictError";
    this.destination = destination;
    this.key = key;
  }
}

const KEY_PATTERN = /^[\x21-\x7e]{1,255}$/;
const REPLAY_LIMIT_MAX = 500;

type Row = Record<string, SQLInputValue>;

function toJob(row: Row): Job {
  return {
    id: Number(row.id),
    destination: String(row.destination),
    idempotencyKey: String(row.idempotency_key),
    payload: row.payload === null ? null : String(row.payload),
    payloadSha256: String(row.payload_sha256),
    state: row.state as JobState,
    attempts: Number(row.attempts),
    nextAttemptAt: row.next_attempt_at === null ? null : Number(row.next_attempt_at),
    claimToken: row.claim_token === null ? null : String(row.claim_token),
    leaseUntil: row.lease_until === null ? null : Number(row.lease_until),
    sendStartedAt: row.send_started_at === null ? null : Number(row.send_started_at),
    reason: row.reason === null ? null : String(row.reason),
    receipt: row.receipt === null ? null : String(row.receipt),
    createdAt: Number(row.created_at),
    updatedAt: Number(row.updated_at),
    settledAt: row.settled_at === null ? null : Number(row.settled_at),
  };
}

function summarize(job: Job): JobSummary {
  return {
    id: job.id,
    destination: job.destination,
    idempotencyKey: job.idempotencyKey,
    state: job.state,
    attempts: job.attempts,
    reason: job.reason,
    createdAt: job.createdAt,
    updatedAt: job.updatedAt,
    nextAttemptAt: job.nextAttemptAt,
  };
}

function requireText(name: string, value: string): string {
  const trimmed = value.trim();
  if (trimmed.length === 0) throw new Error(`${name} is required`);
  return clip(trimmed, 240);
}

export function createOutbox(options: OutboxOptions) {
  const db = options.db;
  const destinations = options.destinations ?? {};
  const clock = options.clock ?? Date.now;
  const leaseMs = options.leaseMs ?? 90_000;
  const maxAttempts = options.maxAttempts ?? 6;
  const ladderMs = options.ladderMs ?? DEFAULT_LADDER_MS;
  const maxDelayMs = options.maxDelayMs ?? DEFAULT_MAX_DELAY_MS;
  const newToken = options.newToken ?? randomUUID;

  if (leaseMs <= 0) throw new Error("leaseMs must be positive");
  if (maxAttempts < 1) throw new Error("maxAttempts must be at least 1");
  if (ladderMs.length === 0) throw new Error("ladderMs needs at least one step");
  for (const [name, destination] of Object.entries(destinations)) {
    if (destination.timeoutMs !== undefined && destination.timeoutMs >= leaseMs) {
      throw new Error(
        `destination ${name} times out after ${destination.timeoutMs} ms, which is not shorter than the ` +
          `${leaseMs} ms lease; a send must finish or give up before another worker can take the job`,
      );
    }
  }

  migrate(db);
  const registered = JSON.stringify(Object.keys(destinations));

  const transaction = <T>(work: () => T): T => inTransaction(db, work);

  function getById(id: number): Job | null {
    const row = db.prepare("SELECT * FROM outbox_jobs WHERE id = ?").get(id) as Row | undefined;
    return row ? toJob(row) : null;
  }

  function get(destination: string, key: string): Job | null {
    const row = db
      .prepare("SELECT * FROM outbox_jobs WHERE destination = ? AND idempotency_key = ?")
      .get(destination, key) as Row | undefined;
    return row ? toJob(row) : null;
  }

  function enqueue(input: EnqueueInput): { job: Job; created: boolean } {
    if (!(input.destination in destinations)) {
      throw new Error(`no destination named ${input.destination} is registered`);
    }
    if (!KEY_PATTERN.test(input.key)) {
      throw new Error("idempotency key must be 1-255 visible ASCII characters with no spaces, since it travels in HTTP headers");
    }
    const body = canonicalJson(input.payload);
    const hash = sha256Hex(body);
    const now = clock();
    const inserted = db
      .prepare(
        `INSERT INTO outbox_jobs
           (destination, idempotency_key, payload, payload_sha256, state, attempts, next_attempt_at, created_at, updated_at)
         VALUES (?, ?, ?, ?, 'pending', 0, ?, ?, ?)
         ON CONFLICT (destination, idempotency_key) DO NOTHING
         RETURNING *`,
      )
      .get(input.destination, input.key, body, hash, now, now, now) as Row | undefined;
    if (inserted) return { job: toJob(inserted), created: true };
    const existing = get(input.destination, input.key);
    if (!existing) throw new Error("enqueue conflicted but the existing job could not be read");
    if (existing.payloadSha256 !== hash) throw new IdempotencyConflictError(input.destination, input.key);
    return { job: existing, created: false };
  }

  function claimNext(): Job | null {
    return transaction(() => {
      const now = clock();
      const token = newToken();
      const row = db
        .prepare(
          `UPDATE outbox_jobs
              SET state = 'claimed', claim_token = ?, lease_until = ?, send_started_at = NULL,
                  attempts = attempts + 1, updated_at = ?
            WHERE id = (SELECT id FROM outbox_jobs
                         WHERE state = 'pending' AND next_attempt_at <= ?
                           AND destination IN (SELECT value FROM json_each(?))
                         ORDER BY next_attempt_at, id LIMIT 1)
              AND state = 'pending'
            RETURNING *`,
        )
        .get(token, now + leaseMs, now, now, registered) as Row | undefined;
      if (!row) return null;
      const job = toJob(row);
      db.prepare(
        "INSERT INTO outbox_attempts (job_id, attempt, claim_token, claimed_at) VALUES (?, ?, ?, ?)",
      ).run(job.id, job.attempts, token, now);
      return job;
    });
  }

  function markSendStarted(job: Job): boolean {
    const now = clock();
    return transaction(() => {
      const changed = db
        .prepare(
          `UPDATE outbox_jobs SET send_started_at = ?, updated_at = ?
            WHERE id = ? AND state = 'claimed' AND claim_token = ? AND lease_until > ?`,
        )
        .run(now, now, job.id, job.claimToken, now).changes;
      if (Number(changed) !== 1) return false;
      db.prepare("UPDATE outbox_attempts SET send_started_at = ? WHERE job_id = ? AND claim_token = ?").run(
        now,
        job.id,
        job.claimToken,
      );
      return true;
    });
  }

  function settle(job: Job, decision: Decision): DeliveryResult {
    const now = clock();
    const detail = clip(decision.detail);
    let state: JobState;
    let nextAttemptAt: number | null = null;
    let reason = detail;
    switch (decision.outcome) {
      case "retry":
        if (job.attempts >= maxAttempts) {
          state = "failed";
          reason = clip(`gave up after ${job.attempts} attempts; last: ${detail}`);
        } else {
          state = "pending";
          nextAttemptAt = now + nextDelayMs(job.attempts, ladderMs, decision.retryAfterMs, maxDelayMs);
        }
        break;
      default:
        state = decision.outcome;
    }
    const receipt = decision.outcome === "accepted" ? (decision.receipt ?? null) : null;
    const settledAt = state === "pending" ? null : now;
    return transaction(() => {
      const before = getById(job.id);
      const late = before?.state === "uncertain";
      const changed = db
        .prepare(
          `UPDATE outbox_jobs
              SET state = ?, next_attempt_at = ?, reason = ?, receipt = ?, settled_at = ?,
                  lease_until = NULL, updated_at = ?
            WHERE id = ? AND claim_token = ? AND state IN ('claimed', 'uncertain')`,
        )
        .run(state, nextAttemptAt, late ? clip(`late answer after the lease ran out: ${reason}`) : reason, receipt, settledAt, now, job.id, job.claimToken).changes;
      if (Number(changed) !== 1) {
        return {
          jobId: job.id,
          destination: job.destination,
          idempotencyKey: job.idempotencyKey,
          outcome: "lease_lost",
          detail: `another worker or an admin owns this job now; dropped: ${detail}`,
        };
      }
      db.prepare(
        "UPDATE outbox_attempts SET finished_at = ?, outcome = ?, detail = ? WHERE job_id = ? AND claim_token = ?",
      ).run(now, state, reason, job.id, job.claimToken);
      return { jobId: job.id, destination: job.destination, idempotencyKey: job.idempotencyKey, outcome: state, detail: reason };
    });
  }

  function expireLeases(): DrainSummary["expired"] {
    return transaction(() => {
      const now = clock();
      const exhausted = db
        .prepare(
          `UPDATE outbox_jobs
              SET state = 'failed', lease_until = NULL, next_attempt_at = NULL, settled_at = ?, updated_at = ?,
                  reason = 'gave up: every attempt ran out of lease before the request was sent'
            WHERE state = 'claimed' AND lease_until <= ? AND send_started_at IS NULL AND attempts >= ?
            RETURNING id, claim_token`,
        )
        .all(now, now, now, maxAttempts) as Row[];
      const requeued = db
        .prepare(
          `UPDATE outbox_jobs
              SET state = 'pending', lease_until = NULL, next_attempt_at = ?, updated_at = ?,
                  reason = 'lease ran out before the request was sent, so it is safe to send again'
            WHERE state = 'claimed' AND lease_until <= ? AND send_started_at IS NULL
            RETURNING id, claim_token`,
        )
        .all(now, now, now) as Row[];
      const uncertain = db
        .prepare(
          `UPDATE outbox_jobs
              SET state = 'uncertain', lease_until = NULL, next_attempt_at = NULL, settled_at = ?, updated_at = ?,
                  reason = 'lease ran out after the request started; the receiver may have it'
            WHERE state = 'claimed' AND lease_until <= ? AND send_started_at IS NOT NULL
            RETURNING id, claim_token`,
        )
        .all(now, now, now) as Row[];
      const mark = db.prepare(
        "UPDATE outbox_attempts SET finished_at = ?, outcome = ? WHERE job_id = ? AND claim_token = ?",
      );
      for (const row of exhausted) mark.run(now, "lease_expired_before_send", row.id ?? null, row.claim_token ?? null);
      for (const row of requeued) mark.run(now, "lease_expired_before_send", row.id ?? null, row.claim_token ?? null);
      for (const row of uncertain) mark.run(now, "lease_expired_after_send", row.id ?? null, row.claim_token ?? null);
      return { requeued: requeued.length, uncertain: uncertain.length, exhausted: exhausted.length };
    });
  }

  async function deliverOne(): Promise<DeliveryResult | null> {
    const job = claimNext();
    if (!job) return null;
    const destination = destinations[job.destination];
    if (!destination) return settle(job, { outcome: "failed", detail: `no destination named ${job.destination}` });
    if (job.payload === null) return settle(job, { outcome: "failed", detail: "payload was pruned" });

    if (destination.revalidate) {
      let verdict: Revalidation;
      try {
        verdict = await destination.revalidate(JSON.parse(job.payload), job);
      } catch (error) {
        return settle(job, { outcome: "retry", detail: `revalidation could not run, nothing sent: ${String(error)}` });
      }
      if (verdict.action === "fail") return settle(job, { outcome: "failed", detail: `revalidation: ${verdict.reason}` });
      if (verdict.action === "suppress") return settle(job, { outcome: "suppressed", detail: verdict.reason });
    }

    if (!markSendStarted(job)) {
      return {
        jobId: job.id,
        destination: job.destination,
        idempotencyKey: job.idempotencyKey,
        outcome: "lease_lost",
        detail: "lease ran out before sending, so this worker did not send",
      };
    }
    let result: SendResult;
    try {
      result = await destination.send({
        destination: job.destination,
        idempotencyKey: job.idempotencyKey,
        body: job.payload,
        attempt: job.attempts,
      });
    } catch (error) {
      result = { kind: "unknown", detail: `sender threw after the send began: ${String(error)}` };
    }
    const decision = (destination.classify ?? classifyHttp)(result, clock());
    return settle(job, decision);
  }

  async function drain(input: { limit?: number } = {}): Promise<DrainSummary> {
    const limit = Math.max(1, Math.trunc(input.limit ?? 25));
    const summary: DrainSummary = {
      expired: expireLeases(),
      attempted: 0,
      accepted: 0,
      retrying: 0,
      failed: 0,
      uncertain: 0,
      suppressed: 0,
      leaseLost: 0,
      results: [],
    };
    for (let index = 0; index < limit; index++) {
      const result = await deliverOne();
      if (!result) break;
      summary.attempted++;
      summary.results.push(result);
      if (result.outcome === "accepted") summary.accepted++;
      else if (result.outcome === "pending") summary.retrying++;
      else if (result.outcome === "failed") summary.failed++;
      else if (result.outcome === "uncertain") summary.uncertain++;
      else if (result.outcome === "suppressed") summary.suppressed++;
      else if (result.outcome === "lease_lost") summary.leaseLost++;
    }
    return summary;
  }

  async function reconcile(input: { minAgeMs?: number; limit?: number } = {}) {
    const minAgeMs = input.minAgeMs ?? 5 * 60_000;
    const limit = Math.max(1, Math.trunc(input.limit ?? 25));
    const withLookup = Object.entries(destinations)
      .filter(([, destination]) => destination.lookup !== undefined)
      .map(([name]) => name);
    const report = { confirmedApplied: 0, confirmedNotApplied: 0, stillUncertain: 0 };
    if (withLookup.length === 0) return report;
    const rows = db
      .prepare(
        `SELECT * FROM outbox_jobs
          WHERE state = 'uncertain' AND settled_at <= ?
            AND destination IN (SELECT value FROM json_each(?))
          ORDER BY settled_at, id LIMIT ?`,
      )
      .all(clock() - minAgeMs, JSON.stringify(withLookup), limit) as Row[];
    for (const row of rows) {
      const job = toJob(row);
      const lookup = destinations[job.destination]?.lookup;
      let answer: string = "unknown";
      try {
        answer = lookup ? await lookup(job.idempotencyKey, job) : "unknown";
      } catch {
        answer = "unknown";
      }
      if (answer !== "applied" && answer !== "not_applied") {
        report.stillUncertain++;
        continue;
      }
      const now = clock();
      const toState: JobState = answer === "applied" ? "accepted" : "pending";
      const changed: boolean = transaction(() => {
        const result = db
          .prepare(
            `UPDATE outbox_jobs
                SET state = ?, next_attempt_at = ?, settled_at = ?, reason = ?, updated_at = ?
              WHERE id = ? AND state = 'uncertain' AND updated_at = ?`,
          )
          .run(
            toState,
            toState === "pending" ? now : null,
            toState === "accepted" ? now : null,
            answer === "applied"
              ? "the receiver's own record shows this key was applied"
              : "the receiver's own record shows this key was never applied, so it is safe to send again",
            now,
            job.id,
            job.updatedAt,
          ).changes;
        if (Number(result) !== 1) return false;
        db.prepare(
          "INSERT INTO outbox_audit (at, actor, action, reason, job_id, from_state, to_state) VALUES (?, 'reconcile', 'lookup', ?, ?, 'uncertain', ?)",
        ).run(now, `receiver lookup answered ${answer}`, job.id, toState);
        return true;
      });
      if (!changed) report.stillUncertain++;
      else if (answer === "applied") report.confirmedApplied++;
      else report.confirmedNotApplied++;
    }
    return report;
  }

  function replay(selector: ReplaySelector, replayOptions: ReplayOptions): ReplayReport {
    const actor = requireText("actor", replayOptions.actor);
    const reason = requireText("reason", replayOptions.reason);
    const dryRun = replayOptions.dryRun ?? false;
    const report: ReplayReport = { dryRun, replayed: [], skipped: [] };
    const candidates: Job[] = [];

    if ("key" in selector) {
      const job = get(selector.destination, selector.key);
      const skip = (why: string) => report.skipped.push({ destination: selector.destination, idempotencyKey: selector.key, why });
      if (!job) skip("no job with this key");
      else if (job.state === "pending") skip("already queued");
      else if (job.state === "claimed") skip("a worker is sending it now");
      else if (job.state === "suppressed") skip("suppressed on purpose, so it is never replayed");
      else if (job.payload === null) skip("payload was pruned");
      else if (job.state === "accepted" && !selector.force) skip("already accepted; pass force to send it again");
      else candidates.push(job);
    } else {
      const limit = Math.min(REPLAY_LIMIT_MAX, Math.max(1, Math.trunc(selector.limit ?? 25)));
      const states = "state" in selector ? [selector.state] : (selector.states ?? ["failed", "uncertain"]);
      for (const state of states) {
        if (state !== "failed" && state !== "uncertain") throw new Error(`cannot replay jobs in state ${String(state)}`);
      }
      const clauses = ["state IN (SELECT value FROM json_each(?))", "payload IS NOT NULL"];
      const params: SQLInputValue[] = [JSON.stringify(states)];
      if (selector.destination !== undefined) {
        clauses.push("destination = ?");
        params.push(selector.destination);
      }
      if ("createdFrom" in selector) {
        clauses.push("created_at >= ?", "created_at < ?");
        params.push(selector.createdFrom, selector.createdTo);
      }
      const rows = db
        .prepare(`SELECT * FROM outbox_jobs WHERE ${clauses.join(" AND ")} ORDER BY created_at, id LIMIT ?`)
        .all(...params, limit) as Row[];
      candidates.push(...rows.map(toJob));
    }

    if (dryRun) {
      report.replayed = candidates.map((job) => ({
        id: job.id,
        destination: job.destination,
        idempotencyKey: job.idempotencyKey,
        fromState: job.state,
      }));
      return report;
    }

    transaction(() => {
      const now = clock();
      for (const job of candidates) {
        const changed = db
          .prepare(
            `UPDATE outbox_jobs
                SET state = 'pending', attempts = 0, next_attempt_at = ?, settled_at = NULL, lease_until = NULL,
                    reason = ?, updated_at = ?
              WHERE id = ? AND state = ?`,
          )
          .run(now, clip(`replayed by ${actor}: ${reason}`), now, job.id, job.state).changes;
        if (Number(changed) !== 1) {
          report.skipped.push({ destination: job.destination, idempotencyKey: job.idempotencyKey, why: "state changed while replaying" });
          continue;
        }
        db.prepare(
          "INSERT INTO outbox_audit (at, actor, action, reason, job_id, from_state, to_state) VALUES (?, ?, 'replay', ?, ?, ?, 'pending')",
        ).run(now, actor, reason, job.id, job.state);
        report.replayed.push({ id: job.id, destination: job.destination, idempotencyKey: job.idempotencyKey, fromState: job.state });
      }
    });
    return report;
  }

  function prune(input: { olderThanMs: number; actor: string; reason: string }): { pruned: number } {
    const actor = requireText("actor", input.actor);
    const reason = requireText("reason", input.reason);
    if (!(input.olderThanMs > 0)) throw new Error("olderThanMs must be positive");
    return transaction(() => {
      const now = clock();
      const rows = db
        .prepare(
          `UPDATE outbox_jobs SET payload = NULL, updated_at = ?
            WHERE state IN ('accepted', 'suppressed') AND payload IS NOT NULL AND settled_at <= ?
            RETURNING id, state`,
        )
        .all(now, now - input.olderThanMs) as Row[];
      const audit = db.prepare(
        "INSERT INTO outbox_audit (at, actor, action, reason, job_id, from_state, to_state) VALUES (?, ?, 'prune', ?, ?, ?, ?)",
      );
      for (const row of rows) audit.run(now, actor, reason, row.id ?? null, row.state ?? null, row.state ?? null);
      return { pruned: rows.length };
    });
  }

  function status(): StatusReport {
    const counts = Object.fromEntries(JOB_STATES.map((state) => [state, 0])) as Record<JobState, number>;
    for (const row of db.prepare("SELECT state, COUNT(*) AS n FROM outbox_jobs GROUP BY state").all() as Row[]) {
      counts[row.state as JobState] = Number(row.n);
    }
    const due = db
      .prepare("SELECT COUNT(*) AS n FROM outbox_jobs WHERE state = 'pending' AND next_attempt_at <= ?")
      .get(clock()) as Row;
    const oldest = db.prepare("SELECT MIN(settled_at) AS at FROM outbox_jobs WHERE state = 'uncertain'").get() as Row;
    return {
      counts,
      dueNow: Number(due.n),
      oldestUncertainAt: oldest.at === null ? null : Number(oldest.at),
    };
  }

  function list(input: { state?: JobState; destination?: string; limit?: number } = {}): JobSummary[] {
    const clauses: string[] = [];
    const params: SQLInputValue[] = [];
    if (input.state !== undefined) {
      clauses.push("state = ?");
      params.push(input.state);
    }
    if (input.destination !== undefined) {
      clauses.push("destination = ?");
      params.push(input.destination);
    }
    const where = clauses.length > 0 ? `WHERE ${clauses.join(" AND ")}` : "";
    const limit = Math.min(REPLAY_LIMIT_MAX, Math.max(1, Math.trunc(input.limit ?? 50)));
    const rows = db.prepare(`SELECT * FROM outbox_jobs ${where} ORDER BY id LIMIT ?`).all(...params, limit) as Row[];
    return rows.map((row) => summarize(toJob(row)));
  }

  function attempts(jobId: number): AttemptRow[] {
    const rows = db
      .prepare("SELECT * FROM outbox_attempts WHERE job_id = ? ORDER BY id")
      .all(jobId) as Row[];
    return rows.map((row) => ({
      attempt: Number(row.attempt),
      claimedAt: Number(row.claimed_at),
      sendStartedAt: row.send_started_at === null ? null : Number(row.send_started_at),
      finishedAt: row.finished_at === null ? null : Number(row.finished_at),
      outcome: row.outcome === null ? null : String(row.outcome),
      detail: row.detail === null ? null : String(row.detail),
    }));
  }

  function audit(jobId?: number) {
    const rows = (
      jobId === undefined
        ? db.prepare("SELECT * FROM outbox_audit ORDER BY id").all()
        : db.prepare("SELECT * FROM outbox_audit WHERE job_id = ? ORDER BY id").all(jobId)
    ) as Row[];
    return rows.map((row) => ({
      at: Number(row.at),
      actor: String(row.actor),
      action: String(row.action),
      reason: String(row.reason),
      jobId: row.job_id === null ? null : Number(row.job_id),
      fromState: row.from_state === null ? null : String(row.from_state),
      toState: row.to_state === null ? null : String(row.to_state),
    }));
  }

  return {
    transaction,
    enqueue,
    get,
    drain,
    deliverOne,
    expireLeases,
    reconcile,
    replay,
    prune,
    status,
    list,
    attempts,
    audit,
  };
}

export type Outbox = ReturnType<typeof createOutbox>;

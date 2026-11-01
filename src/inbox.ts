import type { DatabaseSync, SQLInputValue } from "node:sqlite";
import { DEFAULT_LADDER_MS, DEFAULT_MAX_DELAY_MS, nextDelayMs } from "./classify.ts";
import { clip, sha256Hex } from "./json.ts";
import { beginImmediate, inTransaction, migrate } from "./schema.ts";
import { type HeaderSource, verifyHeaders } from "./signing.ts";
import type { Clock } from "./types.ts";

export interface InboxSource {
  secrets: readonly string[];
  toleranceSec?: number;
}

export interface InboxOptions {
  db: DatabaseSync;
  sources: Record<string, InboxSource>;
  clock?: Clock;
  maxAttempts?: number;
  ladderMs?: readonly number[];
}

export interface InboxEvent {
  source: string;
  eventId: string;
  body: unknown;
  attempts: number;
  receivedAt: number;
}

export type ReceiveResult =
  | { status: "accepted"; eventId: string }
  | { status: "duplicate"; eventId: string; sameBody: boolean }
  | { status: "rejected"; reason: string };

export type InboxHandler = (event: InboxEvent, db: DatabaseSync) => void;

type Row = Record<string, SQLInputValue>;

export function createInbox(options: InboxOptions) {
  const db = options.db;
  const clock = options.clock ?? Date.now;
  const maxAttempts = options.maxAttempts ?? 6;
  const ladderMs = options.ladderMs ?? DEFAULT_LADDER_MS;
  migrate(db);

  function receive(source: string, request: { headers: HeaderSource; body: string | Uint8Array }): ReceiveResult {
    const config = options.sources[source];
    if (!config) return { status: "rejected", reason: "unknown_source" };
    const verdict = verifyHeaders({
      headers: request.headers,
      body: request.body,
      secrets: config.secrets,
      nowSec: Math.floor(clock() / 1000),
      ...(config.toleranceSec === undefined ? {} : { toleranceSec: config.toleranceSec }),
    });
    if (!verdict.ok) return { status: "rejected", reason: verdict.reason };
    const text = typeof request.body === "string" ? request.body : Buffer.from(request.body).toString("utf8");
    try {
      JSON.parse(text);
    } catch {
      return { status: "rejected", reason: "body_not_json" };
    }
    const now = clock();
    const hash = sha256Hex(text);
    const inserted = db
      .prepare(
        `INSERT INTO inbox_events (source, event_id, body, body_sha256, state, attempts, next_attempt_at, received_at)
         VALUES (?, ?, ?, ?, 'pending', 0, ?, ?)
         ON CONFLICT (source, event_id) DO NOTHING
         RETURNING event_id`,
      )
      .get(source, verdict.id, text, hash, now, now) as Row | undefined;
    if (inserted) return { status: "accepted", eventId: verdict.id };
    const existing = db
      .prepare("SELECT body_sha256 FROM inbox_events WHERE source = ? AND event_id = ?")
      .get(source, verdict.id) as Row | undefined;
    return { status: "duplicate", eventId: verdict.id, sameBody: existing?.body_sha256 === hash };
  }

  function process(handler: InboxHandler, input: { source?: string; limit?: number } = {}) {
    const limit = Math.max(1, Math.trunc(input.limit ?? 25));
    const summary = { processed: 0, retrying: 0, failed: 0 };
    for (let index = 0; index < limit; index++) {
      const now = clock();
      let row: Row | undefined;
      let failure: unknown = null;
      beginImmediate(db);
      try {
        row = db
          .prepare(
            `SELECT * FROM inbox_events
              WHERE state = 'pending' AND next_attempt_at <= ? AND (? IS NULL OR source = ?)
              ORDER BY next_attempt_at, received_at, event_id LIMIT 1`,
          )
          .get(now, input.source ?? null, input.source ?? null) as Row | undefined;
        if (!row) {
          db.exec("COMMIT;");
          break;
        }
        const event: InboxEvent = {
          source: String(row.source),
          eventId: String(row.event_id),
          body: JSON.parse(String(row.body)),
          attempts: Number(row.attempts),
          receivedAt: Number(row.received_at),
        };
        const returned: unknown = handler(event, db);
        if (returned !== null && typeof returned === "object" && typeof (returned as { then?: unknown }).then === "function") {
          throw new AsyncHandlerError();
        }
        db.prepare(
          `UPDATE inbox_events SET state = 'processed', processed_at = ?, attempts = attempts + 1, last_error = NULL,
                  next_attempt_at = NULL
            WHERE source = ? AND event_id = ?`,
        ).run(now, row.source ?? null, row.event_id ?? null);
        db.exec("COMMIT;");
        summary.processed++;
      } catch (error) {
        db.exec("ROLLBACK;");
        if (error instanceof AsyncHandlerError || !row) throw error;
        failure = error;
      }
      if (row && failure !== null) {
        const attempts = Number(row.attempts) + 1;
        const giveUp = attempts >= maxAttempts;
        inTransaction(db, () => {
          db.prepare(
            `UPDATE inbox_events SET attempts = ?, state = ?, next_attempt_at = ?, last_error = ?
              WHERE source = ? AND event_id = ? AND state = 'pending'`,
          ).run(
            attempts,
            giveUp ? "failed" : "pending",
            giveUp ? null : now + nextDelayMs(attempts, ladderMs, null, DEFAULT_MAX_DELAY_MS),
            clip(failure instanceof Error ? failure.message : String(failure)),
            row?.source ?? null,
            row?.event_id ?? null,
          );
        });
        if (giveUp) summary.failed++;
        else summary.retrying++;
      }
    }
    return summary;
  }

  function get(source: string, eventId: string) {
    const row = db
      .prepare("SELECT * FROM inbox_events WHERE source = ? AND event_id = ?")
      .get(source, eventId) as Row | undefined;
    if (!row) return null;
    return {
      state: String(row.state),
      attempts: Number(row.attempts),
      lastError: row.last_error === null ? null : String(row.last_error),
      nextAttemptAt: row.next_attempt_at === null ? null : Number(row.next_attempt_at),
      processedAt: row.processed_at === null ? null : Number(row.processed_at),
    };
  }

  function retryFailed(input: { actor: string; reason: string; source?: string; limit?: number }) {
    const actor = input.actor.trim();
    const reason = input.reason.trim();
    if (!actor || !reason) throw new Error("actor and reason are required");
    return inTransaction(db, () => {
      const now = clock();
      const rows = db
        .prepare(
          `UPDATE inbox_events SET state = 'pending', attempts = 0, next_attempt_at = ?, last_error = ?
            WHERE rowid IN (SELECT rowid FROM inbox_events WHERE state = 'failed' AND (? IS NULL OR source = ?)
                             ORDER BY received_at LIMIT ?)
            RETURNING source, event_id`,
        )
        .all(now, clip(`retried by ${actor}: ${reason}`), input.source ?? null, input.source ?? null, Math.max(1, Math.trunc(input.limit ?? 25))) as Row[];
      const audit = db.prepare(
        "INSERT INTO outbox_audit (at, actor, action, reason, job_id, from_state, to_state) VALUES (?, ?, ?, ?, NULL, 'failed', 'pending')",
      );
      for (const row of rows) audit.run(now, actor, `inbox_retry ${String(row.source)}/${String(row.event_id)}`, reason);
      return { retried: rows.length };
    });
  }

  return { receive, process, get, retryFailed };
}

export class AsyncHandlerError extends Error {
  constructor() {
    super("inbox handlers run inside a SQLite transaction and must be synchronous; an async handler's writes would land outside it");
    this.name = "AsyncHandlerError";
  }
}

export type Inbox = ReturnType<typeof createInbox>;

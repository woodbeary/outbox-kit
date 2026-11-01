import { DatabaseSync } from "node:sqlite";

export const SCHEMA = `
CREATE TABLE IF NOT EXISTS outbox_jobs (
  id INTEGER PRIMARY KEY,
  destination TEXT NOT NULL,
  idempotency_key TEXT NOT NULL,
  payload TEXT,
  payload_sha256 TEXT NOT NULL,
  state TEXT NOT NULL DEFAULT 'pending'
    CHECK (state IN ('pending', 'claimed', 'accepted', 'failed', 'uncertain', 'suppressed')),
  attempts INTEGER NOT NULL DEFAULT 0,
  next_attempt_at INTEGER,
  claim_token TEXT,
  lease_until INTEGER,
  send_started_at INTEGER,
  reason TEXT,
  receipt TEXT,
  created_at INTEGER NOT NULL,
  updated_at INTEGER NOT NULL,
  settled_at INTEGER,
  UNIQUE (destination, idempotency_key)
);
CREATE INDEX IF NOT EXISTS outbox_jobs_due ON outbox_jobs (state, next_attempt_at);
CREATE INDEX IF NOT EXISTS outbox_jobs_lease ON outbox_jobs (state, lease_until);
CREATE INDEX IF NOT EXISTS outbox_jobs_created ON outbox_jobs (created_at);

CREATE TABLE IF NOT EXISTS outbox_attempts (
  id INTEGER PRIMARY KEY,
  job_id INTEGER NOT NULL REFERENCES outbox_jobs (id),
  attempt INTEGER NOT NULL,
  claim_token TEXT NOT NULL,
  claimed_at INTEGER NOT NULL,
  send_started_at INTEGER,
  finished_at INTEGER,
  outcome TEXT,
  detail TEXT
);
CREATE INDEX IF NOT EXISTS outbox_attempts_job ON outbox_attempts (job_id, id);

CREATE TABLE IF NOT EXISTS outbox_audit (
  id INTEGER PRIMARY KEY,
  at INTEGER NOT NULL,
  actor TEXT NOT NULL,
  action TEXT NOT NULL,
  reason TEXT NOT NULL,
  job_id INTEGER,
  from_state TEXT,
  to_state TEXT
);

CREATE TABLE IF NOT EXISTS inbox_events (
  source TEXT NOT NULL,
  event_id TEXT NOT NULL,
  body TEXT NOT NULL,
  body_sha256 TEXT NOT NULL,
  state TEXT NOT NULL DEFAULT 'pending' CHECK (state IN ('pending', 'processed', 'failed')),
  attempts INTEGER NOT NULL DEFAULT 0,
  next_attempt_at INTEGER,
  last_error TEXT,
  received_at INTEGER NOT NULL,
  processed_at INTEGER,
  PRIMARY KEY (source, event_id)
);
CREATE INDEX IF NOT EXISTS inbox_events_due ON inbox_events (state, next_attempt_at);
`;

export function migrate(db: DatabaseSync): void {
  db.exec("PRAGMA foreign_keys = ON;");
  db.exec(SCHEMA);
}

export function openDatabase(path: string): DatabaseSync {
  const db = new DatabaseSync(path);
  db.exec("PRAGMA busy_timeout = 5000;");
  if (path !== ":memory:") {
    db.exec("PRAGMA journal_mode = WAL;");
    db.exec("PRAGMA synchronous = FULL;");
  }
  migrate(db);
  return db;
}

export const LOCK_WAIT_MS = 30_000;

const pause = new Int32Array(new SharedArrayBuffer(4));

function isBusy(error: unknown): boolean {
  const code = (error as { errcode?: number } | null)?.errcode;
  return code === 5 || code === 6;
}

export function beginImmediate(db: DatabaseSync, waitMs: number = LOCK_WAIT_MS): void {
  const deadline = Date.now() + waitMs;
  for (let attempt = 0; ; attempt++) {
    try {
      db.exec("BEGIN IMMEDIATE;");
      return;
    } catch (error) {
      if (!isBusy(error) || Date.now() >= deadline) throw error;
      const backoff = Math.min(250, 5 * 2 ** Math.min(attempt, 6));
      Atomics.wait(pause, 0, 0, backoff / 2 + Math.random() * (backoff / 2));
    }
  }
}

export function inTransaction<T>(db: DatabaseSync, work: () => T): T {
  if (db.isTransaction) {
    db.exec("SAVEPOINT outbox_kit;");
    try {
      const result = work();
      db.exec("RELEASE outbox_kit;");
      return result;
    } catch (error) {
      db.exec("ROLLBACK TO outbox_kit; RELEASE outbox_kit;");
      throw error;
    }
  }
  beginImmediate(db);
  try {
    const result = work();
    db.exec("COMMIT;");
    return result;
  } catch (error) {
    db.exec("ROLLBACK;");
    throw error;
  }
}

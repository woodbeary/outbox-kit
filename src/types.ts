export type JobState = "pending" | "claimed" | "accepted" | "failed" | "uncertain" | "suppressed";

export const JOB_STATES: readonly JobState[] = [
  "pending",
  "claimed",
  "accepted",
  "failed",
  "uncertain",
  "suppressed",
];

export interface Job {
  id: number;
  destination: string;
  idempotencyKey: string;
  payload: string | null;
  payloadSha256: string;
  state: JobState;
  attempts: number;
  nextAttemptAt: number | null;
  claimToken: string | null;
  leaseUntil: number | null;
  sendStartedAt: number | null;
  reason: string | null;
  receipt: string | null;
  createdAt: number;
  updatedAt: number;
  settledAt: number | null;
}

export interface SendRequest {
  destination: string;
  idempotencyKey: string;
  body: string;
  attempt: number;
}

export type SendResult =
  | { kind: "response"; status: number; retryAfter?: string | null; receipt?: string | null }
  | { kind: "not_sent"; detail: string }
  | { kind: "unknown"; detail: string };

export type Decision =
  | { outcome: "accepted"; detail: string; receipt?: string | null }
  | { outcome: "retry"; detail: string; retryAfterMs?: number | null }
  | { outcome: "failed"; detail: string }
  | { outcome: "uncertain"; detail: string }
  | { outcome: "suppressed"; detail: string };

export type Revalidation =
  | { action: "send" }
  | { action: "fail"; reason: string }
  | { action: "suppress"; reason: string };

export type LookupAnswer = "applied" | "not_applied" | "unknown";

export interface Destination {
  send: (request: SendRequest) => Promise<SendResult>;
  classify?: (result: SendResult, now: number) => Decision;
  revalidate?: (payload: unknown, job: Job) => Revalidation | Promise<Revalidation>;
  lookup?: (idempotencyKey: string, job: Job) => Promise<LookupAnswer>;
  timeoutMs?: number;
}

export type Clock = () => number;

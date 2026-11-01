import type { Decision, SendResult } from "./types.ts";

export const DEFAULT_LADDER_MS: readonly number[] = [
  60_000,
  5 * 60_000,
  15 * 60_000,
  60 * 60_000,
  6 * 60 * 60_000,
];

export const DEFAULT_MAX_DELAY_MS = 24 * 60 * 60_000;

export function nextDelayMs(
  attempts: number,
  ladderMs: readonly number[],
  retryAfterMs: number | null | undefined,
  maxDelayMs: number,
): number {
  const index = Math.min(ladderMs.length - 1, Math.max(0, attempts - 1));
  const step = ladderMs[index] ?? 0;
  return Math.min(Math.max(step, retryAfterMs ?? 0), maxDelayMs);
}

export function parseRetryAfter(value: string | null | undefined, now: number): number | null {
  if (value === null || value === undefined) return null;
  const trimmed = value.trim();
  if (/^\d+$/.test(trimmed)) return Number(trimmed) * 1000;
  const at = Date.parse(trimmed);
  if (Number.isNaN(at)) return null;
  return Math.max(0, at - now);
}

const NOT_PROCESSED_STATUSES = new Set([408, 425, 429, 503]);
const DEFINITE_SERVER_REFUSALS = new Set([501, 505]);

export function classifyHttp(result: SendResult, now: number): Decision {
  if (result.kind === "not_sent") {
    return { outcome: "retry", detail: `never left this process: ${result.detail}` };
  }
  if (result.kind === "unknown") {
    return { outcome: "uncertain", detail: result.detail };
  }
  const status = result.status;
  if (status >= 200 && status < 300) {
    return { outcome: "accepted", detail: `HTTP ${status}`, receipt: result.receipt ?? null };
  }
  if (NOT_PROCESSED_STATUSES.has(status)) {
    return {
      outcome: "retry",
      detail: `HTTP ${status}`,
      retryAfterMs: parseRetryAfter(result.retryAfter, now),
    };
  }
  if (status === 409) {
    return { outcome: "failed", detail: "HTTP 409: the receiver holds a conflicting record, so a person decides" };
  }
  if (status >= 500 && !DEFINITE_SERVER_REFUSALS.has(status)) {
    return { outcome: "uncertain", detail: `HTTP ${status}: the receiver may have written it before failing` };
  }
  return { outcome: "failed", detail: `HTTP ${status}` };
}

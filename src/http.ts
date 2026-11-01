import { classifyHttp } from "./classify.ts";
import { clip } from "./json.ts";
import { signHeaders } from "./signing.ts";
import type { Clock, Destination, SendRequest, SendResult } from "./types.ts";

export interface HttpDestinationOptions {
  url: string;
  secrets: readonly string[];
  timeoutMs?: number;
  headers?: Record<string, string>;
  fetch?: typeof fetch;
  clock?: Clock;
}

const NEVER_LEFT_CODES = new Set([
  "ECONNREFUSED",
  "ENOTFOUND",
  "EAI_AGAIN",
  "EHOSTUNREACH",
  "ENETUNREACH",
  "UND_ERR_CONNECT_TIMEOUT",
]);

export function httpDestination(options: HttpDestinationOptions): Destination {
  const timeoutMs = options.timeoutMs ?? 10_000;
  const fetchImpl = options.fetch ?? fetch;
  const clock = options.clock ?? Date.now;
  if (options.secrets.length === 0) throw new Error(`destination ${options.url} needs a signing secret`);

  async function send(request: SendRequest): Promise<SendResult> {
    const headers = {
      ...options.headers,
      "content-type": "application/json",
      "idempotency-key": request.idempotencyKey,
      ...signHeaders({
        id: request.idempotencyKey,
        timestampSec: Math.floor(clock() / 1000),
        body: request.body,
        secrets: options.secrets,
      }),
    };
    let response: Response;
    try {
      response = await fetchImpl(options.url, {
        method: "POST",
        headers,
        body: request.body,
        redirect: "manual",
        signal: AbortSignal.timeout(timeoutMs),
      });
    } catch (error) {
      const detail = clip(describe(error));
      return neverLeft(error) ? { kind: "not_sent", detail } : { kind: "unknown", detail };
    }
    await response.body?.cancel().catch(() => undefined);
    return {
      kind: "response",
      status: response.status,
      retryAfter: response.headers.get("retry-after"),
    };
  }

  return { send, classify: classifyHttp, timeoutMs };
}

export function errorCodes(error: unknown, depth = 0): string[] {
  if (depth > 4 || error === null || typeof error !== "object") return [];
  const codes: string[] = [];
  const code = (error as { code?: unknown }).code;
  if (typeof code === "string") codes.push(code);
  const nested = (error as { errors?: unknown }).errors;
  if (Array.isArray(nested)) {
    for (const inner of nested) codes.push(...errorCodes(inner, depth + 1));
  }
  codes.push(...errorCodes((error as { cause?: unknown }).cause, depth + 1));
  return codes;
}

export function neverLeft(error: unknown): boolean {
  const codes = errorCodes(error);
  return codes.length > 0 && codes.every((code) => NEVER_LEFT_CODES.has(code));
}

function describe(error: unknown): string {
  if (!(error instanceof Error)) return String(error);
  const codes = errorCodes(error);
  const name = error.name === "Error" || error.name === "TypeError" ? "" : `${error.name}: `;
  return `${name}${error.message}${codes.length > 0 ? ` (${[...new Set(codes)].join(", ")})` : ""}`;
}

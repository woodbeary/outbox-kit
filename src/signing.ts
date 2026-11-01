import { createHmac, randomBytes, timingSafeEqual } from "node:crypto";

const PREFIX = "whsec_";

export type HeaderSource =
  | Headers
  | Record<string, string | readonly string[] | undefined>;

export type VerifyResult =
  | { ok: true; id: string; timestampSec: number }
  | {
      ok: false;
      reason: "missing_headers" | "bad_timestamp" | "too_old" | "too_new" | "no_matching_signature";
    };

export function generateSecret(): string {
  return `${PREFIX}${randomBytes(32).toString("base64")}`;
}

export function secretKey(secret: string): Buffer {
  const encoded = secret.startsWith(PREFIX) ? secret.slice(PREFIX.length) : secret;
  const key = Buffer.from(encoded, "base64");
  if (key.length < 16 || key.toString("base64") !== normalizePadding(encoded)) {
    throw new Error("webhook secret must be base64 (optionally prefixed whsec_) and decode to at least 16 bytes");
  }
  return key;
}

function normalizePadding(encoded: string): string {
  const unpadded = encoded.replace(/=+$/, "");
  return unpadded + "=".repeat((4 - (unpadded.length % 4)) % 4);
}

function digest(key: Buffer, id: string, timestampSec: number, body: string | Uint8Array): Buffer {
  return createHmac("sha256", key).update(`${id}.${timestampSec}.`).update(body).digest();
}

export function signHeaders(input: {
  id: string;
  timestampSec: number;
  body: string | Uint8Array;
  secrets: readonly string[];
}): Record<string, string> {
  if (input.secrets.length === 0) throw new Error("at least one signing secret is required");
  const signatures = input.secrets.map(
    (secret) => `v1,${digest(secretKey(secret), input.id, input.timestampSec, input.body).toString("base64")}`,
  );
  return {
    "webhook-id": input.id,
    "webhook-timestamp": String(input.timestampSec),
    "webhook-signature": signatures.join(" "),
  };
}

export function readHeader(headers: HeaderSource, name: string): string | undefined {
  if (typeof (headers as Headers).get === "function") {
    return (headers as Headers).get(name) ?? undefined;
  }
  const record = headers as Record<string, string | readonly string[] | undefined>;
  const wanted = name.toLowerCase();
  for (const [key, value] of Object.entries(record)) {
    if (key.toLowerCase() !== wanted || value === undefined) continue;
    return typeof value === "string" ? value : value.join(" ");
  }
  return undefined;
}

export function verifyHeaders(input: {
  headers: HeaderSource;
  body: string | Uint8Array;
  secrets: readonly string[];
  nowSec: number;
  toleranceSec?: number;
}): VerifyResult {
  const id = readHeader(input.headers, "webhook-id");
  const timestamp = readHeader(input.headers, "webhook-timestamp");
  const signatureHeader = readHeader(input.headers, "webhook-signature");
  if (!id || !timestamp || !signatureHeader) return { ok: false, reason: "missing_headers" };
  if (!/^\d{1,12}$/.test(timestamp)) return { ok: false, reason: "bad_timestamp" };
  const timestampSec = Number(timestamp);
  const tolerance = input.toleranceSec ?? 300;
  if (input.nowSec - timestampSec > tolerance) return { ok: false, reason: "too_old" };
  if (timestampSec - input.nowSec > tolerance) return { ok: false, reason: "too_new" };

  const offered = signatureHeader
    .split(" ")
    .filter((entry) => entry.startsWith("v1,"))
    .map((entry) => Buffer.from(entry.slice(3), "base64"));
  for (const secret of input.secrets) {
    const expected = digest(secretKey(secret), id, timestampSec, input.body);
    for (const candidate of offered) {
      if (candidate.length === expected.length && timingSafeEqual(candidate, expected)) {
        return { ok: true, id, timestampSec };
      }
    }
  }
  return { ok: false, reason: "no_matching_signature" };
}

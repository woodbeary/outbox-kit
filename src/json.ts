import { createHash } from "node:crypto";

export function canonicalJson(value: unknown): string {
  const text = JSON.stringify(sortKeys(value));
  if (text === undefined) {
    throw new TypeError("payload must be JSON-serializable (got undefined, a function or a symbol)");
  }
  return text;
}

function sortKeys(value: unknown): unknown {
  if (value !== null && typeof value === "object" && typeof (value as { toJSON?: unknown }).toJSON === "function") {
    return sortKeys((value as { toJSON: () => unknown }).toJSON());
  }
  if (Array.isArray(value)) {
    return value.map(sortKeys);
  }
  if (value !== null && typeof value === "object") {
    const sorted: Record<string, unknown> = {};
    for (const key of Object.keys(value).sort()) {
      const child = (value as Record<string, unknown>)[key];
      if (child !== undefined && typeof child !== "function" && typeof child !== "symbol") {
        sorted[key] = sortKeys(child);
      }
    }
    return sorted;
  }
  return value;
}

export function sha256Hex(text: string): string {
  return createHash("sha256").update(text).digest("hex");
}

export function clip(text: string, max = 240): string {
  const trimmed = text.trim();
  return trimmed.length <= max ? trimmed : `${trimmed.slice(0, max - 1)}…`;
}

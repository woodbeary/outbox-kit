import { mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { openDatabase } from "../src/schema.ts";
import type { Destination, SendRequest, SendResult } from "../src/types.ts";

export const START = Date.UTC(2026, 9, 1, 16, 0, 0);

export function memoryDb() {
  return openDatabase(":memory:");
}

export function tempDir() {
  const dir = mkdtempSync(join(tmpdir(), "outbox-kit-"));
  return { dir, cleanup: () => rmSync(dir, { recursive: true, force: true }) };
}

export function fakeClock(start = START) {
  let now = start;
  return {
    clock: () => now,
    advance(ms: number) {
      now += ms;
    },
    get now() {
      return now;
    },
  };
}

export type Step = SendResult | (() => Promise<SendResult>);

export function scripted(steps: Step[] = []) {
  const calls: SendRequest[] = [];
  const queue = [...steps];
  const destination: Destination = {
    async send(request) {
      calls.push(request);
      const next = queue.shift();
      if (next === undefined) throw new Error(`no scripted result left for ${request.idempotencyKey}`);
      return typeof next === "function" ? next() : next;
    },
  };
  return { destination, calls, push: (...more: Step[]) => queue.push(...more) };
}

export function gate<T>() {
  let release!: (value: T) => void;
  const promise = new Promise<T>((resolve) => {
    release = resolve;
  });
  return { promise, release };
}

export const http = (status: number, retryAfter?: string): SendResult =>
  retryAfter === undefined ? { kind: "response", status } : { kind: "response", status, retryAfter };

export const OK = http(200);

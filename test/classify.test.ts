import assert from "node:assert/strict";
import { describe, it } from "node:test";
import { DEFAULT_LADDER_MS, DEFAULT_MAX_DELAY_MS, classifyHttp, nextDelayMs, parseRetryAfter } from "../src/classify.ts";
import { canonicalJson } from "../src/json.ts";
import { START, http } from "./helpers.ts";

describe("classifyHttp: what a response proves", () => {
  const cases: Array<[number, string]> = [
    [200, "accepted"],
    [201, "accepted"],
    [204, "accepted"],
    [408, "retry"],
    [425, "retry"],
    [429, "retry"],
    [503, "retry"],
    [500, "uncertain"],
    [502, "uncertain"],
    [504, "uncertain"],
    [501, "failed"],
    [505, "failed"],
    [409, "failed"],
    [400, "failed"],
    [401, "failed"],
    [404, "failed"],
    [422, "failed"],
    [302, "failed"],
  ];
  for (const [status, outcome] of cases) {
    it(`HTTP ${status} is ${outcome}`, () => {
      assert.equal(classifyHttp(http(status), START).outcome, outcome);
    });
  }

  it("a request that never left this process is safe to retry", () => {
    assert.equal(classifyHttp({ kind: "not_sent", detail: "ECONNREFUSED" }, START).outcome, "retry");
  });

  it("anything that may have reached the receiver is uncertain", () => {
    assert.equal(classifyHttp({ kind: "unknown", detail: "TimeoutError" }, START).outcome, "uncertain");
  });

  it("409 says a person decides, not a retry", () => {
    assert.match(classifyHttp(http(409), START).detail, /person decides/);
  });

  it("carries Retry-After into the decision", () => {
    const decision = classifyHttp(http(429, "120"), START);
    assert.equal(decision.outcome, "retry");
    assert.equal(decision.outcome === "retry" ? decision.retryAfterMs : null, 120_000);
  });
});

describe("Retry-After and the backoff ladder", () => {
  it("reads seconds and HTTP dates", () => {
    assert.equal(parseRetryAfter("30", START), 30_000);
    assert.equal(parseRetryAfter(new Date(START + 90_000).toUTCString(), START), 90_000);
    assert.equal(parseRetryAfter("soon", START), null);
    assert.equal(parseRetryAfter(null, START), null);
  });

  it("defaults to 1 min, 5 min, 15 min, 1 h, 6 h and stays on the last step", () => {
    const steps = [1, 2, 3, 4, 5, 6, 9].map((attempt) => nextDelayMs(attempt, DEFAULT_LADDER_MS, null, DEFAULT_MAX_DELAY_MS));
    assert.deepEqual(steps, [60_000, 300_000, 900_000, 3_600_000, 21_600_000, 21_600_000, 21_600_000]);
  });

  it("lets Retry-After make a wait longer, never shorter, and caps it at 24 hours", () => {
    assert.equal(nextDelayMs(1, DEFAULT_LADDER_MS, 1_000, DEFAULT_MAX_DELAY_MS), 60_000);
    assert.equal(nextDelayMs(1, DEFAULT_LADDER_MS, 600_000, DEFAULT_MAX_DELAY_MS), 600_000);
    assert.equal(nextDelayMs(1, DEFAULT_LADDER_MS, 10 * 86_400_000, DEFAULT_MAX_DELAY_MS), 86_400_000);
  });
});

describe("canonicalJson", () => {
  it("ignores key order, so the same write hashes the same", () => {
    assert.equal(canonicalJson({ b: 1, a: { d: 2, c: [3, { f: 4, e: 5 }] } }), '{"a":{"c":[3,{"e":5,"f":4}],"d":2},"b":1}');
  });

  it("drops undefined fields and uses toJSON", () => {
    assert.equal(canonicalJson({ at: new Date(START), skip: undefined }), `{"at":"${new Date(START).toISOString()}"}`);
  });

  it("refuses a payload that is not JSON", () => {
    assert.throws(() => canonicalJson(undefined), /JSON-serializable/);
  });
});

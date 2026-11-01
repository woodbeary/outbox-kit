import { existsSync, realpathSync } from "node:fs";
import { parseArgs } from "node:util";
import { type ReplaySelector, type ReplayState, createOutbox } from "./outbox.ts";
import { openDatabase } from "./schema.ts";
import { JOB_STATES, type JobState } from "./types.ts";

export const USAGE = `usage: outbox-kit <command> --db <file> [options]

  status                                   jobs per state, how many are due, the oldest uncertain one
  list [--state <state>] [--destination <d>] [--limit <n>]
  attempts --destination <d> --key <k>     every attempt for one job, and the audit trail
  replay --destination <d> --key <k> --actor <name> --reason <text> [--force] [--dry-run]
  replay --state failed|uncertain [--destination <d>] [--limit <n>] --actor <name> --reason <text> [--dry-run]
  replay --from <ISO time> --to <ISO time> [--state failed|uncertain] [--destination <d>] [--limit <n>]
         --actor <name> --reason <text> [--dry-run]

  --json prints machine-readable output. Replay never touches suppressed jobs, and resends an
  accepted job only by key with --force. Every replay records the actor and the reason.`;

export interface Io {
  out: (text: string) => void;
  err: (text: string) => void;
}

const iso = (ms: number | null) => (ms === null ? "-" : new Date(ms).toISOString());

export async function main(argv: string[], io: Io): Promise<number> {
  let parsed;
  try {
    parsed = parseArgs({
      args: argv,
      allowPositionals: true,
      options: {
        db: { type: "string" },
        state: { type: "string" },
        destination: { type: "string" },
        key: { type: "string" },
        limit: { type: "string" },
        actor: { type: "string" },
        reason: { type: "string" },
        from: { type: "string" },
        to: { type: "string" },
        force: { type: "boolean" },
        "dry-run": { type: "boolean" },
        json: { type: "boolean" },
        help: { type: "boolean", short: "h" },
      },
    });
  } catch (error) {
    io.err(`${(error as Error).message}\n\n${USAGE}`);
    return 2;
  }
  const { values, positionals } = parsed;
  const command = positionals[0];
  if (values.help || !command) {
    io.out(USAGE);
    return values.help ? 0 : 2;
  }
  const usage = (message: string) => {
    io.err(`${message}\n\n${USAGE}`);
    return 2;
  };
  if (!values.db) return usage("--db is required");
  if (!existsSync(values.db)) {
    io.err(`no database at ${values.db}`);
    return 1;
  }
  const state = values.state as JobState | undefined;
  if (state !== undefined && !JOB_STATES.includes(state)) return usage(`unknown state ${state}`);
  const limit = values.limit === undefined ? undefined : Number(values.limit);
  if (limit !== undefined && !(Number.isInteger(limit) && limit > 0)) return usage("--limit must be a positive integer");

  const db = openDatabase(values.db);
  try {
    const outbox = createOutbox({ db });
    const print = (data: unknown, text: () => string) => io.out(values.json ? JSON.stringify(data, null, 2) : text());

    if (command === "status") {
      const report = outbox.status();
      print(report, () =>
        [
          "state        jobs",
          ...JOB_STATES.map((name) => {
            const extra =
              name === "pending" && report.dueNow > 0
                ? `   ${report.dueNow} due now`
                : name === "uncertain" && report.oldestUncertainAt !== null
                  ? `   oldest since ${iso(report.oldestUncertainAt)}; needs a person or a receiver lookup`
                  : "";
            return `${name.padEnd(10)} ${String(report.counts[name]).padStart(6)}${extra}`;
          }),
        ].join("\n"),
      );
      return 0;
    }

    if (command === "list") {
      const rows = outbox.list({
        ...(state === undefined ? {} : { state }),
        ...(values.destination === undefined ? {} : { destination: values.destination }),
        ...(limit === undefined ? {} : { limit }),
      });
      print(rows, () =>
        rows.length === 0
          ? "no jobs"
          : rows
              .map((row) => `#${row.id} ${row.destination} ${row.idempotencyKey} ${row.state} attempts=${row.attempts} ${row.reason ?? ""}`.trim())
              .join("\n"),
      );
      return 0;
    }

    if (command === "attempts") {
      if (!values.destination || !values.key) return usage("attempts needs --destination and --key");
      const job = outbox.get(values.destination, values.key);
      if (!job) {
        io.err(`no job ${values.destination}/${values.key}`);
        return 1;
      }
      const history = { job: { ...job, payload: undefined }, attempts: outbox.attempts(job.id), audit: outbox.audit(job.id) };
      print(history, () =>
        [
          `#${job.id} ${job.destination} ${job.idempotencyKey}: ${job.state} (${job.reason ?? "no reason recorded"})`,
          ...history.attempts.map(
            (row) => `  attempt ${row.attempt} claimed ${iso(row.claimedAt)} sent ${iso(row.sendStartedAt)} -> ${row.outcome ?? "in flight"} ${row.detail ?? ""}`.trimEnd(),
          ),
          ...history.audit.map((row) => `  ${row.action} by ${row.actor} at ${iso(row.at)}: ${row.reason}`),
        ].join("\n"),
      );
      return 0;
    }

    if (command === "replay") {
      if (!values.actor || !values.reason) return usage("replay needs --actor and --reason");
      let selector: ReplaySelector;
      if (values.key !== undefined || values.force) {
        if (!values.destination || !values.key) return usage("replay by key needs --destination and --key");
        selector = { destination: values.destination, key: values.key, force: values.force ?? false };
      } else if (values.from !== undefined || values.to !== undefined) {
        const from = Date.parse(values.from ?? "");
        const to = Date.parse(values.to ?? "");
        if (Number.isNaN(from) || Number.isNaN(to) || to <= from) return usage("--from and --to must be ISO times, with --to after --from");
        if (state !== undefined && state !== "failed" && state !== "uncertain") return usage("replay only takes --state failed or uncertain");
        selector = {
          createdFrom: from,
          createdTo: to,
          ...(state === undefined ? {} : { states: [state as ReplayState] }),
          ...(values.destination === undefined ? {} : { destination: values.destination }),
          ...(limit === undefined ? {} : { limit }),
        };
      } else if (state === "failed" || state === "uncertain") {
        selector = {
          state,
          ...(values.destination === undefined ? {} : { destination: values.destination }),
          ...(limit === undefined ? {} : { limit }),
        };
      } else {
        return usage("replay needs --key, --state failed|uncertain, or --from/--to");
      }
      const report = outbox.replay(selector, { actor: values.actor, reason: values.reason, dryRun: values["dry-run"] ?? false });
      print(report, () =>
        [
          `${report.dryRun ? "dry run: would replay" : "replayed"} ${report.replayed.length} job${report.replayed.length === 1 ? "" : "s"}`,
          ...report.replayed.map((job) => `  #${job.id} ${job.destination} ${job.idempotencyKey} (was ${job.fromState})`),
          ...report.skipped.map((job) => `  skipped ${job.destination} ${job.idempotencyKey}: ${job.why}`),
        ].join("\n"),
      );
      return 0;
    }

    return usage(`unknown command ${command}`);
  } catch (error) {
    io.err((error as Error).message);
    return 1;
  } finally {
    db.close();
  }
}

if (process.argv[1] && realpathSync(process.argv[1]) === import.meta.filename) {
  const code = await main(process.argv.slice(2), {
    out: (text) => process.stdout.write(`${text}\n`),
    err: (text) => process.stderr.write(`${text}\n`),
  });
  process.exitCode = code;
}

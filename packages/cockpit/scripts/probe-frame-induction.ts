// DAT-807 verification: drive all FOUR frame inductions against the real API.
//
// induceConcepts / induceValidations / induceCycles / induceMetrics are reached
// only via `frameStagingSet` from the staging-hub modal, so nothing in the
// engine smoke or the chat surface exercises them. All four carry schemas
// reshaped for constrained decoding (the z.union step variants, the
// id-referenced output step, the array<->map boundary conversion, the ""/[]
// sentinels replacing every optional) — and a schema's first REAL compile
// happens on a request like this one, never in a test.
//
// Frames a DISTINCT vertical (PROBE_VERTICAL) so the existing `finance` model is
// untouched — framing WRITES concepts + overlay rows.
//
// Run with the same env as scripts/smoke-operating-model.ts, plus PROBE_CSV_DIR.

import { readFileSync } from "node:fs";
import { z } from "zod";

import { frame } from "#/tools/frame";

const env = z
	.object({
		// Directory holding the CSVs to frame, one file per table. REQUIRED —
		// the driver names its data explicitly, no hidden fixture default (the
		// `SOURCE_PATH` convention the sibling smoke drivers use).
		PROBE_CSV_DIR: z.string().min(1),
		// The vertical to frame under. Deliberately NOT `finance`: framing writes
		// concepts + overlay rows, and this must not disturb a real model.
		PROBE_VERTICAL: z.string().min(1).default("dat807_probe"),
	})
	.parse(process.env);

const CLEAN = env.PROBE_CSV_DIR.replace(/\/$/, "");
const TABLES = [
	"chart_of_accounts",
	"journal_entries",
	"journal_lines",
	"invoices",
	"payments",
	"bank_transactions",
	"trial_balance",
	"fx_rates",
];

function sniff(name: string) {
	const lines = readFileSync(`${CLEAN}/${name}.csv`, "utf8").split("\n");
	const header = (lines[0] ?? "").split(",").map((h) => h.trim());
	const sample = (lines[1] ?? "").split(",");
	const sample2 = (lines[2] ?? "").split(",");
	return {
		name,
		rowCountEstimate: lines.length - 2,
		columns: header.map((h, i) => {
			const v = sample[i] ?? "";
			const numeric = v !== "" && !Number.isNaN(Number(v));
			const dateish = /^\d{4}-\d{2}-\d{2}/.test(v);
			return {
				name: h,
				position: i,
				sourceType: dateish ? "DATE" : numeric ? "DOUBLE" : "VARCHAR",
				nullable: true,
				sampleValues: [sample[i], sample2[i]].filter((x) => x !== undefined),
			};
		}),
	};
}

const schema = {
	sourceKind: "file" as const,
	source: `${CLEAN}/*.csv`,
	tables: TABLES.map(sniff),
};

console.log(
	`schema: ${schema.tables.length} tables, ` +
		`${schema.tables.reduce((n, t) => n + t.columns.length, 0)} columns`,
);

const started = Date.now();
const result = await frame({
	schema,
	vertical_name: env.PROBE_VERTICAL,
});
const secs = Math.round((Date.now() - started) / 1000);

const n = (x: unknown) => (Array.isArray(x) ? x.length : 0);
console.log(`\n=== frame() completed in ${secs}s ===`);
console.log(`concepts:    ${n(result.concepts)}`);
console.log(`validations: ${n(result.validations)}`);
console.log(`cycles:      ${n(result.cycles)}`);
console.log(`metrics:     ${n(result.metrics)}`);

// NB: `result.metrics` are PAYLOAD-shaped (`toProposedMetric` already ran), so
// the DAG is the `dependencies` MAP keyed by step id, and a step's checks live
// under the SINGULAR `validation` key — reading `steps`/`validations` here
// silently reports every metric as empty.
console.log("\n--- metrics (the reshaped schema) ---");
for (const m of (result.metrics ?? []) as Array<Record<string, unknown>>) {
	const deps = (m.dependencies ?? {}) as Record<string, Record<string, unknown>>;
	const steps = Object.values(deps);
	const out = steps.find((s) => s.output_step === true);
	const checks = (out?.validation ?? []) as unknown[];
	console.log(
		`  ${String(m.graph_id)}  steps=${steps.length}` +
			`  types=[${steps.map((s) => String(s.type)).join(",")}]` +
			`  output_checks=${checks.length}`,
	);
	for (const c of checks as Array<Record<string, unknown>>) {
		console.log(`      check: ${String(c.condition)} (${String(c.severity)})`);
	}
}

console.log("\n--- validations (the reshaped parameters field) ---");
for (const v of (result.validations ?? []) as Array<Record<string, unknown>>) {
	console.log(
		`  ${String(v.validation_id)}  params=${JSON.stringify(v.parameters)}`,
	);
}

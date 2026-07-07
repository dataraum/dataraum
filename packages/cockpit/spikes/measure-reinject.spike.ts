// SPIKE: post-re-injection measurement for parts-at-source (DAT-671).
//
//   1. Parts coverage: every ACCEPTED extract snippet must carry clause parts;
//      failed rows are counted per failure_mode — the direct measurement of
//      what text repair used to hide (repair was removed with this cut).
//   2. Render parity: the persisted `sql` must equal compose_extract_sql over
//      the persisted parts (same rule mirrored here) — parts are the artifact,
//      sql is their render, zero drift allowed.
//   3. Ground truth: execute each accepted extract on the lake and compare the
//      known totals from dataraum-testdata output/clean/ground_truth.yaml
//      (annual revenue 51,766,199.72 — the 48%-error lesson: values, not
//      distributions).
//
// Run:  cd packages/cockpit && bun spikes/measure-reinject.spike.ts

import { and, desc, eq, like } from "drizzle-orm";

import { config } from "#/config";
import { metadataDb } from "#/db/metadata/client";
import { sqlSnippets } from "#/db/metadata/schema";
import { applyEngineScope, closeLake, withLakeConnection } from "#/duckdb/lake";

import { num, run } from "./spike-lib";

// Mirror of the engine's compose_extract_sql (formula_composer.py) — byte parity.
function renderFromParts(parts: unknown): string | null {
	if (typeof parts !== "object" || parts === null) return null;
	const p = parts as Record<string, unknown>;
	const select = Array.isArray(p.select) ? p.select : [];
	const first = select[0] as Record<string, unknown> | undefined;
	const expr = typeof first?.expr === "string" ? first.expr : null;
	if (!expr) return null;
	const fromArr = Array.isArray(p.from) ? p.from : [];
	const relation = typeof fromArr[0] === "string" ? fromArr[0] : null;
	const where = (Array.isArray(p.where) ? p.where : []).filter(
		(w): w is string => typeof w === "string" && w.trim() !== "",
	);
	let sql = `SELECT ${expr} AS value`;
	if (relation) sql += `\nFROM ${relation}`;
	if (where.length > 0) {
		const joined =
			where.length === 1 ? where[0] : where.map((w) => `(${w})`).join(" AND ");
		sql += `\nWHERE ${joined}`;
	}
	return sql;
}

// ground_truth.yaml totals (dataraum-testdata output/clean).
const GROUND_TRUTH: Record<string, number> = {
	revenue: 51766199.72,
};

async function main(): Promise<void> {
	const rows = await metadataDb
		.select({
			standardField: sqlSnippets.standardField,
			sql: sqlSnippets.sql,
			parts: sqlSnippets.parts,
			failureCount: sqlSnippets.failureCount,
			provenance: sqlSnippets.provenance,
		})
		.from(sqlSnippets)
		.where(
			and(
				eq(sqlSnippets.schemaMappingId, config.dataraumWorkspaceId),
				like(sqlSnippets.source, "graph:%"),
				eq(sqlSnippets.snippetType, "extract"),
			),
		)
		.orderBy(desc(sqlSnippets.updatedAt));

	const report: string[] = [];
	let accepted = 0;
	let failed = 0;
	let withParts = 0;
	let renderParity = 0;
	let renderDrift = 0;
	const failureModes = new Map<string, number>();

	const seen = new Set<string>();
	const newest = rows.filter((r) => {
		if (!r.standardField || seen.has(r.standardField)) return false;
		seen.add(r.standardField);
		return true;
	});

	await withLakeConnection(async (conn) => {
		await applyEngineScope(conn);
		for (const r of newest) {
			const ok = (r.failureCount ?? 0) === 0 && !!r.sql;
			if (!ok) {
				failed++;
				const prov = r.provenance as Record<string, unknown> | null;
				const mode = String(prov?.failure_mode ?? "unknown");
				failureModes.set(mode, (failureModes.get(mode) ?? 0) + 1);
				report.push(
					`  ✗ ${r.standardField}: FAILED (${mode}) — ${String(
						prov?.failure_reason ?? "?",
					).slice(0, 140)}`,
				);
				continue;
			}
			accepted++;
			if (r.parts) withParts++;
			const rendered = renderFromParts(r.parts);
			const parity = rendered !== null && rendered === r.sql;
			if (r.parts) parity ? renderParity++ : renderDrift++;

			const exec = await run(conn, r.sql ?? "");
			const value = "rows" in exec ? num(exec.rows[0]?.value) : null;
			const gt = GROUND_TRUTH[r.standardField ?? ""];
			const gtNote =
				gt !== undefined
					? value !== null && Math.abs(value - gt) < 0.01
						? " GT✓"
						: ` GT✗ (expected ${gt})`
					: "";
			report.push(
				`  ✓ ${r.standardField}: parts=${r.parts ? "yes" : "MISSING"} render=${
					parity ? "parity" : "DRIFT"
				} value=${String(value)}${gtNote}`,
			);
			if (!parity && rendered) {
				report.push(`      sql:      ${JSON.stringify(r.sql)}`);
				report.push(`      rendered: ${JSON.stringify(rendered)}`);
			}
		}
	});
	await closeLake();

	report.push(
		`\n=== ${newest.length} concepts | accepted ${accepted} (parts ${withParts}, render parity ${renderParity}, drift ${renderDrift}) | FAILED ${failed}`,
	);
	if (failureModes.size > 0) {
		report.push(
			`    failure modes: ${[...failureModes.entries()].map(([m, c]) => `${m}=${c}`).join(", ")}`,
		);
	}
	console.log(report.join("\n"));
}

await main();

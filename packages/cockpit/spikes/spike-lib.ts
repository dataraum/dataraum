// Shared spike helpers (NOT production code): lake exec helpers for the
// measurement spikes (measure-reinject, resume-om). The old fused-snippet
// parts EXTRACTION bridge died with parts-at-source — the engine persists
// clause parts now (`sql_snippets.parts`), consumed in production by
// `src/duckdb/parts.ts`.

import type { DuckDBConnection } from "@duckdb/node-api";

export async function run(
	conn: DuckDBConnection,
	sqlText: string,
): Promise<{ rows: Record<string, unknown>[] } | { error: string }> {
	try {
		const r = await conn.runAndReadAll(`SELECT * FROM (${sqlText}) LIMIT 20000`);
		return { rows: r.getRowObjectsJson() as Record<string, unknown>[] };
	} catch (err) {
		return {
			error:
				(err instanceof Error ? err.message : String(err)).split("\n")[0] ??
				"?",
		};
	}
}

export const num = (v: unknown): number | null => {
	if (v === null) return null; // Number(null) is 0 — never conflate — with 0
	const n = typeof v === "number" ? v : Number(v);
	return Number.isFinite(n) ? n : null;
};

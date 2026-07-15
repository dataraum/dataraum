// Integration coverage for the typed concept write (DAT-728 config→DB). The
// correctness that matters lives in the DB: an edit is a supersede + insert, and
// the `concepts` partial-unique index must keep at most ONE active row per
// (vertical, name). Like the other *.integration tests this hits a real Postgres
// (gated on METADATA_DATABASE_URL, reusing the running compose stack), not a mock —
// a unit-mocked Drizzle transaction would only prove the mock. Rows are written
// under a synthetic leading-underscore vertical (never a real one), so cleanup is a
// single delete-by-vertical and real seed/frame rows are untouched.

import { beforeAll, describe, expect, it } from "vitest";

const STACK_AVAILABLE = !!process.env.METADATA_DATABASE_URL;

if (STACK_AVAILABLE) {
	const REQUIRED_DEFAULTS: Record<string, string> = {
		COCKPIT_DATABASE_URL:
			process.env.COCKPIT_DATABASE_URL ??
			"postgresql://dataraum:dataraum@127.0.0.1:5432/cockpit_db",
		DATARAUM_WORKSPACE_ID:
			process.env.DATARAUM_WORKSPACE_ID ??
			"00000000-0000-0000-0000-000000000001",
		DATARAUM_CONFIG_PATH:
			process.env.DATARAUM_CONFIG_PATH ?? "/opt/dataraum/config",
		DATARAUM_LAKE_PATH:
			process.env.DATARAUM_LAKE_PATH ?? "s3://dataraum-lake/lake",
		DUCKLAKE_CATALOG_URL:
			process.env.DUCKLAKE_CATALOG_URL ??
			"postgresql://dataraum:dataraum@127.0.0.1:5432/lake_catalog",
		ANTHROPIC_API_KEY:
			process.env.ANTHROPIC_API_KEY ?? "sk-ant-test-placeholder",
		S3_ENDPOINT: process.env.S3_ENDPOINT ?? "127.0.0.1:8333",
		S3_ACCESS_KEY_ID: process.env.S3_ACCESS_KEY_ID ?? "dataraum",
		S3_SECRET_ACCESS_KEY:
			process.env.S3_SECRET_ACCESS_KEY ?? "dataraum-s3-secret",
		S3_BUCKET: process.env.S3_BUCKET ?? "dataraum-lake",
	};
	for (const [k, v] of Object.entries(REQUIRED_DEFAULTS)) {
		if (!process.env[k]) process.env[k] = v;
	}
}

const SCHEMA = STACK_AVAILABLE
	? `ws_${(process.env.DATARAUM_WORKSPACE_ID as string).replaceAll("-", "_")}`
	: "";

// A synthetic, leading-underscore vertical the test rows carry — never a real
// vertical, so cleanup by this value can never touch seed/frame data.
const TEST_VERTICAL = "_dat728_wc_test";

describe.skipIf(!STACK_AVAILABLE)("writeConcept (DAT-728 config→DB)", () => {
	let writer: typeof import("./concept-write");
	// biome-ignore lint/suspicious/noExplicitAny: dynamic-imported Bun SQL client
	let sql: any;

	beforeAll(async () => {
		writer = await import("./concept-write");
		const { SQL } = await import("bun");
		sql = new SQL(process.env.METADATA_DATABASE_URL as string);
		await cleanup();
		return async () => {
			await cleanup();
			await sql.close();
		};
	});

	async function cleanup(): Promise<void> {
		await sql.unsafe(`DELETE FROM "${SCHEMA}".concepts WHERE vertical = $1`, [
			TEST_VERTICAL,
		]);
	}

	async function activeRows(
		name: string,
	): Promise<Array<{ concept_id: string; kind: string; source: string }>> {
		return sql.unsafe(
			`SELECT concept_id, kind, source FROM "${SCHEMA}".concepts
			 WHERE vertical = $1 AND name = $2 AND superseded_at IS NULL`,
			[TEST_VERTICAL, name],
		);
	}

	it("declares a concept as one active row (source='frame')", async () => {
		const { concept_id } = await writer.writeConcept({
			vertical: TEST_VERTICAL,
			name: "revenue",
			kind: "measure",
			indicators: ["rev", "sales"],
		});
		const rows = await activeRows("revenue");
		expect(rows).toHaveLength(1);
		expect(rows[0].concept_id).toBe(concept_id);
		expect(rows[0].kind).toBe("measure");
		expect(rows[0].source).toBe("frame");
	});

	it("an edit supersedes the incumbent and leaves EXACTLY ONE active row", async () => {
		const first = await writer.writeConcept({
			vertical: TEST_VERTICAL,
			name: "cash",
			kind: "measure",
		});
		const second = await writer.writeConcept({
			vertical: TEST_VERTICAL,
			name: "cash",
			kind: "entity", // an edit: changed kind
		});
		expect(second.concept_id).not.toBe(first.concept_id);

		// The partial-unique index invariant: one active row, and it's the edit.
		const active = await activeRows("cash");
		expect(active).toHaveLength(1);
		expect(active[0].concept_id).toBe(second.concept_id);
		expect(active[0].kind).toBe("entity");

		// The incumbent is superseded (history preserved, not deleted).
		const all = await sql.unsafe(
			`SELECT concept_id, superseded_at FROM "${SCHEMA}".concepts
			 WHERE vertical = $1 AND name = $2 ORDER BY created_at`,
			[TEST_VERTICAL, "cash"],
		);
		expect(all).toHaveLength(2);
		const firstRow = all.find(
			(r: { concept_id: string }) => r.concept_id === first.concept_id,
		);
		expect(firstRow.superseded_at).not.toBeNull();
	});
});

// Integration coverage for the typed convention write (DAT-789 config→DB). The
// correctness that matters lives in the DB: an edit is a supersede + insert, and the
// `conventions` partial-unique index must keep at most ONE active row per
// (vertical, name). Like the other *.integration tests this hits a real Postgres (gated
// on METADATA_DATABASE_URL, reusing the running compose stack), not a mock — a
// unit-mocked Drizzle transaction would only prove the mock. Rows are written under a
// synthetic leading-underscore vertical (never a real one), so cleanup is a single
// delete-by-vertical and real seed/frame rows are untouched. Mirrors
// concept-write.integration.test.ts.

import { beforeAll, describe, expect, it } from "vitest";

const STACK_AVAILABLE =
	!!process.env.METADATA_DATABASE_URL &&
	!!process.env.METADATA_WRITER_DATABASE_URL;

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

// A synthetic, leading-underscore vertical the test rows carry — never a real vertical,
// so cleanup by this value can never touch seed/frame data.
const TEST_VERTICAL = "_dat789_conv_test";

describe.skipIf(!STACK_AVAILABLE)("writeConvention (DAT-789 config→DB)", () => {
	let writer: typeof import("./convention-write");
	// biome-ignore lint/suspicious/noExplicitAny: dynamic-imported Bun SQL client
	let sql: any;

	beforeAll(async () => {
		writer = await import("./convention-write");
		const { SQL } = await import("bun");
		// Engine-emulation scaffolding (seed/cleanup raw ws_<id> rows): the app roles
		// deliberately cannot express these — superuser connection.
		sql = new SQL(
			process.env.METADATA_ADMIN_DATABASE_URL ??
				"postgresql://dataraum:dataraum@127.0.0.1:5432/dataraum",
		);
		await cleanup();
		return async () => {
			await cleanup();
			await sql.close();
		};
	});

	async function cleanup(): Promise<void> {
		await sql.unsafe(
			`DELETE FROM "${SCHEMA}".conventions WHERE vertical = $1`,
			[TEST_VERTICAL],
		);
	}

	async function activeRows(name: string): Promise<
		Array<{
			convention_id: string;
			statement: string;
			source: string;
			targets: string[] | null;
			concept_groups: Record<string, string[]> | null;
		}>
	> {
		return sql.unsafe(
			`SELECT convention_id, statement, source, targets, concept_groups
			 FROM "${SCHEMA}".conventions
			 WHERE vertical = $1 AND name = $2 AND superseded_at IS NULL`,
			[TEST_VERTICAL, name],
		);
	}

	it("declares a convention as one active row (source='frame'), envelope typed", async () => {
		const { convention_id } = await writer.writeConvention({
			vertical: TEST_VERTICAL,
			name: "sign_rule",
			statement: "Express every measure so a healthy value reads positive.",
			targets: ["extraction", "validation:sign_check", "qa"],
			concept_groups: { credit_normal: ["revenue"], debit_normal: ["expense"] },
		});
		const rows = await activeRows("sign_rule");
		expect(rows).toHaveLength(1);
		expect(rows[0].convention_id).toBe(convention_id);
		expect(rows[0].source).toBe("frame");
		// The statement is stored VERBATIM.
		expect(rows[0].statement).toContain("reads positive");
		// The routing + partition envelopes round-trip typed (JSON columns).
		expect(rows[0].targets).toContain("validation:sign_check");
		expect(rows[0].concept_groups?.credit_normal).toEqual(["revenue"]);
	});

	it("an edit supersedes the incumbent and leaves EXACTLY ONE active row", async () => {
		const first = await writer.writeConvention({
			vertical: TEST_VERTICAL,
			name: "netting",
			statement: "original rule",
			targets: ["extraction"],
		});
		const second = await writer.writeConvention({
			vertical: TEST_VERTICAL,
			name: "netting",
			statement: "edited rule", // an edit: changed statement
			targets: ["extraction", "qa"],
		});
		expect(second.convention_id).not.toBe(first.convention_id);

		// The partial-unique index invariant: one active row, and it's the edit.
		const active = await activeRows("netting");
		expect(active).toHaveLength(1);
		expect(active[0].convention_id).toBe(second.convention_id);
		expect(active[0].statement).toBe("edited rule");

		// The incumbent is superseded (history preserved, not deleted).
		const all = await sql.unsafe(
			`SELECT convention_id, superseded_at FROM "${SCHEMA}".conventions
			 WHERE vertical = $1 AND name = $2 ORDER BY created_at`,
			[TEST_VERTICAL, "netting"],
		);
		expect(all).toHaveLength(2);
		const firstRow = all.find(
			(r: { convention_id: string }) => r.convention_id === first.convention_id,
		);
		expect(firstRow.superseded_at).not.toBeNull();
	});

	it("a statement-only convention stores NULL targets / concept_groups", async () => {
		await writer.writeConvention({
			vertical: TEST_VERTICAL,
			name: "bare",
			statement: "just prose, no routing or groups",
		});
		const rows = await activeRows("bare");
		expect(rows).toHaveLength(1);
		expect(rows[0].targets).toBeNull();
		expect(rows[0].concept_groups).toBeNull();
	});
});

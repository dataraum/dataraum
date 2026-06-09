// Unit coverage for the `answer` tool's DETERMINISTIC pieces (DAT-485): reuse
// classification (the measurable re-usage surface), the data-quality band read,
// and answer assembly from the captured validated run. The sub-agent's chat()
// loop itself is exercised by the live smoke (it calls the real LLM).

import { describe, expect, it, vi } from "vitest";

vi.mock("#/config", () => ({
	config: { dataraumWorkspaceId: "ws-test", anthropicApiKey: "k" },
}));
vi.mock("#/db/metadata/client", () => ({ metadataDb: {} }));

// findById drives reuse classification; the other library exports exist only so
// snippet-search (pulled via query.ts) imports cleanly.
const findByIdMock = vi.fn();
vi.mock("#/db/metadata/snippet-library", () => ({
	findById: (id: string) => findByIdMock(id),
	findGraphsByKeys: vi.fn(),
	getSearchVocabulary: vi.fn(),
}));

// listTables drives the data-quality band.
const listTablesMock = vi.fn();
vi.mock("#/tools/list-tables", () => ({
	listTables: () => listTablesMock(),
}));

import {
	assembleAnswer,
	classifyComponents,
	type QueryDraft,
	readDataQuality,
} from "./query";

type InvRow = {
	physical_name: string;
	table_name: string;
	worst_band: "ready" | "investigate" | "blocked" | null;
};
const inv = (rows: InvRow[]) => {
	// biome-ignore lint/suspicious/noExplicitAny: a partial inventory row is enough for the band read.
	listTablesMock.mockResolvedValue(rows as any);
};

describe("classifyComponents (the reuse surface)", () => {
	it("tags a step with no snippet_id as fresh", async () => {
		const out = await classifyComponents([
			{ name: "revenue", sql: "SELECT 1", snippet_id: null },
		]);
		expect(out).toEqual([
			{ name: "revenue", sql: "SELECT 1", snippet_id: null, usage: "fresh" },
		]);
		expect(findByIdMock).not.toHaveBeenCalled();
	});

	it("clears a hallucinated (unknown) snippet_id → fresh, SQL kept", async () => {
		findByIdMock.mockResolvedValue(null);
		const out = await classifyComponents([
			{ name: "revenue", sql: "SELECT 9", snippet_id: "ghost" },
		]);
		expect(out[0]).toEqual({
			name: "revenue",
			sql: "SELECT 9",
			snippet_id: null,
			usage: "fresh",
		});
	});

	it("tags exact_reuse when the (canonicalized) SQL matches the stored snippet", async () => {
		// Model writes QUALIFIED, the stored snippet is BARE — canonicalizeForReuse
		// strips the qualifier so it still classifies as exact_reuse. The executable
		// (qualified) SQL is KEPT — never substituted with the unresolvable bare form.
		findByIdMock.mockResolvedValue({
			sql: 'SELECT SUM("Betrag") AS revenue FROM journal_lines',
		});
		const out = await classifyComponents([
			{
				name: "revenue",
				sql: 'SELECT SUM("Betrag") AS revenue FROM lake.typed.journal_lines',
				snippet_id: "s1",
			},
		]);
		expect(out[0].usage).toBe("exact_reuse");
		expect(out[0].snippet_id).toBe("s1");
		// SQL is the model's executable (qualified) form, NOT the stored bare form.
		expect(out[0].sql).toBe(
			'SELECT SUM("Betrag") AS revenue FROM lake.typed.journal_lines',
		);
	});

	it("tags adapted when the SQL genuinely differs", async () => {
		findByIdMock.mockResolvedValue({
			sql: "SELECT SUM(amount) FROM lake.typed.orders",
		});
		const out = await classifyComponents([
			{
				name: "revenue",
				sql: "SELECT SUM(amount) FROM lake.typed.orders WHERE region = 'EMEA'",
				snippet_id: "s1",
			},
		]);
		expect(out[0].usage).toBe("adapted");
		expect(out[0].snippet_id).toBe("s1");
		expect(out[0].sql).toContain("EMEA");
	});
});

describe("readDataQuality", () => {
	it("returns null when no tables were touched", async () => {
		expect(await readDataQuality([])).toBeNull();
	});

	it("reports the worst band across the touched tables, naming them", async () => {
		inv([
			{
				physical_name: "orders",
				table_name: "orders",
				worst_band: "investigate",
			},
			{
				physical_name: "customers",
				table_name: "customers",
				worst_band: "blocked",
			},
			{ physical_name: "regions", table_name: "regions", worst_band: "ready" },
		]);
		const dq = await readDataQuality(["orders", "customers"]);
		expect(dq?.band).toBe("blocked");
		expect(dq?.note).toContain("customers");
	});

	it("matches by display name too (src-prefixed physical name)", async () => {
		inv([
			{
				physical_name: "src_abc__orders",
				table_name: "orders",
				worst_band: "investigate",
			},
		]);
		expect((await readDataQuality(["orders"]))?.band).toBe("investigate");
	});

	it("returns null when nothing matched", async () => {
		inv([
			{ physical_name: "orders", table_name: "orders", worst_band: "ready" },
		]);
		expect(await readDataQuality(["nonexistent"])).toBeNull();
	});

	it("returns null when touched tables are unanalyzed (null band)", async () => {
		inv([{ physical_name: "orders", table_name: "orders", worst_band: null }]);
		expect(await readDataQuality(["orders"])).toBeNull();
	});
});

describe("assembleAnswer", () => {
	const draft = (over: Partial<QueryDraft> = {}): QueryDraft => ({
		answer: "Total revenue is 150.",
		assumptions: ["Treated null amounts as zero."],
		concepts_used: ["revenue"],
		tables_touched: ["orders"],
		...over,
	});

	it("uses the captured composed SQL as the grid + passes provenance + components", () => {
		const out = assembleAnswer(
			draft(),
			{
				composedSql: "WITH revenue AS (SELECT 1 AS r) SELECT r FROM revenue",
				components: [
					{
						name: "revenue",
						sql: "SELECT 1 AS r",
						snippet_id: "s1",
						usage: "exact_reuse",
					},
				],
			},
			{ band: "investigate", note: "n" },
		);
		expect(out.grid).toEqual({
			sql: "WITH revenue AS (SELECT 1 AS r) SELECT r FROM revenue",
		});
		expect(out.answer).toBe("Total revenue is 150.");
		expect(out.assumptions).toEqual(["Treated null amounts as zero."]);
		expect(out.concepts_used).toEqual(["revenue"]);
		expect(out.tables_touched).toEqual(["orders"]);
		expect(out.data_quality).toEqual({ band: "investigate", note: "n" });
		expect(out.components).toEqual([
			{
				name: "revenue",
				sql: "SELECT 1 AS r",
				snippet_id: "s1",
				usage: "exact_reuse",
			},
		]);
		// Reliability aggregates the components: 1 exact_reuse of 1 → fully grounded.
		expect(out.reliability).toEqual({
			grounded_ratio: 1,
			exact_reuse: 1,
			adapted: 0,
			fresh: 0,
		});
	});

	it("yields a null grid + empty components when nothing was validated", () => {
		const out = assembleAnswer(draft(), null, null);
		expect(out.grid).toBeNull();
		expect(out.components).toEqual([]);
		expect(out.data_quality).toBeNull();
	});

	it("yields a null grid when the captured composed SQL is blank", () => {
		const out = assembleAnswer(
			draft(),
			{ composedSql: "   ", components: [] },
			null,
		);
		expect(out.grid).toBeNull();
	});
});

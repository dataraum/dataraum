// Unit coverage for the `answer` tool's DETERMINISTIC pieces (DAT-485): reuse
// resolution, the data-quality band read, and answer assembly. The sub-agent's
// chat() loop itself is exercised by the live smoke (it calls the real LLM); here
// we pin the post-chat logic that turns a draft into the answer.

import { describe, expect, it, vi } from "vitest";

vi.mock("#/config", () => ({
	config: { dataraumWorkspaceId: "ws-test", anthropicApiKey: "k" },
}));
vi.mock("#/db/metadata/client", () => ({ metadataDb: {} }));

// findById drives reuse resolution; the other library exports exist only so
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
	type QueryDraft,
	readDataQuality,
	resolveSnippetReferences,
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

describe("resolveSnippetReferences", () => {
	it("keeps a step with no snippet_id as fresh (snippet_id null)", async () => {
		findByIdMock.mockResolvedValue(null);
		const out = await resolveSnippetReferences([
			{ name: "revenue", sql: "SELECT 1", snippet_id: null },
		]);
		expect(out).toEqual([
			{ name: "revenue", sql: "SELECT 1", snippet_id: null },
		]);
		expect(findByIdMock).not.toHaveBeenCalled();
	});

	it("clears a hallucinated (unknown) snippet_id and keeps the SQL", async () => {
		findByIdMock.mockResolvedValue(null);
		const out = await resolveSnippetReferences([
			{ name: "revenue", sql: "SELECT 9", snippet_id: "ghost" },
		]);
		expect(out).toEqual([
			{ name: "revenue", sql: "SELECT 9", snippet_id: null },
		]);
		expect(out[0].snippet_id).toBeNull();
		expect(out[0].sql).toBe("SELECT 9");
	});

	it("substitutes the stored SQL on exact reuse (normalized match)", async () => {
		// Model SQL differs only by case/whitespace → exact_reuse → canonical stored
		// SQL is substituted.
		findByIdMock.mockResolvedValue({
			sql: 'SELECT SUM("amount") AS revenue FROM orders',
		});
		const out = await resolveSnippetReferences([
			{
				name: "revenue",
				sql: 'select  sum("amount")  as revenue\nfrom orders',
				snippet_id: "s1",
			},
		]);
		expect(out[0].sql).toBe('SELECT SUM("amount") AS revenue FROM orders');
		expect(out[0].snippet_id).toBe("s1");
	});

	it("keeps the model SQL on adaptation (differs), snippet_id tracks provenance", async () => {
		findByIdMock.mockResolvedValue({ sql: "SELECT SUM(amount) FROM orders" });
		const out = await resolveSnippetReferences([
			{
				name: "revenue",
				sql: "SELECT SUM(amount) FROM orders WHERE region = 'EMEA'",
				snippet_id: "s1",
			},
		]);
		expect(out[0].sql).toBe(
			"SELECT SUM(amount) FROM orders WHERE region = 'EMEA'",
		);
		expect(out[0].snippet_id).toBe("s1");
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
		expect(dq).not.toBeNull();
		expect(dq?.band).toBe("blocked");
		expect(dq?.note).toContain("customers");
		expect(dq?.note).toContain("blocked");
	});

	it("matches by display name too (src-prefixed physical name)", async () => {
		inv([
			{
				physical_name: "src_abc__orders",
				table_name: "orders",
				worst_band: "investigate",
			},
		]);
		const dq = await readDataQuality(["orders"]);
		expect(dq?.band).toBe("investigate");
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
		steps: [],
		final_sql: "SELECT SUM(amount) AS total FROM orders",
		assumptions: ["Treated null amounts as zero."],
		concepts_used: ["revenue"],
		tables_touched: ["orders"],
		...over,
	});

	it("composes a grid statement and passes provenance through", () => {
		const out = assembleAnswer(
			draft({
				steps: [],
				final_sql: "SELECT SUM(amount) AS total FROM orders",
			}),
			[],
			{ band: "investigate", note: "n" },
		);
		expect(out.grid).toEqual({
			sql: "SELECT SUM(amount) AS total FROM orders",
		});
		expect(out.answer).toBe("Total revenue is 150.");
		expect(out.assumptions).toEqual(["Treated null amounts as zero."]);
		expect(out.concepts_used).toEqual(["revenue"]);
		expect(out.tables_touched).toEqual(["orders"]);
		expect(out.data_quality).toEqual({ band: "investigate", note: "n" });
	});

	it("folds resolved steps into the grid CTE", () => {
		const out = assembleAnswer(
			draft({ final_sql: "SELECT r FROM revenue" }),
			[
				{
					name: "revenue",
					sql: "SELECT SUM(amount) AS r FROM orders",
					snippet_id: "s1",
				},
			],
			null,
		);
		expect(out.grid?.sql).toBe(
			"WITH revenue AS (\nSELECT SUM(amount) AS r FROM orders\n)\nSELECT r FROM revenue",
		);
		expect(out.data_quality).toBeNull();
	});

	it("yields a null grid when there is no runnable query", () => {
		const out = assembleAnswer(draft({ steps: [], final_sql: "  " }), [], null);
		expect(out.grid).toBeNull();
	});
});

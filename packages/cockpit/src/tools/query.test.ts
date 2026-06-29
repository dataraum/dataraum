// Unit coverage for the `answer` tool's DETERMINISTIC pieces (DAT-485): reuse
// classification (the measurable re-usage surface), the data-quality band read,
// and answer assembly from the captured validated run. The sub-agent's chat()
// loop itself is exercised by the live smoke (it calls the real LLM).

import { beforeEach, describe, expect, it, vi } from "vitest";

vi.mock("#/config", () => ({
	config: { dataraumWorkspaceId: "ws-test", anthropicApiKey: "k" },
}));
vi.mock("#/db/metadata/client", () => ({ metadataDb: {} }));
// query.ts reads the workspace vertical (for DAT-645 conventions) via the cockpit
// registry, which transitively pulls the bun-SQL client — stub it so the unit
// import stays node-resolvable.
vi.mock("#/db/cockpit/registry", () => ({
	resolveActiveWorkspaceRow: async () => ({ vertical: "finance" }),
}));

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

// save-on-clean: saveQuerySnippet is the write boundary — mocked so the persist
// gating/best-effort logic is testable. DAT-506: snippets are workspace-scoped
// (the `workspace_id` column replaced the session FK), so there's no session gate.
const saveQuerySnippetMock = vi.fn();
vi.mock("#/db/metadata/snippet-writer", () => ({
	// biome-ignore lint/suspicious/noExplicitAny: passthrough to the spy.
	saveQuerySnippet: (...a: any[]) => saveQuerySnippetMock(...a),
}));

import {
	assembleAnswer,
	type Component,
	classifyComponents,
	componentsToSave,
	exhaustionDiagnostic,
	isMissingStructuredResult,
	persistLearnedSnippets,
	type QueryDraft,
	readDataQuality,
	salvageDraft,
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
				grainNote: null,
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

	it("appends the grain caveat to assumptions when the run carried one", () => {
		const note = 'Note: this query groups by "txn_id", which is near-unique.';
		const out = assembleAnswer(
			draft(),
			{ composedSql: "SELECT 1", components: [], grainNote: note },
			null,
		);
		// Surfaced deterministically — even if the model omitted it.
		expect(out.assumptions).toContain(note);
		expect(out.assumptions).toContain("Treated null amounts as zero."); // kept
	});

	it("does not duplicate the grain caveat when the model already stated it", () => {
		const note = 'Note: this query groups by "txn_id", which is near-unique.';
		const d = draft();
		d.assumptions = [note];
		const out = assembleAnswer(
			d,
			{ composedSql: "SELECT 1", components: [], grainNote: note },
			null,
		);
		expect(out.assumptions.filter((a) => a === note)).toHaveLength(1);
	});

	it("yields a null grid + empty components when nothing was validated", () => {
		const out = assembleAnswer(draft(), null, null);
		expect(out.grid).toBeNull();
		expect(out.components).toEqual([]);
		expect(out.data_quality).toBeNull();
		// No components → grounded_ratio 0 (not NaN from a 0/0 division).
		expect(out.reliability).toEqual({
			grounded_ratio: 0,
			exact_reuse: 0,
			adapted: 0,
			fresh: 0,
		});
	});

	it("yields a null grid when the captured composed SQL is blank", () => {
		const out = assembleAnswer(
			draft(),
			{ composedSql: "   ", components: [], grainNote: null },
			null,
		);
		expect(out.grid).toBeNull();
	});
});

const comp = (
	name: string,
	usage: Component["usage"],
	sql = "SELECT 1",
): Component => ({
	name,
	sql,
	snippet_id: usage === "fresh" ? null : "s1",
	usage,
});

describe("componentsToSave (save-on-clean gate)", () => {
	it("keeps fresh and adapted, drops exact_reuse", () => {
		const out = componentsToSave([
			comp("a", "fresh"),
			comp("b", "exact_reuse"),
			comp("c", "adapted"),
		]);
		expect(out.map((c) => c.name)).toEqual(["a", "c"]);
	});

	it("returns empty when every component is exact_reuse", () => {
		expect(componentsToSave([comp("a", "exact_reuse")])).toEqual([]);
	});
});

describe("persistLearnedSnippets (save-on-clean)", () => {
	beforeEach(() => {
		saveQuerySnippetMock.mockReset();
	});

	it("saves only fresh/adapted under the workspace, sharing one query: source", async () => {
		saveQuerySnippetMock.mockResolvedValue({ snippetId: "x", deduped: false });

		await persistLearnedSnippets({
			composedSql: "WITH revenue AS (...) SELECT ...",
			components: [
				comp("revenue", "fresh", "SELECT SUM(rev) AS value"),
				comp("reused", "exact_reuse"),
				comp("margin", "adapted", "SELECT SUM(m) AS value"),
			],
			grainNote: null,
		});

		// fresh + adapted only — exact_reuse is skipped.
		expect(saveQuerySnippetMock).toHaveBeenCalledTimes(2);
		const args = saveQuerySnippetMock.mock.calls.map((c) => c[0]);
		expect(args.map((a) => a.standardField)).toEqual(["revenue", "margin"]);
		// Workspace-scoped (DAT-506): both the workspace_id column and the
		// schema_mapping_id key value are the active workspace id.
		expect(args.every((a) => a.workspaceId === "ws-test")).toBe(true);
		expect(args.every((a) => a.schemaMappingId === "ws-test")).toBe(true);
		// one provenance group per answer; the executable sql is carried through.
		expect(args[0].source).toMatch(/^query:/);
		expect(args[1].source).toBe(args[0].source);
		expect(args[0].sql).toBe("SELECT SUM(rev) AS value");
	});

	it("is a no-op for a null run or no fresh/adapted components", async () => {
		await persistLearnedSnippets(null);
		await persistLearnedSnippets({
			composedSql: "x",
			components: [comp("a", "exact_reuse")],
			grainNote: null,
		});
		expect(saveQuerySnippetMock).not.toHaveBeenCalled();
	});

	it("best-effort: swallows a save failure (never fails the answer)", async () => {
		saveQuerySnippetMock.mockRejectedValue(new Error("permission denied"));
		const warnSpy = vi.spyOn(console, "warn").mockImplementation(() => {});

		await expect(
			persistLearnedSnippets({
				composedSql: "x",
				components: [comp("a", "fresh")],
				grainNote: null,
			}),
		).resolves.toBeUndefined();
		expect(warnSpy).toHaveBeenCalled();

		warnSpy.mockRestore();
	});
});

// --- Exhaustion handling (DAT-608) -----------------------------------------------

describe("isMissingStructuredResult", () => {
	it("is true for chat()'s finalization error code", () => {
		const err = Object.assign(new Error("missing structured result"), {
			code: "structured-output-missing-result",
		});
		expect(isMissingStructuredResult(err)).toBe(true);
		// A bare object carrying the code also matches (defensive).
		expect(
			isMissingStructuredResult({ code: "structured-output-missing-result" }),
		).toBe(true);
	});

	it("is false for infra errors, aborts, and non-errors (they must propagate)", () => {
		expect(isMissingStructuredResult(new Error("ECONNREFUSED"))).toBe(false);
		expect(isMissingStructuredResult({ code: "something-else" })).toBe(false);
		expect(isMissingStructuredResult(null)).toBe(false);
		expect(isMissingStructuredResult(undefined)).toBe(false);
		expect(isMissingStructuredResult("missing structured result")).toBe(false);
	});
});

describe("salvageDraft (validated-but-unfinalized run)", () => {
	it("turns the last validated run into a draft: concepts from components, no guessed tables", () => {
		const components: Component[] = [
			{ name: "revenue", sql: "SELECT 1", snippet_id: null, usage: "fresh" },
			{
				name: "by_region",
				sql: "SELECT 2",
				snippet_id: "s1",
				usage: "adapted",
			},
		];
		const out = salvageDraft({
			composedSql: "WITH revenue AS (…) SELECT *",
			components,
			grainNote: null,
		});
		expect(out.concepts_used).toEqual(["revenue", "by_region"]);
		// No hallucinated tables — the salvage doesn't invent tables_touched.
		expect(out.tables_touched).toEqual([]);
		expect(out.answer.length).toBeGreaterThan(0);
		expect(out.assumptions.length).toBeGreaterThan(0);
	});
});

describe("exhaustionDiagnostic (no query ever validated)", () => {
	it("surfaces the last validation error, the steps, and the SQL", () => {
		const msg = exhaustionDiagnostic({
			message: 'Binder Error: column "discount_pct" not found',
			sql: "SELECT discount_pct FROM lake.typed.orders",
			steps: ["discount", "by_customer"],
		});
		expect(msg).toContain('Binder Error: column "discount_pct" not found');
		expect(msg).toContain("discount, by_customer");
		expect(msg).toContain("SELECT discount_pct FROM lake.typed.orders");
	});

	it("truncates a very long SQL", () => {
		const longSql = `SELECT ${"x".repeat(400)}`;
		const msg = exhaustionDiagnostic({
			message: "boom",
			sql: longSql,
			steps: [],
		});
		expect(msg).toContain("…");
		expect(msg).not.toContain(longSql);
	});

	it("falls back to a generic hint when no failure was captured (no 'undefined')", () => {
		const msg = exhaustionDiagnostic(null);
		expect(msg.length).toBeGreaterThan(0);
		expect(msg).not.toContain("undefined");
		expect(msg).not.toContain("null");
	});
});

// The WIRED parts-at-source path (DAT-678): a declaration the model could
// actually emit, proven on a real DuckDB through the real `proveAnswerSource`,
// and carried into the real `assembleAnswer` — asserting `drill_source` comes
// out NON-NULL.
//
// This test exists because of what the first cut of this feature got away with.
// Everything around the proof was unit-tested and green while the proof could
// never pass in production, because nothing exercised the whole path at the
// format the model is told to write. A suite that cannot tell "the proof works"
// from "the proof always returns null" does not cover a feature whose entire
// job is to return something. Only the lake ACCESS is stubbed here (an
// in-memory instance instead of the ducklake ATTACH); the composition, the SQL,
// the comparison and the assembly are the shipping ones.

import { type DuckDBConnection, DuckDBInstance } from "@duckdb/node-api";
import { afterAll, beforeAll, describe, expect, it, vi } from "vitest";

vi.mock("#/config", () => ({
	config: { dataraumWorkspaceId: "ws-test", anthropicApiKey: "k" },
}));
vi.mock("#/config.base", () => ({ baseConfig: {} }));
vi.mock("#/db/metadata/client", () => ({ metadataDb: {} }));
vi.mock("#/db/metadata/snippet-library", () => ({
	findById: vi.fn(),
	findGraphsByKeys: vi.fn(),
	getSearchVocabulary: vi.fn(),
}));
vi.mock("#/db/metadata/snippet-writer", () => ({ saveQuerySnippet: vi.fn() }));
vi.mock("#/tools/list-tables", () => ({ listTables: async () => [] }));

// The one seam that is stubbed: hand out a REAL connection to a real in-memory
// database instead of the lake's ducklake ATTACH. `applyEngineScope` is the
// `USE lake.typed` that makes a bare relation resolve; in-memory `main` already
// is that scope, so it is a no-op here rather than a fiction.
let instance: DuckDBInstance;
let conn: DuckDBConnection;
vi.mock("#/duckdb/lake", () => ({
	withLakeConnection: async <T>(fn: (c: DuckDBConnection) => Promise<T>) =>
		fn(conn),
	applyEngineScope: async () => {},
	getLakeConnection: async () => conn,
}));

import { narrowDeclaredSource } from "#/duckdb/answer-source";

import { proveAnswerSource } from "./answer-source-proof";
import { assembleAnswer } from "./query";

beforeAll(async () => {
	instance = await DuckDBInstance.create(":memory:");
	conn = await instance.connect();
	await conn.run(
		"CREATE TABLE orders (region VARCHAR, year BIGINT, amount DOUBLE)",
	);
	await conn.run(
		"INSERT INTO orders VALUES ('EU',2024,100),('EU',2024,50),('US',2024,25),('EU',2023,999)",
	);
});
afterAll(() => {
	conn?.closeSync();
	instance?.closeSync();
});

/** The answer the sub-agent validated — 2024 revenue = 175. */
const ANSWER_SQL =
	"WITH revenue AS (SELECT SUM(amount) AS value FROM orders WHERE year = 2024) SELECT value FROM revenue";

const draft = {
	answer: "Revenue in 2024 was 175.",
	assumptions: [],
	concepts_used: ["revenue"],
	tables_touched: ["orders"],
};

/** A declaration in the shape the model emits, narrowed as production narrows it. */
function declaredRevenue(filters: string[] = ["year = 2024"]) {
	const parts = narrowDeclaredSource({
		relation: "lake.typed.orders",
		valueExpr: "SUM(amount)",
		filters,
	});
	if (!parts) throw new Error("declaration should narrow");
	return { sources: [{ name: "revenue", parts }], expression: "revenue" };
}

const validated = (
	declaredSource: ReturnType<typeof declaredRevenue> | null,
) => ({
	composedSql: ANSWER_SQL,
	components: [],
	grainNote: null,
	declaredSource,
});

describe("the wired parts-at-source path", () => {
	it("carries a proven declaration onto the answer as drill_source", async () => {
		const candidate = declaredRevenue();
		const proven = await proveAnswerSource(candidate, ANSWER_SQL);
		// The assertion the suite was missing: the proof RETURNS something.
		expect(proven).not.toBeNull();

		const result = assembleAnswer(draft, validated(candidate), null, proven);
		expect(result.drill_source).not.toBeNull();
		expect(result.drill_source?.sources[0].parts.relation).toBe("orders");
		expect(result.grid?.sql).toBe(ANSWER_SQL);
	});

	it("drops a declaration whose filter does not match the answer, and still answers", async () => {
		const candidate = declaredRevenue([]);
		const proven = await proveAnswerSource(candidate, ANSWER_SQL);
		expect(proven).toBeNull();

		const result = assembleAnswer(draft, validated(candidate), null, proven);
		// The answer itself is untouched — only the drill affordance is absent.
		expect(result.drill_source).toBeNull();
		expect(result.answer).toBe("Revenue in 2024 was 175.");
		expect(result.grid?.sql).toBe(ANSWER_SQL);
	});

	it("returns null rather than throwing when the model declared nothing", async () => {
		expect(await proveAnswerSource(null, ANSWER_SQL)).toBeNull();
	});
});

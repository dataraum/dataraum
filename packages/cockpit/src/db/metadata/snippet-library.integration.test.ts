// Conformance oracle for the read-only snippet library (DAT-484 P0), ported from
// the engine's tests/unit/query/test_snippet_search.py + the vocabulary case in
// test_snippet_library.py. It proves byte-parity of the TS lookup against the
// Python producer's semantics — the parity risk lives in the SQL (LIKE / IN /
// IS NULL / DISTINCT), so this must hit a real Postgres, not a mock.
//
// Harness: the established *.integration.test pattern — gated on
// METADATA_DATABASE_URL, REUSING the running compose Postgres (no per-test
// container). A single rich fixture is seeded under one synthetic
// schema_mapping_id ("dat484-test") so it never interacts with real producer
// rows (which carry the real workspace_id). Seeding writes the underlying
// ws_<id>.sql_snippets TABLE directly via raw bun SQL (P0 has no write path);
// the lib reads it through the `sqlSnippets` view. All seeded rows carry one
// synthetic workspace_id (DAT-506: snippets are workspace-scoped, no
// investigation_sessions FK), so cleanup is a single delete-by-workspace_id.

import { afterAll, beforeAll, describe, expect, it } from "vitest";

const STACK_AVAILABLE = !!process.env.METADATA_DATABASE_URL;

// Stub the required cockpit env so config.ts loads (snippet-library imports the
// metadata client → config). Gated on the stack so a skipped run never mutates
// process.env for sibling test files sharing the worker.
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

// A synthetic workspace_id the seeded rows carry (DAT-506) — cleanup is by it.
const TEST_WORKSPACE = "dat484-test-workspace";
// Synthetic schema_mapping_id — isolates this fixture from real producer rows
// (which carry the real workspace_id). The lib filters on it, so our queries see
// only our rows, and a real producer run never perturbs these assertions.
const MAP = "dat484-test";
const WRONG_MAP = "dat484-wrong-map";

describe.skipIf(!STACK_AVAILABLE)("snippet-library reads (DAT-484)", () => {
	let lib: typeof import("./snippet-library");
	// biome-ignore lint/suspicious/noExplicitAny: dynamic-imported Bun SQL client
	let sql: any;
	// config.dataraumWorkspaceId — the dashed-UUID VALUE (not the schema name).
	let workspaceId: string;
	let arSnippetId = "";

	beforeAll(async () => {
		lib = await import("./snippet-library");
		const cfg = await import("../../config");
		workspaceId = cfg.config.dataraumWorkspaceId;
		const { SQL } = await import("bun");
		sql = new SQL(process.env.METADATA_DATABASE_URL as string);

		await cleanup();
		await seedFixture();
		// The keying-trap guard: a row under the REAL workspace_id value, with a
		// unique source so it never collides with real producer data.
		await seed({
			snippetType: "extract",
			schemaMappingId: workspaceId,
			source: "graph:dat484_trap",
			standardField: "trap_concept",
			statement: "income_statement",
			aggregation: "sum",
			sql: "SELECT 1 AS value",
			description: "Keying-trap guard",
		});
	});

	afterAll(async () => {
		if (sql) {
			await cleanup();
			await sql.close();
		}
	});

	async function cleanup(): Promise<void> {
		await sql.unsafe(
			`DELETE FROM "${SCHEMA}".sql_snippets WHERE workspace_id = $1`,
			[TEST_WORKSPACE],
		);
	}

	async function seed(o: {
		snippetType: string;
		schemaMappingId: string;
		source: string;
		sql: string;
		description?: string;
		standardField?: string;
		statement?: string;
		aggregation?: string;
		parameterValue?: string;
		normalizedExpression?: string;
	}): Promise<string> {
		const id = crypto.randomUUID();
		await sql.unsafe(
			`INSERT INTO "${SCHEMA}".sql_snippets
			 (snippet_id, workspace_id, snippet_type, standard_field, statement,
			  aggregation, schema_mapping_id, parameter_value, normalized_expression,
			  sql, description, column_mappings, source, execution_count,
			  failure_count, created_at, updated_at)
			 VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,'{}'::json,$12,0,0,now(),now())`,
			[
				id,
				TEST_WORKSPACE,
				o.snippetType,
				o.standardField ?? null,
				o.statement ?? null,
				o.aggregation ?? null,
				o.schemaMappingId,
				o.parameterValue ?? null,
				o.normalizedExpression ?? null,
				o.sql,
				o.description ?? "",
				o.source,
			],
		);
		return id;
	}

	// One rich fixture under MAP: a full DSO graph (3 extracts + 1 constant + 1
	// formula), a second graph sharing the 'revenue' concept, an independent
	// gross_margin graph, and a learned `query:` snippet keyed by concept.
	async function seedFixture(): Promise<void> {
		arSnippetId = await seed({
			snippetType: "extract",
			schemaMappingId: MAP,
			source: "graph:dso",
			standardField: "accounts_receivable",
			statement: "balance_sheet",
			aggregation: "end_of_period",
			sql: "SELECT SUM(ar) AS value",
			description: "Accounts receivable (end of period)",
		});
		await seed({
			snippetType: "extract",
			schemaMappingId: MAP,
			source: "graph:dso",
			standardField: "revenue",
			statement: "income_statement",
			aggregation: "sum",
			sql: "SELECT SUM(rev) AS value",
			description: "Revenue (sum)",
		});
		await seed({
			snippetType: "extract",
			schemaMappingId: MAP,
			source: "graph:dso",
			standardField: "cost_of_goods_sold",
			statement: "income_statement",
			aggregation: "sum",
			sql: "SELECT SUM(cost) AS value",
			description: "Cost of goods sold (sum)",
		});
		await seed({
			snippetType: "constant",
			schemaMappingId: MAP,
			source: "graph:dso",
			standardField: "days_in_period",
			parameterValue: "30",
			sql: "SELECT 30 AS value",
			description: "Days in period",
		});
		await seed({
			snippetType: "formula",
			schemaMappingId: MAP,
			source: "graph:dso",
			normalizedExpression: "({A} / {B}) * {C}",
			sql: "SELECT (ar / rev) * 30 AS value",
			description: "DSO = (AR / Revenue) * Days",
		});
		// Second graph: shares the 'revenue' concept (different aggregation → no
		// unique-key collision), so a revenue search returns TWO graphs.
		await seed({
			snippetType: "extract",
			schemaMappingId: MAP,
			source: "graph:trend",
			standardField: "revenue",
			statement: "income_statement",
			aggregation: "average",
			sql: "SELECT AVG(rev) AS value",
			description: "Revenue (avg)",
		});
		// Independent graph for union / multiple-graph-id cases.
		await seed({
			snippetType: "extract",
			schemaMappingId: MAP,
			source: "graph:gross_margin",
			standardField: "gross_margin",
			statement: "income_statement",
			aggregation: "sum",
			sql: "SELECT SUM(margin) AS value",
			description: "Gross margin",
		});
		// Learned ad-hoc snippet: concept 'adhoc_metric' exists ONLY here, under a
		// `query:` source — excluded from the vocabulary, but retrievable by key.
		await seed({
			snippetType: "query",
			schemaMappingId: MAP,
			source: "query:exec_adhoc",
			standardField: "adhoc_metric",
			sql: "SELECT 1 AS value",
			description: "Ad-hoc learned snippet",
		});
	}

	it("single concept returns its graph (whole 5-snippet chain)", async () => {
		const graphs = await lib.findGraphsByKeys(MAP, {
			standardFields: ["accounts_receivable"],
		});
		expect(graphs).toHaveLength(1);
		expect(graphs[0].graphId).toBe("dso");
		expect(graphs[0].snippets).toHaveLength(5);
	});

	it("shared concept returns multiple graphs", async () => {
		const graphs = await lib.findGraphsByKeys(MAP, {
			standardFields: ["revenue"],
		});
		expect(new Set(graphs.map((g) => g.graphId))).toEqual(
			new Set(["dso", "trend"]),
		);
	});

	it("direct graph_id returns the full graph", async () => {
		const graphs = await lib.findGraphsByKeys(MAP, { graphIds: ["dso"] });
		expect(graphs).toHaveLength(1);
		expect(graphs[0].snippets).toHaveLength(5);
	});

	it("multiple graph_ids return all matches", async () => {
		const graphs = await lib.findGraphsByKeys(MAP, {
			graphIds: ["dso", "gross_margin"],
		});
		expect(graphs).toHaveLength(2);
	});

	it("concept + graph_id is a union (not an intersection)", async () => {
		const graphs = await lib.findGraphsByKeys(MAP, {
			standardFields: ["accounts_receivable"],
			graphIds: ["gross_margin"],
		});
		expect(new Set(graphs.map((g) => g.graphId))).toEqual(
			new Set(["dso", "gross_margin"]),
		);
	});

	it("no keys returns empty", async () => {
		expect(await lib.findGraphsByKeys(MAP, {})).toEqual([]);
	});

	it("unknown keys return empty", async () => {
		expect(
			await lib.findGraphsByKeys(MAP, {
				standardFields: ["nonexistent_concept"],
				statements: ["nonexistent_statement"],
			}),
		).toEqual([]);
	});

	it("unknown graph_id returns empty", async () => {
		expect(
			await lib.findGraphsByKeys(MAP, { graphIds: ["nonexistent_graph"] }),
		).toEqual([]);
	});

	it("a different schema_mapping_id returns empty", async () => {
		expect(
			await lib.findGraphsByKeys(WRONG_MAP, { standardFields: ["revenue"] }),
		).toEqual([]);
	});

	it("a graph carries source / graphId / sourceType / snippet types", async () => {
		const [graph] = await lib.findGraphsByKeys(MAP, { graphIds: ["dso"] });
		expect(graph.source).toBe("graph:dso");
		expect(graph.graphId).toBe("dso");
		expect(graph.sourceType).toBe("graph");
		expect(new Set(graph.snippets.map((s) => s.snippetType))).toEqual(
			new Set(["extract", "constant", "formula"]),
		);
	});

	it("findById returns the row, or null for an unknown id", async () => {
		const row = await lib.findById(arSnippetId);
		expect(row?.standardField).toBe("accounts_receivable");
		expect(row?.source).toBe("graph:dso");
		expect(await lib.findById("no-such-snippet")).toBeNull();
	});

	describe("vocabulary is curated from graph:% snippets only", () => {
		it("surfaces exactly the four buckets from graph: snippets (sorted)", async () => {
			// Exact equality (the fixture is fully controlled): proves no rogue term
			// — e.g. a query: concept — leaks in, AND that the output is sorted.
			const vocab = await lib.getSearchVocabulary(MAP);
			expect(vocab.standardFields).toEqual([
				"accounts_receivable",
				"cost_of_goods_sold",
				"days_in_period",
				"gross_margin",
				"revenue",
			]);
			expect(vocab.statements).toEqual(["balance_sheet", "income_statement"]);
			expect(vocab.aggregations).toEqual(["average", "end_of_period", "sum"]);
			expect(vocab.graphIds).toEqual(["dso", "gross_margin", "trend"]);
		});

		it("excludes query: snippets from the vocabulary, yet keeps them retrievable by concept", async () => {
			const vocab = await lib.getSearchVocabulary(MAP);
			expect(vocab.standardFields).not.toContain("adhoc_metric");
			expect(vocab.graphIds).not.toContain("exec_adhoc");

			// ...but a key search still returns it (no source filter on the match).
			const graphs = await lib.findGraphsByKeys(MAP, {
				standardFields: ["adhoc_metric"],
			});
			expect(graphs).toHaveLength(1);
			expect(graphs[0].source).toBe("query:exec_adhoc");
			expect(graphs[0].sourceType).toBe("query");
		});
	});

	describe("keying: the VALUE, not the schema name (the DAT-484 fault line)", () => {
		it("the dashed-UUID workspace_id finds rows; the ws_ schema-name form reads empty", async () => {
			const byValue = await lib.findGraphsByKeys(workspaceId, {
				graphIds: ["dat484_trap"],
			});
			expect(byValue).toHaveLength(1);
			expect(byValue[0].snippets[0].standardField).toBe("trap_concept");

			const schemaNameForm = `ws_${workspaceId.replaceAll("-", "_")}`;
			const bySchemaName = await lib.findGraphsByKeys(schemaNameForm, {
				graphIds: ["dat484_trap"],
			});
			expect(bySchemaName).toEqual([]);
		});
	});
});

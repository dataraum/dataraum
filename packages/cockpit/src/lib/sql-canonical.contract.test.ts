// Format-stability contract for DuckDB's `json_serialize_sql` (DAT-654).
//
// `sql-canonical.ts` keys snippet identity off the SHAPE of the serialized parse
// tree — specific field names on specific node classes. That shape is a DuckDB
// implementation detail: a major upgrade could rename a field or restructure a node
// and silently break canonicalization (every snippet would look "adapted", reuse
// would quietly die) with NO type error, since the tree is `unknown` at the boundary.
//
// This test drives the REAL installed DuckDB parser and pins every field the
// normalizer depends on. If a DuckDB bump changes the serialization, THIS fails
// loudly and points at exactly what moved — the tripwire, analogous to the
// `tool-chip-state.contract.test.ts` guard on the TanStack AI SDK shape.

import { type DuckDBConnection, DuckDBInstance } from "@duckdb/node-api";
import { afterAll, beforeAll, describe, expect, it } from "vitest";

let instance: DuckDBInstance;
let conn: DuckDBConnection;

beforeAll(async () => {
	instance = await DuckDBInstance.create(":memory:");
	conn = await instance.connect();
});

afterAll(() => {
	conn?.closeSync();
	instance?.closeSync();
});

/** The serialized-tree envelope — the untyped DuckDB boundary this test pins. */
interface SerializedTree {
	error?: boolean;
	statements?: { node: Record<string, unknown> }[];
}

async function serialize(sql: string): Promise<SerializedTree> {
	const reader = await conn.runAndReadAll(
		"SELECT json_serialize_sql($1::VARCHAR) AS tree",
		[sql],
	);
	const raw = reader.getRowObjectsJson()[0]?.tree;
	if (typeof raw !== "string") {
		throw new Error("json_serialize_sql returned non-string");
	}
	return JSON.parse(raw);
}

/** Walk to the single SELECT node's fields under the stable envelope. */
function selectNode(tree: SerializedTree): Record<string, unknown> {
	const node = tree.statements?.[0]?.node;
	if (!node) throw new Error("no statement node in serialized tree");
	return node;
}

describe("json_serialize_sql format contract", () => {
	it("signals a clean parse with error:false and a statements envelope", async () => {
		const tree = await serialize("SELECT a FROM t");
		expect(tree.error).toBe(false);
		expect(Array.isArray(tree.statements)).toBe(true);
		expect(selectNode(tree).type).toBe("SELECT_NODE");
	});

	it("signals a parse failure with error:true (the fail-soft trigger)", async () => {
		const tree = await serialize("this is not <<< valid sql ;;;");
		expect(tree.error).toBe(true);
	});

	it("emits COLUMN_REF with a column_names array and a query_location byte offset", async () => {
		const node = selectNode(await serialize('SELECT "Amount" FROM t')) as {
			select_list: Record<string, unknown>[];
		};
		const col = node.select_list[0];
		expect(col.class).toBe("COLUMN_REF");
		expect(col.column_names).toEqual(["Amount"]); // case preserved pre-normalization
		expect(typeof col.query_location).toBe("number"); // stripped by the normalizer
	});

	it("emits arithmetic operators as FUNCTION with is_operator + a children array", async () => {
		const node = selectNode(await serialize("SELECT a + b AS v FROM t")) as {
			select_list: Record<string, unknown>[];
		};
		const fn = node.select_list[0];
		expect(fn.class).toBe("FUNCTION");
		expect(fn.function_name).toBe("+");
		expect(fn.is_operator).toBe(true);
		expect(Array.isArray(fn.children)).toBe(true);
		expect((fn.children as unknown[]).length).toBe(2);
	});

	it("flattens AND/OR into one n-ary CONJUNCTION with a typed children array", async () => {
		const node = selectNode(
			await serialize("SELECT * FROM t WHERE a AND b AND c"),
		) as { where_clause: Record<string, unknown> };
		const conj = node.where_clause;
		expect(conj.class).toBe("CONJUNCTION");
		expect(conj.type).toBe("CONJUNCTION_AND"); // OR → CONJUNCTION_OR
		expect((conj.children as unknown[]).length).toBe(3); // n-ary, not nested
	});

	it("emits BASE_TABLE with catalog_name / schema_name / table_name for qualifier strip", async () => {
		const node = selectNode(
			await serialize("SELECT * FROM lake.typed.orders"),
		) as { from_table: Record<string, unknown> };
		const table = node.from_table;
		expect(table.type).toBe("BASE_TABLE");
		expect(table.catalog_name).toBe("lake");
		expect(table.schema_name).toBe("typed");
		expect(table.table_name).toBe("orders");
	});

	it("emits `=` as a COMPARISON with left/right (NOT a sortable children array)", async () => {
		// Comparisons carry operands in `left`/`right`, not `children` — which is why
		// the normalizer never reorders them (`a = b` must stay distinct from `b = a`).
		const node = selectNode(
			await serialize("SELECT * FROM t WHERE x = 'Sale'"),
		) as { where_clause: Record<string, unknown> };
		const cmp = node.where_clause;
		expect(cmp.class).toBe("COMPARISON");
		expect(cmp.children).toBeUndefined();
		expect((cmp.left as { class: string }).class).toBe("COLUMN_REF");
		expect((cmp.right as { class: string }).class).toBe("CONSTANT");
	});

	it("keeps string literals in a CONSTANT node (identifier folding must not touch them)", async () => {
		const node = selectNode(
			await serialize("SELECT * FROM t WHERE x = 'Sale'"),
		) as { where_clause: { right: Record<string, unknown> } };
		const literal = node.where_clause.right;
		expect(literal.class).toBe("CONSTANT");
		expect((literal.value as { value: unknown }).value).toBe("Sale");
	});
});

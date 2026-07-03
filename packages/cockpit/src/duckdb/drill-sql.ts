// Tier-B drill composition: AST injection via DuckDB's own parser (DAT-672).
//
// When a drill references columns that are NOT on the base result (a scalar
// metric aggregate hides its dimensions inside the statement), the dimension
// must be injected INTO the statement: `json_serialize_sql` → mutate the
// select node (COLUMN_REF into select_list + group_expressions + group_sets;
// pins AND-merged into where_clause) → `json_deserialize_sql` → validate with
// a bound DESCRIBE. There are no skip heuristics: DuckDB's binder is the gate,
// and a BinderException is the deterministic "cannot slice this" refusal the
// UI renders (an LLM fallback is P2, DAT-673).
//
// Everything runs on a caller-provided connection: the API route passes a
// lake connection scoped like the engine's (`USE lake.typed`), unit tests an
// in-memory one. Parse trees round-trip through JS JSON, which mangles
// DuckDB's u64-max "unset" `query_location` sentinels — they are zeroed
// before deserialization (locations only feed error messages).

import type { DuckDBConnection } from "@duckdb/node-api";

import {
	type BaseColumn,
	type ComposedDrill,
	composeTierA,
	type DrillPinValue,
	type DrillStep,
	pinSteps,
	referencedColumns,
	sliceColumns,
} from "./drill";

export interface DrillComposeRequest {
	sql: string;
	params: DrillPinValue[];
	steps: DrillStep[];
}

export type DrillComposeResult =
	| {
			ok: true;
			tier: "A" | "B";
			sql: string;
			params: DrillPinValue[];
			columns: BaseColumn[];
	  }
	| { ok: false; reason: string };

const refuse = (reason: string): DrillComposeResult => ({ ok: false, reason });

/** The first line of a DuckDB error — `Binder Error: …` etc.; the rest is
 *  candidate-list noise the refusal state doesn't need. */
const errorLine = (err: unknown): string =>
	(err instanceof Error ? err.message : String(err)).split("\n")[0] ??
	"unknown error";

/** DESCRIBE the (possibly parameterized) query — DuckDB binds and plans
 *  without executing, so this both yields the result columns and surfaces
 *  binder errors. Params are required for binding parameterized SQL. */
export async function describeColumns(
	conn: DuckDBConnection,
	sql: string,
	params: DrillPinValue[],
): Promise<BaseColumn[]> {
	const reader =
		params.length > 0
			? await conn.runAndReadAll(`DESCRIBE ${sql}`, params)
			: await conn.runAndReadAll(`DESCRIBE ${sql}`);
	return reader.getRowObjectsJson().map((r) => ({
		name: String(r.column_name),
		type: String(r.column_type),
	}));
}

// --- Serialized parse-tree plumbing ------------------------------------------

/** Pragmatic node view: DuckDB's serialized tree is versioned engine output —
 *  narrowed where we mutate, refused where the shape surprises. */
type AstNode = Record<string, unknown>;

const isRecord = (v: unknown): v is AstNode =>
	typeof v === "object" && v !== null && !Array.isArray(v);

const colRef = (name: string): AstNode => ({
	class: "COLUMN_REF",
	type: "COLUMN_REF",
	alias: "",
	column_names: [name],
});

/** Zero every `query_location` in the tree: DuckDB emits u64-max sentinels
 *  that lose precision through JS `JSON.parse`/`stringify` and then fail
 *  `json_deserialize_sql`'s uint64 check. */
const zeroLocations = (v: unknown): unknown => {
	if (Array.isArray(v)) return v.map(zeroLocations);
	if (isRecord(v)) {
		const out: AstNode = {};
		for (const [k, val] of Object.entries(v)) {
			out[k] = k === "query_location" ? 0 : zeroLocations(val);
		}
		return out;
	}
	return v;
};

async function serializeSql(
	conn: DuckDBConnection,
	sql: string,
): Promise<AstNode | null> {
	const reader = await conn.runAndReadAll(
		"SELECT json_serialize_sql($1::VARCHAR) AS tree",
		[sql],
	);
	const raw = reader.getRowObjectsJson()[0]?.tree;
	if (typeof raw !== "string") return null;
	const tree: unknown = JSON.parse(raw);
	return isRecord(tree) ? tree : null;
}

async function deserializeSql(
	conn: DuckDBConnection,
	tree: AstNode,
): Promise<string | null> {
	const reader = await conn.runAndReadAll(
		"SELECT json_deserialize_sql($1::JSON) AS sql",
		[JSON.stringify(zeroLocations(tree))],
	);
	const sql = reader.getRowObjectsJson()[0]?.sql;
	return typeof sql === "string" ? sql : null;
}

// --- Tier B injection ---------------------------------------------------------

/** Inject slice dims + pins into a SELECT node. Returns the pin params it
 *  appended (numbered after the base params), or a refusal reason. */
function injectSteps(
	node: AstNode,
	steps: DrillStep[],
	baseParamCount: number,
): { pinParams: DrillPinValue[] } | { refusal: string } {
	const dims = sliceColumns(steps);
	const pins = pinSteps(steps);

	const selectList = node.select_list;
	const groupExpressions = node.group_expressions;
	const groupSets = node.group_sets;
	if (
		!Array.isArray(selectList) ||
		!Array.isArray(groupExpressions) ||
		!Array.isArray(groupSets)
	) {
		return { refusal: "statement shape not recognized" };
	}
	// GROUPING SETS produce several grouping combinations — there is no single
	// deterministic place to add a dimension.
	if (groupSets.length > 1) {
		return { refusal: "statement uses GROUPING SETS" };
	}

	if (dims.length > 0) {
		const firstIdx = groupExpressions.length;
		const refs = dims.map(colRef);
		node.select_list = [...refs, ...selectList];
		groupExpressions.push(...refs);
		const added = refs.map((_, i) => firstIdx + i);
		const existing = groupSets[0];
		node.group_sets = [
			Array.isArray(existing) ? [...existing, ...added] : added,
		];
	}

	if (pins.length > 0) {
		const pinParams: DrillPinValue[] = [];
		const predicates = pins.map((p) => {
			if (p.value === null) {
				return {
					class: "OPERATOR",
					type: "OPERATOR_IS_NULL",
					alias: "",
					children: [colRef(p.column)],
				};
			}
			pinParams.push(p.value);
			return {
				class: "COMPARISON",
				type: "COMPARE_EQUAL",
				alias: "",
				left: colRef(p.column),
				right: {
					class: "PARAMETER",
					type: "VALUE_PARAMETER",
					alias: "",
					identifier: String(baseParamCount + pinParams.length),
				},
			};
		});
		const existing = node.where_clause;
		const children = isRecord(existing)
			? [existing, ...predicates]
			: predicates;
		node.where_clause =
			children.length === 1
				? children[0]
				: {
						class: "CONJUNCTION",
						type: "CONJUNCTION_AND",
						alias: "",
						children,
					};
		return { pinParams };
	}
	return { pinParams: [] };
}

async function composeTierB(
	conn: DuckDBConnection,
	req: DrillComposeRequest,
): Promise<{ composed: ComposedDrill } | { refusal: string }> {
	const tree = await serializeSql(conn, req.sql);
	if (!tree) return { refusal: "statement not parseable" };
	// Parse failures come back IN-BAND (`error: true`), not thrown.
	if (tree.error !== false) return { refusal: "statement not parseable" };

	const statements = tree.statements;
	if (!Array.isArray(statements) || statements.length !== 1) {
		return { refusal: "expected exactly one statement" };
	}
	const stmt: unknown = statements[0];
	const node = isRecord(stmt) ? stmt.node : null;
	// CTEs still top out in a SELECT_NODE (cte_map); set operations do not —
	// there is no single select list to inject into.
	if (!isRecord(node) || node.type !== "SELECT_NODE") {
		return { refusal: "only plain SELECT statements can be drilled" };
	}

	const injected = injectSteps(node, req.steps, req.params.length);
	if ("refusal" in injected) return injected;

	const sql = await deserializeSql(conn, tree);
	if (!sql) return { refusal: "statement did not re-serialize" };
	return { composed: { sql, params: [...req.params, ...injected.pinParams] } };
}

// --- The composer -------------------------------------------------------------

/**
 * Compose a drilled statement from a base query + step stack.
 *
 * Tier decision is data-driven: every referenced column present on the base
 * RESULT (per DESCRIBE) → tier A outer wrap; anything else → tier B AST
 * injection. Either way the composed statement is validated with a bound
 * DESCRIBE before it is returned — the caller never receives SQL that will
 * not bind, and a binder failure IS the refusal.
 */
export async function composeDrill(
	conn: DuckDBConnection,
	req: DrillComposeRequest,
): Promise<DrillComposeResult> {
	if (req.steps.length === 0) return refuse("no drill steps");

	let baseColumns: BaseColumn[];
	try {
		baseColumns = await describeColumns(conn, req.sql, req.params);
	} catch (err) {
		return refuse(`base query does not bind: ${errorLine(err)}`);
	}

	const baseNames = new Set(baseColumns.map((c) => c.name));
	const tierA = referencedColumns(req.steps).every((c) => baseNames.has(c));

	let tier: "A" | "B";
	let composed: ComposedDrill;
	if (tierA) {
		tier = "A";
		composed = composeTierA(req.sql, req.params, baseColumns, req.steps);
	} else {
		tier = "B";
		const result = await composeTierB(conn, req);
		if ("refusal" in result) return refuse(result.refusal);
		composed = result.composed;
	}

	try {
		const columns = await describeColumns(conn, composed.sql, composed.params);
		return {
			ok: true,
			tier,
			sql: composed.sql,
			params: composed.params,
			columns,
		};
	} catch (err) {
		return refuse(errorLine(err));
	}
}

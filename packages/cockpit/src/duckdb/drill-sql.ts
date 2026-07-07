// Tier-B drill composition: AST injection via DuckDB's own parser (DAT-672).
//
// When a drill references columns that are NOT on the base result (a scalar
// metric aggregate hides its dimensions inside the statement), the dimension
// must be injected INTO the statement: `json_serialize_sql` → mutate the
// select node (COLUMN_REF into select_list + group_expressions + group_sets;
// pins AND-merged into where_clause) → `json_deserialize_sql` → validate with
// a bound DESCRIBE. There are no skip heuristics: DuckDB's binder is the gate,
// and a BinderException is the deterministic "cannot slice this" refusal the
// UI renders. Engine-composed metrics are handled per NODE instead — rebuilt
// from persisted parts in `metric-compose.ts` behind `/api/drill/node`
// (DAT-702), never by mutating the flattened statement here.
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
} from "./drill";

export interface DrillComposeRequest {
	sql: string;
	params: DrillPinValue[];
	steps: DrillStep[];
}

export type DrillComposeResult =
	| {
			ok: true;
			tier: "A" | "B" | "C";
			sql: string;
			params: DrillPinValue[];
			columns: BaseColumn[];
	  }
	| { ok: false; reason: string };

const refuse = (reason: string): DrillComposeResult => ({ ok: false, reason });

/** The first line of a DuckDB error — `Binder Error: …` etc.; the rest is
 *  candidate-list noise the refusal state doesn't need. (Exported for the
 *  `/api/drill/node` route, which shares the binder-as-gate refusal shape.) */
export const errorLine = (err: unknown): string =>
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

const colRef = (name: string, qualifier?: string): AstNode => ({
	class: "COLUMN_REF",
	type: "COLUMN_REF",
	alias: "",
	column_names: qualifier ? [qualifier, name] : [name],
});

/** The relations of the statement's OWN scope — BASE_TABLE nodes under
 *  `from_table` only (a subquery's relations live in a different scope and
 *  must not qualify a top-level reference). */
const collectScopeRelations = (
	node: unknown,
	out: { table: string; alias: string }[],
): void => {
	if (Array.isArray(node)) {
		for (const v of node) collectScopeRelations(v, out);
		return;
	}
	if (!isRecord(node)) return;
	if (node.type === "BASE_TABLE" && typeof node.table_name === "string") {
		out.push({
			table: node.table_name,
			alias: typeof node.alias === "string" ? node.alias : "",
		});
	}
	// Recurse only through join structure, not into subquery select nodes.
	if (node.type === "SUBQUERY" || node.type === "SELECT_NODE") return;
	for (const v of Object.values(node)) collectScopeRelations(v, out);
};

/**
 * The qualifier for an injected axis reference. The axis carries its home
 * relations (`source`: the fact table + its enriched view); when the
 * statement's scope reads exactly one of them, qualify with its alias so a
 * column name shared with a joined dim/CTE (`f.business_id` vs
 * `d.business_id`) binds to the axis's actual home — the catalog's
 * `column_id` points at the fact, so this is resolution, not guessing.
 */
const qualifierFor = (
	source: string[] | undefined,
	relations: { table: string; alias: string }[],
): { qualifier?: string } | { refusal: string } => {
	if (!source || source.length === 0) return {};
	const matches = relations.filter((r) => source.includes(r.table));
	if (matches.length === 0) return {}; // home not in scope (e.g. behind a CTE) — bare, binder decides
	if (matches.length > 1) {
		return {
			refusal: `the statement reads ${matches[0].table} more than once — the axis reference is ambiguous`,
		};
	}
	return { qualifier: matches[0].alias || matches[0].table };
};

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
	// First slice step per column wins (dedup preserving order), keeping each
	// step's `source` so its reference can be qualified against the scope.
	const dimSteps: Extract<DrillStep, { kind: "slice" }>[] = [];
	for (const s of steps) {
		if (s.kind === "slice" && !dimSteps.some((d) => d.column === s.column)) {
			dimSteps.push(s);
		}
	}
	const pins = pinSteps(steps);

	const scopeRelations: { table: string; alias: string }[] = [];
	collectScopeRelations(node.from_table, scopeRelations);
	const refFor = (
		column: string,
		source: string[] | undefined,
	): { ref: AstNode } | { refusal: string } => {
		const q = qualifierFor(source, scopeRelations);
		if ("refusal" in q) return q;
		return { ref: colRef(column, q.qualifier) };
	};

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

	if (dimSteps.length > 0) {
		const refs: AstNode[] = [];
		for (const d of dimSteps) {
			const r = refFor(d.column, d.source);
			if ("refusal" in r) return r;
			refs.push(r.ref);
		}
		const firstIdx = groupExpressions.length;
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
		const predicates: AstNode[] = [];
		// pinParams grows INSIDE the loop: `$n` identifiers must be sequential
		// in pin order, and only non-NULL pins consume a slot — the push/length
		// pairing below is that ordering, don't lift it out of the loop.
		for (const p of pins) {
			const r = refFor(p.column, p.source);
			if ("refusal" in r) return r;
			const ref = r.ref;
			if (p.value === null) {
				predicates.push({
					class: "OPERATOR",
					type: "OPERATOR_IS_NULL",
					alias: "",
					children: [ref],
				});
				continue;
			}
			pinParams.push(p.value);
			predicates.push({
				class: "COMPARISON",
				type: "COMPARE_EQUAL",
				alias: "",
				left: ref,
				right: {
					class: "PARAMETER",
					type: "VALUE_PARAMETER",
					alias: "",
					identifier: String(baseParamCount + pinParams.length),
				},
			});
		}
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

async function parseSingleSelect(
	conn: DuckDBConnection,
	sql: string,
): Promise<{ tree: AstNode; node: AstNode } | { refusal: string }> {
	const tree = await serializeSql(conn, sql);
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
	return { tree, node };
}

/** Tier B end-to-end: parse the single SELECT, inject the steps into its top
 *  scope, re-serialize. The bound-DESCRIBE gate runs in `composeDrill` — one
 *  gate for every tier. */
async function composeTierB(
	conn: DuckDBConnection,
	req: DrillComposeRequest,
): Promise<{ composed: ComposedDrill } | { refusal: string }> {
	const parsed = await parseSingleSelect(conn, req.sql);
	if ("refusal" in parsed) return parsed;
	const injected = injectSteps(parsed.node, req.steps, req.params.length);
	if ("refusal" in injected) return injected;
	const sql = await deserializeSql(conn, parsed.tree);
	if (!sql) return { refusal: "statement did not re-serialize" };
	return { composed: { sql, params: [...req.params, ...injected.pinParams] } };
}

// --- The composer -------------------------------------------------------------

/**
 * Compose a drilled statement from a base query + step stack.
 *
 * Tier decision is data-driven: every referenced column present on the base
 * RESULT (per DESCRIBE) → tier A outer wrap; anything else → tier B AST
 * injection into the top scope. Every tier's output is validated with a bound
 * DESCRIBE before it is returned — the caller never receives SQL that will
 * not bind, and a binder failure IS the refusal. An engine-composed metric
 * (scalar step-CTEs — every dimension aggregated away inside the extracts)
 * is NOT recomposed here: its drill entry is per NODE, rebuilt from the
 * metric's persisted parts (`metric-compose.ts` behind `/api/drill/node`,
 * DAT-702 — tier "C" in the response taxonomy). Until DAT-703 lands grouped
 * per-node composition, slicing a composed metric refuses honestly with the
 * tier-B binder error.
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
		const result = await composeTierB(conn, req);
		if ("refusal" in result) return refuse(result.refusal);
		tier = "B";
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

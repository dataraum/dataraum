// Shared spike helpers (NOT production code): parts extraction from persisted
// snippet strings via DuckDB's own printer, plus lake exec helpers. The parts
// extraction is the BRIDGE for spiking against today's fused snippets — in the
// parts-at-source design the engine persists these fields and this file dies.

import type { DuckDBConnection } from "@duckdb/node-api";

type AstNode = Record<string, unknown>;
const isRecord = (v: unknown): v is AstNode =>
	typeof v === "object" && v !== null && !Array.isArray(v);

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

async function serialize(
	conn: DuckDBConnection,
	sqlText: string,
): Promise<AstNode | null> {
	const r = await conn.runAndReadAll(
		"SELECT json_serialize_sql($1::VARCHAR) AS t",
		[sqlText],
	);
	const raw = r.getRowObjectsJson()[0]?.t;
	if (typeof raw !== "string") return null;
	const tree: unknown = JSON.parse(raw);
	return isRecord(tree) && tree.error === false ? tree : null;
}

async function deserialize(
	conn: DuckDBConnection,
	tree: AstNode,
): Promise<string | null> {
	const r = await conn.runAndReadAll(
		"SELECT json_deserialize_sql($1::JSON) AS s",
		[JSON.stringify(zeroLocations(tree))],
	);
	const s = r.getRowObjectsJson()[0]?.s;
	return typeof s === "string" ? s : null;
}

const STAR: AstNode = {
	class: "STAR",
	type: "STAR",
	alias: "",
	relation_name: "",
	exclude_list: [],
	replace_list: [],
	rename_list: [],
	expr: null,
	columns: false,
	unpacked: false,
};

/** The clause parts of a simple extract snippet. */
export interface SnippetParts {
	/** "SUM(credit) - SUM(debit) AS value" */
	selectItems: string;
	/** The value expression WITHOUT its alias: "SUM(credit) - SUM(debit)" —
	 *  builders that control aliasing themselves (mosaic-sql) consume this. */
	valueExpr: string;
	/** "enriched_journal_lines" (whatever the FROM clause prints as) */
	fromText: string;
	/** Predicate text WITHOUT the WHERE keyword, or null. */
	whereText: string | null;
}

/** Decompose a fused snippet into clause parts using DuckDB's printer as the
 *  normalizer (suffix/prefix arithmetic on printed clones — no regex over
 *  user SQL, no tree mutation of the original). */
export async function extractParts(
	conn: DuckDBConnection,
	snippetSql: string,
): Promise<SnippetParts | { miss: string }> {
	const tree = await serialize(conn, snippetSql);
	if (!tree) return { miss: "unparseable" };
	const stmts = tree.statements;
	if (!Array.isArray(stmts) || stmts.length !== 1)
		return { miss: "multi-statement" };
	const node = isRecord(stmts[0]) ? (stmts[0] as AstNode).node : null;
	if (!isRecord(node) || node.type !== "SELECT_NODE")
		return { miss: "not a plain SELECT" };
	const cteMap = isRecord(node.cte_map) ? node.cte_map.map : null;
	if (Array.isArray(cteMap) && cteMap.length > 0) return { miss: "has CTEs" };
	if (
		Array.isArray(node.group_expressions) &&
		node.group_expressions.length > 0
	) {
		return { miss: "already grouped" };
	}
	if (!isRecord(node.from_table) || node.from_table.type === "EMPTY") {
		return { miss: "no FROM" };
	}

	const clone = (): AstNode => JSON.parse(JSON.stringify(tree)) as AstNode;
	const nodeOf = (t: AstNode): AstNode =>
		(t.statements as AstNode[])[0]?.node as AstNode;

	const full = await deserialize(conn, clone());
	const noWhereTree = clone();
	nodeOf(noWhereTree).where_clause = null;
	const noWhere = await deserialize(conn, noWhereTree);
	const starTree = clone();
	nodeOf(starTree).where_clause = null;
	nodeOf(starTree).select_list = [STAR];
	const star = await deserialize(conn, starTree);
	if (!full || !noWhere || !star) return { miss: "did not re-print" };
	if (!noWhere.startsWith("SELECT ") || !star.startsWith("SELECT * FROM ")) {
		return { miss: `unexpected print shape: ${star.slice(0, 40)}` };
	}
	const fromText = star.slice("SELECT * FROM ".length);
	const suffix = ` FROM ${fromText}`;
	if (!noWhere.endsWith(suffix)) return { miss: "from-suffix mismatch" };
	const selectItems = noWhere.slice(
		"SELECT ".length,
		noWhere.length - suffix.length,
	);
	const aliasMatch = /\s+AS\s+"?value"?$/i.exec(selectItems);
	if (!aliasMatch) return { miss: `select item not aliased AS value: ${selectItems.slice(0, 60)}` };
	const valueExpr = selectItems.slice(0, aliasMatch.index);
	const whereRaw = full.slice(noWhere.length); // "" or " WHERE …"
	const whereText = whereRaw.startsWith(" WHERE ")
		? whereRaw.slice(" WHERE ".length)
		: null;
	return { selectItems, valueExpr, fromText, whereText };
}

// --- exec helpers --------------------------------------------------------------

export const canon = (rows: Record<string, unknown>[]): string =>
	JSON.stringify(
		rows
			.map((r) => Object.values(r).map((v) => String(v)))
			.sort((a, b) => a.join(" ").localeCompare(b.join(" "))),
	);

export async function run(
	conn: DuckDBConnection,
	sqlText: string,
): Promise<{ rows: Record<string, unknown>[] } | { error: string }> {
	try {
		const r = await conn.runAndReadAll(`SELECT * FROM (${sqlText}) LIMIT 20000`);
		return { rows: r.getRowObjectsJson() as Record<string, unknown>[] };
	} catch (err) {
		return {
			error:
				(err instanceof Error ? err.message : String(err)).split("\n")[0] ??
				"?",
		};
	}
}

export const num = (v: unknown): number | null => {
	const n = typeof v === "number" ? v : Number(v);
	return Number.isFinite(n) ? n : null;
};

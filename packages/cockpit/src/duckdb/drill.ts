// Drill-down step model + the tier-A composer (DAT-672).
//
// A drill is an ordered stack of steps over a base query: `slice(column)` groups
// the result by a catalog dimension; `pin(column, value)` filters to one group's
// value (always PRE-aggregation — a pinned dimension may no longer be in the
// sliced output, so the filter can never ride on the grouped result).
//
// Drill composes UPSTREAM of the grid: each drill state yields a new effective
// base SQL + params, and the existing grid machinery (`buildGridQuery`,
// `/api/run-sql`) wraps it unchanged. Three composition paths (DAT-703,
// DAT-678):
//   - Tier A (this module, pure): every referenced column exists on the base
//     result → wrap it in an outer GROUP BY with default aggregates. The
//     ad-hoc grid path (`/api/drill/compose`) — the honest fallback on EVERY
//     result surface, and the only path when nothing upstream is known.
//   - Per-node (`parts.ts`, behind `/api/drill/node`): a canvas node rebuilds
//     from its persisted clause parts with the steps as clause appends —
//     never by parsing or mutating SQL text (tier-B AST injection is gone).
//   - Parts-at-source for an ANSWER (`parts.ts`, behind `/api/drill/parts`,
//     DAT-678): the answer sub-agent DECLARED its step's clause parts and the
//     declaration was PROVEN against the answer's own value, so the same
//     `composeNodeQuery` rebuilds it — letting a scalar answer be sliced by a
//     dimension it never projected. Unproven → no parts → tier A.
// This module is neo-free so widgets can import the types (grid-query.ts is
// the precedent).

// Type-only, erased at compile time — keeps this module neo-free (widgets
// import these types) while the parts handle keeps its home next to the
// composer that produces it.
import type { AnswerDrillSource } from "./answer-source";
import type { TemporalKind } from "./grain";
import { quoteIdentifier } from "./grid-query";

/** A pin carries the clicked cell's JSON value — bigints/dates arrive as
 *  strings and DuckDB casts the bound param to the column type. */
export type DrillPinValue = string | number | boolean | null;

export type DrillStep =
	| { kind: "slice"; column: string; grain?: string }
	| { kind: "pin"; column: string; value: DrillPinValue; grain?: string };

/** A canvas NODE the drill recomposes from persisted clause parts: exactly one
 *  of the two keys. Both `/api/drill/node` and the metric axis path key off
 *  this. Shared client↔server so the wire contract can't silently drift (this
 *  module is the neo-free home for drill types). */
export type DrillNodeRef =
	| { metricKey: string; standardField?: undefined }
	| { standardField: string; metricKey?: undefined };

/**
 * Axis-resolution request (`/api/drill/axes`) — one variant per compose path
 * (DAT-678), because what a surface can honestly slice BY is decided by how it
 * will recompose:
 *   - a node ref → the metric/measure catalog read (`resolveDrillAxes`);
 *   - `resultSql` → TIER A: the axes must exist as COLUMNS of the base result,
 *     since the tier-A wrap can only group by what the subquery projects. The
 *     server DESCRIBEs the statement (no execution) and intersects those names
 *     with the slice catalog — the client never has to wait for the grid's
 *     stream header to ask;
 *   - `partsSources` → PARTS-AT-SOURCE: the drill recomposes from the answer's
 *     own clause parts, so the axes are its RELATIONS' cataloged dimensions —
 *     including dimensions the answer never projected (the whole point: a
 *     scalar answer has no columns to group by).
 */
export type DrillAxesRequest =
	| DrillNodeRef
	| { resultSql: string; resultParams?: DrillPinValue[] }
	| { partsSources: { relation: string; selectExpr: string }[] };

/**
 * How a grid recomposes when a drill is applied — the ONE selector, so a
 * surface cannot half-declare its capability. Absent = tier A: wrap the base
 * result (`/api/drill/compose`), which needs nothing but the result itself and
 * is therefore always available.
 */
export type DrillSource =
	| { kind: "node"; ref: DrillNodeRef }
	| { kind: "parts"; source: AnswerDrillSource };

/** One sliceable dimension of a node's fact catalog (`/api/drill/axes`). */
export interface DrillAxis {
	/** The dimension column — addressable verbatim in the extract's SQL scope
	 *  (the enriched view exposes FK-prefixed dim columns; whether it binds in
	 *  a given composition stays the compose-time binder's call). */
	column: string;
	priority: number;
	sliceType: string;
	/** Catalog sample of the dimension's values (display hint, not exhaustive). */
	values: string[];
	valueCount: number | null;
	businessContext: string | null;
	/** The column's temporal resolution from the catalog's `resolved_type`
	 *  (never a name heuristic) — non-null makes the slice grain-able and
	 *  decides which grains the chip offers (DAT-712). */
	temporal: TemporalKind | null;
}

export const sliceColumns = (steps: DrillStep[]): string[] => {
	const seen = new Set<string>();
	const out: string[] = [];
	for (const s of steps) {
		if (s.kind !== "slice" || seen.has(s.column)) continue;
		seen.add(s.column);
		out.push(s.column);
	}
	return out;
};

/** The slice steps themselves (deduped by column, first wins) — the node path
 *  needs the grain riding each slice, not just the column names. A `grain` on
 *  a temporal slice buckets it via `time_bucket` (DAT-712; grain.ts owns the
 *  token grammar). Tier A keeps `sliceColumns` and its route rejects grained
 *  steps outright (strict zod → 400) — a grain can never be silently dropped
 *  into raw grouping there. */
export const sliceSteps = (
	steps: DrillStep[],
): { column: string; grain?: string }[] => {
	const out: { column: string; grain?: string }[] = [];
	for (const s of steps) {
		if (s.kind !== "slice" || out.some((d) => d.column === s.column)) continue;
		out.push(
			s.grain === undefined
				? { column: s.column }
				: { column: s.column, grain: s.grain },
		);
	}
	return out;
};

export const pinSteps = (
	steps: DrillStep[],
): Extract<DrillStep, { kind: "pin" }>[] =>
	steps.filter((s) => s.kind === "pin");

/** Every column a step stack references (tier decision: ⊆ base columns → A). */
export const referencedColumns = (steps: DrillStep[]): string[] => [
	...new Set(steps.map((s) => s.column)),
];

// --- Tier A: outer-wrap GROUP BY over a detail result ------------------------

/** One column of the base result, as DESCRIBE reports it. */
export interface BaseColumn {
	name: string;
	/** DuckDB type string, e.g. `DECIMAL(18,3)` — SUM-able types get a default
	 *  aggregate. */
	type: string;
}

/** Types that take the default SUM aggregate in a tier-A wrap. */
const SUMMABLE = /^(U?(TINY|SMALL|BIG|HUGE)INT|U?INTEGER|FLOAT|DOUBLE|DECIMAL)/;

/** An aggregate's output alias, de-collided against the base columns (the
 *  wanted name, else prefixed with underscores until free — deterministic,
 *  never a rename map). */
export function aggregateAlias(
	wanted: string,
	taken: ReadonlySet<string>,
): string {
	let alias = wanted;
	while (taken.has(alias)) alias = `_${alias}`;
	return alias;
}

/** The row-count alias, de-collided against the base columns. */
export function countAlias(columns: BaseColumn[]): string {
	return aggregateAlias("count", new Set(columns.map((c) => c.name)));
}

export interface ComposedDrill {
	sql: string;
	/** The FULL positional param array: base params first, pin params appended
	 *  (`$baseCount+1 …`), matching the composed SQL's placeholders. */
	params: DrillPinValue[];
}

/**
 * Compose a tier-A drill: wrap the base result in an outer GROUP BY.
 *
 * `SELECT dims…, COUNT(*), SUM(numeric)… FROM (base) AS _drill WHERE pins GROUP BY dims…`
 *
 * Pins bind as `$n` numbered AFTER the base params (the same convention
 * `buildFilterClause` uses, so wrapping never perturbs the caller's positional
 * params); a NULL pin becomes `IS NULL`. Aggregates: `COUNT(*)` plus `SUM`
 * over every summable base column not referenced by a step.
 *
 * Each aggregate is aliased `sum(<col>)`, NOT back onto the source column's own
 * name (DAT-678). Tier A wraps an arbitrary result the composer knows nothing
 * about, so a summable column may well be a rate, an average, or a balance —
 * things that do not sum. Re-using the source name would present `SUM(avg_price)`
 * under the header `avg_price` and read as the same quantity the undrilled grid
 * showed; naming the aggregate makes the arithmetic the user is looking at
 * visible instead. (The per-node path never takes this route: it recomposes from
 * clause parts under the additivity doctrine, where the engine's verdict decides
 * what may be summed at all.)
 */
export function composeTierA(
	baseSql: string,
	baseParams: DrillPinValue[],
	baseColumns: BaseColumn[],
	steps: DrillStep[],
): ComposedDrill {
	const dims = sliceColumns(steps);
	const pins = pinSteps(steps);
	const stepCols = new Set(referencedColumns(steps));

	// Aliases de-collide against the base columns AND against each other, so a
	// base column literally named `sum(x)` can't silently shadow the aggregate.
	const taken = new Set(baseColumns.map((c) => c.name));
	const claim = (wanted: string): string => {
		const alias = aggregateAlias(wanted, taken);
		taken.add(alias);
		return alias;
	};

	const aggregates = [`COUNT(*) AS ${quoteIdentifier(claim("count"))}`];
	for (const c of baseColumns) {
		if (stepCols.has(c.name) || !SUMMABLE.test(c.type)) continue;
		aggregates.push(
			`SUM(${quoteIdentifier(c.name)}) AS ${quoteIdentifier(claim(`sum(${c.name})`))}`,
		);
	}

	const select = [...dims.map(quoteIdentifier), ...aggregates].join(", ");
	const parts = [`SELECT ${select} FROM (${baseSql}) AS _drill`];

	const params = [...baseParams];
	if (pins.length > 0) {
		const predicates = pins.map((p) => {
			if (p.value === null) return `${quoteIdentifier(p.column)} IS NULL`;
			params.push(p.value);
			return `${quoteIdentifier(p.column)} = $${params.length}`;
		});
		parts.push(`WHERE ${predicates.join(" AND ")}`);
	}
	if (dims.length > 0) {
		parts.push(`GROUP BY ${dims.map(quoteIdentifier).join(", ")}`);
	}
	return { sql: parts.join(" "), params };
}

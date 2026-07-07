// Drill-down step model + the tier-A composer (DAT-672).
//
// A drill is an ordered stack of steps over a base query: `slice(column)` groups
// the result by a catalog dimension; `pin(column, value)` filters to one group's
// value (always PRE-aggregation — a pinned dimension may no longer be in the
// sliced output, so the filter can never ride on the grouped result).
//
// Drill composes UPSTREAM of the grid: each drill state yields a new effective
// base SQL + params, and the existing grid machinery (`buildGridQuery`,
// `/api/run-sql`) wraps it unchanged. Two composition paths (DAT-703):
//   - Tier A (this module, pure): every referenced column exists on the base
//     result → wrap it in an outer GROUP BY with default aggregates. The
//     ad-hoc grid path (`/api/drill/compose`).
//   - Per-node (`parts.ts`, behind `/api/drill/node`): a canvas node rebuilds
//     from its persisted clause parts with the steps as clause appends —
//     never by parsing or mutating SQL text (tier-B AST injection is gone).
// This module is neo-free so widgets can import the types (grid-query.ts is
// the precedent).

import { quoteIdentifier } from "./grid-query";

/** A pin carries the clicked cell's JSON value — bigints/dates arrive as
 *  strings and DuckDB casts the bound param to the column type. */
export type DrillPinValue = string | number | boolean | null;

export type DrillStep =
	| { kind: "slice"; column: string }
	| { kind: "pin"; column: string; value: DrillPinValue };

/** Axis-resolution request (`/api/drill/axes`, metric path in P1): exactly one
 *  of the two keys. Shared client↔server so the wire contract can't silently
 *  drift (this module is the neo-free home for drill types). */
export type DrillAxesRequest =
	| { metricKey: string; standardField?: undefined }
	| { standardField: string; metricKey?: undefined };

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

/** The row-count alias, de-collided against the base columns ("count", else
 *  prefixed with underscores until free — deterministic, never a rename map). */
export function countAlias(columns: BaseColumn[]): string {
	const taken = new Set(columns.map((c) => c.name));
	let alias = "count";
	while (taken.has(alias)) alias = `_${alias}`;
	return alias;
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
 * over every summable base column not referenced by a step — the default
 * requested on the ticket, refined per-surface later if usage demands it.
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

	const aggregates = [
		`COUNT(*) AS ${quoteIdentifier(countAlias(baseColumns))}`,
	];
	for (const c of baseColumns) {
		if (stepCols.has(c.name) || !SUMMABLE.test(c.type)) continue;
		aggregates.push(
			`SUM(${quoteIdentifier(c.name)}) AS ${quoteIdentifier(c.name)}`,
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

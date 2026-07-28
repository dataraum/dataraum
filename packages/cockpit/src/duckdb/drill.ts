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

// DAT-673 guidance fallback shared constants: BOTH the client (drillable-
// grid.tsx, which caps its request before sending) and the server (the
// axis-guidance route's zod schema + the agent module) need the SAME number,
// or the two drift and the client can send a payload the route rejects with
// a raw zod error (the review-round Critical 1 bug: the client sent every
// axis, the route's `.max(8)` 400'd, and the fix must not become two
// hand-copied literals under two different names). Defined here — not in the
// server-only agent module — because this module is neo-free and widgets
// already import types from it.
export const MAX_GUIDANCE_AXES = 8;

/** Bounds the Haiku call itself (server-side, inside the agent module) AND
 *  the client's fetch (drillable-grid.tsx) — a hung model must not leave
 *  "Asking…" disabled forever, and the route handler must not hold the
 *  connection open indefinitely either. One shared number for both ends. */
export const DRILL_GUIDANCE_TIMEOUT_MS = 20_000;

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
	| {
			partsSources: { relation: string; selectExpr: string }[];
			/** The answer's own BASE statement — `state.sql` in
			 *  answer-result.tsx, the ORIGINAL undrilled query the widget mounted
			 *  with, NOT `shownSql` (which tracks whatever's currently displayed,
			 *  base or drilled) — carried ONLY so the server can determine which
			 *  candidate axes are already non-measure identifier columns of THIS
			 *  exact result (DAT-671, "we should not slice on already existing
			 *  slices"), via a structural (one-hop) read of its outer projection/
			 *  GROUP BY. Never executed for this purpose — a parse/DESCRIBE read
			 *  only, same as the tier-A path's `resultSql` above. Absent = the
			 *  determination can't run, so nothing is greyed by this rule (never a
			 *  guess) — the axes still resolve exactly as before. */
			baseSql?: string;
	  };

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
	sliceType: string;
	/** Catalog sample of the dimension's values (display hint, not exhaustive). */
	values: string[];
	valueCount: number | null;
	businessContext: string | null;
	/** The column's temporal resolution from the catalog's `resolved_type`
	 *  (never a name heuristic) — non-null makes the slice grain-able and
	 *  decides which grains the chip offers (DAT-712). */
	temporal: TemporalKind | null;
	/** DAT-673 guidance: the max measured `driver_rankings` gain naming this
	 *  column, or `null` when no measured ranking exists for it. This is the
	 *  SAME number `orderAxesByDrivers` already uses to reorder the menu — it
	 *  used to be thrown away after ordering; now it rides the wire so the
	 *  chip can disclose WHY an axis leads (fact-scoped: only ever set on the
	 *  node/measure path — tier A doesn't know which fact backs a result
	 *  column, so it stays null there). */
	driverGain: number | null;
	/** DAT-879 measured slice relevance in [0,1] when this column is
	 *  catalogued, else `null` (substrate-only column — nothing curated it).
	 *  Populated on BOTH the node and tier-A paths (the catalog read is
	 *  fact-agnostic, only its dimension is). */
	sliceRelevance: number | null;
	/** 'primary' | 'supporting' | null — the cataloguing agent's absolute
	 *  judgement, or null when the column is catalogued but never judged
	 *  (still has `sliceRelevance`) or not catalogued at all (`sliceRelevance`
	 *  also null). Populated on both paths, same as `sliceRelevance`. */
	sliceInterest: string | null;
	/** DAT-673 hierarchy descent: the next-finer column in a CONFIRMED
	 *  drill-down chain this axis belongs to (`dimension_hierarchies`,
	 *  kind='drilldown', needs_confirmation=false), when that next column is
	 *  also among the currently-resolved axes — else `null`. Fact-scoped like
	 *  `driverGain`: node/measure path only, always null on tier A. */
	hierarchyNext: string | null;
	/** DAT-671 ("we should not slice on already existing slices," grey-out
	 *  amendment): set when this axis's column NAME already matches one of the
	 *  result's own non-measure identifier columns — the result is already
	 *  broken out by this dimension (a GROUP BY it already carries, read
	 *  structurally off the base statement — `sql-ast.ts`'s
	 *  `existingIdentifierColumns`), so slicing by it again would be a no-op
	 *  re-group. The item stays in the menu (never removed) but renders
	 *  DISABLED with this reason. `null` when offered normally — either the
	 *  axis genuinely isn't already in the result, or the determination
	 *  couldn't decide structurally (never a guess): the post-execution fold
	 *  probe (`drill-sql.ts`'s `foldsNothing`, tier-A only) remains the net for
	 *  what this schema/name-only check misses on that path. Populated on the
	 *  tier-A and parts-at-source (answer) paths only — the metric/measure node
	 *  path (`resolveDrillAxes`) never re-wraps an already-drilled statement, so
	 *  it stays null there. */
	disabledReason: string | null;
	/** DAT-857: why this DATE column is offered without a grain. Set only when the
	 *  engine's per-(target × axis) verdict withheld the bucketing — `temporal` is
	 *  then null, so the column stays available as a raw slice, but it says why it
	 *  cannot be bucketed and ranks LAST once anything else can be. Undefined
	 *  means "not a withheld time axis", which includes every non-temporal axis. */
	temporalWithheldReason?: string;
	/** DAT-857/730: the finest bucket this axis's data actually supports (`day` |
	 *  `month` | `quarter` | `year`), from its observed cadence. The grain menu
	 *  offers this rung and coarser; `undefined` is no claim, so the full preset
	 *  list stands. */
	bucketGrain?: string;
}

/**
 * Blank the drilled total where the parts do not add up to it (DAT-857).
 *
 * A recomputed measure — a ratio, an average, a distinct count — is correct in
 * every bucket and meaningless summed across them. The footer's `value` comes
 * from the UNRESTRICTED scalar, so printing it under a column of recomputed
 * per-bucket values invites the one false read the drill exists to prevent:
 * that the rows above it add up to it. It becomes an explicit `null`, which the
 * grid renders as the same honest `—` it already uses for an unobserved cell.
 *
 * Only `value` is masked. The operand columns beside it stay real totals: the
 * time gate offers a recompute bucketing ONLY when every carrier is additive on
 * that axis, so those columns genuinely do sum to their footers.
 *
 * Returns the footer unchanged when nothing is drilled, when no verdict was
 * consulted, or when the drilled axes all reconcile. `undefined` in, `undefined`
 * out — the footer is suppressed entirely on an undrilled grid.
 */
export function maskNonReconcilingTotal<V>(
	footer: Record<string, V | null> | undefined,
	steps: readonly DrillStep[],
	axes: readonly DrillAxis[],
	reconciles: { time: boolean; categorical: boolean } | undefined,
): Record<string, V | null> | undefined {
	if (footer === undefined || reconciles === undefined) return footer;
	const sliced = steps.filter((s) => s.kind === "slice");
	if (sliced.length === 0) return footer;
	const temporalColumns = new Set(
		axes.filter((a) => a.temporal !== null).map((a) => a.column),
	);
	// A slice on a date column that IS bucketed is a time axis; every other
	// slice — including a raw-date slice offered without a grain — folds rows
	// the categorical way.
	const reconcilesAll = sliced.every((s) =>
		temporalColumns.has(s.column) && s.grain !== undefined
			? reconciles.time
			: reconciles.categorical,
	);
	if (reconcilesAll) return footer;
	return { ...footer, value: null };
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

/**
 * Compounded aggregate labels a RE-WRAP of an already-drilled statement
 * produces, collapsed back to a clean face — `sum(sum(x))` → `sum(x)`,
 * `sum(count)` → `count`, a de-collided `_count`/`__count` → `groups` — de-
 * colliding against whatever else the SAME output already claims (DAT-671
 * drilled-projection hygiene).
 *
 * WHY this exists: `composeTierA` always emits `COUNT(*)` plus `SUM(<col>)`
 * over every summable non-dim base column, with no awareness that a base
 * column might ITSELF already be a prior wrap's own aggregate output — exactly
 * what happens re-drilling an already-minted report (`SELECT dim, SUM(x) AS
 * "sum(x)", COUNT(*) AS count FROM ... GROUP BY dim`, then sliced/pinned
 * further): the base's `"sum(x)"` gets summed again → `sum(sum(x))`; its
 * `"count"` gets summed → `sum(count)`; and the new wrap's OWN fresh
 * `COUNT(*)` collides with the base's existing `"count"` and de-collides to
 * `"_count"` — a real, rendered column that reads exactly like leaked internal
 * plumbing.
 *
 * `_count`/`__count` renames to `groups` rather than being dropped or reusing
 * `count` (owner ruling): it carries REAL information — how many of the
 * PRIOR wrap's sub-groups folded into this new, coarser group — distinct from
 * `sum(count)`'s rolled-up ORIGINAL row total, so the two must never collide
 * onto the same name. `groups` names what it counts and satisfies the "never
 * render `_count`" rule without discarding a real number. Since the two now
 * target genuinely different clean names, no priority ordering is needed
 * between them — the generic de-collision below (against whatever the same
 * output already claims, itself or another renamed column) is enough.
 *
 * String-level, over OUR OWN deterministic alias vocabulary (composeTierA's
 * `count`/`sum(<col>)` shapes) — never a read of the SQL that produced them.
 * `composeTierA`/`countAlias`/`foldsNothing` are UNTOUCHED and stay in exact
 * parity with each other: this only relabels the OUTER-most face of an
 * already-validated, already-fold-probed result, as one more thin wrap
 * (`drill-sql.ts`'s `composeDrill` applies it AFTER the fold probe has already
 * run against the un-renamed SQL).
 */
export function deCompoundedColumnRenames(
	columns: BaseColumn[],
): Map<string, string> {
	const proposals: { original: string; wanted: string }[] = [];
	const unchanged = new Set<string>();
	for (const c of columns) {
		const nestedSum = /^sum\((sum\(.+\))\)$/.exec(c.name);
		if (nestedSum) {
			proposals.push({ original: c.name, wanted: nestedSum[1] });
		} else if (c.name === "sum(count)") {
			proposals.push({ original: c.name, wanted: "count" });
		} else if (/^_+count$/.test(c.name)) {
			proposals.push({ original: c.name, wanted: "groups" });
		} else {
			unchanged.add(c.name);
		}
	}
	const taken = new Set(unchanged);
	const renames = new Map<string, string>();
	for (const p of proposals) {
		const final = aggregateAlias(p.wanted, taken);
		taken.add(final);
		if (final !== p.original) renames.set(p.original, final);
	}
	return renames;
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

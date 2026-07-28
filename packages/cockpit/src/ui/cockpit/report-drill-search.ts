// The report-detail route's `?drill=` search param (DAT-676) — pure narrowing
// logic, extracted to its own module (cockpit React convention 10) so it is
// unit-testable without mounting the route.
//
// TanStack Router's default search serialization (`stringifySearchWith`/
// `parseSearchWith(JSON.stringify, JSON.parse)`, router-core's searchParams.ts)
// already JSON-encodes/decodes a non-primitive search VALUE for us — a route
// can hand `search: { drill: steps }` straight to `navigate()`/`Link` and read
// `Route.useSearch().drill` back as already-parsed JSON. So there is no wire
// FORMAT to invent here, only a NARROWER: the parsed value is whatever a
// pasted/hand-edited URL, or a stale shape from a previous step model, happens
// to contain — never trusted as a `DrillStep[]` without a shape check (the
// same "tool/LLM output is unknown at the boundary" posture this codebase
// applies to every other untrusted input).
//
// The decision this module encodes (refine, 2026-07-03, DAT-676): persist the
// STEPS, not the composed SQL — the drilled statement is deterministically
// re-derivable (the resolver + compose on rehydrate, drillable-grid.tsx). An
// entry that fails to narrow is DROPPED, not the whole array: a partially
// valid link should restore what it still can, rather than falling back to
// nothing over one bad segment.

import type { DrillPinValue, DrillStep } from "#/duckdb/drill";

// Caps mirror /api/drill/compose's BodySchema exactly (column names 256,
// string values 1024, steps 64) — the report route is tier A ONLY, so this
// narrower's output is always headed for that one endpoint; matching its
// bounds means a decode that succeeds here is guaranteed not to 400 there on
// SIZE alone.
const MAX_STEPS = 64;
const MAX_COLUMN_LENGTH = 256;
const MAX_VALUE_LENGTH = 1024;

function isPinValue(v: unknown): v is DrillPinValue {
	if (v === null || typeof v === "number" || typeof v === "boolean")
		return true;
	return typeof v === "string" && v.length <= MAX_VALUE_LENGTH;
}

/** Narrow one parsed entry to a `DrillStep`, or null when it doesn't match
 *  either step shape. Structure only — no interpretation of whether the
 *  column/value is still valid (that's the server compose call's job).
 *
 *  Never reads/preserves `grain`: it's a NODE-path capability
 *  (drillable-grid.tsx) the report route can't reach (tier A only, no
 *  `source`), and `/api/drill/compose`'s `StepSchema` is a `strictObject`
 *  with NO `grain` key on either variant — a decoded step that carried one
 *  would 400 the whole rehydrate compose call (unrecognized key), needlessly
 *  degrading a step that would otherwise have composed fine. Symmetry with
 *  the one endpoint a report's drill can ever recompose through. */
function narrowDrillStep(raw: unknown): DrillStep | null {
	if (typeof raw !== "object" || raw === null) return null;
	const r = raw as Record<string, unknown>;
	if (
		typeof r.column !== "string" ||
		r.column.length === 0 ||
		r.column.length > MAX_COLUMN_LENGTH
	)
		return null;
	if (r.kind === "slice") return { kind: "slice", column: r.column };
	if (r.kind === "pin" && isPinValue(r.value))
		return { kind: "pin", column: r.column, value: r.value };
	return null;
}

/**
 * Narrow the report route's `drill` search param (untrusted) into a
 * `DrillStep[]`. Not an array at all → empty (no drill). Each array entry is
 * checked independently; an entry that doesn't narrow is dropped rather than
 * invalidating the whole link. Capped at `MAX_STEPS` (excess entries simply
 * aren't decoded) — the same resource-use bound `/api/drill/compose` enforces
 * on its own `steps` array.
 */
export function decodeDrillSearch(raw: unknown): DrillStep[] {
	if (!Array.isArray(raw)) return [];
	const out: DrillStep[] = [];
	for (const item of raw) {
		if (out.length >= MAX_STEPS) break;
		const step = narrowDrillStep(item);
		if (step) out.push(step);
	}
	return out;
}

/** The search value to navigate with for a given step stack — `undefined`
 *  (omit the param) for an empty stack, so a cleared drill produces a clean
 *  URL rather than `?drill=[]`. */
export function encodeDrillSearch(steps: DrillStep[]): DrillStep[] | undefined {
	return steps.length > 0 ? steps : undefined;
}

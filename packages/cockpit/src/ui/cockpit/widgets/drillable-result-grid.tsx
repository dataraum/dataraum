// The registered `result-grid` canvas widget (DAT-385, drillable DAT-678) —
// also the report-detail table.
//
// It owns the BASE query (the agent's `run_sql` call, or a report's frozen SQL)
// and hands it to `DrillableGrid` with no drill source, which is exactly what
// tier A means: a result nobody can recompose upstream can still be grouped BY
// ITS OWN COLUMNS, because the composer wraps the statement it was given. That
// is the whole capability here — no parts, no persisted node, nothing to be
// stale — and it is why this path is the fallback everywhere.
//
// It lives beside `result-grid.tsx` rather than inside it: `DrillableGrid`
// imports `WindowedGrid` from there, so registering a drillable widget in that
// same module would close an import cycle between the two.

import { useMemo } from "react";

import type { CanvasState } from "#/ui/cockpit/canvas-state";
import { DrillableGrid } from "#/ui/cockpit/widgets/drillable-grid";

export function DrillableResultGridWidget({
	state,
}: {
	state: Extract<CanvasState, { kind: "result-grid" }>;
}) {
	// The provider derives a fresh canvas object on every message tick; serialize
	// sql+params so a new `key` is produced only when the QUERY actually changes,
	// not on per-tick object churn.
	const baseKey = useMemo(
		() => JSON.stringify([state.sql, state.params ?? null]),
		[state.sql, state.params],
	);
	// The axes request is part of that identity too: a stable object keeps the
	// resolver's query key from flipping between renders.
	const axesRequest = useMemo(
		() =>
			state.params && state.params.length > 0
				? { resultSql: state.sql, resultParams: state.params }
				: { resultSql: state.sql },
		[state.sql, state.params],
	);
	return (
		// REMOUNT on a new base query (React rule 5): the drill stack, the grid's
		// sort/filters and the authored chart are all state ABOUT this query, and
		// the focus canvas swaps `state` without remounting the widget itself — so
		// without this key a fresh run_sql result would inherit the previous
		// query's slices and show a composition of a query that is gone.
		<DrillableGrid
			key={baseKey}
			sql={state.sql}
			params={state.params}
			axesRequest={axesRequest}
		/>
	);
}

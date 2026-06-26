// The Vega chart renderer (DAT-626 / ADR-0015) — a thin DOM shell over vega +
// vega-lite, no wrapper dep.
//
// CLIENT-ONLY: vega renders to a <canvas> and measures the DOM, so it must never
// run during SSR. Two guards, both load-bearing:
//   1. Callers mount this under TanStack Start's <ClientOnly> (the prescribed
//      mechanism — same as the xyflow operating-model canvas), so there's no SSR
//      attempt and a clean fallback.
//   2. vega + vega-lite are pulled via a DYNAMIC import() inside the effect, so the
//      ~heavy libs are code-split into a client chunk and never enter the server
//      bundle at all (belt-and-suspenders with #1; the effect also can't run on the
//      server).
//
// The stored config carries NO data (it's frozen; data is re-run live). We resolve
// it to a Vega-Lite spec, compile to Vega, and bind the live rows to the named
// `table` source before the first render — so the same frozen config renders over
// whatever the SQL returns today.
//
// This is the cockpit's fourth effect (React rule 2: effects are for external
// systems with cleanup). vega is exactly that — an imperative view we create,
// run, resize, and finalize. Justified, like the chat scroll-pin and NDJSON fold.

import { Alert } from "@mantine/core";
import { useEffect, useMemo, useRef, useState } from "react";
import type { ChartConfig } from "#/charts/chart-config";
import type { ChartRow } from "#/charts/chart-data";
import { CHART_DATA_NAME, resolveSpec } from "#/charts/resolve";

/** The shape we use off the dynamically-imported vega module — typed loosely so we
 * don't pull vega's value types into this module's static graph (it's lazy). */
interface VegaView {
	initialize(el: HTMLElement): VegaView;
	runAsync(): Promise<VegaView>;
	resize(): VegaView;
	finalize(): void;
}

export function ChartView({
	config,
	rows,
	height = 300,
	testId = "chart-view",
}: {
	config: ChartConfig;
	rows: ChartRow[];
	/** Container height in px; width is container-driven (vega `width:"container"`). */
	height?: number;
	testId?: string;
}) {
	const containerRef = useRef<HTMLDivElement>(null);
	const [error, setError] = useState<string | null>(null);

	// Resolve + serialize the inputs so the render effect re-fires only when the
	// chart actually changes, not on every parent re-render (the modal re-renders
	// per keystroke while the user types an instruction). The rows array is fresh
	// each fetch, so key on a cheap structural digest, not identity.
	const specJson = useMemo(() => JSON.stringify(resolveSpec(config)), [config]);
	const rowsKey = useMemo(() => JSON.stringify(rows), [rows]);

	useEffect(() => {
		let view: VegaView | null = null;
		let ro: ResizeObserver | null = null;
		let disposed = false;
		setError(null);

		void (async () => {
			try {
				// Dynamic import: vega/vega-lite load only in the browser, in their own
				// chunk — never in the server bundle.
				const [vega, vl] = await Promise.all([
					import("vega"),
					import("vega-lite"),
				]);
				const vlSpec = JSON.parse(specJson);
				// Compile VL → Vega, then bind the live rows to the named `table` source
				// (the frozen config references it by name and carries no data).
				const vgSpec = vl.compile(vlSpec).spec as {
					data?: Array<{ name?: string; values?: unknown }>;
				};
				const table = vgSpec.data?.find((d) => d.name === CHART_DATA_NAME);
				if (table) table.values = JSON.parse(rowsKey);

				const el = containerRef.current;
				if (disposed || !el) return;
				const runtime = vega.parse(vgSpec as Parameters<typeof vega.parse>[0]);
				view = new vega.View(runtime, {
					renderer: "canvas",
				}) as unknown as VegaView;
				view.initialize(el);
				await view.runAsync();

				// Re-fit on a layout-driven container resize (modal/card width change):
				// vega's own listener is window-only, so re-read the container width and
				// re-run. Owned by this effect so it shares the View's lifecycle.
				if (typeof ResizeObserver !== "undefined") {
					ro = new ResizeObserver(() => {
						void view?.resize().runAsync();
					});
					ro.observe(el);
				}
			} catch (err) {
				if (disposed) return;
				setError(err instanceof Error ? err.message : String(err));
			}
		})();

		return () => {
			disposed = true;
			ro?.disconnect();
			view?.finalize();
		};
	}, [specJson, rowsKey]);

	if (error) {
		return (
			<Alert color="red" data-testid={`${testId}-error`}>
				Couldn’t render the chart: {error}
			</Alert>
		);
	}
	return (
		<div
			ref={containerRef}
			data-testid={testId}
			style={{ width: "100%", height, minHeight: height }}
		/>
	);
}

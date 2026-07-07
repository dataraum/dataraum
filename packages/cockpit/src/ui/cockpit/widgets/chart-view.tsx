// The Vega chart renderer (see CLAUDE.md § Charting) — a thin DOM shell over vega +
// vega-lite, no wrapper dep.
//
// CLIENT-ONLY: this component is always mounted under TanStack Start's <ClientOnly>
// (the chart modal, report detail, gallery thumbnail), which skips it during SSR so
// the Vega effect never runs server-side. All DOM-dependent vega calls (initialize,
// runAsync) live inside the effect, which doesn't run on the server regardless.
//
// The stored config carries NO data (it's frozen; data is re-run live). We resolve
// it to a Vega-Lite spec, compile to Vega, and bind the live rows to the named
// `table` source before the first render — so the same frozen config renders over
// whatever the SQL returns today.
//
// The vega View is an imperative external system (create → run → resize → finalize),
// so its lifecycle lives in an effect with cleanup (React rule 2) — like the chat
// scroll-pin and NDJSON fold.

import { Alert } from "@mantine/core";
import { useEffect, useMemo, useRef, useState } from "react";
import { parse, View } from "vega";
import { compile } from "vega-lite";
import type { ChartConfig } from "#/charts/chart-config";
import type { ChartRow } from "#/charts/chart-data";
import { CHART_DATA_NAME, resolveSpec } from "#/charts/resolve";

export function ChartView({
	config,
	rows,
	height = 300,
	testId = "chart-view",
}: {
	config: ChartConfig;
	rows: ChartRow[];
	/** Container height in px; both axes are container-driven (vega `"container"`). */
	height?: number;
	testId?: string;
}) {
	const containerRef = useRef<HTMLDivElement>(null);
	const [error, setError] = useState<string | null>(null);

	// Serialize the resolved spec so the render effect re-fires only when the chart
	// actually changes, not on every parent re-render (the modal re-renders per
	// keystroke). `rows` is already memoized on its query result by every caller, so
	// its identity is a stable effect dep.
	const specJson = useMemo(() => JSON.stringify(resolveSpec(config)), [config]);

	useEffect(() => {
		const el = containerRef.current;
		if (!el) return;
		let view: View | null = null;
		let ro: ResizeObserver | null = null;
		let disposed = false;
		setError(null);
		const fail = (err: unknown) => {
			if (!disposed) setError(err instanceof Error ? err.message : String(err));
		};

		try {
			// Compile VL → Vega, then bind the live rows to the named `table` source
			// (the frozen config references it by name and carries no data). Vega tags
			// ingested datums, so hand it a private shallow copy, not the React array.
			const vgSpec = compile(JSON.parse(specJson)).spec as {
				data?: Array<{ name?: string; values?: unknown }>;
			};
			const table = vgSpec.data?.find((d) => d.name === CHART_DATA_NAME);
			if (table) table.values = rows.map((r) => ({ ...r }));

			view = new View(parse(vgSpec as Parameters<typeof parse>[0]), {
				renderer: "canvas",
			});
			view.initialize(el);
			// runAsync is fire-and-forget here, but its rejection must surface — an
			// uncaught async vega error would otherwise leave a blank canvas with no
			// signal. Re-fit on a layout-driven container resize once it's drawn.
			view
				.runAsync()
				.then(() => {
					if (disposed || !view) return;
					if (typeof ResizeObserver !== "undefined") {
						ro = new ResizeObserver(() => {
							void view?.resize().runAsync().catch(fail);
						});
						ro.observe(el);
					}
				})
				.catch(fail);
		} catch (err) {
			fail(err);
		}

		return () => {
			disposed = true;
			ro?.disconnect();
			view?.finalize();
		};
	}, [specJson, rows]);

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

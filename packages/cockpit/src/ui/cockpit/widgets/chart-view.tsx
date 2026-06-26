// The Vega chart renderer (DAT-626 / ADR-0015) — a thin DOM shell over vega +
// vega-lite, no wrapper dep.
//
// CLIENT-ONLY by construction: vega + vega-lite are STATIC imports here, and this
// widget is only ever mounted inside TanStack Start's <ClientOnly> (the chart modal,
// report detail, gallery thumbnail). The Start plugin strips <ClientOnly> children —
// and their imports — out of the SERVER bundle, so vega never loads server-side and
// there's no hydration risk. That's the framework's mechanism; we don't hand-roll a
// dynamic import() or an SSR guard.
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
	/** Container height in px; width is container-driven (vega `width:"container"`). */
	height?: number;
	testId?: string;
}) {
	const containerRef = useRef<HTMLDivElement>(null);
	const [error, setError] = useState<string | null>(null);

	// Serialize the inputs so the render effect re-fires only when the chart actually
	// changes, not on every parent re-render (the modal re-renders per keystroke). The
	// rows array is fresh each fetch, so key on a structural digest, not identity.
	const specJson = useMemo(() => JSON.stringify(resolveSpec(config)), [config]);
	const rowsKey = useMemo(() => JSON.stringify(rows), [rows]);

	useEffect(() => {
		const el = containerRef.current;
		if (!el) return;
		let view: View | null = null;
		let ro: ResizeObserver | null = null;
		setError(null);

		try {
			// Compile VL → Vega, then bind the live rows to the named `table` source
			// (the frozen config references it by name and carries no data).
			const vgSpec = compile(JSON.parse(specJson)).spec as {
				data?: Array<{ name?: string; values?: unknown }>;
			};
			const table = vgSpec.data?.find((d) => d.name === CHART_DATA_NAME);
			if (table) table.values = JSON.parse(rowsKey);

			view = new View(parse(vgSpec as Parameters<typeof parse>[0]), {
				renderer: "canvas",
			});
			view.initialize(el);
			void view.runAsync();

			// Re-fit on a layout-driven container resize (modal/card width change):
			// vega's own listener is window-only.
			if (typeof ResizeObserver !== "undefined") {
				ro = new ResizeObserver(() => {
					void view?.resize().runAsync();
				});
				ro.observe(el);
			}
		} catch (err) {
			setError(err instanceof Error ? err.message : String(err));
		}

		return () => {
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

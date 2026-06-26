// Frozen-chart renderers for reports (DAT-626). A report's `chartConfig` carries no
// data; both surfaces render it over LIVE re-run rows (the same chart-data fetch the
// modal uses), so the chart stays current like the table does. Null config → these
// don't render (the report is table-only, first-class).
//
//   - ReportChart: the full chart on report detail.
//   - ReportChartThumbnail: a small chart on each gallery card, LAZY via an
//     IntersectionObserver so a 200-card gallery doesn't fire 200 live queries +
//     canvases at once — only cards scrolled into view fetch and render (cockpit
//     "bound every data surface" rule). Once shown, it stays (no churn on scroll).
//
// Vega measures the DOM → mounted under <ClientOnly> (the renderer is client-only).

import { Alert, Center, Loader, Paper } from "@mantine/core";
import { ClientOnly } from "@tanstack/react-router";
import { useEffect, useMemo, useRef, useState } from "react";
import type { ChartConfig } from "#/charts/chart-config";
import { gridViewToRows } from "#/charts/chart-data";
import { ChartView } from "#/ui/cockpit/widgets/chart-view";
import { useChartStore } from "#/ui/cockpit/widgets/use-chart-store";

/** The frozen chart over live data — the report-detail surface. */
export function ReportChart({
	sql,
	params,
	config,
	height = 320,
}: {
	sql: string;
	params?: (string | number | boolean | null)[];
	config: ChartConfig;
	height?: number;
}) {
	const { data: store, isLoading, error } = useChartStore(sql, params);
	const rows = useMemo(() => (store ? gridViewToRows(store) : []), [store]);

	if (isLoading) {
		return (
			<Center h={height}>
				<Loader size="sm" />
			</Center>
		);
	}
	if (error) {
		return (
			<Alert color="red" data-testid="report-chart-error">
				Couldn’t render the chart: {String(error)}
			</Alert>
		);
	}
	if (!store || store.columns.length === 0) return null;

	return (
		<Paper withBorder p="sm" data-testid="report-chart">
			<ClientOnly
				fallback={
					<Center h={height}>
						<Loader size="sm" />
					</Center>
				}
			>
				<ChartView
					config={config}
					rows={rows}
					height={height}
					testId="report-chart-view"
				/>
			</ClientOnly>
		</Paper>
	);
}

/** Fire once when the element first scrolls within view (+100px), then latch on —
 * an external DOM observer with cleanup (React rule 2). Gates the thumbnail's fetch
 * so off-screen cards stay cheap. */
function useInView(): [React.RefObject<HTMLDivElement | null>, boolean] {
	const ref = useRef<HTMLDivElement | null>(null);
	const [inView, setInView] = useState(false);
	useEffect(() => {
		const el = ref.current;
		if (!el || inView || typeof IntersectionObserver === "undefined") return;
		const io = new IntersectionObserver(
			(entries) => {
				if (entries.some((e) => e.isIntersecting)) setInView(true);
			},
			{ rootMargin: "100px" },
		);
		io.observe(el);
		return () => io.disconnect();
	}, [inView]);
	return [ref, inView];
}

/** A small frozen chart on a gallery card — rendered only once its card is in view. */
export function ReportChartThumbnail({
	sql,
	config,
	height = 140,
}: {
	sql: string;
	config: ChartConfig;
	height?: number;
}) {
	const [ref, inView] = useInView();
	const { data: store } = useChartStore(sql, undefined, inView);
	const rows = useMemo(() => (store ? gridViewToRows(store) : []), [store]);
	const ready = store && store.columns.length > 0;

	return (
		<div ref={ref} style={{ height }} data-testid="report-thumbnail">
			{inView && ready ? (
				<ClientOnly fallback={null}>
					<ChartView
						config={config}
						rows={rows}
						height={height}
						testId="report-thumbnail-view"
					/>
				</ClientOnly>
			) : null}
		</div>
	);
}

// Result-grid widget (DAT-385 P2) — the human-facing SQL result surface.
//
// Splits cleanly in two:
//   - ResultGridView: PURE render of a ColumnStore via TanStack Table with
//     index-rows + accessorFn (no row-object rematerialization). Trivially
//     testable with a pre-seeded store, no I/O.
//   - ResultGridWidget: owns the I/O — POSTs the carried SQL to the P1
//     `/api/run-sql` NDJSON endpoint, folds frames into a ColumnStore as they
//     arrive, and aborts the fetch on unmount/query-change (the server then
//     emits a `cancelled` footer). The only widget that does I/O — the baseline
//     widgets are static — so the streaming state is contained here.
//
// P2 scope: read-only, server-side sort/filter is P3. The body IS virtualized
// (only the visible window hits the DOM) — load-bearing for the 50k streaming
// cap, not optional.

import type { Json } from "@duckdb/node-api";
import { Alert, Badge, Group, Table, Text } from "@mantine/core";
import {
	type ColumnDef,
	flexRender,
	getCoreRowModel,
	type RowData,
	useReactTable,
} from "@tanstack/react-table";
import { useVirtualizer } from "@tanstack/react-virtual";
import { useEffect, useMemo, useRef, useState } from "react";
import { ColumnStore, readNdjsonStream } from "#/duckdb/ndjson-stream";
import type { CanvasState } from "#/ui/cockpit/canvas-state";

// §7.3 hook: carry the neo column type metadata on each TanStack column. P2
// does not consume it; P3 type-driven formatting (right-align numerics, render
// timestamps) + sort/filter dispatch on `columnDef.meta.duckdbType`. Kept type-
// only (Json) so the neo native driver never reaches the client bundle.
declare module "@tanstack/react-table" {
	interface ColumnMeta<TData extends RowData, TValue> {
		duckdbType?: Json;
	}
}

/** JSON-safe cell → display string. Columnar values are already coerced
 * server-side (bigint→string, dates→ISO, nested→plain JSON); we only pick a
 * readable rendering: null as an em-dash, objects/arrays as compact JSON. */
function formatCell(value: unknown): string {
	if (value === null || value === undefined) return "—";
	if (typeof value === "object") return JSON.stringify(value);
	return String(value);
}

const STATUS_COLOR = {
	streaming: "blue",
	done: "green",
	truncated: "yellow",
	cancelled: "gray",
	error: "red",
} as const;

/** Pure presentation of a (possibly still-filling) ColumnStore. */
export function ResultGridView({
	store,
	fatal,
}: {
	store: ColumnStore;
	fatal?: string | null;
}) {
	// Index-rows: TanStack Table iterates row indices; each accessor reads its
	// column array at that index — O(1), no row objects ever built.
	const data = useMemo<number[]>(
		() => Array.from({ length: store.rowCount }, (_, i) => i),
		[store.rowCount],
	);
	const columns = useMemo<ColumnDef<number>[]>(() => {
		const typeList = Array.isArray(store.types) ? (store.types as Json[]) : [];
		return store.columns.map((name, c) => ({
			id: `c${c}`,
			header: name,
			// accessorFn closes over `store` by REFERENCE and reads the column array
			// lazily at render time (not at memo creation), so cells fill in as
			// streamed batches grow store.cols — don't freeze or copy the store.
			accessorFn: (rowIndex: number) => store.cols[c]?.[rowIndex] ?? null,
			meta: { duckdbType: typeList[c] },
		}));
	}, [store.columns, store]);
	const table = useReactTable({
		data,
		columns,
		getCoreRowModel: getCoreRowModel(),
	});

	// Virtualize the body: only the visible window (+overscan) is ever in the
	// DOM, so a 50k-row result is ~40 <tr> nodes, not 50k. The columnar store +
	// index rows make this the intended, cheap path. `initialRect` gives a sane
	// window before the real ResizeObserver measurement (and in tests, which have
	// no layout). Rows are uniform-height text, so a fixed estimate is fine — no
	// per-row measureElement (P3 can add it if variable heights ever land).
	const rows = table.getRowModel().rows;
	const scrollRef = useRef<HTMLDivElement>(null);
	const rowVirtualizer = useVirtualizer({
		count: rows.length,
		getScrollElement: () => scrollRef.current,
		estimateSize: () => 36,
		overscan: 16,
		initialRect: { width: 800, height: 600 },
	});
	const virtualRows = rowVirtualizer.getVirtualItems();
	const totalSize = rowVirtualizer.getTotalSize();
	const padTop = virtualRows.length > 0 ? virtualRows[0].start : 0;
	const padBottom =
		virtualRows.length > 0
			? totalSize - virtualRows[virtualRows.length - 1].end
			: 0;
	const colCount = store.columns.length;

	const status = fatal ? "error" : store.status;

	return (
		<div data-testid="canvas-result-grid">
			<Group justify="space-between" mb="xs">
				<Text size="sm" fw={500}>
					{store.rowCount} row{store.rowCount === 1 ? "" : "s"}
				</Text>
				<Badge color={STATUS_COLOR[status]} variant="light" size="sm">
					{status}
				</Badge>
			</Group>

			{(fatal || store.error) && (
				<Alert color="red" mb="xs" data-testid="canvas-result-grid-error">
					{fatal ?? store.error}
				</Alert>
			)}
			{store.truncated && (
				<Alert
					color="yellow"
					mb="xs"
					data-testid="canvas-result-grid-truncated"
				>
					Showing the first {store.cap ?? store.rowCount} rows — the result has
					more. Narrow the query to see the rest.
				</Alert>
			)}

			{colCount > 0 && (
				<div
					ref={scrollRef}
					style={{ maxHeight: 480, overflow: "auto" }}
					data-testid="canvas-result-grid-scroll"
				>
					<Table striped highlightOnHover stickyHeader>
						<Table.Thead>
							<Table.Tr>
								{table.getFlatHeaders().map((header) => (
									<Table.Th key={header.id}>
										{flexRender(
											header.column.columnDef.header,
											header.getContext(),
										)}
									</Table.Th>
								))}
							</Table.Tr>
						</Table.Thead>
						<Table.Tbody>
							{/* Spacer rows reserve the off-screen scroll height so only the
							    visible window carries real <tr> cells. */}
							{padTop > 0 && (
								<Table.Tr aria-hidden>
									<Table.Td
										colSpan={colCount}
										style={{ height: padTop, padding: 0, border: 0 }}
									/>
								</Table.Tr>
							)}
							{virtualRows.map((vr) => {
								const row = rows[vr.index];
								return (
									<Table.Tr key={row.id}>
										{row.getVisibleCells().map((cell) => (
											<Table.Td key={cell.id}>
												{formatCell(cell.getValue())}
											</Table.Td>
										))}
									</Table.Tr>
								);
							})}
							{padBottom > 0 && (
								<Table.Tr aria-hidden>
									<Table.Td
										colSpan={colCount}
										style={{ height: padBottom, padding: 0, border: 0 }}
									/>
								</Table.Tr>
							)}
						</Table.Tbody>
					</Table>
				</div>
			)}
		</div>
	);
}

/** The registered widget: streams `/api/run-sql` for the carried query. */
export function ResultGridWidget({
	state,
}: {
	state: Extract<CanvasState, { kind: "result-grid" }>;
}) {
	const storeRef = useRef(new ColumnStore());
	const [, bump] = useState(0);
	const [fatal, setFatal] = useState<string | null>(null);

	// Value-stable query identity: ChatRail re-dispatches a fresh canvasState
	// object on every message tick, so keying the stream effect on object
	// identity would re-fire (and re-stream) constantly. Serialize sql+params so
	// the effect only re-runs when the QUERY actually changes.
	const queryKey = useMemo(
		() => JSON.stringify([state.sql, state.params ?? null]),
		[state.sql, state.params],
	);
	useEffect(() => {
		// Parse the query back out of the value-stable key so the effect's ONLY
		// dependency is the key itself — re-stream iff the query actually changed,
		// never on ChatRail's per-tick fresh-object churn.
		const [sql, params] = JSON.parse(queryKey) as [
			string,
			(string | number | boolean | null)[] | null,
		];
		const store = new ColumnStore();
		storeRef.current = store;
		setFatal(null);
		bump((v) => v + 1);

		const ac = new AbortController();
		void (async () => {
			try {
				const res = await fetch("/api/run-sql", {
					method: "POST",
					headers: { "Content-Type": "application/json" },
					body: JSON.stringify({ sql, params: params ?? undefined }),
					signal: ac.signal,
				});
				if (!res.ok || !res.body) {
					const detail = await res.text().catch(() => res.statusText);
					throw new Error(detail || `run-sql failed (${res.status})`);
				}
				await readNdjsonStream(res.body, (frame) => {
					store.apply(frame);
					bump((v) => v + 1);
				});
			} catch (err) {
				// An aborted fetch (unmount / new query) is expected — not an error.
				if (ac.signal.aborted) return;
				setFatal(err instanceof Error ? err.message : String(err));
			}
		})();
		return () => ac.abort();
	}, [queryKey]);

	return <ResultGridView store={storeRef.current} fatal={fatal} />;
}

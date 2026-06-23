// Result-grid widget (DAT-385 P2 grid + P3 server-side sort) — the human-facing
// SQL result surface.
//
// Splits cleanly in three:
//   - ResultGridView: PURE render of a ColumnStore via TanStack Table with
//     index-rows + accessorFn (no row-object rematerialization). Headers are
//     interactive when given `onToggleSort`. Trivially testable, no I/O.
//   - ResultGridWidget: the registered entry. Owns the BASE query (the agent's
//     run_sql call) and `key`s the inner grid on it, so a new agent query
//     remounts the grid and resets the sort cleanly.
//   - StreamingGrid: owns the I/O + the grid-local sort. POSTs sql+params+sort
//     to the P1 `/api/run-sql` NDJSON endpoint, folds frames into a ColumnStore
//     as they arrive, and aborts the fetch on unmount/query-change/sort-change
//     (the server then emits a `cancelled` footer).
//
// Sort is SERVER-SIDE (re-issue with ORDER BY), not a client reorder: the grid
// caps at 50k and can truncate, so the sort must run before the cap to show the
// true top-N. Filter + keyset paging stay deferred (a connected, researched P3/P4
// effort). The body IS virtualized (only the visible window hits the DOM) —
// load-bearing for the 50k streaming cap, not optional.

import type { Json } from "@duckdb/node-api";
import {
	Alert,
	Badge,
	Collapse,
	Group,
	Table,
	Text,
	UnstyledButton,
} from "@mantine/core";
import {
	type ColumnDef,
	flexRender,
	getCoreRowModel,
	type RowData,
	useReactTable,
} from "@tanstack/react-table";
import { useVirtualizer } from "@tanstack/react-virtual";
import { useCallback, useEffect, useMemo, useRef, useState } from "react";
import { cellAlign, formatCell } from "#/duckdb/cell-format";
import { ColumnStore, readNdjsonStream } from "#/duckdb/ndjson-stream";
import type { GridSort } from "#/duckdb/stream-sql";
import type { CanvasState } from "#/ui/cockpit/canvas-state";
import { SqlBlock } from "#/ui/cockpit/widgets/sql-block";

// §7.3 hook: carry the neo column type metadata on each TanStack column. The
// type-driven cell formatting (right-align numerics, render timestamps) lives in
// cell-format.ts (DAT-575) and dispatches on `columnDef.meta.duckdbType`; sort/
// filter will too. Kept type-only (Json) so the neo native driver never reaches
// the client bundle.
declare module "@tanstack/react-table" {
	interface ColumnMeta<TData extends RowData, TValue> {
		duckdbType?: Json;
	}
}

const STATUS_COLOR = {
	streaming: "blue",
	done: "green",
	truncated: "yellow",
	cancelled: "gray",
	error: "red",
} as const;

/**
 * The next sort after a header click: unsorted → asc → desc → unsorted. Clicking
 * a DIFFERENT column starts that column at asc. Pure, so the state machine is
 * unit-testable without the streaming widget.
 */
export function cycleSort(
	current: GridSort | null,
	column: string,
): GridSort | null {
	if (!current || current.column !== column) return { column, dir: "asc" };
	if (current.dir === "asc") return { column, dir: "desc" };
	return null;
}

/** Pure presentation of a (possibly still-filling) ColumnStore.
 *
 * `sort` + `onToggleSort` make the column headers interactive (DAT-385 P3): a
 * click asks the OWNER to re-issue the query with a new server-side sort. The
 * view itself never reorders rows — sort runs before the cap, server-side, so a
 * truncated result still shows the true top-N. Omit `onToggleSort` (e.g. in a
 * pure-render test) and the headers stay static. */
export function ResultGridView({
	store,
	fatal,
	sort,
	onToggleSort,
}: {
	store: ColumnStore;
	fatal?: string | null;
	sort?: GridSort | null;
	onToggleSort?: (column: string) => void;
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
								{table.getFlatHeaders().map((header) => {
									const name = String(header.column.columnDef.header ?? "");
									const active = sort?.column === name;
									const clickable = onToggleSort !== undefined;
									// Right-align numeric headers so they sit over their
									// right-aligned cells (DAT-575).
									const alignRight =
										cellAlign(header.column.columnDef.meta?.duckdbType) ===
										"right";
									return (
										<Table.Th
											key={header.id}
											onClick={clickable ? () => onToggleSort(name) : undefined}
											style={{
												...(clickable
													? { cursor: "pointer", userSelect: "none" }
													: {}),
												...(alignRight ? { textAlign: "right" } : {}),
											}}
											data-testid={`canvas-result-grid-header-${name}`}
										>
											<Group
												gap={4}
												wrap="nowrap"
												justify={alignRight ? "flex-end" : "flex-start"}
											>
												{flexRender(
													header.column.columnDef.header,
													header.getContext(),
												)}
												{active && (
													<Text
														span
														size="xs"
														c="dimmed"
														aria-label={
															sort.dir === "asc"
																? "sorted ascending"
																: "sorted descending"
														}
													>
														{sort.dir === "asc" ? "▲" : "▼"}
													</Text>
												)}
											</Group>
										</Table.Th>
									);
								})}
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
										{row.getVisibleCells().map((cell) => {
											const type = cell.column.columnDef.meta?.duckdbType;
											return (
												<Table.Td
													key={cell.id}
													style={
														cellAlign(type) === "right"
															? {
																	textAlign: "right",
																	fontVariantNumeric: "tabular-nums",
																}
															: undefined
													}
												>
													{formatCell(cell.getValue(), type)}
												</Table.Td>
											);
										})}
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

/**
 * A collapsible "SQL" disclosure for a grid (DAT-577): the literal query behind
 * the result, plus its bind params when present. Read-only — Analyse never edits
 * SQL (editing is a probe-only capability). Pure (local open-state only), so it's
 * unit-testable without the streaming grid. No SQL → nothing renders.
 */
export function GridSqlDisclosure({
	sql,
	params,
}: {
	sql: string;
	params?: (string | number | boolean | null)[];
}) {
	// Open-state is intentionally NOT reset on a new query: only StreamingGrid is
	// keyed (to reset sort). If the user opened the SQL and a new query streams in,
	// the disclosure stays open showing the new query's SQL — the prop updates.
	const [open, setOpen] = useState(false);
	if (!sql) return null;
	return (
		<div data-testid="canvas-result-grid-sql">
			<UnstyledButton
				onClick={() => setOpen((o) => !o)}
				aria-expanded={open}
				aria-label="Toggle SQL"
				mb={open ? "xs" : 0}
			>
				<Text size="xs" c="dimmed" fw={500}>
					{open ? "▾" : "▸"} SQL
				</Text>
			</UnstyledButton>
			<Collapse expanded={open}>
				<SqlBlock sql={sql} params={params} maxHeight={200} />
			</Collapse>
		</div>
	);
}

/**
 * The registered widget. Owns the BASE query (the agent's `run_sql` call) and
 * remounts the inner grid whenever that query changes, via a value-stable `key`.
 *
 * The remount is deliberate: the inner grid holds the grid-local sort state, and
 * remounting on a new base query resets the sort cleanly to "unsorted" without a
 * reset effect (which would fire a redundant second stream). The agent's
 * `state.sql`/`params` stay immutable — sort is a VIEW concern, never written
 * back to the canvas state.
 *
 * The SQL disclosure sits above the grid so it covers BOTH `run_sql` grids and
 * `answer` grids (AnswerResultWidget composes this widget with the composed
 * final SQL) in one place.
 */
export function ResultGridWidget({
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
	return (
		<div>
			<GridSqlDisclosure sql={state.sql} params={state.params} />
			<StreamingGrid
				key={baseKey}
				endpoint="/api/run-sql"
				body={{ sql: state.sql, params: state.params }}
			/>
		</div>
	);
}

/** The grid stream routes return a 400 body as `{ "error": "<message>" }`; surface
 * the message, not the raw JSON. Falls back to the raw text for any other body. */
function extractError(text: string): string {
	try {
		const parsed = JSON.parse(text) as { error?: unknown };
		if (parsed && typeof parsed.error === "string") return parsed.error;
	} catch {
		// not JSON — use the text as-is
	}
	return text;
}

/**
 * Streams an NDJSON grid endpoint for the carried request and owns the grid-local
 * sort. Endpoint-agnostic, so it backs BOTH `/api/run-sql` (the agent's lake query)
 * and `/api/probe-sql` (the editable probe surface) — only the `body` shape
 * differs; the grid injects its own `sort`. Reset sort by remounting on a new base
 * request (a `key` on the parent), never a reset effect.
 */
export function StreamingGrid({
	endpoint,
	body,
}: {
	endpoint: string;
	/** The base request body (WITHOUT `sort` — the grid appends its own). */
	body: Record<string, unknown>;
}) {
	const storeRef = useRef(new ColumnStore());
	const [, bump] = useState(0);
	const [fatal, setFatal] = useState<string | null>(null);
	// Grid-local view state: which column the SERVER should order by. Reset to
	// null on a new base request (this component remounts — see the `key`).
	const [sort, setSort] = useState<GridSort | null>(null);

	// Header click cycles the sort for that column: unsorted → asc → desc →
	// unsorted. Switching to a different column starts at asc. Stable identity
	// (setSort is stable) so a future React.memo on the view doesn't re-render on
	// every sort-irrelevant parent render.
	const toggleSort = useCallback((column: string) => {
		setSort((cur) => cycleSort(cur, column));
	}, []);

	// Value-stable request identity: re-stream iff the body OR sort changed. Parse
	// it back inside the effect so the effect's ONLY dependency is the key — no
	// stale closures, no churn from a fresh `body` object each parent render.
	const requestKey = useMemo(() => JSON.stringify([body, sort]), [body, sort]);
	useEffect(() => {
		const [qBody, qSort] = JSON.parse(requestKey) as [
			Record<string, unknown>,
			GridSort | null,
		];
		const store = new ColumnStore();
		storeRef.current = store;
		setFatal(null);
		bump((v) => v + 1);

		const ac = new AbortController();
		void (async () => {
			try {
				const res = await fetch(endpoint, {
					method: "POST",
					headers: { "Content-Type": "application/json" },
					body: JSON.stringify({ ...qBody, sort: qSort ?? undefined }),
					signal: ac.signal,
				});
				if (!res.ok || !res.body) {
					const detail = await res.text().catch(() => res.statusText);
					throw new Error(
						extractError(detail) || `request failed (${res.status})`,
					);
				}
				await readNdjsonStream(res.body, (frame) => {
					store.apply(frame);
					bump((v) => v + 1);
				});
			} catch (err) {
				// An aborted fetch (unmount / new request / new sort) is expected.
				if (ac.signal.aborted) return;
				setFatal(err instanceof Error ? err.message : String(err));
			}
		})();
		return () => ac.abort();
	}, [requestKey, endpoint]);

	return (
		<ResultGridView
			store={storeRef.current}
			fatal={fatal}
			sort={sort}
			onToggleSort={toggleSort}
		/>
	);
}

// Result-grid widget (DAT-385 grid + server-side sort + DAT-613 windowed
// paging) — the human-facing SQL result surface.
//
// Splits cleanly in four:
//   - ResultGridView: PURE render of a GridView via TanStack Table with
//     index-rows + accessorFn (no row-object rematerialization). Headers are
//     interactive when given `onToggleSort`; `onReachEnd` fires when the
//     virtualized body nears its end (the windowed grid pages on it). Trivially
//     testable, no I/O.
//   - ResultGridWidget: the registered entry. Owns the BASE query (the agent's
//     run_sql call) and `key`s the inner grid on it, so a new agent query
//     remounts the grid and resets the sort cleanly.
//   - WindowedGrid (lake, DAT-613): the human-facing lake grid. A Mosaic-style
//     window onto a re-runnable query — `useInfiniteQuery` fetches one
//     LIMIT/OFFSET page per scroll-window from `/api/run-sql`, each folded into
//     its own ColumnStore, assembled into a PagedGridView. No 50k cap: only the
//     loaded windows live in memory; the result set itself is unbounded.
//   - StreamingGrid (probe): the one-shot grid the editable probe uses. POSTs
//     sql+params+sort to an NDJSON endpoint and folds the WHOLE result into a
//     single ColumnStore (capped). Kept for `/api/probe-sql`, whose per-request
//     ATTACH can't yet back windowed paging (needs kept-alive sessions).
//
// Sort is SERVER-SIDE (re-issue with ORDER BY), not a client reorder: a window
// is only a slice, so the sort must run across the full result before the slice
// is cut to show the true order. The body IS virtualized (only the visible window
// hits the DOM) — load-bearing for an unbounded result, not optional.

import type { Json } from "@duckdb/node-api";
import {
	ActionIcon,
	Alert,
	Badge,
	Button,
	Group,
	Indicator,
	Modal,
	Table,
	TextInput,
} from "@mantine/core";
import { useInfiniteQuery } from "@tanstack/react-query";
import {
	type ColumnDef,
	flexRender,
	getCoreRowModel,
	type RowData,
	useReactTable,
} from "@tanstack/react-table";
import { useVirtualizer } from "@tanstack/react-virtual";
import {
	ChevronDown,
	ChevronsUpDown,
	ChevronUp,
	Code,
	Filter,
} from "lucide-react";
import {
	type ReactNode,
	useCallback,
	useEffect,
	useMemo,
	useRef,
	useState,
} from "react";
import { cellAlign, columnFilterKind, formatCell } from "#/duckdb/cell-format";
import {
	GRID_PAGE_SIZE,
	type GridFilter,
	type GridSort,
	parseColumnFilterInput,
} from "#/duckdb/grid-query";
import {
	ColumnStore,
	type GridStatus,
	type GridView,
	PagedGridView,
	readNdjsonIntoStore,
	readNdjsonStream,
} from "#/duckdb/ndjson-stream";
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

/** Pure presentation of a (possibly still-filling / still-paging) GridView.
 *
 * `sort` + `onToggleSort` make the column headers interactive (DAT-385): a
 * click asks the OWNER to re-issue the query with a new server-side sort. The
 * view itself never reorders rows — sort runs across the full result, server-
 * side. `onReachEnd` (DAT-613) fires when the virtualized body scrolls within an
 * overscan of the last loaded row, so the windowed owner can fetch the next page;
 * omit it (probe, pure-render test) and the grid never asks for more.
 * `onFilterCommit` (DAT-613) renders a per-column filter row whose inputs commit
 * (Enter/blur) a push-down filter to the owner; omit it and no filter row shows.
 * Omit `onToggleSort` and the headers stay static. */
export function ResultGridView({
	store,
	fatal,
	sort,
	onToggleSort,
	onReachEnd,
	onFilterCommit,
	activeFilterCount = 0,
	scrollResetKey,
	sql,
	sqlParams,
	toolbarActions,
	toolbarStart,
	fillHeight = false,
	onRowClick,
	onRowHover,
	footerRow,
	footerLabel = "Total",
	columnAccents,
	columnUnits,
}: {
	store: GridView;
	fatal?: string | null;
	sort?: GridSort | null;
	onToggleSort?: (column: string) => void;
	onReachEnd?: () => void;
	onFilterCommit?: (column: string, raw: string) => void;
	/** Row-click action (DAT-672: the drill's row-pin). When set, body rows read
	 * as clickable and deliver the row as a column→cell object. Rows are also
	 * keyboard-focusable then — Enter/Space fires the same action (DAT-712 a11y). */
	onRowClick?: (row: Record<string, Json | null>) => void;
	/** Row hover/focus tracking (DAT-712: the equation header's rebind). Fired
	 * with the row object on mouseenter/focus, with null when the pointer leaves
	 * the body. Purely observational — never changes grid behavior. */
	onRowHover?: (row: Record<string, Json | null> | null) => void;
	/** A pinned summary row rendered as a sticky footer under the body (DAT-712's
	 * total row): cells matched to columns BY NAME and formatted like body cells;
	 * the first column without a value carries `footerLabel`. */
	footerRow?: Record<string, Json | null>;
	footerLabel?: string;
	/** CSS color per column name, applied to the column's header text and cell
	 * values (DAT-712's ledger ink — the equation layer owns the assignment; the
	 * grid only renders it, so equation and columns can never disagree). */
	columnAccents?: Record<string, string>;
	/** Unit chip per column name, shown next to the header text (e.g. value → %). */
	columnUnits?: Record<string, string>;
	/** How many push-down filters are currently active (drives the funnel toggle's
	 * active state + count). Owner-tracked; the view only renders the row. */
	activeFilterCount?: number;
	/** Changes when the owner re-pages from offset 0 (sort/filter change); the body
	 * scrolls back to the top so the new top-N is visible, not a clamped middle. */
	scrollResetKey?: string;
	/** The query behind the grid — when present, the toolbar shows a "Show SQL"
	 * button opening a read-only modal. Omitted by the probe (it has its own
	 * editor), so no button there. */
	sql?: string;
	sqlParams?: (string | number | boolean | null)[];
	/** Extra toolbar actions rendered to the LEFT of "View SQL" — the answer surface
	 * mounts its mint-to-Report button here so it sits with the grid's own actions
	 * instead of floating above the grid. Omitted for plain run_sql / probe grids. */
	toolbarActions?: ReactNode;
	/** Rendered in the toolbar's LEFT slot (where the row count used to sit —
	 * it lives in the status pill now): the drill surface mounts its slice
	 * controls here (DAT-712 iteration 3). */
	toolbarStart?: ReactNode;
	/** Fill the parent's height instead of capping the body at 480px — the
	 * analyse modal sizes ITSELF and the grid body must be the modal's ONLY
	 * vertical scroller (double scrollbars otherwise). The parent chain must
	 * be a flex column with minHeight 0. */
	fillHeight?: boolean;
}) {
	// The filter row is hidden by default (a clean grid) and toggled by the funnel
	// in the toolbar. An applied-but-hidden filter isn't stranded: the funnel stays
	// active + shows the count, so the user can re-open to edit it.
	const [filtersOpen, setFiltersOpen] = useState(false);
	const [sqlOpen, setSqlOpen] = useState(false);
	const showFilterRow = onFilterCommit !== undefined && filtersOpen;
	// Index-rows: TanStack Table iterates row indices; each accessor reads its
	// column array at that index — O(1), no row objects ever built.
	//
	// `store` is a dep, not just `store.rowCount`: a windowed re-sort/filter SWAPS
	// the store for one with DIFFERENT values at the SAME row indices (e.g. row 0
	// goes min→max). TanStack caches `row.getValue()` per Row, and Rows are
	// memoized on this `data` array — so if its reference were stable across a
	// store swap, the table would keep serving the OLD store's cached cell values
	// even though the accessor now closes over the new store. Tying `data`'s
	// identity to `store` rebuilds the row model on a swap, busting that cache.
	// (The streaming/probe store mutates in place — stable ref — so this still
	// only refreshes on rowCount growth there.)
	const data = useMemo<number[]>(
		() => Array.from({ length: store.rowCount }, (_, i) => i),
		[store.rowCount, store],
	);
	const columns = useMemo<ColumnDef<number>[]>(() => {
		const typeList = Array.isArray(store.types) ? (store.types as Json[]) : [];
		return store.columns.map((name, c) => ({
			id: `c${c}`,
			header: name,
			// accessorFn closes over `store` by REFERENCE and reads the cell lazily at
			// render time (not at memo creation), so cells fill in as streamed batches
			// grow the store / new pages append — don't freeze or copy the store.
			accessorFn: (rowIndex: number) => store.cell(c, rowIndex),
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
	// no layout). Rows are uniform-height text, so a fixed estimate is fine — 	// per-row measureElement (add it if variable heights ever land).
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

	/** The row at a store index as a column→cell object — what row-level
	 *  callbacks (click-to-pin, hover-rebind) deliver. */
	const rowObject = (index: number): Record<string, Json | null> =>
		Object.fromEntries(
			store.columns.map((name, c) => [name, store.cell(c, index)]),
		);

	// The footer's label column: the first column the footer row has no value
	// for (computed once, not per cell).
	const footerLabelIndex = footerRow
		? store.columns.findIndex((n) => footerRow[n] === undefined)
		: -1;

	// Load-on-scroll (DAT-613): when the virtualized body reaches within an
	// overscan of the last loaded row, ask the owner for the next page. This is a
	// scroll-driven side effect (a DOM/measurement signal), so it lives in an
	// effect per React rule 2 — the third such effect in the cockpit, justified
	// here. `lastIndex` is a scalar, so the effect re-fires only when the visible
	// end actually moves; `onReachEnd` self-guards against re-fetching while a page
	// is already in flight, and content shorter than the viewport keeps it firing
	// until the window is filled or the result is exhausted.
	const lastIndex =
		virtualRows.length > 0 ? virtualRows[virtualRows.length - 1].index : -1;
	useEffect(() => {
		if (onReachEnd && lastIndex >= 0 && lastIndex >= rows.length - 1 - 8) {
			onReachEnd();
		}
	}, [onReachEnd, lastIndex, rows.length]);

	// Re-page from 0 (sort/filter change) → scroll the body back to the top, so a
	// deep-scrolled grid doesn't strand the user at a clamped offset of the new,
	// shorter result. DOM scroll sync → an effect (React rule 2).
	useEffect(() => {
		if (scrollResetKey !== undefined && scrollRef.current) {
			scrollRef.current.scrollTop = 0;
		}
	}, [scrollResetKey]);

	const status = fatal ? "error" : store.status;

	return (
		<div
			data-testid="canvas-result-grid"
			style={
				fillHeight
					? {
							display: "flex",
							flexDirection: "column",
							flex: 1,
							minHeight: 0,
						}
					: undefined
			}
		>
			<Group justify="space-between" mb="xs" align="flex-start">
				{/* The row count lives in the status pill (a finished grid IS its
				    row count) — the left slot hosts the owner's controls (the
				    drill's slice chips, DAT-712 iteration 3). */}
				<Group gap="xs" wrap="wrap" style={{ flex: 1, minWidth: 0 }}>
					{toolbarStart}
				</Group>
				<Group gap="xs">
					{toolbarActions}
					{sql && (
						<Button
							variant="subtle"
							color="gray"
							size="compact-xs"
							leftSection={<Code size={13} />}
							data-testid="canvas-result-grid-sql-toggle"
							onClick={() => setSqlOpen(true)}
						>
							View SQL
						</Button>
					)}
					{onFilterCommit && (
						<Indicator
							label={activeFilterCount}
							size={15}
							offset={2}
							color="blue"
							disabled={activeFilterCount === 0}
							aria-label={`${activeFilterCount} filters active`}
						>
							<ActionIcon
								variant={
									activeFilterCount > 0 || filtersOpen ? "light" : "subtle"
								}
								color={activeFilterCount > 0 ? "blue" : "gray"}
								size="sm"
								aria-label={filtersOpen ? "Hide filters" : "Show filters"}
								aria-pressed={filtersOpen}
								title={
									activeFilterCount > 0
										? `${activeFilterCount} filter${activeFilterCount === 1 ? "" : "s"} active`
										: "Filter rows"
								}
								data-testid="canvas-result-grid-filter-toggle"
								onClick={() => setFiltersOpen((o) => !o)}
							>
								<Filter size={14} />
							</ActionIcon>
						</Indicator>
					)}
					<Badge color={STATUS_COLOR[status]} variant="light" size="sm">
						{status === "done"
							? `${store.rowCount} row${store.rowCount === 1 ? "" : "s"}`
							: status}
					</Badge>
				</Group>
			</Group>

			{sql && (
				<Modal
					opened={sqlOpen}
					onClose={() => setSqlOpen(false)}
					title="SQL"
					size="lg"
					data-testid="canvas-result-grid-sql-modal"
				>
					<SqlBlock sql={sql} params={sqlParams} maxHeight={420} />
				</Modal>
			)}

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
					style={
						fillHeight
							? { flex: 1, minHeight: 0, overflow: "auto" }
							: { maxHeight: 480, overflow: "auto" }
					}
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
												<span
													style={
														columnAccents?.[name]
															? { color: columnAccents[name] }
															: undefined
													}
												>
													{flexRender(
														header.column.columnDef.header,
														header.getContext(),
													)}
												</span>
												{columnUnits?.[name] && (
													<Badge
														size="xs"
														variant="light"
														color="gray"
														data-testid={`canvas-result-grid-unit-${name}`}
													>
														{columnUnits[name]}
													</Badge>
												)}
												{active ? (
													sort.dir === "asc" ? (
														<ChevronUp
															size={14}
															color="var(--mantine-color-gray-7)"
															aria-label="sorted ascending"
														/>
													) : (
														<ChevronDown
															size={14}
															color="var(--mantine-color-gray-7)"
															aria-label="sorted descending"
														/>
													)
												) : (
													clickable && (
														// Neutral handle so EVERY sortable column reads as
														// clickable, not just the active one (DAT-613 review).
														<ChevronsUpDown
															size={14}
															color="var(--mantine-color-gray-5)"
															aria-hidden
														/>
													)
												)}
											</Group>
										</Table.Th>
									);
								})}
							</Table.Tr>
							{/* Filter row (DAT-613): one input per column, toggled by the
							    toolbar funnel. Text columns match a substring; numeric/
							    temporal accept a leading comparison operator (>1000,
							    >=2024-01-01). Uncontrolled — commit on Enter/blur so we
							    re-page once, not per keystroke. */}
							{showFilterRow && (
								<Table.Tr>
									{table.getFlatHeaders().map((header) => {
										const name = String(header.column.columnDef.header ?? "");
										const kind = columnFilterKind(
											header.column.columnDef.meta?.duckdbType,
										);
										return (
											<Table.Th
												key={`${header.id}-filter`}
												style={{
													padding: "4px 6px 8px",
													borderBottom:
														"2px solid var(--mantine-color-default-border)",
												}}
											>
												<TextInput
													size="xs"
													variant="default"
													styles={{
														input: {
															height: 24,
															minHeight: 24,
															fontWeight: 400,
														},
													}}
													placeholder={
														kind === "text"
															? "contains…"
															: kind === "temporal"
																? ">2024-01-01"
																: ">100"
													}
													aria-label={`Filter ${name}`}
													data-testid={`canvas-result-grid-filter-${name}`}
													onKeyDown={(e) => {
														if (e.key === "Enter")
															onFilterCommit(name, e.currentTarget.value);
													}}
													onBlur={(e) =>
														onFilterCommit(name, e.currentTarget.value)
													}
												/>
											</Table.Th>
										);
									})}
								</Table.Tr>
							)}
						</Table.Thead>
						<Table.Tbody
							onMouseLeave={onRowHover ? () => onRowHover(null) : undefined}
							// The keyboard counterpart of mouseleave: focus moving OUT of
							// the body releases the binding (a focused row has no leave
							// event, and virtualization can unmount it silently).
							onBlur={
								onRowHover
									? (e) => {
											if (
												!(e.relatedTarget instanceof Node) ||
												!e.currentTarget.contains(e.relatedTarget)
											) {
												onRowHover(null);
											}
										}
									: undefined
							}
						>
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
									<Table.Tr
										key={row.id}
										onClick={
											onRowClick
												? () => {
														// Selecting/copying cell text must not fire the
														// row action — a click that ends a selection is
														// a copy gesture, not a pin.
														if (window.getSelection()?.toString()) return;
														onRowClick(rowObject(vr.index));
													}
												: undefined
										}
										// Clickable rows are keyboard rows: focus rebinds (same
										// signal as hover), Enter/Space pins (DAT-712 a11y).
										tabIndex={onRowClick ? 0 : undefined}
										onKeyDown={
											onRowClick
												? (e) => {
														if (e.key !== "Enter" && e.key !== " ") return;
														if (e.target !== e.currentTarget) return;
														e.preventDefault();
														onRowClick(rowObject(vr.index));
													}
												: undefined
										}
										onMouseEnter={
											onRowHover
												? () => onRowHover(rowObject(vr.index))
												: undefined
										}
										onFocus={
											onRowHover
												? () => onRowHover(rowObject(vr.index))
												: undefined
										}
										style={onRowClick ? { cursor: "pointer" } : undefined}
										title={onRowClick ? "Pin this row's values" : undefined}
									>
										{row.getVisibleCells().map((cell) => {
											const type = cell.column.columnDef.meta?.duckdbType;
											const accent =
												columnAccents?.[
													String(cell.column.columnDef.header ?? "")
												];
											return (
												<Table.Td
													key={cell.id}
													style={{
														...(cellAlign(type) === "right"
															? {
																	textAlign: "right",
																	fontVariantNumeric: "tabular-nums",
																}
															: {}),
														...(accent ? { color: accent } : {}),
													}}
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
						{footerRow && (
							<Table.Tfoot
								style={{
									position: "sticky",
									bottom: 0,
									zIndex: 1,
									backgroundColor: "var(--mantine-color-body)",
								}}
								data-testid="canvas-result-grid-footer"
							>
								<Table.Tr style={{ fontWeight: 600 }}>
									{store.columns.map((name, c) => {
										const typeList = Array.isArray(store.types)
											? (store.types as Json[])
											: [];
										const type = typeList[c];
										const value = footerRow[name];
										// The first column without a total carries the label —
										// under a slice that's the dimension column.
										const label = c === footerLabelIndex;
										const accent = columnAccents?.[name];
										return (
											<Table.Td
												key={name}
												style={{
													borderTop:
														"2px solid var(--mantine-color-default-border)",
													...(cellAlign(type) === "right"
														? {
																textAlign: "right",
																fontVariantNumeric: "tabular-nums",
															}
														: {}),
													...(accent ? { color: accent } : {}),
												}}
											>
												{label
													? footerLabel
													: value === undefined
														? ""
														: formatCell(value, type)}
											</Table.Td>
										);
									})}
								</Table.Tr>
							</Table.Tfoot>
						)}
					</Table>
				</div>
			)}
		</div>
	);
}

/**
 * The registered widget. Owns the BASE query (the agent's `run_sql` call) and
 * remounts the inner grid whenever that query changes, via a value-stable `key`.
 *
 * The remount is deliberate: the inner grid holds the grid-local sort + filter
 * state, and remounting on a new base query resets them cleanly without a reset
 * effect (which would fire a redundant second stream). The agent's
 * `state.sql`/`params` stay immutable — sort/filter are VIEW concerns, never
 * written back to the canvas state.
 *
 * The query itself is reachable from the grid's toolbar ("Show SQL" → modal),
 * covering BOTH `run_sql` grids and `answer` grids (AnswerResultWidget composes
 * this widget with the composed final SQL) in one place.
 */
export function ResultGridWidget({
	state,
	toolbarActions,
}: {
	state: Extract<CanvasState, { kind: "result-grid" }>;
	/** Forwarded to the grid toolbar (left of "View SQL") — see ResultGridView. */
	toolbarActions?: ReactNode;
}) {
	// The provider derives a fresh canvas object on every message tick; serialize
	// sql+params so a new `key` is produced only when the QUERY actually changes,
	// not on per-tick object churn.
	const baseKey = useMemo(
		() => JSON.stringify([state.sql, state.params ?? null]),
		[state.sql, state.params],
	);
	return (
		<WindowedGrid
			key={baseKey}
			endpoint="/api/run-sql"
			body={{ sql: state.sql, params: state.params }}
			sql={state.sql}
			sqlParams={state.params}
			toolbarActions={toolbarActions}
		/>
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
 * The windowed lake grid (DAT-613). A Mosaic-style window onto a re-runnable
 * query: `useInfiniteQuery` fetches one LIMIT/OFFSET page per scroll-window from
 * the NDJSON endpoint, folds each into its own ColumnStore, and assembles the
 * loaded pages into a PagedGridView the view renders. The 50k cap is gone — only
 * the windows scrolled into ever live in memory, so the result set is unbounded.
 *
 * Paging goes through TanStack Query (`useInfiniteQuery`), not a hand-rolled
 * effect (React rule 3): the query key carries the body + sort + filters, so a
 * sort or filter change transparently re-pages from offset 0, and Query owns
 * fetch dedup, cancellation of superseded windows, and the loading state. Sort +
 * filters are grid-local and reset by remounting on a new base query
 * (ResultGridWidget's `key`).
 */
export function WindowedGrid({
	endpoint,
	body,
	sql,
	sqlParams,
	toolbarActions,
	toolbarStart,
	fillHeight,
	onRowClick,
	onRowHover,
	footerRow,
	footerLabel,
	columnAccents,
	columnUnits,
}: {
	endpoint: string;
	/** The base request body (WITHOUT sort/filters/limit/offset — the grid appends those). */
	body: Record<string, unknown>;
	/** The query behind the grid, surfaced via the toolbar "Show SQL" modal. */
	sql?: string;
	sqlParams?: (string | number | boolean | null)[];
	/** Forwarded to the grid toolbar (left of "View SQL") — see ResultGridView. */
	toolbarActions?: ReactNode;
	/** Forwarded toolbar LEFT slot — see ResultGridView (DAT-712). */
	toolbarStart?: ReactNode;
	/** Forwarded fill-the-parent sizing — see ResultGridView (DAT-712). */
	fillHeight?: boolean;
	/** Forwarded row-click action — see ResultGridView. */
	onRowClick?: (row: Record<string, Json | null>) => void;
	/** Forwarded row hover/focus tracking — see ResultGridView (DAT-712). */
	onRowHover?: (row: Record<string, Json | null> | null) => void;
	/** Forwarded sticky total row — see ResultGridView (DAT-712). */
	footerRow?: Record<string, Json | null>;
	footerLabel?: string;
	/** Forwarded per-column accents / unit chips — see ResultGridView (DAT-712). */
	columnAccents?: Record<string, string>;
	columnUnits?: Record<string, string>;
}) {
	const [sort, setSort] = useState<GridSort | null>(null);
	const [filters, setFilters] = useState<GridFilter[]>([]);
	const toggleSort = useCallback((column: string) => {
		setSort((cur) => cycleSort(cur, column));
	}, []);

	// Value-stable body identity so the query key doesn't churn on a fresh `body`
	// object each parent render; parsed back inside the queryFn.
	const bodyKey = useMemo(() => JSON.stringify(body), [body]);

	const query = useInfiniteQuery({
		queryKey: ["run-sql-grid", endpoint, bodyKey, sort, filters],
		initialPageParam: 0,
		queryFn: async ({ pageParam, signal }) => {
			const base = JSON.parse(bodyKey) as Record<string, unknown>;
			const res = await fetch(endpoint, {
				method: "POST",
				headers: { "Content-Type": "application/json" },
				body: JSON.stringify({
					...base,
					sort: sort ?? undefined,
					filters: filters.length ? filters : undefined,
					limit: GRID_PAGE_SIZE,
					offset: pageParam,
				}),
				signal,
			});
			if (!res.ok || !res.body) {
				const detail = await res.text().catch(() => res.statusText);
				throw new Error(
					extractError(detail) || `request failed (${res.status})`,
				);
			}
			// Each window is bounded (≤ GRID_PAGE_SIZE rows), so folding the whole
			// page into one ColumnStore is fine — unboundedness is handled by paging,
			// not by streaming one giant result.
			return readNdjsonIntoStore(res.body);
		},
		// A full window (truncated = the route's +1 over-fetch saw more rows) means
		// there's a next page; its offset = pages-so-far × page size.
		getNextPageParam: (lastPage, allPages) =>
			lastPage.truncated ? allPages.length * GRID_PAGE_SIZE : undefined,
	});

	const { data, error, isFetchingNextPage, fetchNextPage, isFetching } = query;

	// `fetchNextPage` is stable (TanStack Query) and internally no-ops when there's
	// no next page or one is already in flight — so the callback needs no volatile
	// deps. Keeping its identity stable means the view's scroll effect re-fires only
	// on actual scroll, not on every paging-state flip. The view gates WHEN to call
	// this (only within an overscan of the loaded end), which also bounds eager
	// auto-paging to filling the viewport.
	const onReachEnd = useCallback(() => {
		void fetchNextPage();
	}, [fetchNextPage]);

	const view = useMemo<PagedGridView>(() => {
		const pages = data?.pages ?? [];
		// An in-band footer error on any page (e.g. a DuckDB binder error) is fatal
		// for the whole grid, same as a failed fetch.
		const inbandError = pages.find((p) => p.error)?.error;
		const message = error
			? error instanceof Error
				? error.message
				: String(error)
			: inbandError;
		const status: GridStatus = message
			? "error"
			: isFetching || isFetchingNextPage
				? "streaming"
				: "done";
		return new PagedGridView(pages, GRID_PAGE_SIZE, status, message);
	}, [data, error, isFetching, isFetchingNextPage]);

	// Map each output column to its DuckDB type so a filter input knows whether to
	// parse comparisons (numeric/temporal) or a substring (text). Types arrive
	// with page 0, before the user can read a column to filter it.
	const typeByColumn = useMemo(() => {
		const map = new Map<string, Json | undefined>();
		const types = Array.isArray(view.types) ? (view.types as Json[]) : [];
		view.columns.forEach((name, i) => {
			map.set(name, types[i]);
		});
		return map;
	}, [view]);

	const onFilterCommit = useCallback(
		(column: string, raw: string) => {
			const kind = columnFilterKind(typeByColumn.get(column));
			const next = parseColumnFilterInput(column, raw, kind);
			setFilters((prev) => {
				const existing = prev.find((f) => f.column === column);
				// No-op commits (blurring an empty input, re-entering the same value)
				// must NOT produce a new array — that would needlessly re-page.
				if (!next)
					return existing ? prev.filter((f) => f.column !== column) : prev;
				if (
					existing &&
					existing.op === next.op &&
					existing.value === next.value
				)
					return prev;
				return [...prev.filter((f) => f.column !== column), next];
			});
		},
		[typeByColumn],
	);

	// PagedGridView carries status + error, so the view renders the badge and the
	// error banner straight off `store` — no separate `fatal` needed here.
	return (
		<ResultGridView
			store={view}
			sort={sort}
			onToggleSort={toggleSort}
			onReachEnd={onReachEnd}
			onFilterCommit={onFilterCommit}
			activeFilterCount={filters.length}
			scrollResetKey={JSON.stringify([sort, filters])}
			sql={sql}
			sqlParams={sqlParams}
			toolbarActions={toolbarActions}
			toolbarStart={toolbarStart}
			fillHeight={fillHeight}
			onRowClick={onRowClick}
			onRowHover={onRowHover}
			footerRow={footerRow}
			footerLabel={footerLabel}
			columnAccents={columnAccents}
			columnUnits={columnUnits}
		/>
	);
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
	// it back inside the effect so the effect's ONLY dependency is the key — 	// stale closures, no churn from a fresh `body` object each parent render.
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

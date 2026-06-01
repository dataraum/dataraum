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
// P2 scope: read-only. Virtualization + server-side sort/filter are P3.

import { Alert, Badge, Group, Table, Text } from "@mantine/core";
import {
	type ColumnDef,
	flexRender,
	getCoreRowModel,
	useReactTable,
} from "@tanstack/react-table";
import { useEffect, useMemo, useRef, useState } from "react";
import { ColumnStore, readNdjsonStream } from "#/duckdb/ndjson-stream";
import type { CanvasState } from "#/ui/cockpit/canvas-state";

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
	const columns = useMemo<ColumnDef<number>[]>(
		() =>
			store.columns.map((name, c) => ({
				id: `c${c}`,
				header: name,
				accessorFn: (rowIndex: number) => store.cols[c]?.[rowIndex] ?? null,
			})),
		[store.columns, store],
	);
	const table = useReactTable({
		data,
		columns,
		getCoreRowModel: getCoreRowModel(),
	});

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

			{store.columns.length > 0 && (
				<Table.ScrollContainer minWidth={320}>
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
							{table.getRowModel().rows.map((row) => (
								<Table.Tr key={row.id}>
									{row.getVisibleCells().map((cell) => (
										<Table.Td key={cell.id}>
											{formatCell(cell.getValue())}
										</Table.Td>
									))}
								</Table.Tr>
							))}
						</Table.Tbody>
					</Table>
				</Table.ScrollContainer>
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

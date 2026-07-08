// DrillableGrid (DAT-672, per-node re-cut DAT-703, analyse re-cut DAT-712):
// the shared result grid with a drill layer on top.
//
// Owns the drill-step stack and the EFFECTIVE query. Drill composes upstream
// of the grid, server-side and binder-validated, into a new effective base
// SQL + params; the ordinary `WindowedGrid` renders it — remounting on the
// effective key so grid-local sort/filters reset exactly as on a new agent
// query (React rule 5). TWO compose paths, chosen by `nodeRef`:
//   - a canvas NODE (metric or measure) recomposes from its persisted clause
//     parts with the steps as clause appends (`/api/drill/node`);
//   - an ad-hoc grid wraps its own visible columns (`/api/drill/compose`,
//     tier A only).
// The stack only ever holds compositions the server ACCEPTED: a candidate
// stack is sent as a user-event mutation and committed on `ok: true`; a
// refusal shows the amber "can't slice this deterministically" state and
// leaves the grid on the last good drill.
//
// TIME GRAIN (DAT-712, node path only): a temporal axis (axis.temporal from
// the catalog's column types) slices at MONTH grain by default — raw day rows
// of a year of bookings answer nothing — and its chip carries the grain
// control: resolution-appropriate presets plus a typed token (`15m`, `2h`,
// `3M`; grain.ts's closed grammar, validated HERE before it ever reaches the
// server, which re-validates). Pins freeze the grain they were created under
// (pin ≡ the row it came from), so re-graining a slice never re-scopes an
// existing pin.
//
// This widget stays GENERIC (the lead's DAT-712 layering constraint): the
// equation header is a parts-context layer ABOVE it. The widget only exposes
// observational hooks — row hover/focus, the committed pin row, step
// changes — plus pass-through rendering props (footer cells, column accents,
// unit chips) whose CONTENT the layer owns.
//
// Axes come from the metric path (`/api/drill/axes`, catalog metadata only —
// DAT-678 adds ad-hoc resolution). This widget fetches the drill routes
// instead of importing server modules (bundle hygiene).

// Type-only, erased at compile time — the same source result-grid.tsx uses.
import type { Json } from "@duckdb/node-api";
import {
	Alert,
	Badge,
	Button,
	Group,
	Menu,
	Pill,
	Text,
	TextInput,
	Tooltip,
} from "@mantine/core";
import { useMutation, useQuery } from "@tanstack/react-query";
import { Check, ChevronDown, Layers, X } from "lucide-react";
import { useMemo, useRef, useState } from "react";
import type { ChartConfig } from "#/charts/chart-config";
import type {
	DrillAxesRequest,
	DrillAxis,
	DrillPinValue,
	DrillStep,
} from "#/duckdb/drill";

import { grainLabel, grainPresets, parseGrainToken } from "#/duckdb/grain";
import { ChartToolbarButton } from "#/ui/cockpit/widgets/chart-toolbar-button";
import { WindowedGrid } from "#/ui/cockpit/widgets/result-grid";

type SqlParams = (string | number | boolean | null)[];

type ComposeResponse =
	| { ok: true; sql: string; params: SqlParams }
	| { ok: false; reason: string };

async function postJson<T>(url: string, body: unknown): Promise<T> {
	const res = await fetch(url, {
		method: "POST",
		headers: { "Content-Type": "application/json" },
		body: JSON.stringify(body),
	});
	if (!res.ok) {
		const detail = (await res.json().catch(() => null)) as {
			error?: unknown;
		} | null;
		throw new Error(
			typeof detail?.error === "string"
				? detail.error
				: `request failed (${res.status})`,
		);
	}
	return (await res.json()) as T;
}

/** Chart state scoped to ONE effective query — mounted with `key=effective`
 *  so drilling resets the authored chart along with sort/filters (rule 5). */
function DrillChartAction({ sql, params }: { sql: string; params: SqlParams }) {
	const [config, setConfig] = useState<ChartConfig | null>(null);
	return (
		<ChartToolbarButton
			sql={sql}
			params={params}
			value={config}
			onChange={setConfig}
		/>
	);
}

const pinLabel = (value: DrillPinValue): string =>
	value === null ? "∅" : String(value);

/** The human name of a slice/pin's grain token — "" for a raw (ungrained)
 *  step or an off-grammar token (which the composer refuses anyway). */
const grainName = (token: string | undefined): string => {
	if (token === undefined) return "";
	const grain = parseGrainToken(token);
	return grain ? grainLabel(grain) : token;
};

/** A clicked grid cell narrowed to a bindable pin value — `undefined` for a
 *  non-scalar cell (nested json), which cannot be pinned and is skipped rather
 *  than silently pinned as NULL. */
const toPinValue = (v: unknown): DrillPinValue | undefined =>
	v === null ||
	typeof v === "string" ||
	typeof v === "number" ||
	typeof v === "boolean"
		? v
		: undefined;

/** The default grain for a fresh temporal slice: month. Raw day rows are the
 *  DAT-703 verdict's "needs far too much domain knowledge" — the evidence
 *  (DAT-673): COGS books 191/365 days, so day grain shows mostly dashes while
 *  every MONTH bucket has both flows. "Exact values" stays one click away. */
const DEFAULT_TEMPORAL_GRAIN = "1M";

/** The grain chip's dropdown: resolution-appropriate presets, exact values,
 *  a typed token (validated locally with the same closed grammar the
 *  composer trusts), and the slice's removal. */
function GrainMenu({
	axis,
	grain,
	onGrain,
	onRemove,
}: {
	axis: DrillAxis;
	grain: string | undefined;
	onGrain: (token: string | undefined) => void;
	onRemove: () => void;
}) {
	const [custom, setCustom] = useState("");
	const [customError, setCustomError] = useState<string | null>(null);
	const presets = grainPresets(axis.temporal ?? "date");

	const commitCustom = () => {
		const token = custom.trim();
		if (token === "") return;
		const grain = parseGrainToken(token);
		if (!grain) {
			// The named refusal, client-side: same grammar, same message shape.
			setCustomError("Not a grain — try 1d, 1w, 1M (m = minutes, M = months)");
			return;
		}
		// The same restriction the presets encode: a DATE column has no hours
		// to bucket — DuckDB's (INTERVAL, DATE) time_bucket would floor
		// non-divisor sub-day widths to the PREVIOUS day and no-op divisors,
		// both silently mislabeled. Refuse by name instead.
		if (
			axis.temporal === "date" &&
			(grain.unit === "s" || grain.unit === "m" || grain.unit === "h")
		) {
			setCustomError("This column has day resolution — use 1d or coarser");
			return;
		}
		setCustomError(null);
		setCustom("");
		onGrain(token);
	};

	return (
		<Menu shadow="md" width={240} position="bottom-start">
			<Menu.Target>
				<Button
					variant="light"
					size="compact-xs"
					rightSection={<ChevronDown size={12} />}
					data-testid={`drill-step-slice-${axis.column}`}
				>
					by {axis.column}
					{grain !== undefined ? ` · ${grainName(grain)}` : ""}
				</Button>
			</Menu.Target>
			<Menu.Dropdown>
				<Menu.Label>Time grain</Menu.Label>
				{presets.map((p) => (
					<Menu.Item
						key={p.token}
						onClick={() => onGrain(p.token)}
						rightSection={grain === p.token ? <Check size={13} /> : undefined}
						data-testid={`drill-grain-${axis.column}-${p.token}`}
					>
						<Group gap={6} wrap="nowrap">
							<Text size="sm">{p.label}</Text>
							<Text size="xs" c="dimmed">
								{p.token}
							</Text>
						</Group>
					</Menu.Item>
				))}
				<Menu.Item
					onClick={() => onGrain(undefined)}
					rightSection={grain === undefined ? <Check size={13} /> : undefined}
					data-testid={`drill-grain-${axis.column}-raw`}
				>
					<Text size="sm">Exact values</Text>
				</Menu.Item>
				<Menu.Divider />
				{/* The typed-token power path: any grammar token composes (15m, 2h,
				    3M), not just the presets. */}
				<div style={{ padding: "4px 12px 8px" }}>
					<TextInput
						size="xs"
						placeholder="Custom: 15m, 2h, 3M…"
						value={custom}
						error={customError}
						onChange={(e) => {
							setCustom(e.currentTarget.value);
							if (customError) setCustomError(null);
						}}
						onKeyDown={(e) => {
							if (e.key === "Enter") {
								e.preventDefault();
								commitCustom();
							}
						}}
						data-testid={`drill-grain-${axis.column}-custom`}
					/>
				</div>
				<Menu.Divider />
				<Menu.Item
					color="red"
					leftSection={<X size={13} />}
					onClick={onRemove}
					data-testid={`drill-step-slice-${axis.column}-remove`}
				>
					Remove slice
				</Menu.Item>
			</Menu.Dropdown>
		</Menu>
	);
}

export function DrillableGrid({
	sql,
	params,
	axesRequest,
	nodeRef,
	footerCells,
	footerLabel,
	columnAccents,
	columnUnits,
	onRowHover,
	onPinnedRow,
	onStepsChange,
}: {
	/** The base query (the node's composed SQL on the canvas path). */
	sql: string;
	params?: SqlParams;
	/** What the Slice control offers — resolved by the metric path in P1. */
	axesRequest: DrillAxesRequest;
	/** Present on the canvas path: drill steps recompose the NODE from its
	 *  persisted parts (`/api/drill/node`) instead of wrapping the base SQL. */
	nodeRef?: DrillAxesRequest;
	/** Total-row cells (column name → value), shown as the grid's sticky footer
	 *  WHILE a drill is active — the anchor a slice would otherwise lose. The
	 *  layer above owns the values (DAT-712). */
	footerCells?: Record<string, Json | null>;
	footerLabel?: string;
	/** Pass-through rendering props — see ResultGridView (DAT-712). */
	columnAccents?: Record<string, string>;
	columnUnits?: Record<string, string>;
	/** Row hover/focus observation, forwarded from the grid (DAT-712). */
	onRowHover?: (row: Record<string, Json | null> | null) => void;
	/** Fired when an apply COMMITS: the clicked row when that apply pinned it,
	 *  null otherwise (pins cleared / slice-only change) — the equation
	 *  layer's lock-on-pin signal (DAT-712). */
	onPinnedRow?: (row: Record<string, Json | null> | null) => void;
	/** Fired with the committed step stack after every accepted apply. */
	onStepsChange?: (steps: DrillStep[]) => void;
}) {
	const baseParams = useMemo<SqlParams>(() => params ?? [], [params]);
	// The committed stack + its server-composed statement move TOGETHER: steps
	// are non-empty iff `composed` holds their accepted composition.
	const [steps, setSteps] = useState<DrillStep[]>([]);
	const [composed, setComposed] = useState<{
		sql: string;
		params: SqlParams;
	} | null>(null);
	const [refusal, setRefusal] = useState<string | null>(null);

	const axesQuery = useQuery({
		queryKey: ["drill-axes", axesRequest],
		queryFn: () =>
			postJson<{ axes: DrillAxis[]; reason?: string }>(
				"/api/drill/axes",
				axesRequest,
			),
		staleTime: 60_000,
	});
	const axes = axesQuery.data?.axes ?? [];
	const axisByColumn = useMemo(
		() => new Map(axes.map((a) => [a.column, a])),
		[axes],
	);

	// Monotonic apply generation, bumped in the EVENT HANDLER so it carries
	// click order. TanStack Query neither serializes nor cancels overlapping
	// `.mutate()` calls — their callbacks fire in network-resolution order — so
	// without this guard two quick applies (row-pin then pill-remove, say)
	// could commit the OLDER composition last and leave the grid on a state
	// that doesn't match the user's latest action. Ref, not state: read/written
	// only in handlers/callbacks (rule 8's render restriction doesn't apply).
	const generationRef = useRef(0);

	// Applying a step stack is a user event → a mutation (rule 4). A refusal is
	// a DOMAIN result (HTTP 200): surface it and keep the last accepted drill.
	const compose = useMutation({
		mutationFn: async ({
			candidate,
			generation,
			pinRow,
		}: {
			candidate: DrillStep[];
			generation: number;
			/** The grid row this apply pinned (row-click applies only). */
			pinRow?: Record<string, Json | null>;
		}) => ({
			candidate,
			generation,
			pinRow,
			result: nodeRef
				? await postJson<ComposeResponse>("/api/drill/node", {
						...nodeRef,
						steps: candidate,
					})
				: await postJson<ComposeResponse>("/api/drill/compose", {
						sql,
						params: baseParams,
						steps: candidate,
					}),
		}),
		onSuccess: ({ candidate, generation, pinRow, result }) => {
			if (generation !== generationRef.current) return; // superseded — drop
			if (result.ok) {
				const prevPinCount = steps.filter((s) => s.kind === "pin").length;
				const nextPinCount = candidate.filter((s) => s.kind === "pin").length;
				setSteps(candidate);
				setComposed({ sql: result.sql, params: result.params });
				setRefusal(null);
				onStepsChange?.(candidate);
				// The grid remounts on the new composition — a hover observed
				// under the OLD one must not outlive it (it would shadow the
				// lock/totals binding; mouse flows only self-heal by DOM-layout
				// accident, and a keyboard focus row has no leave event at all).
				onRowHover?.(null);
				// The lock follows the PINS, not the apply: a row-click pin sets
				// it; a slice-only change (re-grain, extra slice) leaves it
				// standing — the pins' restriction didn't move; but any SHRINK of
				// the pin set releases it, because the locked row was captured
				// under a filter the chips no longer represent.
				if (pinRow !== undefined) {
					onPinnedRow?.(pinRow);
				} else if (nextPinCount < prevPinCount) {
					onPinnedRow?.(null);
				}
			} else {
				setRefusal(result.reason);
			}
		},
		onError: (err, { generation }) => {
			if (generation !== generationRef.current) return; // superseded — drop
			setRefusal(err instanceof Error ? err.message : String(err));
		},
	});

	const apply = (
		candidate: DrillStep[],
		pinRow?: Record<string, Json | null>,
	) => {
		const generation = ++generationRef.current;
		if (candidate.length === 0) {
			// Clearing is synchronous — the bump above also invalidates any
			// still-in-flight compose so it can't resurrect the cleared drill.
			setSteps([]);
			setComposed(null);
			setRefusal(null);
			onStepsChange?.([]);
			onRowHover?.(null);
			onPinnedRow?.(null);
			return;
		}
		compose.mutate({ candidate, generation, pinRow });
	};

	const effective =
		steps.length > 0 && composed ? composed : { sql, params: baseParams };
	const effectiveKey = useMemo(
		() => JSON.stringify([effective.sql, effective.params]),
		[effective.sql, effective.params],
	);

	const activeSlices = steps.filter((s) => s.kind === "slice");
	const slicedColumns = new Set(activeSlices.map((s) => s.column));

	// Pin the clicked grouped row: one pin per active slice dimension, from the
	// row's cell values — each pin FREEZES its slice's current grain (pin ≡ the
	// bucket row that was clicked). Only offered while sliced (a detail row has
	// no group identity to pin).
	const onRowClick =
		activeSlices.length > 0
			? (row: Record<string, unknown>) => {
					const pins: DrillStep[] = [];
					for (const s of activeSlices) {
						const value = toPinValue(row[s.column]);
						if (value === undefined) continue; // non-scalar cell — not pinnable
						const duplicate = steps.some(
							(p) =>
								p.kind === "pin" && p.column === s.column && p.value === value,
						);
						if (!duplicate) {
							pins.push(
								s.kind === "slice" && s.grain !== undefined
									? { kind: "pin", column: s.column, value, grain: s.grain }
									: { kind: "pin", column: s.column, value },
							);
						}
					}
					if (pins.length > 0) {
						apply([...steps, ...pins], row as Record<string, Json | null>);
					}
				}
			: undefined;

	// Grain is a NODE-path capability: composeNodeQuery buckets it; the tier-A
	// route rejects grained steps outright (strict zod). Without a nodeRef the
	// temporal axis slices raw and no grain control renders.
	const grainable = nodeRef !== undefined;

	/** Slice a fresh axis — temporal axes start at the default grain. */
	const slice = (axis: DrillAxis) => {
		apply([
			...steps,
			grainable && axis.temporal !== null
				? {
						kind: "slice",
						column: axis.column,
						grain: DEFAULT_TEMPORAL_GRAIN,
					}
				: { kind: "slice", column: axis.column },
		]);
	};

	/** Re-grain an active temporal slice in place (pins keep their own). */
	const regrain = (column: string, grain: string | undefined) => {
		apply(
			steps.map((s) =>
				s.kind === "slice" && s.column === column
					? grain === undefined
						? { kind: "slice" as const, column: s.column }
						: { kind: "slice" as const, column: s.column, grain }
					: s,
			),
		);
	};

	// The drill controls live in the GRID's toolbar-left slot (where the row
	// count used to sit — iteration 3), not on their own row above it.
	const drillControls = (
		<>
			<Menu shadow="md" width={280} position="bottom-start">
				<Menu.Target>
					<Button
						variant="light"
						size="compact-xs"
						leftSection={<Layers size={13} />}
						loading={compose.isPending}
						disabled={axesQuery.isPending || axes.length === 0}
						data-testid="drill-slice-button"
					>
						Slice
					</Button>
				</Menu.Target>
				<Menu.Dropdown>
					{axes.map((axis) => (
						<Menu.Item
							key={axis.column}
							disabled={slicedColumns.has(axis.column)}
							onClick={() => slice(axis)}
							rightSection={
								grainable && axis.temporal !== null ? (
									<Text size="xs" c="dimmed">
										{grainName(DEFAULT_TEMPORAL_GRAIN)}
									</Text>
								) : axis.valueCount !== null ? (
									<Text size="xs" c="dimmed">
										{axis.valueCount}
									</Text>
								) : undefined
							}
						>
							<Text size="sm">{axis.column}</Text>
							{axis.businessContext && (
								<Text size="xs" c="dimmed" lineClamp={1}>
									{axis.businessContext}
								</Text>
							)}
						</Menu.Item>
					))}
				</Menu.Dropdown>
			</Menu>
			{axes.length === 0 && !axesQuery.isPending && (
				// The resolver names WHY it came back empty (stale snippet, bare
				// catalog, no extracts) — a dead-end badge with no reason reads
				// as a bug (2026-07-06 review).
				<Tooltip
					label={
						axesQuery.data?.reason ??
						"No cataloged dimensions for this metric's facts"
					}
					maw={360}
					multiline
				>
					<Badge color="gray" variant="light" size="sm">
						no axes
					</Badge>
				</Tooltip>
			)}
			{steps.map((step, i) => {
				const axis =
					step.kind === "slice" ? axisByColumn.get(step.column) : undefined;
				if (
					grainable &&
					step.kind === "slice" &&
					axis &&
					axis.temporal !== null
				) {
					// A temporal slice's chip IS the grain control.
					return (
						<GrainMenu
							key={`slice:${step.column}`}
							axis={axis}
							grain={step.grain}
							onGrain={(token) => regrain(step.column, token)}
							onRemove={() => apply(steps.filter((_, j) => j !== i))}
						/>
					);
				}
				return (
					<Pill
						// Value-identity key: slices are unique per column (menu disables
						// re-slicing) and pins per column+value (row-click dedupes).
						key={
							step.kind === "slice"
								? `slice:${step.column}`
								: `pin:${step.column}:${pinLabel(step.value)}`
						}
						withRemoveButton
						onRemove={() => apply(steps.filter((_, j) => j !== i))}
						data-testid={`drill-step-${step.kind}-${step.column}`}
					>
						{step.kind === "slice"
							? `by ${step.column}`
							: `${step.column} = ${pinLabel(step.value)}${
									step.grain !== undefined ? ` · ${grainName(step.grain)}` : ""
								}`}
					</Pill>
				);
			})}
			{steps.length > 0 && (
				<Button
					variant="subtle"
					color="gray"
					size="compact-xs"
					onClick={() => apply([])}
					data-testid="drill-clear"
				>
					Clear
				</Button>
			)}
		</>
	);

	return (
		<div data-testid="drillable-grid">
			{refusal && (
				<Alert
					color="yellow"
					mb="xs"
					withCloseButton
					onClose={() => setRefusal(null)}
					title="Can't slice this deterministically"
					data-testid="drill-refusal"
				>
					{refusal}
				</Alert>
			)}

			<WindowedGrid
				key={effectiveKey}
				endpoint="/api/run-sql"
				body={{ sql: effective.sql, params: effective.params }}
				sql={effective.sql}
				sqlParams={effective.params}
				onRowClick={onRowClick}
				onRowHover={onRowHover}
				// The total row anchors a DRILLED view; the undrilled grid IS the
				// scalar, so a footer there would duplicate the single row.
				footerRow={steps.length > 0 ? footerCells : undefined}
				footerLabel={footerLabel}
				columnAccents={columnAccents}
				columnUnits={columnUnits}
				toolbarStart={drillControls}
				toolbarActions={
					<DrillChartAction
						key={effectiveKey}
						sql={effective.sql}
						params={effective.params}
					/>
				}
			/>
		</div>
	);
}

// DrillableGrid (DAT-672, per-node re-cut DAT-703, analyse re-cut DAT-712):
// the shared result grid with a drill layer on top.
//
// Owns the drill-step stack and the EFFECTIVE query. Drill composes upstream
// of the grid, server-side and binder-validated, into a new effective base
// SQL + params; the ordinary `WindowedGrid` renders it — remounting on the
// effective key so grid-local sort/filters reset exactly as on a new agent
// query (React rule 5). THREE compose paths, chosen by `source`:
//   - a canvas NODE (metric or measure) recomposes from its persisted clause
//     parts with the steps as clause appends (`/api/drill/node`);
//   - an ANSWER recomposes from the clause parts its sub-agent declared and the
//     server PROVED against the answer's own value (`/api/drill/parts`,
//     DAT-678) — the only way a scalar answer is drillable, since it projects
//     no column to group by;
//   - no source: the grid wraps its own visible columns (`/api/drill/compose`,
//     tier A) — always available, and the right path for a result that already
//     carries its dimensions.
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
// Axes come from `/api/drill/axes`, one resolution per compose path (DAT-678):
// the node's own catalog, the answer's proven relation, or — for tier A — the
// catalogued dimensions that are actually COLUMNS of this result, since that is
// all an outer GROUP BY can address. This widget fetches the drill routes
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
import {
	Check,
	ChevronDown,
	ChevronsDown,
	Layers,
	Sparkles,
	X,
} from "lucide-react";
import { type ReactNode, useEffect, useMemo, useRef, useState } from "react";
import type { ChartConfig } from "#/charts/chart-config";
import { useChartData } from "#/charts/use-chart-data";
import {
	DRILL_GUIDANCE_TIMEOUT_MS,
	type DrillAxesRequest,
	type DrillAxis,
	type DrillPinValue,
	type DrillSource,
	type DrillStep,
	MAX_GUIDANCE_AXES,
	totalIsRecomputed,
} from "#/duckdb/drill";

import { grainLabel, grainPresetsFrom, parseGrainToken } from "#/duckdb/grain";
// Type-only (erased at compile time — the canvas-state.ts / tool-result-to-canvas.ts
// precedent for pulling a server tool's RESULT shape without importing its runtime):
// the wire contract for the axes route's response, kept in sync with the server's
// actual return shape instead of hand-duplicated here.
import type { DrillAxesResult } from "#/tools/drill-axes";
import {
	AxisGuidanceBadge,
	axisGuidanceTier,
} from "#/ui/cockpit/widgets/axis-guidance";
import { ChartToolbarButton } from "#/ui/cockpit/widgets/chart-toolbar-button";
import { WindowedGrid } from "#/ui/cockpit/widgets/result-grid";

type SqlParams = (string | number | boolean | null)[];

type ComposeResponse =
	| {
			ok: true;
			sql: string;
			params: SqlParams;
			/** The UNDRILLED scalar, projected with its operand components — the
			 *  footer row's statement. Served by the parts path on every
			 *  composition (an answer grid has no open call) and by the node path
			 *  on its open one, which the analyse overlay reads for itself and
			 *  hands back as `footerCells`. Absent when the composition has no
			 *  honest total to show. */
			totals?: { sql: string };
	  }
	| { ok: false; reason: string };

async function postJson<T>(
	url: string,
	body: unknown,
	signal?: AbortSignal,
): Promise<T> {
	const res = await fetch(url, {
		method: "POST",
		headers: { "Content-Type": "application/json" },
		body: JSON.stringify(body),
		signal,
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
 *  so drilling resets the authored chart along with sort/filters (rule 5).
 *
 *  CONTROLLED when the surface passes `chart`: a surface that OUTLIVES the
 *  chart (the answer's Report mint freezes it) has to own the value, and then
 *  the reset-on-drill is its handler's job rather than this remount's. Still
 *  ONE button either way — a second chart affordance beside this one would be
 *  two sources of truth for the same picture. */
function DrillChartAction({
	sql,
	params,
	chart,
}: {
	sql: string;
	params: SqlParams;
	chart?: {
		value: ChartConfig | null;
		onChange: (config: ChartConfig | null) => void;
	};
}) {
	const [config, setConfig] = useState<ChartConfig | null>(null);
	return (
		<ChartToolbarButton
			sql={sql}
			params={params}
			value={chart ? chart.value : config}
			onChange={chart ? chart.onChange : setConfig}
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
	// Floored at the axis's observed cadence (DAT-857): a monthly measure is not
	// offered day buckets it has no data to fill.
	const presets = grainPresetsFrom(axis.temporal ?? "date", axis.bucketGrain);

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
	source,
	initialSteps,
	footerCells,
	footerLabel,
	columnAccents,
	columnUnits,
	onRowHover,
	onPinnedRow,
	onStepsChange,
	toolbarActions,
	chart,
	fillHeight,
}: {
	/** The base query (the node's composed SQL on the canvas path). */
	sql: string;
	params?: SqlParams;
	/** What the Slice control offers, resolved per compose path. */
	axesRequest: DrillAxesRequest;
	/** How a drill recomposes. Absent = tier A: wrap this result's own columns
	 *  (`/api/drill/compose`). Present = recompose UPSTREAM from clause parts —
	 *  a canvas node's persisted ones (`/api/drill/node`) or an answer's proven
	 *  declared ones (`/api/drill/parts`, DAT-678). */
	source?: DrillSource;
	/** Rehydrate a drill on mount (DAT-676) — the report-detail route's search
	 *  param is the one caller today: on load it decodes its `?drill=` steps
	 *  and hands them here so the grid re-composes and opens ALREADY drilled,
	 *  instead of a reload silently snapping back to the base result. Composed
	 *  the same way a live apply is (same endpoint, same acceptance rule); a
	 *  step that no longer composes (the catalog or data changed underneath
	 *  the saved link) degrades to the base result with a visible notice —
	 *  never a silently different grid. Omitted/empty = no rehydrate, the
	 *  ordinary empty-stack start every other caller gets. */
	initialSteps?: DrillStep[];
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
	/** Fired with the committed step stack after every accepted apply, together
	 *  with the statement now on screen. A surface that freezes or exports what
	 *  the user is looking at (the answer's Report mint) needs the EFFECTIVE
	 *  query, not the base one — otherwise it captures numbers the grid stopped
	 *  showing the moment a slice was applied. */
	onStepsChange?: (
		steps: DrillStep[],
		effective: { sql: string; params: SqlParams },
	) => void;
	/** Extra grid-toolbar actions, rendered after the drill's own chart button
	 *  (the surface owns their content — this widget stays generic). */
	toolbarActions?: ReactNode;
	/** Take ownership of the chart the toolbar authors — for a surface that has
	 *  to READ it (the answer freezes it into a report). Omitted = grid-local,
	 *  reset on every drill. */
	chart?: {
		value: ChartConfig | null;
		onChange: (config: ChartConfig | null) => void;
	};
	/** Fill the parent's height (flex column) instead of the grid's default
	 *  480px body cap — see ResultGridView (DAT-712). */
	fillHeight?: boolean;
}) {
	const baseParams = useMemo<SqlParams>(() => params ?? [], [params]);
	// The committed stack + its server-composed statement move TOGETHER: steps
	// are non-empty iff `composed` holds their accepted composition.
	const [steps, setSteps] = useState<DrillStep[]>([]);
	const [composed, setComposed] = useState<{
		sql: string;
		params: SqlParams;
	} | null>(null);
	// The footer statement the LAST accepted composition served (DAT-671 R2).
	// Kept beside `composed` because it belongs to the same response: a drill
	// that was accepted is a drill whose undrilled total is still the honest
	// anchor for what is now on screen.
	const [composedTotalsSql, setComposedTotalsSql] = useState<string | null>(
		null,
	);
	const [refusal, setRefusal] = useState<string | null>(null);
	// A REHYDRATE-specific notice (DAT-676), distinct from `refusal`: a live
	// apply's refusal is the user's OWN action failing right now (dismissable,
	// tied to the control they just used); a rehydrate failure is a SAVED link
	// that no longer resolves — the grid opens on the base result instead, and
	// this says why, once, on mount.
	const [rehydrateNotice, setRehydrateNotice] = useState<string | null>(null);

	// The axes request CARRIES THE DRILL STACK on a parts-at-source grid
	// (DAT-671 R2). Without it the server could not know what this grid has
	// already sliced by, so an axis the practitioner just used came back offered
	// and was greyed here, client-side, with no reason text and no tooltip — a
	// dead menu item explaining nothing. Sending the steps makes "still worth
	// offering?" one server-side answer WITH its reason. It also re-keys the
	// query per drill, which is the point: the menu is drill-dependent.
	// Derived during render, not memoized: `queryKey` is hashed BY VALUE, so
	// object identity buys nothing here (React idiom rule 1 + rule 6 — a
	// `useMemo` has to earn its line).
	const resolvedAxesRequest =
		"partsSources" in axesRequest && steps.length > 0
			? { ...axesRequest, steps }
			: axesRequest;
	const axesQuery = useQuery({
		queryKey: ["drill-axes", resolvedAxesRequest],
		queryFn: () =>
			postJson<DrillAxesResult>("/api/drill/axes", resolvedAxesRequest),
		staleTime: 60_000,
	});
	const axes = axesQuery.data?.axes ?? [];
	const axisByColumn = useMemo(
		() => new Map(axes.map((a) => [a.column, a])),
		[axes],
	);

	// DAT-671: excludes a greyed (already-in-result) axis from BOTH the
	// all-unmeasured gate and the guidance payload below — Haiku has no
	// business suggesting slicing by a column the menu already shows disabled.
	const enabledAxes = axes.filter((a) => a.disabledReason === null);
	// Haiku guidance fallback (DAT-673): offered ONLY when the node has NO
	// measured signal at all — every ENABLED axis's tier is null. A node with
	// even one measured/curated axis never shows this; mixing a real badge
	// with an unmeasured guess on the same menu would blur exactly the honesty
	// this chip exists to preserve. Session-local state (no persistence — this
	// is an on-demand affordance, and a fresh mount naturally clears it, React
	// idiom #5).
	const allUnmeasured =
		enabledAxes.length > 0 &&
		enabledAxes.every((a) => axisGuidanceTier(a) === null);
	// Tri-state, not a plain nullable Map (fold-in #4): a successful call that
	// returns ZERO surviving suggestions must not look identical to "never
	// asked" — an empty-but-truthy Map made the Suggest action vanish with no
	// text and no error, a dead end. "empty" renders an explicit "No
	// suggestions" state instead.
	const [guidance, setGuidance] = useState<
		Map<string, string> | "empty" | null
	>(null);
	const [guidanceError, setGuidanceError] = useState<string | null>(null);
	const guidanceMutation = useMutation({
		mutationFn: () => {
			// Bound the call — the SAME shared constant the route's schema and
			// the agent module cap with (duckdb/drill.ts); review-round Critical
			// 1: a pure-substrate node routinely has MORE than MAX_GUIDANCE_AXES
			// axes, and sending all of them 400'd on the route's own cap with a
			// raw zod message. DAT-671: `enabledAxes`, not `axes` — never send a
			// greyed (already-in-result) column for Haiku to suggest slicing by.
			const capped = enabledAxes.slice(0, MAX_GUIDANCE_AXES);
			// Fold-in #5: a hung model must not leave "Asking…" disabled
			// forever — bound the client's own fetch independently of the
			// server-side timeout (axis-guidance-agent.ts bounds its chat() call
			// with the same duration).
			const controller = new AbortController();
			const timer = setTimeout(
				() => controller.abort(),
				DRILL_GUIDANCE_TIMEOUT_MS,
			);
			return postJson<{ suggestions: { column: string; guidance: string }[] }>(
				"/api/drill/axis-guidance",
				{
					// No richer label is available at this layer (a node ref names a
					// metric/measure key, an ad-hoc result has none at all) — the
					// axes' own request shape is the best context on hand.
					measureLabel:
						"metricKey" in axesRequest
							? axesRequest.metricKey
							: "standardField" in axesRequest
								? axesRequest.standardField
								: "this result",
					axes: capped.map((a) => ({
						column: a.column,
						sliceType: a.sliceType,
					})),
				},
				controller.signal,
			).finally(() => clearTimeout(timer));
		},
		onSuccess: (res) => {
			setGuidanceError(null);
			setGuidance(
				res.suggestions.length > 0
					? new Map(res.suggestions.map((s) => [s.column, s.guidance]))
					: "empty",
			);
		},
		onError: (err) => {
			setGuidanceError(err instanceof Error ? err.message : String(err));
		},
	});

	// Monotonic apply generation, bumped in the EVENT HANDLER so it carries
	// click order. TanStack Query neither serializes nor cancels overlapping
	// `.mutate()` calls — their callbacks fire in network-resolution order — so
	// without this guard two quick applies (row-pin then pill-remove, say)
	// could commit the OLDER composition last and leave the grid on a state
	// that doesn't match the user's latest action. Ref, not state: read/written
	// only in handlers/callbacks (rule 8's render restriction doesn't apply).
	const generationRef = useRef(0);

	// The ONE compose call, shared by a live apply (the mutation below) and the
	// mount-time rehydrate (DAT-676) — same endpoint selection by `source`, so
	// a rehydrate is composed exactly the way a live apply would be, never a
	// second, drifting code path.
	const runCompose = (candidate: DrillStep[]): Promise<ComposeResponse> =>
		source === undefined
			? postJson<ComposeResponse>("/api/drill/compose", {
					sql,
					params: baseParams,
					steps: candidate,
				})
			: source.kind === "node"
				? postJson<ComposeResponse>("/api/drill/node", {
						...source.ref,
						steps: candidate,
					})
				: postJson<ComposeResponse>("/api/drill/parts", {
						...source.source,
						steps: candidate,
					});

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
			result: await runCompose(candidate),
		}),
		onSuccess: ({ candidate, generation, pinRow, result }) => {
			if (generation !== generationRef.current) return; // superseded — drop
			if (result.ok) {
				const prevPinCount = steps.filter((s) => s.kind === "pin").length;
				const nextPinCount = candidate.filter((s) => s.kind === "pin").length;
				setSteps(candidate);
				setComposed({ sql: result.sql, params: result.params });
				setComposedTotalsSql(result.totals?.sql ?? null);
				setRefusal(null);
				onStepsChange?.(candidate, {
					sql: result.sql,
					params: result.params,
				});
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

	// Rehydrate a saved drill on mount (DAT-676): the report-detail route
	// decodes its `?drill=` search param and hands the steps here as
	// `initialSteps`. Composed exactly like a live apply (same `runCompose`,
	// same acceptance rule) — a step that no longer resolves (the catalog or
	// data changed under a saved/shared link) leaves `steps`/`composed` at
	// their empty defaults and surfaces `rehydrateNotice` instead: the grid
	// opens on the BASE result with a visible reason, never a silently
	// different grid and never a throw. Runs from the URL's state AT LOAD;
	// a LATER change rides through `apply`/`onStepsChange`, never back
	// through here — an external-system sync (React idiom rule 2), not a
	// state mirror, and the one-time nature is the point.
	// biome-ignore lint/correctness/useExhaustiveDependencies: mount-only rehydrate from the URL's state AT LOAD — a later initialSteps/onStepsChange/runCompose identity change must NOT re-fire this (that would re-hydrate on every live apply, fighting the user's own action).
	useEffect(() => {
		if (!initialSteps || initialSteps.length === 0) return;
		const generation = ++generationRef.current;
		let live = true;
		void (async () => {
			let result: ComposeResponse;
			try {
				result = await runCompose(initialSteps);
			} catch (err) {
				if (live && generation === generationRef.current) {
					setRehydrateNotice(
						`This link's saved slice couldn't be restored — showing the base result (${
							err instanceof Error ? err.message : String(err)
						}).`,
					);
				}
				return;
			}
			if (!live || generation !== generationRef.current) return;
			if (result.ok) {
				setSteps(initialSteps);
				setComposed({ sql: result.sql, params: result.params });
				setComposedTotalsSql(result.totals?.sql ?? null);
				onStepsChange?.(initialSteps, {
					sql: result.sql,
					params: result.params,
				});
			} else {
				setRehydrateNotice(
					`This link's saved slice couldn't be restored — showing the base result (${result.reason}).`,
				);
			}
		})();
		return () => {
			live = false;
		};
	}, []);

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
			setComposedTotalsSql(null);
			setRefusal(null);
			onStepsChange?.([], { sql, params: baseParams });
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

	// Grain is a capability of RECOMPOSING AT SOURCE — node or parts, the two
	// paths that rebuild from clause parts and can therefore bucket the raw
	// column before aggregating (DAT-671 R2; it used to be node-only, which made
	// an answer's month grain a path privilege rather than a data one). Tier A is
	// excluded for a structural reason, not a doctrinal one: it WRAPS an already
	// aggregated result, so there is no raw date left to bucket, and
	// `/api/drill/compose` refuses a grained step outright.
	//
	// WHETHER a given axis may be bucketed is not decided here at all — the axes
	// resolver withholds `temporal` (with a reason) unless the engine's verdict
	// licenses it, so `axis.temporal !== null` below is the data-driven half.
	const grainable = source !== undefined;

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

	// Hierarchy descent (DAT-673): the LAST committed pin governs the
	// suggestion — each new pin refines it further, matching the AC "after a
	// pin, the hierarchy's next level is the first suggestion." Derived during
	// render (idiom #1) — no effect, no memo (cheap over a handful of steps/
	// axes and not a dependency of any hook here, so memoizing it wouldn't
	// earn its line per idiom #6).
	const lastPin = [...steps].reverse().find((s) => s.kind === "pin");
	const hierarchyNextColumn = lastPin
		? axisByColumn.get(lastPin.column)?.hierarchyNext
		: undefined;
	// DAT-671: never promote a "Suggested: Descend to X" for an axis that's
	// simultaneously shown greyed (already-in-result) in the list below —
	// same filter shape as the existing already-sliced check just beside it.
	const hierarchySuggestion =
		hierarchyNextColumn &&
		!slicedColumns.has(hierarchyNextColumn) &&
		axisByColumn.get(hierarchyNextColumn)?.disabledReason === null
			? axisByColumn.get(hierarchyNextColumn)
			: undefined;

	// Safe lookup over the tri-state (fold-in #4's "empty" isn't a Map).
	const guidanceTextFor = (column: string): string | undefined =>
		guidance instanceof Map ? guidance.get(column) : undefined;

	/** One axis's rightSection (grain name / valueCount) — SHARED by the plain
	 *  list entry and the promoted hierarchy-descent suggestion (fold-in #7:
	 *  the same axis must never look like two different things depending on
	 *  which entry renders it). */
	const axisRightSection = (axis: DrillAxis): ReactNode =>
		grainable && axis.temporal !== null ? (
			<Text size="xs" c="dimmed">
				{grainName(DEFAULT_TEMPORAL_GRAIN)}
			</Text>
		) : axis.valueCount !== null ? (
			<Text size="xs" c="dimmed">
				{axis.valueCount}
			</Text>
		) : undefined;

	/** One axis's FULL Menu.Item body — the primary label (either the bare
	 *  column name or the hierarchy-descent "Descend to X" framing) inline
	 *  with its guidance badge, then curated business context and (DAT-673)
	 *  any on-demand Haiku suggestion below. SHARED by the plain list entry
	 *  and the promoted hierarchy-descent suggestion (fold-in #7: the same
	 *  axis must never look like two different things depending on which
	 *  entry renders it — the earlier version dropped the badge AND
	 *  valueCount from the promoted entry). */
	const axisItemBody = (axis: DrillAxis, label: string): ReactNode => (
		<>
			<Group gap={6} wrap="nowrap">
				<Text size="sm">{label}</Text>
				<AxisGuidanceBadge axis={axis} />
			</Group>
			{axis.businessContext && (
				<Text size="xs" c="dimmed" lineClamp={1}>
					{axis.businessContext}
				</Text>
			)}
			{/* An on-demand Haiku suggestion, never dressed as measured —
			    visually distinct (italic, muted, own prefix) from
			    businessContext above, which is real catalog data. */}
			{guidanceTextFor(axis.column) && (
				<Text size="xs" c="dimmed" fs="italic" lineClamp={2}>
					Suggested (unmeasured): {guidanceTextFor(axis.column)}
				</Text>
			)}
			{/* DAT-671: the short inline reason a greyed axis carries — the item
			    STAYS in the menu (never removed), disabled, with this label; the
			    fuller Tooltip on the item itself (below) carries the same text on
			    hover. */}
			{axis.disabledReason && (
				<Text size="xs" c="dimmed" fs="italic">
					{axis.disabledReason}
				</Text>
			)}
			{/* DAT-857: a date column the engine's verdict will not let us BUCKET.
			    Unlike disabledReason this axis stays fully selectable — it is
			    offered as a raw date slice — so the note explains the missing
			    grain control, not a disabled item. It also ranks last. */}
			{axis.temporalWithheldReason && (
				<Text size="xs" c="dimmed" fs="italic">
					{axis.temporalWithheldReason}
				</Text>
			)}
		</>
	);

	// The footer's own row, when the composition served its statement (DAT-671
	// R2 — the answer path). One bounded query on the shared chart-data cache,
	// exactly how the analyse overlay fetches the node path's.
	const totalsQuery = useChartData(
		composedTotalsSql ?? "",
		[],
		composedTotalsSql !== null,
	);

	// The total row anchors a DRILLED view; the undrilled grid IS the scalar, so
	// a footer there would duplicate the single row. A recomputed value (ratio,
	// average) PRINTS its real total — it is the formula over the carrier totals
	// beside it, the same number the header shows — and the label below says so
	// (DAT-857; lead ruling 2026-07-29 retired the dash mask).
	//
	// A caller-supplied row WINS: the analyse overlay reads the node path's open
	// call for its own equation header and hands the same row down, so honouring
	// its copy keeps the header and the footer showing one number rather than two
	// fetches of it.
	const footerRow =
		steps.length > 0
			? (footerCells ??
				(totalsQuery.data?.rows[0] as Record<string, Json | null> | undefined))
			: undefined;
	const recomputedTotal =
		steps.length > 0 &&
		totalIsRecomputed(steps, axes, axesQuery.data?.reconciles);

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
					{hierarchySuggestion && (
						<>
							<Menu.Label>Suggested</Menu.Label>
							<Menu.Item
								key={`suggested:${hierarchySuggestion.column}`}
								leftSection={<ChevronsDown size={13} />}
								onClick={() => slice(hierarchySuggestion)}
								rightSection={axisRightSection(hierarchySuggestion)}
								data-testid={`drill-hierarchy-suggestion-${hierarchySuggestion.column}`}
							>
								{axisItemBody(
									hierarchySuggestion,
									`Descend to ${hierarchySuggestion.column}`,
								)}
							</Menu.Item>
							<Menu.Divider />
						</>
					)}
					{axes.map((axis) => {
						const item = (
							<Menu.Item
								key={axis.column}
								disabled={
									slicedColumns.has(axis.column) || axis.disabledReason !== null
								}
								onClick={() => slice(axis)}
								rightSection={axisRightSection(axis)}
								data-testid={`drill-axis-${axis.column}`}
							>
								{axisItemBody(axis, axis.column)}
							</Menu.Item>
						);
						// DAT-671: a disabled-because-already-in-result item stays in the
						// menu (never removed) and carries its reason on hover too, not
						// just the inline label inside axisItemBody.
						return axis.disabledReason ? (
							<Tooltip
								key={axis.column}
								label={axis.disabledReason}
								position="right"
								maw={280}
								multiline
							>
								<div>{item}</div>
							</Tooltip>
						) : (
							item
						);
					})}
					{allUnmeasured && guidance === null && (
						<>
							<Menu.Divider />
							<Menu.Item
								leftSection={
									guidanceMutation.isPending ? undefined : (
										<Sparkles size={13} />
									)
								}
								onClick={() => guidanceMutation.mutate()}
								disabled={guidanceMutation.isPending}
								data-testid="drill-suggest-guidance"
							>
								<Text size="sm">
									{guidanceMutation.isPending
										? "Asking…"
										: "Suggest which dimensions might matter"}
								</Text>
							</Menu.Item>
						</>
					)}
					{/* Fold-in #4: a successful call with ZERO surviving suggestions
					    must not be a silent dead end — say so explicitly rather than
					    letting the Suggest action just vanish with nothing to show. */}
					{allUnmeasured && guidance === "empty" && (
						<>
							<Menu.Divider />
							<Menu.Item disabled data-testid="drill-suggest-guidance-empty">
								<Text size="sm" c="dimmed">
									No suggestions
								</Text>
							</Menu.Item>
						</>
					)}
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
			{axesQuery.data?.temporalGateReason && (
				// The time gate has exactly two sources (DAT-725): the engine's
				// persisted verdict, or an honest withhold when none exists yet — "if
				// we do not have data, we honestly say so" (the lead's ruling that
				// killed the old silent column-level heuristic this replaces). Either
				// way the user must be able to tell WHY no grain control appeared for
				// a date/timestamp axis, so this renders unconditionally whenever the
				// gate fired — colored to distinguish a determined "no" (yellow, same
				// hue as the refusal alert below) from an honest "not yet known" (gray,
				// same hue as the "no axes" badge above).
				<Tooltip label={axesQuery.data.temporalGateReason} maw={360} multiline>
					<Badge
						color={
							axesQuery.data.temporalGateSource === "withheld-no-verdict"
								? "gray"
								: "yellow"
						}
						variant="light"
						size="sm"
						data-testid="drill-temporal-gate-reason"
					>
						{axesQuery.data.temporalGateSource === "withheld-no-verdict"
							? "time grain withheld"
							: "time grain off"}
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
		<div
			data-testid="drillable-grid"
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
			{rehydrateNotice && (
				<Alert
					color="yellow"
					mb="xs"
					withCloseButton
					onClose={() => setRehydrateNotice(null)}
					title="Showing the base result"
					data-testid="drill-rehydrate-notice"
				>
					{rehydrateNotice}
				</Alert>
			)}
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
			{guidanceError && (
				<Alert
					color="gray"
					mb="xs"
					withCloseButton
					onClose={() => setGuidanceError(null)}
					title="Couldn't fetch suggestions"
					data-testid="drill-guidance-error"
				>
					{guidanceError}
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
				footerRow={footerRow}
				// The note keeps anyone from reading the rows above as summing to a
				// recomputed value. WindowedGrid defaults an ABSENT label to "Total" —
				// composing here happens before that default, so repeat it or an
				// unlabeled caller renders a literal "undefined".
				footerLabel={
					recomputedTotal
						? `${footerLabel ?? "Total"} — value recomputed`
						: footerLabel
				}
				columnAccents={columnAccents}
				columnUnits={columnUnits}
				toolbarStart={drillControls}
				fillHeight={fillHeight}
				toolbarActions={
					<>
						<DrillChartAction
							key={effectiveKey}
							sql={effective.sql}
							params={effective.params}
							chart={chart}
						/>
						{toolbarActions}
					</>
				}
			/>
		</div>
	);
}

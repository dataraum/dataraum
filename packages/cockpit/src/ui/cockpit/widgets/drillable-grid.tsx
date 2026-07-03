// DrillableGrid (DAT-672): the shared result grid with a drill layer on top.
//
// Owns the drill-step stack and the EFFECTIVE query. Drill composes upstream
// of the grid: every accepted step stack is composed server-side
// (`/api/drill/compose`, binder-validated) into a new effective base SQL +
// params, and the ordinary `WindowedGrid` renders it — remounting on the
// effective key so grid-local sort/filters reset exactly as on a new agent
// query (React rule 5). The stack only ever holds compositions the server
// ACCEPTED: a candidate stack is sent as a user-event mutation and committed
// on `ok: true`; a refusal shows the amber "can't slice this deterministically"
// state and leaves the grid on the last good drill.
//
// Axes come from the metric path (`/api/drill/axes`, catalog metadata only —
// DAT-678 adds ad-hoc resolution). This widget fetches both drill routes
// instead of importing server modules (bundle hygiene).

import {
	Alert,
	Badge,
	Button,
	Group,
	Menu,
	Pill,
	Text,
	Tooltip,
} from "@mantine/core";
import { useMutation, useQuery } from "@tanstack/react-query";
import { Layers } from "lucide-react";
import { useMemo, useRef, useState } from "react";

import type { ChartConfig } from "#/charts/chart-config";
import type {
	DrillAxesRequest,
	DrillAxis,
	DrillPinValue,
	DrillStep,
} from "#/duckdb/drill";
import { ChartToolbarButton } from "#/ui/cockpit/widgets/chart-toolbar-button";
import { WindowedGrid } from "#/ui/cockpit/widgets/result-grid";

type SqlParams = (string | number | boolean | null)[];

type ComposeResponse =
	| { ok: true; tier: "A" | "B"; sql: string; params: SqlParams }
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

export function DrillableGrid({
	sql,
	params,
	axesRequest,
}: {
	/** The base query (a metric's flattened SQL on the canvas path). */
	sql: string;
	params?: SqlParams;
	/** What the Slice control offers — resolved by the metric path in P1. */
	axesRequest: DrillAxesRequest;
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
			postJson<{ axes: DrillAxis[] }>("/api/drill/axes", axesRequest),
		staleTime: 60_000,
	});
	const axes = axesQuery.data?.axes ?? [];

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
		}: {
			candidate: DrillStep[];
			generation: number;
		}) => ({
			candidate,
			generation,
			result: await postJson<ComposeResponse>("/api/drill/compose", {
				sql,
				params: baseParams,
				steps: candidate,
			}),
		}),
		onSuccess: ({ candidate, generation, result }) => {
			if (generation !== generationRef.current) return; // superseded — drop
			if (result.ok) {
				setSteps(candidate);
				setComposed({ sql: result.sql, params: result.params });
				setRefusal(null);
			} else {
				setRefusal(result.reason);
			}
		},
		onError: (err, { generation }) => {
			if (generation !== generationRef.current) return; // superseded — drop
			setRefusal(err instanceof Error ? err.message : String(err));
		},
	});

	const apply = (candidate: DrillStep[]) => {
		const generation = ++generationRef.current;
		if (candidate.length === 0) {
			// Clearing is synchronous — the bump above also invalidates any
			// still-in-flight compose so it can't resurrect the cleared drill.
			setSteps([]);
			setComposed(null);
			setRefusal(null);
			return;
		}
		compose.mutate({ candidate, generation });
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
	// row's cell values. Only offered while sliced (a detail row has no group
	// identity to pin).
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
						if (!duplicate) pins.push({ kind: "pin", column: s.column, value });
					}
					if (pins.length > 0) apply([...steps, ...pins]);
				}
			: undefined;

	return (
		<div data-testid="drillable-grid">
			<Group gap="xs" mb="xs" wrap="wrap">
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
								onClick={() =>
									apply([...steps, { kind: "slice", column: axis.column }])
								}
								rightSection={
									axis.valueCount !== null ? (
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
					<Tooltip label="No cataloged dimensions for this metric's facts">
						<Badge color="gray" variant="light" size="sm">
							no axes
						</Badge>
					</Tooltip>
				)}
				{steps.map((step, i) => (
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
							: `${step.column} = ${pinLabel(step.value)}`}
					</Pill>
				))}
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
			</Group>

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

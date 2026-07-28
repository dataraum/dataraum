// The Model section (DAT-591 metrics; DAT-737 concepts) — a standing page with
// TWO complementary graphs over the same operating model: the metric
// composition DAG (metric → metric → measure → table, the original view) and
// the concept VOCABULARY graph (part_of/disjoint_with/reconciles_with +
// groundings — a faithful lens on the engine's own concept traversal). Data is
// read server-side (the metadata Drizzle client never reaches the client
// bundle); the xyflow metric canvas is rendered client-only (React Flow
// measures the DOM, so it must not run during SSR) — the concept view is a
// plain accordion list and needs no such gate.
// (Validation/cycle/driver get their own graphs in a future follow-up.)
//
// TWO INDEPENDENT LOADS, fault-isolated (spec-review CRITICAL): `loadModel`
// and `loadConcepts` read unrelated substrate on unrelated lifecycles (a
// concept vocabulary exists from `frame`-time; the metric graph needs a
// promoted operating_model run) — a `Promise.all` would let either failure
// blank BOTH panes and mislabel which one broke. `Promise.allSettled` +
// `PaneResult` keep them apart: a concepts-read failure shows a read-error
// alert in the CONCEPTS pane while Metrics renders normally, and vice versa.
// `errorComponent: ModelError` is now a true last-resort fallback (something
// outside both loads throwing) — its copy is deliberately generic, since it
// can no longer assume which pane's read failed.
//
// TAB RULING (owner, spec review): both panes stay MOUNTED always; the
// SegmentedControl toggles CSS visibility, never a conditional unmount — an
// unmount would reset the Metrics canvas's xyflow pan/zoom + dagre layout on
// every switch, a real courtesy loss at practitioner scale. `view` rides the
// route's search params (the reports `?drill=` precedent) so the active tab
// survives a reload or a shared deep link.

import {
	Alert,
	Box,
	Center,
	Code,
	ScrollArea,
	SegmentedControl,
	Stack,
	Text,
} from "@mantine/core";
import {
	ClientOnly,
	createFileRoute,
	type ErrorComponentProps,
} from "@tanstack/react-router";
import type { ConceptGraph } from "#/tools/concept-graph";
import type { LoadOperatingModelResult } from "#/tools/operating-model-load";
import { ConceptGraphView } from "#/ui/cockpit/operating-model/concept-graph-view";
import { ModelIcon } from "#/ui/cockpit/operating-model/nodes";
import { OperatingModelCanvas } from "#/ui/cockpit/operating-model/operating-model-canvas";
import { loadConcepts, loadModel } from "./operating-model.functions";

/** One pane's independent read outcome — never let one pane's failure blank
 *  the other or get mislabeled as the other's error. */
type PaneResult<T> =
	| { status: "ok"; data: T }
	| { status: "error"; message: string };

function toPaneResult<T>(settled: PromiseSettledResult<T>): PaneResult<T> {
	if (settled.status === "fulfilled")
		return { status: "ok", data: settled.value };
	const { reason } = settled;
	return {
		status: "error",
		message: reason instanceof Error ? reason.message : String(reason),
	};
}

type ViewMode = "metrics" | "concepts";

export const Route = createFileRoute("/(app)/operating-model")({
	validateSearch: (search: Record<string, unknown>): { view?: "concepts" } =>
		// The KEY itself is omitted (not present-with-undefined) at the default
		// "metrics" tab — mirrors the reports `?drill=` convention of not
		// cluttering the URL with the no-op state, and keeps `view` a truly
		// OPTIONAL search param so a bare `{ to: "/operating-model" }` link
		// (governance.tsx) stays valid without threading a search object.
		search.view === "concepts" ? { view: "concepts" } : {},
	loader: async () => {
		const [modelResult, conceptsResult] = await Promise.allSettled([
			loadModel(),
			loadConcepts(),
		]);
		return {
			model: toPaneResult(modelResult),
			concepts: toPaneResult(conceptsResult),
		};
	},
	component: ModelSection,
	// A genuinely unexpected throw OUTSIDE both loads (Promise.allSettled
	// itself never rejects) — last-resort fallback, so this must stay
	// pane-agnostic; it can no longer assume "the metric graph" broke.
	errorComponent: ModelError,
});

function ModelError({ error }: ErrorComponentProps) {
	return (
		<Center h="100%">
			<Stack gap="xs" align="center" maw={560}>
				<ModelIcon size={32} color="var(--mantine-color-red-6)" />
				<Text fw={600}>Couldn't load the operating model</Text>
				<Text size="sm" c="dimmed" ta="center">
					This page failed to load. This is usually a metadata read error —
					check the run, or that the cockpit build matches the engine schema.
				</Text>
				<ScrollArea.Autosize mah={200} w="100%">
					<Code block>{error.message}</Code>
				</ScrollArea.Autosize>
			</Stack>
		</Center>
	);
}

function EmptyState({ title, detail }: { title: string; detail: string }) {
	return (
		<Center h="100%">
			<Stack gap="xs" align="center" maw={420}>
				<ModelIcon size={32} color="var(--mantine-color-dimmed)" />
				<Text fw={600}>{title}</Text>
				<Text size="sm" c="dimmed" ta="center">
					{detail}
				</Text>
			</Stack>
		</Center>
	);
}

/** One pane's own read-error state — distinct wording per pane, so a
 *  concepts-read failure never reads as "the metric graph failed" (or the
 *  reverse). */
function PaneError({ title, message }: { title: string; message: string }) {
	return (
		<Center h="100%">
			<Stack gap="xs" align="center" maw={420}>
				<Alert color="red" title={title} w="100%">
					<Text size="sm" c="dimmed">
						{message}
					</Text>
				</Alert>
			</Stack>
		</Center>
	);
}

function MetricsView({
	model,
}: {
	model: PaneResult<LoadOperatingModelResult>;
}) {
	if (model.status === "error") {
		return (
			<PaneError
				title="Couldn't load the metric graph"
				message={model.message}
			/>
		);
	}
	const { analyzed, graph } = model.data;

	if (!analyzed) {
		return (
			<EmptyState
				title="No operating model yet"
				detail="Run the operating model over a framed session to populate the metric graph — every metric, the measures it reads, and how the metrics compose."
			/>
		);
	}
	if (graph.nodes.length === 0) {
		return (
			<EmptyState
				title="Operating model is empty"
				detail="The operating model ran but produced no artifacts to map. Check the run for grounding gaps."
			/>
		);
	}

	return (
		// React Flow needs a DEFINITE height. AppShell.Main only sets min-height
		// (its `height` is auto), so `h="100%"` here resolves to 0 and the canvas
		// renders blank. The parent Box (below) supplies the concrete height via
		// flex — this Box just fills it.
		<Box style={{ height: "100%" }}>
			<ClientOnly fallback={<EmptyState title="Loading canvas…" detail="" />}>
				<OperatingModelCanvas graph={graph} />
			</ClientOnly>
		</Box>
	);
}

function ConceptsView({ concepts }: { concepts: PaneResult<ConceptGraph> }) {
	if (concepts.status === "error") {
		return (
			<PaneError
				title="Couldn't load the concept vocabulary"
				message={concepts.message}
			/>
		);
	}
	return <ConceptGraphView graph={concepts.data} />;
}

function ModelSection() {
	const { model, concepts } = Route.useLoaderData();
	const search = Route.useSearch();
	const navigateSearch = Route.useNavigate();
	const view: ViewMode = search.view ?? "metrics";

	return (
		<Stack
			gap="sm"
			style={{
				height: "calc(100dvh - var(--app-shell-header-offset, 3rem) - 2rem)",
			}}
		>
			<SegmentedControl
				aria-label="Choose which operating-model graph to view"
				data-testid="operating-model-view-toggle"
				value={view}
				onChange={(v) =>
					navigateSearch({
						search: { view: v === "concepts" ? "concepts" : undefined },
						replace: true,
						resetScroll: false,
					})
				}
				data={[
					{ label: "Metrics", value: "metrics" },
					{ label: "Concepts", value: "concepts" },
				]}
				style={{ alignSelf: "flex-start" }}
			/>
			<Box style={{ flex: 1, minHeight: 0, position: "relative" }}>
				{/* Both panes stay MOUNTED always (owner ruling) — visibility
				    toggles via CSS so the Metrics canvas's xyflow pan/zoom + dagre
				    layout survive a tab switch instead of resetting on remount. */}
				<Box
					style={{
						display: view === "metrics" ? "block" : "none",
						height: "100%",
					}}
				>
					<MetricsView model={model} />
				</Box>
				<Box
					style={{
						display: view === "concepts" ? "block" : "none",
						height: "100%",
					}}
				>
					<ConceptsView concepts={concepts} />
				</Box>
			</Box>
		</Stack>
	);
}

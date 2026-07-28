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

import {
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
import { useState } from "react";

import { ConceptGraphView } from "#/ui/cockpit/operating-model/concept-graph-view";
import { ModelIcon } from "#/ui/cockpit/operating-model/nodes";
import { OperatingModelCanvas } from "#/ui/cockpit/operating-model/operating-model-canvas";
import { loadConcepts, loadModel } from "./operating-model.functions";

export const Route = createFileRoute("/(app)/operating-model")({
	loader: async () => {
		const [model, concepts] = await Promise.all([loadModel(), loadConcepts()]);
		return { model, concepts };
	},
	component: ModelSection,
	// A loader failure (e.g. a metadata read against a drifted view) must degrade
	// to a readable error, never a white screen — the route renders server-side,
	// so an unhandled loader throw would otherwise blank the page.
	errorComponent: ModelError,
});

function ModelError({ error }: ErrorComponentProps) {
	return (
		<Center h="100%">
			<Stack gap="xs" align="center" maw={560}>
				<ModelIcon size={32} color="var(--mantine-color-red-6)" />
				<Text fw={600}>Couldn't load the operating model</Text>
				<Text size="sm" c="dimmed" ta="center">
					The metric graph failed to load. This is usually a metadata read error
					— check the run, or that the cockpit build matches the engine schema.
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

function MetricsView() {
	const { model } = Route.useLoaderData();
	const { analyzed, graph } = model;

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
		// renders blank. Size the container to the viewport minus the header offset
		// (--app-shell-header-offset, 3rem) and the Main's 1rem top + 1rem bottom
		// padding — a concrete height the flow pane and its children resolve against.
		<Box style={{ height: "100%" }}>
			<ClientOnly fallback={<EmptyState title="Loading canvas…" detail="" />}>
				<OperatingModelCanvas graph={graph} />
			</ClientOnly>
		</Box>
	);
}

function ModelSection() {
	const { concepts } = Route.useLoaderData();
	// Concepts are seeded at `frame`-time (config→DB, DAT-728) — independent of
	// whether the operating_model stage has run — so this view gets its OWN
	// empty state (inside ConceptGraphView) rather than the Metrics tab's
	// `analyzed` gate.
	const [view, setView] = useState<"metrics" | "concepts">("metrics");

	return (
		<Stack
			gap="sm"
			style={{
				height: "calc(100dvh - var(--app-shell-header-offset, 3rem) - 2rem)",
			}}
		>
			<SegmentedControl
				data-testid="operating-model-view-toggle"
				value={view}
				onChange={(v) => setView(v as "metrics" | "concepts")}
				data={[
					{ label: "Metrics", value: "metrics" },
					{ label: "Concepts", value: "concepts" },
				]}
				style={{ alignSelf: "flex-start" }}
			/>
			<Box style={{ flex: 1, minHeight: 0 }}>
				{view === "metrics" ? (
					<MetricsView />
				) : (
					<ConceptGraphView graph={concepts} />
				)}
			</Box>
		</Stack>
	);
}

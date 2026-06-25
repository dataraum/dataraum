// The Model section (DAT-591) — a standing page rendering the workspace's
// concept-spine operating-model DAG. Data is read server-side (the metadata
// Drizzle client never reaches the client bundle); the xyflow canvas is rendered
// client-only (React Flow measures the DOM, so it must not run during SSR).

import { Box, Center, Stack, Text } from "@mantine/core";
import { ClientOnly, createFileRoute } from "@tanstack/react-router";

import { ModelIcon } from "#/ui/cockpit/operating-model/nodes";
import { OperatingModelCanvas } from "#/ui/cockpit/operating-model/operating-model-canvas";
import { loadModel } from "./operating-model.functions";

export const Route = createFileRoute("/(app)/workspace/$wsId/operating-model")({
	loader: () => loadModel(),
	component: ModelSection,
});

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

function ModelSection() {
	const { analyzed, graph } = Route.useLoaderData();

	if (!analyzed) {
		return (
			<EmptyState
				title="No operating model yet"
				detail="Run the operating model over a framed session to populate the concept-spine canvas — metrics, cycles, validations and their drivers."
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
		<Box h="100%">
			<ClientOnly fallback={<EmptyState title="Loading canvas…" detail="" />}>
				<OperatingModelCanvas graph={graph} />
			</ClientOnly>
		</Box>
	);
}

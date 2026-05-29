import { Anchor, Card, Stack, Text, Title } from "@mantine/core";
import { createFileRoute } from "@tanstack/react-router";
import { createServerFn } from "@tanstack/react-start";
import { config } from "#/config";

// The Temporal Web UI runs as its own service; we embed it rather than
// reimplement workflow inspection. The URL is config-driven (TEMPORAL_UI_URL,
// default http://localhost:8080) and read server-side so the server-only
// config never leaks into the client bundle.
const getTemporalUiUrl = createServerFn({ method: "GET" }).handler(
	() => config.temporalUiUrl,
);

export const Route = createFileRoute("/(app)/workspace/$wsId/workflows")({
	loader: () => getTemporalUiUrl(),
	component: WorkflowsSection,
});

function WorkflowsSection() {
	const temporalUiUrl = Route.useLoaderData();
	return (
		<Stack gap="md" h="100%">
			<Stack gap="xs">
				<Title order={2}>Workflows</Title>
				<Text c="dimmed" size="sm">
					Durable execution runs in Temporal. Inspect workflows and activities
					in the Temporal Web UI.
				</Text>
			</Stack>
			<Card withBorder padding="md" flex={1}>
				<Stack gap="sm" h="100%">
					<Anchor href={temporalUiUrl} target="_blank" rel="noreferrer">
						Open Temporal UI ({temporalUiUrl})
					</Anchor>
					<iframe
						title="Temporal Web UI"
						src={temporalUiUrl}
						style={{ flex: 1, width: "100%", border: "none" }}
					/>
				</Stack>
			</Card>
		</Stack>
	);
}

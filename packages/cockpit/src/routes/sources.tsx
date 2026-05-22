import { Alert, Stack, Text, Title } from "@mantine/core";
import { createFileRoute } from "@tanstack/react-router";

// Phase 1 of the DAT-339 pivot wires this up via the Drizzle metadata
// client (`src/db/metadata/`) — a typed read straight against the engine's
// ws_<workspace_id> schema, no REST round-trip. Kept as a placeholder for
// now so the nav link in __root.tsx still resolves.

export const Route = createFileRoute("/sources")({ component: Sources });

function Sources() {
	return (
		<Stack p="xl" gap="md">
			<Title order={1}>Sources</Title>
			<Text c="dimmed">
				Registered in the workspace via the engine substrate.
			</Text>
			<Alert color="gray" title="Coming soon">
				The sources list is rewired to the Drizzle metadata client in Phase 1 of
				the DAT-339 pivot.
			</Alert>
		</Stack>
	);
}

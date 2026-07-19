import { Alert, Stack, Text, Title } from "@mantine/core";
import { createFileRoute } from "@tanstack/react-router";

// Was /sources. Phase 1 of the DAT-339 pivot rewires this to the Drizzle
// metadata client (`src/db/metadata/`) — a typed read straight against the
// engine's ws_<workspace_id> schema, no REST round-trip.

export const Route = createFileRoute("/(app)/library")({
	component: LibrarySection,
});

function LibrarySection() {
	return (
		<Stack gap="md">
			<Title order={2}>Library</Title>
			<Text c="dimmed" size="sm">
				Sources registered in the workspace via the engine substrate.
			</Text>
			<Alert color="gray" title="Coming soon">
				The sources list is rewired to the Drizzle metadata client in Phase 1 of
				the DAT-339 pivot.
			</Alert>
		</Stack>
	);
}

import { Stack, Text, Title } from "@mantine/core";
import { createFileRoute } from "@tanstack/react-router";

export const Route = createFileRoute("/")({ component: Home });

function Home() {
	return (
		<Stack p="xl" gap="md">
			<Title order={1}>DataRaum Cockpit</Title>
			<Text c="dimmed">
				Scaffold ready. Read surfaces (sources, tables, snippets) land in Phase
				1 of the DAT-339 pivot — wired via the Drizzle metadata client straight
				into the engine substrate.
			</Text>
		</Stack>
	);
}

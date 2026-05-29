import { createFileRoute } from "@tanstack/react-router";
import { SectionPlaceholder } from "#/ui/section-placeholder";

export const Route = createFileRoute("/(app)/workspace/$wsId/metadata")({
	component: MetadataSection,
});

function MetadataSection() {
	return (
		<SectionPlaceholder title="Metadata">
			Tables, columns, concepts and relationships read from the engine's
			ws_&lt;id&gt; schema via the Drizzle metadata client.
		</SectionPlaceholder>
	);
}

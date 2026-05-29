import { createFileRoute } from "@tanstack/react-router";
import { SectionPlaceholder } from "#/ui/section-placeholder";

export const Route = createFileRoute("/(app)/workspace/$wsId/cockpit")({
	component: CockpitSection,
});

function CockpitSection() {
	return (
		<SectionPlaceholder title="Cockpit">
			Agentic cockpit (DAT-347) — the three-region chat / canvas / inspector
			view lands here.
		</SectionPlaceholder>
	);
}

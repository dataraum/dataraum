import { createFileRoute } from "@tanstack/react-router";
import { SectionPlaceholder } from "#/ui/section-placeholder";

export const Route = createFileRoute("/(app)/workspace/$wsId/governance")({
	component: GovernanceSection,
});

function GovernanceSection() {
	return (
		<SectionPlaceholder title="Governance">
			Entropy contracts, teach/replay history and operating-model policy.
		</SectionPlaceholder>
	);
}

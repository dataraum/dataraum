import { createFileRoute } from "@tanstack/react-router";
import { SectionPlaceholder } from "#/ui/section-placeholder";

// Global (not workspace-scoped) — app-wide preferences and connections.

export const Route = createFileRoute("/(app)/settings")({
	component: SettingsSection,
});

function SettingsSection() {
	return (
		<SectionPlaceholder title="Settings">
			Global preferences, workspace connections and account.
		</SectionPlaceholder>
	);
}

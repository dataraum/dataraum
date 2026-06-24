import { Card, Group, SimpleGrid, Stack, Text, Title } from "@mantine/core";
import { createFileRoute, Link } from "@tanstack/react-router";
import { createServerFn } from "@tanstack/react-start";
import { Library } from "lucide-react";
import { resolveActiveWorkspace } from "#/db/cockpit/registry";
import { listReports } from "#/db/cockpit/reports";
import { BandBadge } from "#/ui/cockpit/widgets/band-badge";

// The reports gallery (DAT-624) — a workspace's library of minted widgets. Each
// card links to the detail view, which re-runs the frozen SQL live. The list is
// bounded server-side (REPORTS_LIMIT); cards are lightweight, so the page scrolls.
//
// The loader's server fn is defined inline (the cockpit route convention) so the
// plugin strips its cockpit_db handler from the client bundle.

const loadReports = createServerFn({ method: "GET" }).handler(async () => {
	const workspaceId = await resolveActiveWorkspace();
	return listReports(workspaceId);
});

export const Route = createFileRoute("/(app)/workspace/$wsId/reports/")({
	loader: () => loadReports(),
	component: ReportsGallery,
});

function ReportsGallery() {
	const reports = Route.useLoaderData();
	const { wsId } = Route.useParams();

	return (
		<Stack p="md" gap="md" data-testid="reports-gallery">
			<Group gap="xs">
				<Library size={20} />
				<Title order={3}>Reports</Title>
			</Group>

			{reports.length === 0 ? (
				<Text c="dimmed" size="sm" data-testid="reports-empty">
					No reports yet. Open an answer and choose “Report” to mint one — it
					freezes the query and re-runs it live whenever you open the report.
				</Text>
			) : (
				<SimpleGrid cols={{ base: 1, sm: 2, lg: 3 }} spacing="md">
					{reports.map((r) => (
						<Card
							key={r.id}
							withBorder
							padding="md"
							data-testid="report-card"
							renderRoot={(props) => (
								<Link
									to="/workspace/$wsId/reports/$reportId"
									params={{ wsId, reportId: r.id }}
									{...props}
								/>
							)}
						>
							<Stack gap="xs">
								<Group justify="space-between" wrap="nowrap" gap="xs">
									<Text fw={600} lineClamp={1}>
										{r.title}
									</Text>
									<BandBadge band={r.confidence.band} />
								</Group>
								<Text size="sm" c="dimmed" lineClamp={3}>
									{r.summary}
								</Text>
								<Text size="xs" c="dimmed">
									{/* Deterministic UTC date — `toLocaleDateString()` differs between the
									    server (container locale) and the browser, which trips React's
									    hydration check (#418). */}
									{new Date(r.createdAt).toISOString().slice(0, 10)}
								</Text>
							</Stack>
						</Card>
					))}
				</SimpleGrid>
			)}
		</Stack>
	);
}

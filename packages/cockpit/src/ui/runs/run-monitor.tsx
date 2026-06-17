// Native run monitor (DAT-550) — a workspace-wide view of stage runs, replacing
// the external Temporal-UI iframe. Pure render over the loader's runs (cockpit
// React rule 12: widgets render persisted values, they don't recompute). The
// list is BOUNDED by the loader's `limit` (newest-first); a full page discloses
// the cap rather than dumping an unbounded set into the DOM (the UI quality bar).

import { Anchor, Badge, Group, Stack, Table, Text, Title } from "@mantine/core";
import type { WorkspaceRun } from "#/db/cockpit/runs";
import { formatStartedAt, stageLabel, statusTone } from "#/ui/runs/run-row";

export interface RunMonitorProps {
	runs: ReadonlyArray<WorkspaceRun>;
	/** The query bound — when `runs.length === limit` the view discloses the cap. */
	limit: number;
	/** Deep-link to the raw Temporal Web UI (kept for debugging). */
	temporalUiUrl: string;
}

export function RunMonitor({ runs, limit, temporalUiUrl }: RunMonitorProps) {
	return (
		<Stack gap="md" h="100%" data-testid="run-monitor">
			<Group justify="space-between" align="flex-end">
				<Stack gap="xs">
					<Title order={2}>Runs</Title>
					<Text c="dimmed" size="sm">
						Stage runs across this workspace — onboarding, sessions, and the
						operating model. Updates as the orchestration worker advances them.
					</Text>
				</Stack>
				<Anchor href={temporalUiUrl} target="_blank" rel="noreferrer" size="sm">
					Open Temporal UI
				</Anchor>
			</Group>

			{runs.length === 0 ? (
				<Text c="dimmed" size="sm" data-testid="run-monitor-empty">
					No runs yet — onboard a source or start a session to see runs here.
				</Text>
			) : (
				<>
					<Table highlightOnHover stickyHeader>
						<Table.Thead>
							<Table.Tr>
								<Table.Th>Stage</Table.Th>
								<Table.Th>Status</Table.Th>
								<Table.Th>Started</Table.Th>
								<Table.Th>Workflow</Table.Th>
							</Table.Tr>
						</Table.Thead>
						<Table.Tbody>
							{runs.map((run) => (
								<Table.Tr
									key={`${run.workflowId}:${run.runId}`}
									data-testid="run-monitor-row"
								>
									<Table.Td>{stageLabel(run.stage)}</Table.Td>
									<Table.Td>
										<Badge
											size="sm"
											variant="light"
											color={statusTone(run.status)}
											data-testid="run-status"
										>
											{run.status}
										</Badge>
									</Table.Td>
									<Table.Td>
										<Text size="sm" c="dimmed">
											{formatStartedAt(run.startedAt)}
										</Text>
									</Table.Td>
									<Table.Td>
										<Text size="xs" c="dimmed" ff="monospace" truncate="end">
											{run.workflowId}
										</Text>
									</Table.Td>
								</Table.Tr>
							))}
						</Table.Tbody>
					</Table>
					{runs.length >= limit && (
						<Text c="dimmed" size="xs" data-testid="run-monitor-capped">
							Showing the latest {limit} runs.
						</Text>
					)}
				</>
			)}
		</Stack>
	);
}

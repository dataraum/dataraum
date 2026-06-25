// The Governance section render (DAT-633) — a standing, read-only "state of the
// union" over the WorkspaceBriefing (DAT-632). Pure render of engine-persisted
// values (cockpit React rule 12): it colors, groups and paginates, never
// recomputes analysis. Actions are deep-links the route turns into a seeded Stage
// chat (the page has no chat context of its own).
//
// Governance AGGREGATES and POINTS, it does not duplicate other routes: the data
// inventory is a band summary + the full SOURCE-QUALIFIED table list (paginated,
// never capped — table names repeat across sources, so the source is always
// shown), and operating-model artifacts are a count that links to the Model route.

import {
	Anchor,
	Badge,
	Group,
	Pagination,
	Stack,
	Table,
	Text,
	Title,
} from "@mantine/core";

import type { StageStatus, WorkspaceBriefing } from "#/db/metadata/briefing";
import { BandBadge } from "#/ui/cockpit/widgets/band-badge";
import {
	readinessDrillSeed,
	tableDrillSeed,
} from "#/ui/governance/governance-target";
import { usePaged } from "#/ui/governance/use-paged";

const PAGE_SIZE = 15;

export interface GovernanceOverviewProps {
	briefing: WorkspaceBriefing;
	/** Open a Stage chat seeded with this prompt (drill / replay). */
	onDrill: (seed: string) => void;
	/** Apply pending teaches — opens a seeded Stage chat asking to replay. */
	onReplay: () => void;
	/** Open the Model route (operating-model detail). */
	onOpenModel: () => void;
}

function plural(n: number, one: string, many: string): string {
	return n === 1 ? one : many;
}

const STAGE_STATUS_TONE: Record<StageStatus, string> = {
	empty: "gray",
	in_progress: "blue",
	ready: "green",
	needs_attention: "yellow",
};

const STAGE_STATUS_LABEL: Record<StageStatus, string> = {
	empty: "Not started",
	in_progress: "Running",
	ready: "Ready",
	needs_attention: "Needs attention",
};

// Friendly singular/plural per artifact type for the operating-model summary.
const ARTIFACT_LABEL: Record<string, [string, string]> = {
	metric: ["metric", "metrics"],
	business_cycle: ["cycle", "cycles"],
	validation: ["validation", "validations"],
};

function StageStatusBadge({ status }: { status: StageStatus }) {
	return (
		<Badge variant="light" color={STAGE_STATUS_TONE[status]} size="sm">
			{STAGE_STATUS_LABEL[status]}
		</Badge>
	);
}

function ProgressRow({ briefing }: { briefing: WorkspaceBriefing }) {
	const stages: { label: string; status: StageStatus }[] = [
		{ label: "Connect", status: briefing.progress.connect },
		{ label: "Stage", status: briefing.progress.stage },
		{ label: "Analyse", status: briefing.progress.analyse },
	];
	return (
		<Group gap="lg" data-testid="governance-progress">
			{stages.map((s) => (
				<Group key={s.label} gap="xs">
					<Text size="sm" fw={500}>
						{s.label}
					</Text>
					<StageStatusBadge status={s.status} />
				</Group>
			))}
		</Group>
	);
}

function Pager({
	page,
	totalPages,
	setPage,
}: {
	page: number;
	totalPages: number;
	setPage: (p: number) => void;
}) {
	if (totalPages <= 1) return null;
	return (
		<Group justify="flex-end">
			<Pagination
				total={totalPages}
				value={page}
				onChange={setPage}
				size="sm"
			/>
		</Group>
	);
}

function BlockersTable({
	briefing,
	onDrill,
}: Pick<GovernanceOverviewProps, "briefing" | "onDrill">) {
	const { page, setPage, totalPages, pageItems } = usePaged(
		briefing.attention.readinessBlockers,
		PAGE_SIZE,
	);
	if (briefing.attention.readinessBlockers.length === 0) return null;
	return (
		<Stack gap="xs">
			<Table data-testid="governance-blockers">
				<Table.Thead>
					<Table.Tr>
						<Table.Th>Blocked</Table.Th>
						<Table.Th>Source</Table.Th>
						<Table.Th>Band</Table.Th>
						<Table.Th>Top driver</Table.Th>
					</Table.Tr>
				</Table.Thead>
				<Table.Tbody>
					{pageItems.map((b) => (
						<Table.Tr key={b.target} data-testid="governance-blocker-row">
							<Table.Td>
								<Anchor
									component="button"
									type="button"
									size="sm"
									ff="monospace"
									onClick={() =>
										onDrill(readinessDrillSeed(b.target, b.source, b.label))
									}
								>
									{b.label}
								</Anchor>
							</Table.Td>
							<Table.Td>
								<Text size="sm" c="dimmed">
									{b.source || "—"}
								</Text>
							</Table.Td>
							<Table.Td>
								<BandBadge band={b.band} />
							</Table.Td>
							<Table.Td>
								<Text size="sm" c="dimmed">
									{b.topDriver ?? "—"}
								</Text>
							</Table.Td>
						</Table.Tr>
					))}
				</Table.Tbody>
			</Table>
			<Pager page={page} totalPages={totalPages} setPage={setPage} />
		</Stack>
	);
}

function AttentionSection({
	briefing,
	onDrill,
	onReplay,
	onOpenModel,
}: GovernanceOverviewProps) {
	const { attention } = briefing;
	const stuck = attention.stuckArtifacts;
	const nothing =
		attention.readinessBlockers.length === 0 &&
		stuck.total === 0 &&
		attention.awaitingInput.length === 0 &&
		!attention.pendingTeaches.needsReplay;

	const stuckPhrase = stuck.byType
		.map(({ type, count }) => {
			const [one, many] = ARTIFACT_LABEL[type] ?? [type, `${type}s`];
			return `${count} ${plural(count, one, many)}`;
		})
		.join(" · ");

	return (
		<Stack gap="sm" data-testid="governance-attention">
			<Title order={3}>Needs attention</Title>

			{nothing ? (
				<Text c="dimmed" size="sm" data-testid="governance-attention-clear">
					{attention.columnsInvestigate > 0
						? `Nothing urgent — ${attention.columnsInvestigate} ${plural(attention.columnsInvestigate, "column", "columns")} to investigate.`
						: "Nothing needs your attention."}
				</Text>
			) : (
				<>
					{attention.pendingTeaches.needsReplay && (
						<Group
							justify="space-between"
							data-testid="governance-pending-teaches"
						>
							<Text size="sm">
								{attention.pendingTeaches.count} teach
								{attention.pendingTeaches.count === 1 ? "" : "es"} pending — not
								yet applied to this workspace.
							</Text>
							<Anchor
								component="button"
								type="button"
								size="sm"
								onClick={onReplay}
							>
								Replay
							</Anchor>
						</Group>
					)}

					{attention.awaitingInput.length > 0 && (
						<Stack gap={4} data-testid="governance-awaiting">
							{attention.awaitingInput.map((item) => (
								<Group key={item.workflowId} gap="xs">
									<Badge size="sm" variant="light" color="orange">
										Needs you
									</Badge>
									<Text size="sm" c="dimmed">
										{item.note ?? `A ${item.stage} run is waiting on you.`}
									</Text>
								</Group>
							))}
						</Stack>
					)}

					{stuck.total > 0 && (
						<Group gap="xs" data-testid="governance-stuck">
							<Text size="sm">
								Operating model — {stuckPhrase} need grounding.
							</Text>
							<Anchor
								component="button"
								type="button"
								size="sm"
								onClick={onOpenModel}
							>
								Model
							</Anchor>
						</Group>
					)}

					<BlockersTable briefing={briefing} onDrill={onDrill} />
				</>
			)}
		</Stack>
	);
}

function InventorySection({
	briefing,
	onDrill,
}: Pick<GovernanceOverviewProps, "briefing" | "onDrill">) {
	const { sourceCount, tableCount, bandCounts, tables } = briefing.inventory;
	const { page, setPage, totalPages, pageItems } = usePaged(tables, PAGE_SIZE);
	return (
		<Stack gap="sm" data-testid="governance-inventory">
			<Title order={3}>Data</Title>
			<Text size="sm" data-testid="governance-inventory-summary">
				{sourceCount} {plural(sourceCount, "source", "sources")} · {tableCount}{" "}
				{plural(tableCount, "table", "tables")} · {bandCounts.ready} ready,{" "}
				{bandCounts.investigate} investigate, {bandCounts.blocked} blocked
				{bandCounts.unknown > 0 ? `, ${bandCounts.unknown} not analyzed` : ""}.
			</Text>
			<Table highlightOnHover data-testid="governance-inventory-table">
				<Table.Thead>
					<Table.Tr>
						<Table.Th>Table</Table.Th>
						<Table.Th>Source</Table.Th>
						<Table.Th>Readiness</Table.Th>
					</Table.Tr>
				</Table.Thead>
				<Table.Tbody>
					{pageItems.map((t) => (
						<Table.Tr key={t.tableId} data-testid="governance-inventory-row">
							<Table.Td>
								<Anchor
									component="button"
									type="button"
									size="sm"
									onClick={() => onDrill(tableDrillSeed(t.source, t.name))}
								>
									{t.name}
								</Anchor>
							</Table.Td>
							<Table.Td>
								<Text size="sm" c="dimmed">
									{t.source || "—"}
								</Text>
							</Table.Td>
							<Table.Td>
								<BandBadge band={t.band} />
							</Table.Td>
						</Table.Tr>
					))}
				</Table.Tbody>
			</Table>
			<Pager page={page} totalPages={totalPages} setPage={setPage} />
		</Stack>
	);
}

export function GovernanceOverview(props: GovernanceOverviewProps) {
	const { briefing } = props;
	const vertical = briefing.workspace.vertical;

	return (
		<Stack gap="lg" h="100%" data-testid="governance-overview">
			<Stack gap="xs">
				<Title order={2}>Governance</Title>
				<Text c="dimmed" size="sm">
					The workspace state of the union — what's processed, what needs
					action, and what's available
					{vertical ? ` · vertical ${vertical}` : ""}.
				</Text>
			</Stack>

			{/* Truly-empty is "nothing imported" (connect === "empty"), NOT
			    "inventory tableCount === 0" — raw tables can exist before readiness
			    has run, and showing "No data yet" while Connect reads Ready would
			    contradict itself. When imported, render the page; the inventory
			    honestly shows 0 tables until readiness lands. */}
			{briefing.progress.connect === "empty" ? (
				<Text c="dimmed" size="sm" data-testid="governance-empty">
					No data yet — import sources in a Connect chat to populate the
					workspace.
				</Text>
			) : (
				<>
					<ProgressRow briefing={briefing} />
					<AttentionSection {...props} />
					<InventorySection briefing={briefing} onDrill={props.onDrill} />
				</>
			)}
		</Stack>
	);
}

// Workspace Briefing canvas (DAT-634) — the LANDING orientation for a fresh
// stage/analyse chat. Pure render of the briefing (cockpit React rule 12): it
// projects client-side (`projectBriefing`) to foreground THIS chat's actions and
// points at the others. Chips dispatch through the action context — a foreground
// chip seeds a turn into this chat; a background pointer switches chat type.
// Yields to any live tool canvas on the first turn; the durable view is Governance.

import { Badge, Button, Group, Stack, Text, Title } from "@mantine/core";

// Import the PURE project/types modules directly — NOT the `#/db/metadata/briefing`
// barrel, which re-exports build.ts (the postgres IO). A client widget pulling the
// barrel would drag `metadataDb` (postgres-at-import) into the client bundle.
import { projectBriefing } from "#/db/metadata/briefing/project";
import type { StageStatus } from "#/db/metadata/briefing/types";
import type { CanvasState } from "#/ui/cockpit/canvas-state";
import { useCockpitActions } from "#/ui/cockpit/cockpit-state";
import { nextActionSeed } from "#/ui/cockpit/widgets/briefing-seeds";

const STAGE_TONE: Record<StageStatus, string> = {
	empty: "gray",
	in_progress: "blue",
	ready: "green",
	needs_attention: "yellow",
	// A handled-but-invalid terminal state (DAT-845) — orange sets it apart from
	// needs_attention's yellow while still reading as "needs a human".
	nothing_declared: "orange",
};
const STAGE_LABEL: Record<StageStatus, string> = {
	empty: "Not started",
	in_progress: "Running",
	ready: "Ready",
	needs_attention: "Needs attention",
	nothing_declared: "No operating model",
};

const CHAT_LABEL = { connect: "Connect", stage: "Stage", analyse: "Analyse" };

function plural(n: number, one: string, many: string): string {
	return n === 1 ? one : many;
}

export function BriefingWidget({
	state,
}: {
	state: Extract<CanvasState, { kind: "briefing" }>;
}) {
	const { briefing, chatKind } = state;
	const { sendMessage, typeNav } = useCockpitActions();
	const projected = projectBriefing(briefing, chatKind);
	const a = briefing.attention;

	// Compact attention summary — the facts, not the full Governance detail.
	const facts: string[] = [];
	if (a.columnsBlocked > 0)
		facts.push(
			`${a.columnsBlocked} ${plural(a.columnsBlocked, "column", "columns")} blocked`,
		);
	if (a.columnsInvestigate > 0)
		facts.push(`${a.columnsInvestigate} to investigate`);
	if (a.stuckArtifacts.total > 0)
		facts.push(
			`${a.stuckArtifacts.total} operating-model ${plural(a.stuckArtifacts.total, "item", "items")} need grounding`,
		);
	if (a.pendingTeaches.needsReplay)
		facts.push(
			`${a.pendingTeaches.count} ${plural(a.pendingTeaches.count, "teach", "teaches")} pending`,
		);

	const progress: { label: string; status: StageStatus }[] = [
		{ label: "Connect", status: briefing.progress.connect },
		{ label: "Stage", status: briefing.progress.stage },
		{ label: "Analyse", status: briefing.progress.analyse },
	];

	return (
		<Stack gap="md" h="100%" p="md" data-testid="canvas-briefing">
			<Stack gap={4}>
				<Title order={4}>Where you are</Title>
				<Text c="dimmed" size="sm">
					{briefing.workspace.vertical
						? `Workspace vertical ${briefing.workspace.vertical}. `
						: ""}
					Pick up where it makes sense — the full picture is in Governance.
				</Text>
			</Stack>

			<Group gap="md">
				{progress.map((s) => (
					<Group key={s.label} gap={6}>
						<Text size="sm" fw={500}>
							{s.label}
						</Text>
						<Badge variant="light" color={STAGE_TONE[s.status]} size="sm">
							{STAGE_LABEL[s.status]}
						</Badge>
					</Group>
				))}
			</Group>

			{facts.length > 0 && (
				<Text size="sm" data-testid="briefing-facts">
					{facts.join(" · ")}.
				</Text>
			)}

			{projected.foreground.length > 0 && (
				<Stack gap="xs" data-testid="briefing-foreground">
					<Text size="xs" c="dimmed" tt="uppercase" fw={600}>
						Do next
					</Text>
					<Group gap="xs">
						{projected.foreground.map((action) => (
							<Button
								key={`${action.kind}:${action.label}`}
								size="xs"
								variant="light"
								onClick={() => sendMessage(nextActionSeed(action))}
							>
								{action.label}
							</Button>
						))}
					</Group>
				</Stack>
			)}

			{projected.background.length > 0 && typeNav && (
				<Stack gap="xs" data-testid="briefing-background">
					<Text size="xs" c="dimmed" tt="uppercase" fw={600}>
						Elsewhere
					</Text>
					<Group gap="xs">
						{projected.background.map((ptr) => (
							<Button
								key={ptr.chat}
								size="xs"
								variant="subtle"
								onClick={() => typeNav.onOpen(ptr.chat)}
							>
								{CHAT_LABEL[ptr.chat]}: {ptr.label} →
							</Button>
						))}
					</Group>
				</Stack>
			)}

			{facts.length === 0 &&
				projected.foreground.length === 0 &&
				projected.background.length === 0 && (
					<Text c="dimmed" size="sm" data-testid="briefing-clear">
						Nothing needs attention — ask away in this {CHAT_LABEL[chatKind]}{" "}
						chat.
					</Text>
				)}
		</Stack>
	);
}

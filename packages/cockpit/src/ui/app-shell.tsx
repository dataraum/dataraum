// OUTER SHELL (DAT-380, C0).
//
// Thin chrome around the active section: a slim left rail of the six section
// icons + a thin top bar (workspace-switcher placeholder + a ⌘K hint). Uses
// Mantine AppShell. Every dimension / color reads from src/ui/theme.ts — no
// hardcoded px/hex here.

import {
	ActionIcon,
	AppShell,
	Group,
	Stack,
	Text,
	Tooltip,
	UnstyledButton,
} from "@mantine/core";
import { useDisclosure, useHotkeys } from "@mantine/hooks";
import { Link, useParams } from "@tanstack/react-router";
import type { ReactNode } from "react";
import { CockpitHeaderNav } from "#/ui/cockpit/cockpit-header-nav";
import { CommandPalette } from "#/ui/command-palette";
import { type Section, sections } from "#/ui/sections";
import { tokens } from "#/ui/theme";

/**
 * One rail icon. Branches on global vs workspace-scoped so each `Link` carries
 * concrete typed `to`/`params` (TanStack Router can't type-check a spread
 * union of link props). `wsId` is always defined (the shell falls back to the
 * active workspace), so workspace links resolve even from global routes like
 * /settings instead of dropping to `/`.
 */
function RailItem({ section, wsId }: { section: Section; wsId: string }) {
	const Icon = section.icon;
	const inner = <Icon size={20} aria-hidden />;
	const common = {
		variant: "subtle" as const,
		size: "lg" as const,
		"aria-label": section.label,
		"data-testid": `rail-${section.id}`,
	};

	return (
		<Tooltip label={section.label} position="right" withArrow>
			{section.global ? (
				<ActionIcon
					{...common}
					renderRoot={(props) => (
						<Link
							to={section.to}
							activeProps={{ "data-active": "true" }}
							{...props}
						/>
					)}
				>
					{inner}
				</ActionIcon>
			) : (
				<ActionIcon
					{...common}
					renderRoot={(props) => (
						<Link
							to={section.to}
							params={{ wsId }}
							activeProps={{ "data-active": "true" }}
							{...props}
						/>
					)}
				>
					{inner}
				</ActionIcon>
			)}
		</Tooltip>
	);
}

export function CockpitShell({
	children,
	activeWorkspaceId,
}: {
	children: ReactNode;
	activeWorkspaceId: string;
}) {
	const [paletteOpened, palette] = useDisclosure(false);
	useHotkeys([["mod+K", () => palette.open()]]);

	// wsId from the current route when it's workspace-scoped (`strict: false`
	// lets the shell mount above routes that have none, e.g. /settings), else
	// the active workspace. Always defined, so the rail's workspace links resolve
	// even from a global route instead of falling back to "/".
	const params = useParams({ strict: false });
	const routeWsId = (params as { wsId?: string }).wsId;
	const wsId = routeWsId ?? activeWorkspaceId;

	return (
		<>
			<AppShell
				header={{ height: tokens.shell.topBarHeight }}
				navbar={{ width: tokens.shell.railWidth, breakpoint: 0 }}
				padding="md"
			>
				<AppShell.Header>
					{/* Wordmark left; the cockpit chat-type switcher right (DAT-542 —
					    renders only on cockpit routes, nothing elsewhere). The ⌘K trigger
					    moved to the lower-left rail corner. */}
					<Group h="100%" px="md" justify="space-between" wrap="nowrap">
						<UnstyledButton data-testid="workspace-switcher">
							{/* Brand wordmark — never the raw workspace UUID. A real workspace
							    name lands with the workspaces registry (DAT-339 slice 1). */}
							<Text size="sm" fw={600} c="text">
								DataRaum
							</Text>
						</UnstyledButton>
						<CockpitHeaderNav />
					</Group>
				</AppShell.Header>

				<AppShell.Navbar p="xs">
					{/* The section icons sit at the top; the ⌘K command-palette trigger
					    anchors to the lower-left corner (DAT-542). */}
					<Stack h="100%" gap="xs" align="center" justify="space-between">
						<Stack gap="xs" align="center" data-testid="section-rail">
							{sections.map((section) => (
								<RailItem key={section.id} section={section} wsId={wsId} />
							))}
						</Stack>
						<Tooltip label="Command palette" position="right" withArrow>
							<ActionIcon
								variant="default"
								onClick={palette.open}
								aria-label="Open command palette"
								data-testid="command-hint"
							>
								<Text size="xs" c="dimmed">
									⌘K
								</Text>
							</ActionIcon>
						</Tooltip>
					</Stack>
				</AppShell.Navbar>

				<AppShell.Main>{children}</AppShell.Main>
			</AppShell>

			<CommandPalette opened={paletteOpened} onClose={palette.close} />
		</>
	);
}

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
import { CommandPalette } from "#/ui/command-palette";
import { type Section, sections } from "#/ui/sections";
import { tokens } from "#/ui/theme";

/**
 * One rail icon. Branches on global vs workspace-scoped so each `Link` carries
 * concrete typed `to`/`params` (TanStack Router can't type-check a spread
 * union of link props). Without an active workspace the workspace links fall
 * back to `/`, which redirects to the active workspace's cockpit.
 */
function RailItem({
	section,
	wsId,
}: {
	section: Section;
	wsId: string | undefined;
}) {
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
			) : wsId ? (
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
			) : (
				<ActionIcon
					{...common}
					renderRoot={(props) => <Link to="/" {...props} />}
				>
					{inner}
				</ActionIcon>
			)}
		</Tooltip>
	);
}

export function CockpitShell({ children }: { children: ReactNode }) {
	const [paletteOpened, palette] = useDisclosure(false);
	useHotkeys([["mod+K", () => palette.open()]]);

	// Active workspace, if the current route is workspace-scoped. `strict: false`
	// lets the shell mount above routes that have no wsId param (e.g. /settings).
	const params = useParams({ strict: false });
	const wsId = (params as { wsId?: string }).wsId;

	return (
		<>
			<AppShell
				header={{ height: tokens.shell.topBarHeight }}
				navbar={{ width: tokens.shell.railWidth, breakpoint: 0 }}
				padding="md"
			>
				<AppShell.Header>
					<Group h="100%" px="md" justify="space-between" wrap="nowrap">
						<UnstyledButton data-testid="workspace-switcher">
							<Text size="sm" fw={600} c="text">
								{wsId ? `Workspace ${wsId}` : "DataRaum"}
							</Text>
						</UnstyledButton>
						<Tooltip label="Command palette">
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
					</Group>
				</AppShell.Header>

				<AppShell.Navbar p="xs">
					<Stack gap="xs" align="center" data-testid="section-rail">
						{sections.map((section) => (
							<RailItem key={section.id} section={section} wsId={wsId} />
						))}
					</Stack>
				</AppShell.Navbar>

				<AppShell.Main>{children}</AppShell.Main>
			</AppShell>

			<CommandPalette opened={paletteOpened} onClose={palette.close} />
		</>
	);
}

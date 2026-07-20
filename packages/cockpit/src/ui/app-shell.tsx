// OUTER SHELL (DAT-380).
//
// Thin chrome around the active section: a slim left rail of the six section
// icons + a thin top bar (the workspace switcher, DAT-821 + a ⌘K hint). Uses
// Mantine AppShell. Every dimension / color reads from src/ui/theme.ts — no
// hardcoded px/hex here.

import {
	ActionIcon,
	AppShell,
	Group,
	Stack,
	Text,
	Tooltip,
} from "@mantine/core";
import { useDisclosure, useHotkeys } from "@mantine/hooks";
import { Link } from "@tanstack/react-router";
import type { ReactNode } from "react";
import { CommandPalette } from "#/ui/command-palette";
import { RunRailBadge } from "#/ui/runs/run-rail-badge";
import { type Section, sections } from "#/ui/sections";
import { tokens } from "#/ui/theme";
import { WorkspaceSwitcher } from "#/ui/workspace-switcher";

/** One rail icon. Every section is a fixed flat path (DAT-822) — one cockpit
 * per workspace, so no link carries params. */
function RailItem({ section }: { section: Section }) {
	const Icon = section.icon;
	// The Runs rail icon carries a badge — a yellow "Needs you (N)" count when the
	// grounding loop parked runs for a human (DAT-553), else a processing dot while
	// the workspace has in-flight runs (DAT-550). Polled tab-independently.
	const icon = <Icon size={20} aria-hidden />;
	const inner =
		section.id === "workflows" ? <RunRailBadge>{icon}</RunRailBadge> : icon;

	return (
		<Tooltip label={section.label} position="right" withArrow>
			<ActionIcon
				variant="subtle"
				size="lg"
				aria-label={section.label}
				data-testid={`rail-${section.id}`}
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
		</Tooltip>
	);
}

export function CockpitShell({ children }: { children: ReactNode }) {
	const [paletteOpened, palette] = useDisclosure(false);
	useHotkeys([["mod+K", () => palette.open()]]);

	return (
		<>
			<AppShell
				header={{ height: tokens.shell.topBarHeight }}
				navbar={{ width: tokens.shell.railWidth, breakpoint: 0 }}
				padding="md"
			>
				<AppShell.Header>
					{/* The workspace switcher left (DAT-821); the ⌘K command-palette
					    trigger top-right. The chat-type nav lives in the composer
					    drop-up (cockpit-only), so the header is global chrome only. */}
					<Group h="100%" px="md" justify="space-between" wrap="nowrap">
						<WorkspaceSwitcher />
						<Tooltip label="Command palette" position="bottom" withArrow>
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
					{/* The six section icons. */}
					<Stack gap="xs" align="center" data-testid="section-rail">
						{sections.map((section) => (
							<RailItem key={section.id} section={section} />
						))}
					</Stack>
				</AppShell.Navbar>

				<AppShell.Main>{children}</AppShell.Main>
			</AppShell>

			<CommandPalette opened={paletteOpened} onClose={palette.close} />
		</>
	);
}

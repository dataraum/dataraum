// The workspace switcher (DAT-821) — the top-bar control that replaced the
// static wordmark placeholder. Quiet chrome in the chat-switcher's idiom: a
// flat target showing WHERE YOU ARE (the boot workspace's name), dropping
// into the user's workspaces from `memberships`:
//
//   - the current workspace is checked (and a no-op — you are here);
//   - every other `ready` workspace is a plain <a> to its subdomain —
//     switching IS navigation, there is no client-side workspace state;
//   - `creating`/`archiving` are disabled IN PLACE with a state badge (the
//     menu shows lifecycle truth rather than hiding it); `archived` never
//     arrives from the server;
//   - "New workspace" leads to the portal's create flow.
//
// Until the memberships query resolves, the target renders the brand
// wordmark, menu-less: the shell never shows a raw workspace UUID. The query
// is client-gated (`typeof window`, the RunRailBadge idiom) because the shell
// is always-rendered chrome: during SSR the queryFn would fire outside the
// document request's server context, so the server renders the fallback and
// the client polls once hydrated.

import { Badge, Group, Menu, Text, UnstyledButton } from "@mantine/core";
import { useQuery } from "@tanstack/react-query";
import { Check, ChevronDown, Plus } from "lucide-react";
import { getSwitcherWorkspaces } from "#/server/switcher-workspaces";
import { tokens } from "#/ui/theme";

const targetStyle = {
	display: "inline-flex",
	alignItems: "center",
	gap: 6,
	padding: `2px ${tokens.spacing.xs}`,
	borderRadius: tokens.radii.sm,
	color: tokens.colors.text,
} as const;

export function WorkspaceSwitcher() {
	const { data } = useQuery({
		queryKey: ["switcher-workspaces"],
		queryFn: () => getSwitcherWorkspaces(),
		// Memberships/lifecycle move on human timescales; refresh when the menu's
		// tab regains focus (query default) rather than polling.
		staleTime: 30_000,
		enabled: typeof window !== "undefined",
	});

	if (!data) {
		return (
			<UnstyledButton data-testid="workspace-switcher" style={targetStyle}>
				<Text size="sm" fw={600} c="text">
					DataRaum
				</Text>
			</UnstyledButton>
		);
	}

	return (
		<Menu position="bottom-start" withArrow shadow="md" width={260}>
			<Menu.Target>
				<UnstyledButton
					data-testid="workspace-switcher"
					aria-label="Switch workspace"
					style={targetStyle}
				>
					<Text size="sm" fw={600} truncate maw={220}>
						{data.currentName}
					</Text>
					<ChevronDown size={14} aria-hidden color={tokens.colors.textMuted} />
				</UnstyledButton>
			</Menu.Target>
			<Menu.Dropdown>
				<Menu.Label>Workspaces</Menu.Label>
				{data.workspaces.map((workspace) =>
					workspace.url ? (
						// Ready, elsewhere: switching is a full navigation to the
						// workspace's subdomain (its own cockpit instance).
						<Menu.Item
							key={workspace.id}
							component="a"
							href={workspace.url}
							data-testid={`switcher-item-${workspace.id}`}
						>
							<Text size="sm" truncate>
								{workspace.name}
							</Text>
						</Menu.Item>
					) : (
						<Menu.Item
							key={workspace.id}
							disabled={!workspace.current}
							data-testid={`switcher-item-${workspace.id}`}
							leftSection={
								workspace.current ? <Check size={14} aria-hidden /> : undefined
							}
							rightSection={
								workspace.state !== "ready" ? (
									<Badge
										variant="light"
										size="xs"
										color={workspace.state === "creating" ? "yellow" : "gray"}
									>
										{workspace.state}
									</Badge>
								) : workspace.current ? undefined : (
									// Ready but unroutable: the bare host-dev seed has no
									// subdomain — reachable only on its direct port.
									<Text size="xs" c="dimmed">
										no subdomain
									</Text>
								)
							}
						>
							<Group gap={6} wrap="nowrap">
								<Text size="sm" fw={workspace.current ? 600 : 400} truncate>
									{workspace.name}
								</Text>
							</Group>
						</Menu.Item>
					),
				)}
				<Menu.Divider />
				<Menu.Item
					component="a"
					href={data.createUrl}
					leftSection={<Plus size={14} aria-hidden />}
					data-testid="switcher-new-workspace"
				>
					New workspace
				</Menu.Item>
			</Menu.Dropdown>
		</Menu>
	);
}

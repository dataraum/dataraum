// The chat-type switcher (DAT-533; reshaped for the composer drop-up). It folds
// the three types — Connect · Stage · Analyse — into ONE quiet control that sits
// in the composer's bottom row (like a model selector), NOT as header chrome: a
// flat target showing the current kind, that drops UP into a menu of the types +
// "New chat". Language is the primary nav (the composer + the landing nav-agent);
// this is the light indicator / manual switch.
//
//   - the target shows the ACTIVE chat's kind (the where-am-I hint) + a chevron;
//   - a type item opens that kind (resume-latest-or-create — the route decides);
//   - an unavailable type is disabled IN PLACE with its reason (not removed, so
//     the menu's shape is stable);
//   - "New chat" forces a FRESH chat of the active kind (vs the resume an item does).
//
// Pure + presentational: it takes availability + active kind + two callbacks, so
// it unit-tests without a router or cockpit_db. The route (cockpit/$conversationId)
// wires the callbacks to the resume/create server-fns + navigation and hands this
// down through the provider as `typeNav`.

import { Menu, Text, UnstyledButton } from "@mantine/core";
import {
	Cable,
	Check,
	ChevronUp,
	Layers,
	LineChart,
	type LucideIcon,
	Plus,
} from "lucide-react";
import type { ConversationKind } from "#/db/cockpit/conversations";
import type { ChatTypeAvailability } from "#/lib/chat-availability";
import { tokens } from "#/ui/theme";

const ICON: Record<ConversationKind, LucideIcon> = {
	connect: Cable,
	stage: Layers,
	analyse: LineChart,
};
const LABEL: Record<ConversationKind, string> = {
	connect: "Connect",
	stage: "Stage",
	analyse: "Analyse",
};

/** The nav wiring the route resolves (router-bound) and threads to the composer
 * through the provider. Presentational here — no router, no cockpit_db. */
export interface ChatTypeNav {
	availability: ReadonlyArray<ChatTypeAvailability>;
	/** The current chat's kind (the target label + the checked item), or null. */
	activeKind: ConversationKind | null;
	/** Open a type: resume its latest chat or create one if none. */
	onOpen: (kind: ConversationKind) => void;
	/** Force a fresh chat of the given kind ("New chat"). */
	onNew: (kind: ConversationKind) => void;
}

export function ChatSwitcher({
	availability,
	activeKind,
	onOpen,
	onNew,
}: ChatTypeNav) {
	const ActiveIcon = activeKind ? ICON[activeKind] : Cable;

	return (
		<Menu position="top-start" withArrow shadow="md" width={220}>
			<Menu.Target>
				<UnstyledButton
					data-testid="chat-switcher"
					aria-label="Chat type"
					style={{
						display: "inline-flex",
						alignItems: "center",
						gap: 6,
						padding: `2px ${tokens.spacing.xs}`,
						borderRadius: tokens.radii.sm,
						color: tokens.colors.textMuted,
					}}
				>
					<ActiveIcon size={16} aria-hidden />
					<Text size="xs" fw={500}>
						{activeKind ? LABEL[activeKind] : "Chat type"}
					</Text>
					<ChevronUp size={14} aria-hidden />
				</UnstyledButton>
			</Menu.Target>

			<Menu.Dropdown>
				{availability.map(({ kind, available, reason }) => {
					const Icon = ICON[kind];
					const isActive = kind === activeKind;
					// The ACTIVE chat's type is always enabled — you're in it, so it can't
					// be "unavailable" (no checked-yet-disabled contradiction). Disabling
					// applies only to NON-active types that aren't startable yet.
					const enabled = available || isActive;
					return (
						<Menu.Item
							key={kind}
							data-testid={`switch-${kind}`}
							data-active={isActive ? "true" : undefined}
							data-available={enabled ? "true" : "false"}
							aria-disabled={!enabled}
							disabled={!enabled}
							leftSection={<Icon size={16} aria-hidden />}
							rightSection={
								isActive ? <Check size={14} aria-hidden /> : undefined
							}
							onClick={() => {
								if (enabled) onOpen(kind);
							}}
						>
							<Text size="sm">{LABEL[kind]}</Text>
							{!enabled && reason && (
								<Text size="xs" c="dimmed">
									{reason}
								</Text>
							)}
						</Menu.Item>
					);
				})}

				{activeKind !== null && (
					<>
						<Menu.Divider />
						<Menu.Item
							data-testid="switch-new"
							leftSection={<Plus size={16} aria-hidden />}
							onClick={() => onNew(activeKind)}
						>
							New {LABEL[activeKind]} chat
						</Menu.Item>
					</>
				)}
			</Menu.Dropdown>
		</Menu>
	);
}

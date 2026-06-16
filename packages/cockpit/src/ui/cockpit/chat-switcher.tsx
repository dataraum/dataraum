// The 3-icon chat-type switcher (DAT-533) — Connect · Stage · Analyse, in the
// cockpit top strip. The active chat's kind is highlighted (the where-am-I hint);
// clicking a type opens it (resume-latest-or-create — the layout decides which);
// an unavailable type dims IN PLACE with a tooltip reason (not removed, so the
// switcher's shape is stable). A "+" beside them forces a FRESH chat of the
// active kind (vs the resume that a type-icon click does) — shown only inside a
// chat, since the history landing already has type chips to create from.
//
// Pure + presentational: it takes the availability + active kind + two callbacks,
// so it unit-tests without a router or cockpit_db. The layout (cockpit/route.tsx)
// wires the callbacks to the resume/create server-fns + navigation.

import { ActionIcon, Group, Tooltip } from "@mantine/core";
import { Cable, Layers, LineChart, type LucideIcon, Plus } from "lucide-react";
import type { ConversationKind } from "#/db/cockpit/conversations";
import type { ChatTypeAvailability } from "#/lib/chat-availability";

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

export function ChatSwitcher({
	availability,
	activeKind,
	onOpen,
	onNew,
}: {
	availability: ReadonlyArray<ChatTypeAvailability>;
	/** The current chat's kind (highlighted), or null on the history landing. */
	activeKind: ConversationKind | null;
	/** Open a type: resume its latest chat or create one if none. */
	onOpen: (kind: ConversationKind) => void;
	/** Force a fresh chat of the given kind (the "+"). */
	onNew: (kind: ConversationKind) => void;
}) {
	return (
		<Group gap="xs" data-testid="chat-switcher">
			{availability.map(({ kind, available, reason }) => {
				const Icon = ICON[kind];
				const isActive = kind === activeKind;
				// The ACTIVE chat's type is always enabled — you're in it, so it can't
				// be "unavailable" (no highlighted-yet-dimmed contradiction). Dimming
				// applies only to NON-active types that aren't startable yet.
				const enabled = available || isActive;
				return (
					<Tooltip
						key={kind}
						label={enabled ? LABEL[kind] : reason}
						position="bottom"
						withArrow
					>
						{/* Unavailable (non-active): dimmed + non-navigating, but still
						    hoverable so the tooltip reason shows (so NOT the `disabled`
						    prop, which kills pointer events). The click is guarded. */}
						<ActionIcon
							data-testid={`switch-${kind}`}
							data-active={isActive ? "true" : undefined}
							data-available={enabled ? "true" : "false"}
							aria-label={LABEL[kind]}
							aria-disabled={!enabled}
							variant={isActive ? "filled" : "subtle"}
							size="lg"
							style={
								enabled ? undefined : { opacity: 0.4, cursor: "not-allowed" }
							}
							onClick={() => {
								if (enabled) onOpen(kind);
							}}
						>
							<Icon size={18} aria-hidden />
						</ActionIcon>
					</Tooltip>
				);
			})}
			{activeKind !== null && (
				<Tooltip
					label={`New ${LABEL[activeKind]} chat`}
					position="bottom"
					withArrow
				>
					<ActionIcon
						data-testid="switch-new"
						aria-label={`New ${LABEL[activeKind]} chat`}
						variant="subtle"
						size="lg"
						onClick={() => onNew(activeKind)}
					>
						<Plus size={18} aria-hidden />
					</ActionIcon>
				</Tooltip>
			)}
		</Group>
	);
}

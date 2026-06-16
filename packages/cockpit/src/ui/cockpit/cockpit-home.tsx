// Cockpit home (DAT-528) — the landing + chat history, shown at /cockpit.
//
// A chat now has a TYPE chosen up front (connect | stage | analyse) that binds
// its toolstack (the binding is S2). So the entry point is no longer one free-text
// composer: it's the recent history plus type chips that mint a typed chat and
// deep-link into it. The Haiku entry-router that infers the type from a sentence
// is S4 (DAT-534); for now the user picks. A chat cannot change type once created,
// and there is no jump between types within a chat (DD/36667393).
//
// Pure + props-driven: it renders OUTSIDE a CockpitProvider (no conversation yet),
// so it takes `conversations` + `onOpen`/`onCreate` rather than reading chat state.

import {
	Badge,
	Group,
	Stack,
	Text,
	Title,
	UnstyledButton,
} from "@mantine/core";
import type {
	ConversationKind,
	ConversationSummary,
} from "#/db/cockpit/conversations";
import { tokens } from "#/ui/theme";

/** The three chat types, with the one-line "what this is for" the chip shows. */
const CHAT_TYPES: ReadonlyArray<{
	kind: ConversationKind;
	label: string;
	blurb: string;
}> = [
	{
		kind: "connect",
		label: "Connect",
		blurb: "Bring in data — choose a vertical and add sources.",
	},
	{
		kind: "stage",
		label: "Stage",
		blurb: "Teach the model and run a session over your typed tables.",
	},
	{
		kind: "analyse",
		label: "Analyse",
		blurb: "Ask questions across everything that's been staged.",
	},
];

/** Human label for a kind badge in the history list. */
const KIND_LABEL: Record<ConversationKind, string> = {
	connect: "Connect",
	stage: "Stage",
	analyse: "Analyse",
};

export function CockpitHome({
	conversations,
	onOpen,
	onCreate,
}: {
	conversations: ReadonlyArray<ConversationSummary>;
	/** Open an existing chat by id. */
	onOpen: (conversationId: string) => void;
	/** Mint a new typed chat and open it. */
	onCreate: (kind: ConversationKind) => void;
}) {
	return (
		<Stack
			align="center"
			gap="xl"
			py="xl"
			px="md"
			data-testid="cockpit-home"
			style={{ height: "100%", overflowY: "auto" }}
		>
			<Stack align="center" gap="xs" maw={680}>
				<Title order={1} ta="center">
					Start a chat
				</Title>
				<Text c="dimmed" ta="center" size="lg">
					Pick what you want to do. Each chat is one focused thread — connect
					data, stage a session, or analyse what's ready.
				</Text>
			</Stack>

			{/* Type chips — each mints a typed chat and navigates into it. */}
			<Group gap="md" justify="center" wrap="wrap" maw={760}>
				{CHAT_TYPES.map((t) => (
					<UnstyledButton
						key={t.kind}
						onClick={() => onCreate(t.kind)}
						data-testid={`new-chat-${t.kind}`}
						style={{
							flex: "1 1 14rem",
							maxWidth: "16rem",
							borderRadius: tokens.radii.md,
							borderWidth: 1,
							borderStyle: "solid",
							borderColor: tokens.colors.border,
							backgroundColor: tokens.colors.surface,
							padding: tokens.spacing.md,
						}}
					>
						<Stack gap={4}>
							<Text fw={600}>{t.label}</Text>
							<Text size="sm" c="dimmed">
								{t.blurb}
							</Text>
						</Stack>
					</UnstyledButton>
				))}
			</Group>

			{/* Recent history — bounded server-side; resume by opening one. */}
			<Stack w="100%" maw={760} gap="xs">
				<Text size="sm" fw={600} c="dimmed">
					Recent
				</Text>
				{conversations.length === 0 ? (
					<Text size="sm" c="dimmed" data-testid="history-empty">
						No chats yet — start one above.
					</Text>
				) : (
					conversations.map((c) => (
						<UnstyledButton
							key={c.id}
							onClick={() => onOpen(c.id)}
							data-testid="history-item"
							style={{
								borderRadius: tokens.radii.sm,
								borderWidth: 1,
								borderStyle: "solid",
								borderColor: tokens.colors.border,
								backgroundColor: tokens.colors.surface,
								padding: `${tokens.spacing.xs} ${tokens.spacing.sm}`,
							}}
						>
							<Group justify="space-between" wrap="nowrap" gap="sm">
								<Text size="sm" truncate="end">
									{c.title ?? "Untitled chat"}
								</Text>
								<Badge size="sm" variant="light" data-testid="history-kind">
									{KIND_LABEL[c.kind]}
								</Badge>
							</Group>
						</UnstyledButton>
					))
				)}
			</Stack>
		</Stack>
	);
}

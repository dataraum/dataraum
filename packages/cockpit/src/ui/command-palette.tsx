// ⌘K command palette scaffold (DAT-380).
//
// A placeholder overlay opened by cmd/ctrl+K. The real command list lands
// later; for now it documents the affordance and proves the hotkey wiring.
// We use a plain Mantine Modal rather than @mantine/spotlight to avoid adding
// a dependency before the command surface is designed.

import { Kbd, Modal, Stack, Text } from "@mantine/core";

export function CommandPalette({
	opened,
	onClose,
}: {
	opened: boolean;
	onClose: () => void;
}) {
	return (
		<Modal
			opened={opened}
			onClose={onClose}
			title="Command palette"
			centered
			size="lg"
			data-testid="command-palette"
		>
			<Stack gap="sm">
				<Text c="dimmed" size="sm">
					Quick actions and navigation land here. Press <Kbd>Esc</Kbd> to close.
				</Text>
				<Text c="dimmed" size="sm">
					Open any time with <Kbd>⌘</Kbd> <Kbd>K</Kbd>.
				</Text>
			</Stack>
		</Modal>
	);
}

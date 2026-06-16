// In-chat readiness banner (DAT-534) — a greyed, NON-BLOCKING strip shown at the
// top of a chat when its kind can't act yet (do-X-first / wait-for-Y). Advisory:
// it never disables the composer. Rendered only when `chatReadiness` returns a
// signal; a ready chat shows nothing.

import { Alert } from "@mantine/core";
import type { ChatReadiness } from "#/lib/chat-readiness";

export function ChatReadinessBanner({
	readiness,
}: {
	readiness: ChatReadiness;
}) {
	return (
		<Alert
			variant="light"
			color="gray"
			radius="sm"
			py="xs"
			data-testid="chat-readiness"
			data-tone={readiness.tone}
		>
			{readiness.message}
		</Alert>
	);
}

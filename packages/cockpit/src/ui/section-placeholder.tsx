// Thin placeholder for a stub section page (DAT-380, C0). Reads theme tokens
// only — no hardcoded px/hex. Real section content lands in later tickets.

import { Stack, Text, Title } from "@mantine/core";
import type { ReactNode } from "react";

export function SectionPlaceholder({
	title,
	children,
}: {
	title: string;
	children: ReactNode;
}) {
	return (
		<Stack gap="md">
			<Title order={2}>{title}</Title>
			<Text c="dimmed" size="sm">
				{children}
			</Text>
		</Stack>
	);
}

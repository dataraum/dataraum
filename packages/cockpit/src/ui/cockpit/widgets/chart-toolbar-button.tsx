// The result-grid toolbar's chart affordance (DAT-626) — a button that opens the
// chart authoring modal, sitting LEFT of the Report / View-SQL actions. Owns the
// modal open state; the AUTHORED config is owned by the caller (the answer surface
// holds it so minting can freeze it), so this is a thin controlled wrapper.

import { Button } from "@mantine/core";
import { ChartColumnBig } from "lucide-react";
import { useState } from "react";
import type { ChartConfig } from "#/charts/chart-config";
import { ChartModal } from "#/ui/cockpit/widgets/chart-modal";

export function ChartToolbarButton({
	sql,
	params,
	value,
	onChange,
}: {
	sql: string;
	params?: (string | number | boolean | null)[];
	/** The currently-attached chart config (controlled by the caller), or null. */
	value: ChartConfig | null;
	/** Set/clear the attached chart config. */
	onChange: (config: ChartConfig | null) => void;
}) {
	const [opened, setOpened] = useState(false);
	return (
		<>
			<Button
				variant={value ? "light" : "subtle"}
				color={value ? "blue" : "gray"}
				size="compact-xs"
				leftSection={<ChartColumnBig size={13} />}
				data-testid="chart-open"
				onClick={() => setOpened(true)}
			>
				{value ? "Chart ✓" : "Chart"}
			</Button>
			<ChartModal
				opened={opened}
				onClose={() => setOpened(false)}
				sql={sql}
				params={params}
				value={value}
				onAccept={onChange}
			/>
		</>
	);
}

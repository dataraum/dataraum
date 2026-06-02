// @vitest-environment happy-dom
//
// Render tests for the WorkspaceInventoryWidget (DAT-349). A plain Mantine table
// (workspace metadata — bounded, not a result set), so rows render under
// happy-dom. We assert the per-table rows + provenance + readiness badge, the
// empty state, the in-widget SourceCard drill-in (local — no agent round-trip),
// and the two click-throughs that DO route through the chat loop (table-name →
// look_table, Refresh → list_tables). The live DB read is smoke-covered.

import { MantineProvider } from "@mantine/core";
import {
	cleanup,
	fireEvent,
	render,
	screen,
	within,
} from "@testing-library/react";
import { afterEach, describe, expect, it, vi } from "vitest";

import type { InventoryTable } from "#/tools/list-tables";
import { WorkspaceInventoryWidget } from "#/ui/cockpit/widgets/workspace-inventory";
import { theme } from "#/ui/theme";

// The widget reads the chat-loop hook from the cockpit context. Mock it so the
// render tests don't need a CockpitProvider and the click tests can observe the
// dispatched request.
const sendChatMessage = vi.fn();
vi.mock("#/ui/cockpit/cockpit-state", () => ({
	useCockpit: () => ({ sendChatMessage }),
}));

function renderWidget(tables: InventoryTable[]) {
	render(
		<MantineProvider theme={theme} env="test">
			<WorkspaceInventoryWidget
				state={{ kind: "workspace-inventory", tables }}
			/>
		</MantineProvider>,
	);
}

function table(overrides: Partial<InventoryTable> = {}): InventoryTable {
	return {
		table_id: "t_orders",
		table_name: "orders",
		layer: "typed",
		row_count: 1000,
		column_count: 5,
		source_id: "s_sales",
		source_name: "sales.csv",
		source_type: "file",
		source_backend: "duckdb",
		source_status: "ready",
		analyzed: true,
		worst_band: "blocked",
		readiness: { ready: 3, investigate: 1, blocked: 1, unanalyzed: 0 },
		...overrides,
	};
}

// Two sources: sales (orders + items) and crm (users, not yet analyzed).
const inventory: InventoryTable[] = [
	table(),
	table({
		table_id: "t_items",
		table_name: "items",
		row_count: 50,
		column_count: 2,
		worst_band: "ready",
		readiness: { ready: 2, investigate: 0, blocked: 0, unanalyzed: 0 },
	}),
	table({
		table_id: "t_users",
		table_name: "users",
		row_count: 200,
		column_count: 4,
		source_id: "s_crm",
		source_name: "crm",
		source_type: "database",
		source_backend: "postgres",
		source_status: null,
		analyzed: false,
		worst_band: null,
		readiness: { ready: 0, investigate: 0, blocked: 0, unanalyzed: 4 },
	}),
];

describe("WorkspaceInventoryWidget (DAT-349)", () => {
	afterEach(() => {
		cleanup();
		sendChatMessage.mockClear();
	});

	it("renders a row per table with provenance + a readiness band", () => {
		renderWidget(inventory);
		expect(screen.getByTestId("canvas-workspace-inventory")).toBeTruthy();
		expect(screen.getByTestId("inventory-row-t_orders")).toBeTruthy();
		expect(screen.getByTestId("inventory-row-t_items")).toBeTruthy();
		expect(screen.getByTestId("inventory-row-t_users")).toBeTruthy();

		// The analyzed table shows its worst band; the un-analyzed one shows a dash,
		// never a band, so "not measured" doesn't read as "ready".
		const ordersRow = within(screen.getByTestId("inventory-row-t_orders"));
		expect(ordersRow.getByText("blocked")).toBeTruthy();
		expect(ordersRow.getByText(/sales\.csv/)).toBeTruthy();
		const usersRow = within(screen.getByTestId("inventory-row-t_users"));
		expect(usersRow.queryByText("ready")).toBeNull();
		expect(usersRow.queryByText("blocked")).toBeNull();
	});

	it("renders the empty state when there are no tables", () => {
		renderWidget([]);
		expect(screen.getByTestId("canvas-workspace-inventory-empty")).toBeTruthy();
	});

	it("table-name click routes a look_table request through the chat loop", () => {
		renderWidget(inventory);
		fireEvent.click(screen.getByTestId("inventory-table-t_orders"));
		expect(sendChatMessage).toHaveBeenCalledTimes(1);
		const text = sendChatMessage.mock.calls[0][0] as string;
		expect(text).toContain("t_orders");
		expect(text).toContain("orders");
		expect(text).toContain("look_table");
	});

	it("Refresh re-lists the inventory through the chat loop", () => {
		renderWidget(inventory);
		fireEvent.click(screen.getByTestId("inventory-refresh"));
		expect(sendChatMessage).toHaveBeenCalledTimes(1);
		expect(sendChatMessage.mock.calls[0][0]).toContain("list_tables");
	});

	it("clicking a source badge opens the SourceCard with that source's tables — no agent round-trip", () => {
		renderWidget(inventory);
		// No card until a badge is clicked.
		expect(screen.queryByTestId("inventory-source-card")).toBeNull();

		// The sales badge appears on both its rows; click the first.
		fireEvent.click(screen.getAllByTestId("inventory-source-badge-s_sales")[0]);

		const card = within(screen.getByTestId("inventory-source-card"));
		expect(card.getByText("sales.csv")).toBeTruthy();
		expect(card.getByText("duckdb")).toBeTruthy();
		// Both sales tables are listed; the crm table is NOT.
		expect(card.getByText("orders")).toBeTruthy();
		expect(card.getByText("items")).toBeTruthy();
		expect(card.queryByText("users")).toBeNull();
		// Local navigation — it must NOT spend an agent turn.
		expect(sendChatMessage).not.toHaveBeenCalled();
	});

	it("closing the SourceCard returns to the bare inventory", () => {
		renderWidget(inventory);
		fireEvent.click(screen.getAllByTestId("inventory-source-badge-s_crm")[0]);
		expect(screen.getByTestId("inventory-source-card")).toBeTruthy();
		fireEvent.click(screen.getByTestId("inventory-source-card-close"));
		expect(screen.queryByTestId("inventory-source-card")).toBeNull();
	});
});

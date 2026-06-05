// @vitest-environment jsdom
//
// Render tests for the WorkspaceInventoryWidget (DAT-349). A plain Mantine table
// (workspace metadata — bounded, not a result set), so rows render under
// jsdom. We assert the per-table rows + provenance + readiness badge, the
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
const sendMessage = vi.fn();
vi.mock("#/ui/cockpit/cockpit-state", () => ({
	useCockpitActions: () => ({ sendMessage }),
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

// Fixtures mirror the list_tables PROJECTION (DAT-433): `table_name` is the
// display form (digest prefix already stripped in the tool), the raw DuckDB
// name rides in `physical_name`, and an upload's `source_name` is the uploaded
// FILE's name (the content-keyed `src_<digest>` source name is never emitted).
function table(overrides: Partial<InventoryTable> = {}): InventoryTable {
	return {
		table_id: "t_orders",
		table_name: "orders",
		physical_name: "src_aaa__orders",
		layer: "typed",
		row_count: 1000,
		column_count: 5,
		source_id: "src_aaa",
		source_name: "orders.csv",
		source_type: "csv",
		source_backend: null,
		analyzed: true,
		worst_band: "blocked",
		readiness: { ready: 3, investigate: 1, blocked: 1, unanalyzed: 0 },
		...overrides,
	};
}

// Two UPLOADED FILES — each its own content-keyed source — plus one db_recipe
// CONNECTION. The two uploads must collapse under ONE "Uploads" group.
const inventory: InventoryTable[] = [
	table(), // orders.csv → orders
	table({
		table_id: "t_items",
		table_name: "items",
		physical_name: "src_bbb__items",
		source_id: "src_bbb",
		source_name: "items.csv",
		row_count: 50,
		column_count: 2,
		worst_band: "ready",
		readiness: { ready: 2, investigate: 0, blocked: 0, unanalyzed: 0 },
	}),
	table({
		table_id: "t_users",
		table_name: "users",
		physical_name: "crm__users",
		source_id: "s_crm",
		source_name: "crm",
		source_type: "db_recipe",
		source_backend: "postgres",
		row_count: 200,
		column_count: 4,
		analyzed: false,
		worst_band: null,
		readiness: { ready: 0, investigate: 0, blocked: 0, unanalyzed: 4 },
	}),
];

describe("WorkspaceInventoryWidget (DAT-349)", () => {
	afterEach(() => {
		cleanup();
		sendMessage.mockClear();
	});

	it("renders a row per table with provenance + a readiness band", () => {
		renderWidget(inventory);
		expect(screen.getByTestId("canvas-workspace-inventory")).toBeTruthy();
		expect(screen.getByTestId("inventory-row-t_orders")).toBeTruthy();
		expect(screen.getByTestId("inventory-row-t_items")).toBeTruthy();
		expect(screen.getByTestId("inventory-row-t_users")).toBeTruthy();

		// The analyzed table shows its worst band (title-cased); the un-analyzed one
		// shows a dash, never a band, so "not measured" doesn't read as "ready".
		const ordersRow = within(screen.getByTestId("inventory-row-t_orders"));
		expect(ordersRow.getByText("Blocked")).toBeTruthy();
		// The uploaded file shows under the "Uploads" umbrella — never its src_<digest>.
		expect(ordersRow.getByText("Uploads")).toBeTruthy();
		expect(ordersRow.queryByText(/src_aaa/)).toBeNull();
		const usersRow = within(screen.getByTestId("inventory-row-t_users"));
		expect(usersRow.queryByText(/ready/i)).toBeNull();
		expect(usersRow.queryByText(/blocked/i)).toBeNull();
	});

	it("collapses content-keyed uploads under ONE 'Uploads' badge — the digest is never shown (DAT-424)", () => {
		renderWidget(inventory);
		// Two uploaded files = two content-keyed sources → ONE shared "Uploads" badge
		// (one per upload row), NOT two hash-named peer source badges.
		const uploadBadges = screen.getAllByTestId(
			"inventory-source-badge-uploads",
		);
		expect(uploadBadges).toHaveLength(2);
		expect(uploadBadges[0].textContent).toBe("Uploads");
		// The connection keeps its named origin.
		expect(
			screen.getByTestId("inventory-source-badge-s_crm").textContent,
		).toContain("crm");
		// No `src_<digest>` hash anywhere in the inventory (AC1).
		expect(
			screen.getByTestId("canvas-workspace-inventory").textContent,
		).not.toContain("src_");
	});

	it("collapses raw/typed/quarantine layers into one row and surfaces quarantine as a modal-opening red count", () => {
		renderWidget([
			table({
				table_id: "raw1",
				layer: "raw",
				row_count: 1000,
				worst_band: null,
			}),
			table({
				table_id: "typed1",
				layer: "typed",
				row_count: 998,
				worst_band: "investigate",
			}),
			table({
				table_id: "q1",
				layer: "quarantine",
				row_count: 2,
				worst_band: null,
			}),
		]);
		// One row — the typed representative; the raw/quarantine layers don't get
		// their own rows.
		expect(screen.getByTestId("inventory-row-typed1")).toBeTruthy();
		expect(screen.queryByTestId("inventory-row-raw1")).toBeNull();
		expect(screen.queryByTestId("inventory-row-q1")).toBeNull();
		// The display name shows (the digest only ever rides in physical_name).
		const row = within(screen.getByTestId("inventory-row-typed1"));
		expect(row.getByText("orders")).toBeTruthy();
		// The quarantine count opens the detail modal.
		fireEvent.click(screen.getByTestId("inventory-quarantine-typed1"));
		expect(screen.getByTestId("modal-quarantine-count").textContent).toContain(
			"2",
		);
		// The modal is a third source-display surface — it must also show the group
		// ("Uploads"), never the `src_<digest>` hash.
		const modal = within(screen.getByTestId("inventory-detail-modal"));
		expect(modal.getByText("Uploads")).toBeTruthy();
		expect(modal.queryByText(/src_/)).toBeNull();
	});

	it("renders the empty state when there are no tables", () => {
		renderWidget([]);
		expect(screen.getByTestId("canvas-workspace-inventory-empty")).toBeTruthy();
	});

	it("table-name click routes a look_table request through the chat loop", () => {
		renderWidget(inventory);
		fireEvent.click(screen.getByTestId("inventory-table-t_orders"));
		expect(sendMessage).toHaveBeenCalledTimes(1);
		const text = sendMessage.mock.calls[0][0] as string;
		expect(text).toContain("t_orders");
		expect(text).toContain("orders");
		expect(text).toContain("look_table");
	});

	it("Refresh re-lists the inventory through the chat loop", () => {
		renderWidget(inventory);
		fireEvent.click(screen.getByTestId("inventory-refresh"));
		expect(sendMessage).toHaveBeenCalledTimes(1);
		expect(sendMessage.mock.calls[0][0]).toContain("list_tables");
	});

	it("clicking a source badge opens the SourceCard with that source's tables — no agent round-trip", () => {
		renderWidget(inventory);
		// No card until a badge is clicked.
		expect(screen.queryByTestId("inventory-source-card")).toBeNull();

		// The two uploads share ONE "Uploads" badge (one per upload row); click it.
		fireEvent.click(screen.getAllByTestId("inventory-source-badge-uploads")[0]);

		const card = within(screen.getByTestId("inventory-source-card"));
		expect(card.getByText("Uploads")).toBeTruthy();
		expect(card.getByText("uploaded files")).toBeTruthy();
		// Both uploaded files are listed; the connection's table is NOT.
		expect(card.getByText("orders")).toBeTruthy();
		expect(card.getByText("items")).toBeTruthy();
		expect(card.queryByText("users")).toBeNull();
		// Local navigation — it must NOT spend an agent turn.
		expect(sendMessage).not.toHaveBeenCalled();
	});

	it("closing the SourceCard returns to the bare inventory", () => {
		renderWidget(inventory);
		fireEvent.click(screen.getAllByTestId("inventory-source-badge-s_crm")[0]);
		expect(screen.getByTestId("inventory-source-card")).toBeTruthy();
		fireEvent.click(screen.getByTestId("inventory-source-card-close"));
		expect(screen.queryByTestId("inventory-source-card")).toBeNull();
	});

	it("switches the SourceCard to a different source on a new badge click", () => {
		renderWidget(inventory);
		fireEvent.click(screen.getAllByTestId("inventory-source-badge-uploads")[0]);
		expect(
			within(screen.getByTestId("inventory-source-card")).getByText("Uploads"),
		).toBeTruthy();
		// Click the connection's badge — the card body switches to the named origin.
		fireEvent.click(screen.getAllByTestId("inventory-source-badge-s_crm")[0]);
		const card = within(screen.getByTestId("inventory-source-card"));
		expect(card.getByText("crm")).toBeTruthy();
		expect(card.getByText("users")).toBeTruthy();
		expect(card.queryByText("orders")).toBeNull();
	});

	it("SourceCard aggregates readiness totals across the group's tables", () => {
		renderWidget(inventory);
		fireEvent.click(screen.getAllByTestId("inventory-source-badge-uploads")[0]);
		const card = within(screen.getByTestId("inventory-source-card"));
		// Uploads = orders {r3,i1,b1} + items {r2} → 5 ready, 1 investigate, 1 blocked.
		expect(card.getByText("5 ready")).toBeTruthy();
		expect(card.getByText("1 investigate")).toBeTruthy();
		expect(card.getByText("1 blocked")).toBeTruthy();
	});

	it("caps the master list and shows an overflow tail past the row limit", () => {
		const many: InventoryTable[] = Array.from({ length: 120 }, (_, i) =>
			table({ table_id: `t_${i}`, table_name: `tbl_${i}` }),
		);
		renderWidget(many);
		// The first 100 render; the 101st does not.
		expect(screen.getByTestId("inventory-row-t_0")).toBeTruthy();
		expect(screen.getByTestId("inventory-row-t_99")).toBeTruthy();
		expect(screen.queryByTestId("inventory-row-t_100")).toBeNull();
		const tail = screen.getByTestId("inventory-overflow");
		expect(tail.textContent).toContain("20 more");
	});
});

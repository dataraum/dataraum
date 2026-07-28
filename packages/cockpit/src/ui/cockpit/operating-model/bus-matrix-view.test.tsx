// @vitest-environment jsdom
import { MantineProvider } from "@mantine/core";
import { cleanup, render, screen } from "@testing-library/react";
import { afterEach, describe, expect, it } from "vitest";

import { type BusMatrixRow, buildBusMatrix } from "#/tools/bus-matrix";
import { theme } from "#/ui/theme";
import { BusMatrixView } from "./bus-matrix-view";

// The view is rendered from the REAL builder rather than hand-written view models:
// drillability and ordering are the builder's decisions, and asserting the rendered
// result of a fabricated matrix would prove nothing about what a practitioner sees.

function ref(
	fact: string,
	role: string,
	group: string,
	over: Partial<BusMatrixRow> = {},
) {
	return {
		factTableId: fact,
		attachment: "referenced",
		conceptLabel: "accounts",
		dimensionTableId: "t_dim",
		roles: [role],
		attributes: [],
		confirmationSource: "judge",
		conformedGroup: group,
		needsConfirmation: false,
		signature: `bus:referenced:${fact}:t_dim:${role}`,
		...over,
	};
}

const TABLES = [
	{ tableId: "t_gl", tableName: "gl_entries" },
	{ tableId: "t_ap", tableName: "ap_balances" },
];

function renderMatrix(cells: BusMatrixRow[]) {
	render(
		<MantineProvider theme={theme} env="test">
			<BusMatrixView matrix={buildBusMatrix({ cells, tables: TABLES })} />
		</MantineProvider>,
	);
}

afterEach(cleanup);

describe("BusMatrixView (DAT-740)", () => {
	it("shows an explained empty state, not a blank pane", () => {
		renderMatrix([]);
		const empty = screen.getByTestId("bus-matrix-empty");
		expect(empty.textContent).toContain("derived during analysis");
	});

	it("renders the grid with a drillable axis marked", () => {
		renderMatrix([
			ref("t_gl", "account_id", "ref:t_dim:account_id"),
			ref("t_ap", "acct", "ref:t_dim:account_id"),
		]);
		expect(screen.getByTestId("bus-matrix-grid")).toBeTruthy();
		expect(screen.getByTestId("bus-axis-drillable")).toBeTruthy();
		// Both facts appear as rows even though they spell the dimension differently.
		expect(screen.getByText("gl_entries")).toBeTruthy();
		expect(screen.getByText("ap_balances")).toBeTruthy();
		expect(screen.getAllByTestId("bus-matrix-cell")).toHaveLength(2);
	});

	it("summarises how many dimensions can be drilled across", () => {
		renderMatrix([
			ref("t_gl", "account_id", "ref:t_dim:account_id"),
			ref("t_ap", "acct", "ref:t_dim:account_id"),
		]);
		const summary = screen.getByTestId("bus-matrix-summary");
		expect(summary.textContent).toContain("2 facts");
		expect(summary.textContent).toContain("1 can be drilled across");
		expect(summary.textContent).toContain("never joined to each other");
	});

	it("marks an unconfirmed pairing as not drillable", () => {
		renderMatrix([
			ref("t_gl", "account_id", "ref:t_dim:account_id", {
				confirmationSource: "unconfirmed",
			}),
			ref("t_ap", "acct", "ref:t_dim:account_id"),
		]);
		expect(screen.getByTestId("bus-axis-blocked")).toBeTruthy();
		expect(screen.queryByTestId("bus-axis-drillable")).toBeNull();
	});

	it("says so loudly when NOTHING is drillable", () => {
		// The state a practitioner most needs explained: the grid has content, but no
		// cross-fact comparison can be composed from it.
		renderMatrix([ref("t_gl", "account_id", "ref:t_dim:account_id")]);
		const alert = screen.getByTestId("bus-matrix-none-drillable");
		expect(alert.textContent).toContain("Confirming a dimension pairing");
	});

	it("shows an em dash where a fact does not carry a dimension", () => {
		renderMatrix([
			ref("t_gl", "account_id", "ref:t_dim:account_id"),
			ref("t_ap", "acct", "ref:t_dim:account_id"),
			ref("t_gl", "cost_centre", "ref:t_cc:cost_centre", {
				conceptLabel: "cost centres",
			}),
		]);
		// ap_balances carries no cost centre — that absence is rendered, not blank.
		expect(screen.getAllByText("—").length).toBeGreaterThan(0);
	});

	it("flags a cell awaiting review", () => {
		renderMatrix([
			ref("t_gl", "account_id", "ref:t_dim:account_id", {
				needsConfirmation: true,
			}),
			ref("t_ap", "acct", "ref:t_dim:account_id"),
		]);
		expect(screen.getByText("review")).toBeTruthy();
	});
});

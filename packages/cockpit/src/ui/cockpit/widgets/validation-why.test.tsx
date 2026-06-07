// @vitest-environment jsdom
//
// Render tests for the ValidationWhyWidget (DAT-440): the not-found state, the
// blocked reason as a first-class alert ("visibly impossible"), and the
// executed drill-down (verdict, message, SQL, grounded/details blocks). All
// values are the engine's persisted strings — the widget only formats.

import { MantineProvider } from "@mantine/core";
import { cleanup, render, screen } from "@testing-library/react";
import { afterEach, describe, expect, it } from "vitest";

import type { WhyValidationResult } from "#/tools/why-validation";
import { ValidationWhyWidget } from "#/ui/cockpit/widgets/validation-why";
import { theme } from "#/ui/theme";

function renderWidget(why: WhyValidationResult) {
	render(
		<MantineProvider theme={theme} env="test">
			<ValidationWhyWidget state={{ kind: "validation-why", why }} />
		</MantineProvider>,
	);
}

const EXECUTED: WhyValidationResult = {
	validation_id: "gl_invoice_match",
	found: true,
	state: "executed",
	state_reason: null,
	strictness: 0.8,
	grounded_against: JSON.stringify({ tables: ["invoices", "payments"] }),
	status: "executed",
	severity: "error",
	passed: false,
	message: "12 invoices have no matching journal entry",
	sql_used:
		"SELECT i.id FROM invoices i LEFT JOIN payments p ON i.id = p.invoice_id",
	executed_at: "2026-06-07T12:00:00.000Z",
	details: JSON.stringify({ failing_rows: 12 }),
	pending_teaches: 0,
};

const BLOCKED: WhyValidationResult = {
	...EXECUTED,
	state: "declared",
	state_reason:
		"Missing required tables: journal_entries and journal_lines are not in the session",
	status: null,
	passed: null,
	message: null,
	sql_used: null,
	executed_at: null,
	grounded_against: "",
	details: "",
};

afterEach(cleanup);

describe("ValidationWhyWidget (DAT-440)", () => {
	it("renders the not-found state", () => {
		renderWidget({ ...EXECUTED, found: false });
		expect(screen.getByTestId("canvas-validation-why-notfound")).toBeTruthy();
	});

	it("renders the blocked reason as a first-class alert (visibly impossible)", () => {
		renderWidget(BLOCKED);
		expect(
			screen.getByTestId("canvas-validation-why-reason").textContent,
		).toContain("Missing required tables: journal_entries");
		// Not executed → no verdict badge content, no SQL block.
		expect(screen.queryByTestId("canvas-validation-why-sql")).toBeNull();
		expect(screen.getByText("Declared")).toBeTruthy();
	});

	it("renders the executed drill-down: verdict, message, SQL, detail blocks", () => {
		renderWidget(EXECUTED);
		expect(screen.getByText("Gl invoice match")).toBeTruthy();
		expect(screen.getByText("Executed")).toBeTruthy();
		expect(screen.getByText("Failed")).toBeTruthy();
		expect(
			screen.getByTestId("canvas-validation-why-message").textContent,
		).toContain("12 invoices have no matching journal entry");
		expect(
			screen.getByTestId("canvas-validation-why-sql").textContent,
		).toContain("LEFT JOIN payments");
		expect(
			screen.getByTestId("canvas-validation-why-severity").textContent,
		).toContain("error");
		// The grounded/details JSON renders through EvidenceDetail (key→value
		// rows), not as a raw JSON dump.
		expect(screen.getByText("Grounded against")).toBeTruthy();
		expect(screen.getByText("Result details")).toBeTruthy();
		// EvidenceDetail renders `key: ` + value as separate nodes — match loosely.
		expect(screen.getByText(/failing_rows/)).toBeTruthy();
		// No reason alert when the validation executed.
		expect(screen.queryByTestId("canvas-validation-why-reason")).toBeNull();
	});

	it("surfaces the pending-teach note", () => {
		renderWidget({ ...EXECUTED, pending_teaches: 2 });
		expect(
			screen.getByTestId("canvas-validation-why-pending").textContent,
		).toContain("2 pending teaches");
	});
});

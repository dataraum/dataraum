// @vitest-environment jsdom
//
// Render tests for the ValidationListWidget (DAT-440): rows with humanized
// keys + state/verdict badges, the blocked reason readable in the row
// ("visibly impossible"), the not-run / empty states, the overflow cap
// (rule 15), and the why_validation click-through — the id in the model-only
// refs part, never the bubble (the relationship-list precedent).

import { MantineProvider } from "@mantine/core";
import { cleanup, fireEvent, render, screen } from "@testing-library/react";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";

import { isAgentRefsPart } from "#/lib/agent-refs";
import type { LookValidationResult } from "#/tools/look-validation";
import { ValidationListWidget } from "#/ui/cockpit/widgets/validation-list";
import { theme } from "#/ui/theme";

// The widget reads the chat-loop hook from the cockpit context. Mock it so the
// render tests don't need a CockpitProvider and the click test can observe the
// dispatched request.
const sendMessage = vi.fn();
vi.mock("#/ui/cockpit/cockpit-state", () => ({
	useCockpitActions: () => ({ sendMessage }),
}));

function renderWidget(look: LookValidationResult) {
	render(
		<MantineProvider theme={theme} env="test">
			<ValidationListWidget state={{ kind: "validation-list", look }} />
		</MantineProvider>,
	);
}

const EXECUTED = {
	validation_id: "gl_invoice_match",
	state: "executed",
	state_reason: null,
	severity: "error",
	status: "executed",
	passed: false,
	message: "12 invoices have no matching journal entry",
};

const BLOCKED = {
	validation_id: "trial_balance_zero",
	state: "declared",
	state_reason:
		"Missing required tables: journal_entries and journal_lines are not in the session",
	severity: null,
	status: null,
	passed: null,
	message: null,
};

const analyzed: LookValidationResult = {
	session_id: "sess-1",
	analyzed: true,
	pending_teaches: 0,
	validations: [EXECUTED, BLOCKED],
};

beforeEach(() => {
	sendMessage.mockClear();
});
afterEach(cleanup);

describe("ValidationListWidget (DAT-440)", () => {
	it("renders a row per validation with humanized key, state + verdict", () => {
		renderWidget(analyzed);
		expect(screen.getByTestId("canvas-validation-list")).toBeTruthy();
		expect(screen.getByText("Gl invoice match")).toBeTruthy();
		expect(screen.getByText("Trial balance zero")).toBeTruthy();
		// Lifecycle state + executed verdict are two distinct badges.
		expect(screen.getByText("Executed")).toBeTruthy();
		expect(screen.getByText("Declared")).toBeTruthy();
		expect(screen.getByText("Failed")).toBeTruthy();
		// No raw snake_case keys leak into the visible text.
		expect(document.body.textContent).not.toContain("gl_invoice_match");
		expect(document.body.textContent).not.toContain("sess-1");
	});

	it("keeps a blocked validation's reason readable IN the row (visibly impossible)", () => {
		renderWidget(analyzed);
		expect(
			screen.getByText(/Missing required tables: journal_entries/),
		).toBeTruthy();
	});

	it("shows the executed message as the row detail", () => {
		renderWidget(analyzed);
		expect(
			screen.getByText("12 invoices have no matching journal entry"),
		).toBeTruthy();
	});

	it("renders the not-run state pointing at the operating-model stage", () => {
		renderWidget({ ...analyzed, analyzed: false, validations: [] });
		expect(
			screen.getByTestId("canvas-validation-list-unanalyzed"),
		).toBeTruthy();
	});

	it("renders the empty state for a run that declared no validations", () => {
		renderWidget({ ...analyzed, validations: [] });
		expect(screen.getByTestId("canvas-validation-list-empty")).toBeTruthy();
	});

	it("caps rendered rows and shows the overflow tail (rule 15)", () => {
		const many = Array.from({ length: 120 }, (_, i) => ({
			...EXECUTED,
			validation_id: `check_${i}`,
		}));
		renderWidget({ ...analyzed, validations: many });
		// 100 rendered + the tail; never all 120.
		expect(screen.getAllByText("Executed")).toHaveLength(100);
		expect(
			screen.getByTestId("validation-list-overflow").textContent,
		).toContain("…and 20 more");
	});

	it("surfaces the pending-teach note", () => {
		renderWidget({ ...analyzed, pending_teaches: 1 });
		expect(
			screen.getByTestId("canvas-validation-list-pending").textContent,
		).toContain("1 pending teach");
	});

	it("click-through dispatches a why_validation request — the id in the refs part, never the bubble", () => {
		renderWidget(analyzed);
		fireEvent.click(screen.getByTestId("validation-why-gl_invoice_match"));
		expect(sendMessage).toHaveBeenCalledTimes(1);
		const turn = sendMessage.mock.calls[0][0] as {
			content: Array<{ type: "text"; content: string }>;
		};
		expect(turn.content).toHaveLength(2);
		const [bubble, refs] = turn.content;
		// The bubble: the humanized label + the tool name, NO internal ids.
		expect(bubble?.content).toContain("Gl invoice match");
		expect(bubble?.content).toContain("why_validation");
		expect(bubble?.content).not.toContain("gl_invoice_match");
		expect(bubble?.content).not.toContain("sess-1");
		// The refs part: marked model-only, key=value imperative form.
		expect(refs && isAgentRefsPart(refs.content)).toBe(true);
		expect(refs?.content).toContain("session_id=sess-1");
		expect(refs?.content).toContain("validation_id=gl_invoice_match");
		expect(refs?.content).toContain("Internal only");
	});
});

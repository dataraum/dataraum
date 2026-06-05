// @vitest-environment jsdom

// Rendered repro of the stuck tool-chip spinner (DAT-436) — reconstructs the
// REAL part lifecycle of a multi-poll agent turn in the rail and pins the chip
// states. The message fixtures mirror what the @tanstack/ai 0.26.1 client
// actually produces (verified by driving the real server chat() loop + client
// StreamProcessor):
//
//   - a completed call    → state "complete" + output            → no spinner
//   - an ERRORED call     → state "input-complete" + output.error
//                           (+ tool-result state "error")        → "failed",
//                           never a spinner (there is NO error ToolCallState)
//   - an orphaned call    → state "input-complete", no output, and the
//                           conversation moved on                → "failed"
//   - a live in-flight call (current turn)                       → spinner
//
// Lives in its own file (not chat-rail.test.tsx) so the DAT-437 lane's
// text-part work doesn't collide.

import { MantineProvider } from "@mantine/core";
import { cleanup, render, screen } from "@testing-library/react";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { ChatRail } from "#/ui/cockpit/chat-rail";
import { CockpitProvider } from "#/ui/cockpit/cockpit-state";

// Mock useChat at the SDK boundary — the test controls the message list and
// asserts OUR chip rendering (same harness shape as chat-rail.test.tsx).
const h = vi.hoisted(() => ({
	messages: [] as unknown[],
	isLoading: false,
	error: undefined as Error | undefined,
	sendMessage: vi.fn(),
	stop: vi.fn(),
	addToolApprovalResponse: vi.fn(),
}));

vi.mock("@tanstack/ai-react", () => ({
	useChat: () => ({
		messages: h.messages,
		isLoading: h.isLoading,
		error: h.error,
		sendMessage: h.sendMessage,
		stop: h.stop,
		addToolApprovalResponse: h.addToolApprovalResponse,
	}),
	fetchServerSentEvents: () => ({}),
}));

function renderRail() {
	return render(
		<MantineProvider env="test">
			<CockpitProvider>
				<ChatRail />
			</CockpitProvider>
		</MantineProvider>,
	);
}

beforeEach(() => {
	h.messages = [];
	h.isLoading = false;
	h.error = undefined;
});
afterEach(() => cleanup());

/** A Loader exists inside the given chip card? Mantine's Loader renders a
 * <span class="mantine-Loader-root">. */
function chipHasLoader(callId: string): boolean {
	const card = screen.getByTestId(`tool-call-${callId}`);
	return card.querySelector(".mantine-Loader-root") !== null;
}

describe("ChatRail chip terminal states (DAT-436)", () => {
	it("a completed call's chip stops spinning; an errored call shows failed — in the SAME turn", () => {
		// One agent turn: look_relationships completed; workflow_status errored
		// (the live-smoke transcript shape). The errored part is EXACTLY what the
		// SDK produces: state stays "input-complete", output carries { error },
		// and a sibling tool-result part has state "error".
		h.messages = [
			{ id: "u1", role: "user", parts: [{ type: "text", content: "check" }] },
			{
				id: "a1",
				role: "assistant",
				parts: [
					{
						type: "tool-call",
						id: "tc-ok",
						name: "look_relationships",
						state: "complete",
						arguments: "{}",
						output: { relationships: [] },
					},
					{
						type: "tool-call",
						id: "tc-err",
						name: "workflow_status",
						state: "input-complete",
						arguments: '{"workflow_id":"wf-1","run_id":"r1"}',
						output: { error: "Temporal query failed" },
					},
					{
						type: "tool-result",
						toolCallId: "tc-err",
						state: "error",
						error: "Temporal query failed",
						content: '{"error":"Temporal query failed"}',
					},
				],
			},
		];
		renderRail();

		expect(chipHasLoader("tc-ok")).toBe(false);
		// The errored chip shows the explicit error state, never a spinner.
		expect(chipHasLoader("tc-err")).toBe(false);
		expect(screen.getByTestId("tool-error-tc-err").textContent).toBe("failed");
		expect(screen.getByTestId("tool-error-tc-err").getAttribute("title")).toBe(
			"Temporal query failed",
		);
	});

	it("an output-less call from a PAST turn shows failed, not an eternal spinner", () => {
		// The severed-drain orphan: the polling turn's stream was cut (stop / a
		// new send / network), so tc-stale never received its result. A LATER
		// user message proves the conversation moved on — it can never finish.
		h.messages = [
			{ id: "u1", role: "user", parts: [{ type: "text", content: "import" }] },
			{
				id: "a1",
				role: "assistant",
				parts: [
					{
						type: "tool-call",
						id: "tc-stale",
						name: "workflow_status",
						state: "input-complete",
						arguments: "{}",
					},
				],
			},
			{ id: "u2", role: "user", parts: [{ type: "text", content: "hello?" }] },
		];
		renderRail();

		expect(chipHasLoader("tc-stale")).toBe(false);
		expect(screen.getByTestId("tool-error-tc-stale").textContent).toBe(
			"failed",
		);
	});

	it("a genuinely in-flight call (current turn) still spins", () => {
		h.messages = [
			{ id: "u1", role: "user", parts: [{ type: "text", content: "go" }] },
			{
				id: "a1",
				role: "assistant",
				parts: [
					{
						type: "tool-call",
						id: "tc-live",
						name: "workflow_status",
						state: "input-streaming",
						arguments: '{"workflow_id"',
					},
				],
			},
		];
		renderRail();
		expect(chipHasLoader("tc-live")).toBe(true);
		expect(screen.queryByTestId("tool-error-tc-live")).toBeNull();
	});

	it("a denied approval still reads denied (not failed, not spinning)", () => {
		h.messages = [
			{ id: "u1", role: "user", parts: [{ type: "text", content: "select" }] },
			{
				id: "a1",
				role: "assistant",
				parts: [
					{
						type: "tool-call",
						id: "tc-denied",
						name: "select",
						state: "approval-responded",
						arguments: "{}",
						approval: { id: "ap-1", needsApproval: true, approved: false },
					},
				],
			},
			{ id: "u2", role: "user", parts: [{ type: "text", content: "later" }] },
		];
		renderRail();
		expect(chipHasLoader("tc-denied")).toBe(false);
		expect(screen.getByTestId("tool-denied-tc-denied").textContent).toBe(
			"denied",
		);
		expect(screen.queryByTestId("tool-error-tc-denied")).toBeNull();
	});

	it("a pending approval request keeps its buttons across turns (never orphaned)", () => {
		h.messages = [
			{ id: "u1", role: "user", parts: [{ type: "text", content: "select" }] },
			{
				id: "a1",
				role: "assistant",
				parts: [
					{
						type: "tool-call",
						id: "tc-pending",
						name: "select",
						state: "approval-requested",
						arguments: "{}",
						approval: { id: "ap-2", needsApproval: true },
					},
				],
			},
			// The user typed instead of approving — the request stays answerable.
			{ id: "u2", role: "user", parts: [{ type: "text", content: "hmm" }] },
		];
		renderRail();
		expect(screen.getByTestId("tool-approve-tc-pending")).toBeTruthy();
		expect(screen.queryByTestId("tool-error-tc-pending")).toBeNull();
	});
});

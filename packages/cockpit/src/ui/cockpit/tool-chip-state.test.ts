// Unit pins for the tool-call chip terminal-state mapping (DAT-436).
//
// The part shapes below are NOT invented — they were derived empirically by
// driving the REAL @tanstack/ai 0.26.1 pipeline (server chat() loop with a
// scripted adapter → toServerSentEventsResponse → fetchServerSentEvents →
// ChatClient/StreamProcessor) and dumping the resulting UIMessage parts:
//
//   clean multi-poll turn   → tool-call { state: "complete", output: {...} }
//                             + tool-result { state: "complete" }
//   errored execution       → tool-call { state: "input-complete",
//                             output: { error: "..." } }  ← NO error state!
//                             + tool-result { state: "error", error: "..." }
//   severed stream (stop /  → tool-call { state: "input-complete" } with NO
//   new send / network cut)   output, forever
//
// The old rail condition `done = state === "complete"` therefore spun forever
// on the last two — the live stuck workflow_status chips.

import { describe, expect, it } from "vitest";

import {
	lastUserMessageIndex,
	type MessageLike,
	type ToolCallPartLike,
	toolChipStatus,
	toolResultErrorsById,
} from "#/ui/cockpit/tool-chip-state";

function part(over: Partial<ToolCallPartLike>): ToolCallPartLike {
	return {
		type: "tool-call",
		id: "tc-1",
		name: "workflow_status",
		state: "input-complete",
		...over,
	};
}

describe("toolChipStatus (DAT-436)", () => {
	it("maps the canonical completed call to complete", () => {
		expect(
			toolChipStatus(part({ state: "complete", output: { done: false } })),
		).toEqual({ kind: "complete" });
	});

	it("treats ANY delivered output as terminal, even without the state flip", () => {
		// Defensive: output attached but a stream hiccup never set "complete".
		expect(
			toolChipStatus(part({ state: "input-complete", output: { ok: 1 } })),
		).toEqual({ kind: "complete" });
	});

	it("maps an errored execution to an explicit error — never a spinner", () => {
		// The SDK's errored-call shape: state stays "input-complete", the error
		// rides in output.error (there is NO error-terminal ToolCallState).
		const status = toolChipStatus(
			part({
				state: "input-complete",
				output: { error: "Temporal query failed" },
			}),
		);
		expect(status).toEqual({
			kind: "error",
			message: "Temporal query failed",
		});
	});

	it("prefers the correlated tool-result error when provided", () => {
		const status = toolChipStatus(part({ state: "input-complete" }), {
			resultError: "boom",
		});
		expect(status).toEqual({ kind: "error", message: "boom" });
	});

	it("an error-shaped output is never read as success", () => {
		// Order pin: the error check runs BEFORE the any-output completeness rule.
		const status = toolChipStatus(
			part({ state: "complete", output: { error: "late failure" } }),
		);
		expect(status.kind).toBe("error");
	});

	it("maps a denied approval to denied (terminal — the tool never runs)", () => {
		expect(
			toolChipStatus(
				part({
					state: "approval-responded",
					approval: { id: "a1", needsApproval: true, approved: false },
				}),
			),
		).toEqual({ kind: "denied" });
	});

	it("spins while a call is genuinely in flight (current turn, no output)", () => {
		for (const state of [
			"awaiting-input",
			"input-streaming",
			"input-complete",
		]) {
			expect(
				toolChipStatus(part({ state }), { conversationMovedOn: false }),
			).toEqual({ kind: "running" });
		}
	});

	it("marks an output-less call from a PAST turn as interrupted (error, not spinner)", () => {
		// The severed-drain orphan: stop()/a new send/a network cut killed the
		// stream that owned this call — it can never receive its result.
		const status = toolChipStatus(part({ state: "input-complete" }), {
			conversationMovedOn: true,
		});
		expect(status.kind).toBe("error");
		expect((status as { message: string }).message).toMatch(/didn't finish/);
	});

	it("never orphans a still-answerable approval request", () => {
		// Approve/Deny buttons stay live across turns — awaiting the user is not
		// dead, even when later user messages exist.
		expect(
			toolChipStatus(
				part({
					state: "approval-requested",
					approval: { id: "a1", needsApproval: true },
				}),
				{ conversationMovedOn: true },
			),
		).toEqual({ kind: "running" });
	});
});

describe("toolResultErrorsById / lastUserMessageIndex", () => {
	const messages: MessageLike[] = [
		{ role: "user", parts: [{ type: "text", content: "go" }] },
		{
			role: "assistant",
			parts: [
				{ type: "tool-call" },
				{
					type: "tool-result",
					toolCallId: "tc-err",
					state: "error",
					error: "Temporal query failed",
				},
				{ type: "tool-result", toolCallId: "tc-ok", state: "complete" },
			],
		},
		{ role: "user", parts: [{ type: "text", content: "next" }] },
		{ role: "assistant", parts: [] },
	];

	it("collects only errored tool-results, keyed by call id", () => {
		const errors = toolResultErrorsById(messages);
		expect(errors.get("tc-err")).toBe("Temporal query failed");
		expect(errors.has("tc-ok")).toBe(false);
	});

	it("falls back to string content, then a generic message, for error text", () => {
		const errors = toolResultErrorsById([
			{
				role: "assistant",
				parts: [
					{
						type: "tool-result",
						toolCallId: "tc-2",
						state: "error",
						content: "raw failure text",
					},
					{ type: "tool-result", toolCallId: "tc-3", state: "error" },
				],
			},
		]);
		expect(errors.get("tc-2")).toBe("raw failure text");
		expect(errors.get("tc-3")).toBe("Tool execution failed");
	});

	it("finds the LAST user message (the moved-on boundary)", () => {
		expect(lastUserMessageIndex(messages)).toBe(2);
		expect(lastUserMessageIndex([])).toBe(-1);
		expect(lastUserMessageIndex([{ role: "assistant", parts: [] }])).toBe(-1);
	});
});

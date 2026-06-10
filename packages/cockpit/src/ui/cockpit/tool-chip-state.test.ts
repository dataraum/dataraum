// Unit pins for the tool-call chip terminal-state mapping (DAT-436).
//
// The part shapes below are NOT invented — they were derived empirically by
// driving the REAL installed @tanstack/ai pipeline (bun.lock owns the version;
// tool-chip-state.contract.test.ts re-pins the shapes on every run) — server
// chat() loop with a scripted adapter → toServerSentEventsResponse →
// fetchServerSentEvents → ChatClient/StreamProcessor — and dumping the
// resulting UIMessage parts:
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

	it("recognizes the SDK's PLAIN-STRING errored-output shape too", () => {
		// tool-calls.js's ToolCallManager.executeTools path emits the raw string
		// `Error executing tool: <msg>` — the client's JSON.parse of it fails, so
		// the part's output stays that string (no {error} wrapper). Dead in the
		// installed SDK's chat() loop but still in the SDK source; a chip seeing
		// it must read "failed", never success or an eternal spinner.
		const status = toolChipStatus(
			part({
				state: "input-complete",
				output: "Error executing tool: Temporal query failed",
			}),
		);
		expect(status).toEqual({
			kind: "error",
			message: "Error executing tool: Temporal query failed",
		});
	});

	it("does NOT read an ordinary string output as an error", () => {
		// Only the `Error executing tool:` prefix marks a string output as the
		// SDK's error shape — a tool that legitimately returns prose completes.
		expect(
			toolChipStatus(part({ state: "input-complete", output: "all good" })),
		).toEqual({ kind: "complete" });
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

	it("spins while a call is genuinely in flight (current turn, stream loading, no output)", () => {
		for (const state of [
			"awaiting-input",
			"input-streaming",
			"input-complete",
		]) {
			expect(
				toolChipStatus(part({ state }), {
					// Explicit: no correlated tool-result error exists for an in-flight
					// call — the spinner is the no-error, no-output, current-turn,
					// stream-loading cell of the matrix. streamIdle false = the drain
					// is still live, so the result can still arrive (no false-failure
					// window during a live back-fill).
					resultError: undefined,
					conversationMovedOn: false,
					streamIdle: false,
				}),
			).toEqual({ kind: "running" });
		}
	});

	it("marks an output-less call from a PAST turn as interrupted (error, not spinner)", () => {
		// The severed-drain orphan: stop()/a new send/a network cut killed the
		// stream that owned this call — it can never receive its result. streamIdle
		// false isolates the moved-on axis: even with a NEW turn's stream live,
		// the old call is dead.
		const status = toolChipStatus(part({ state: "input-complete" }), {
			conversationMovedOn: true,
			streamIdle: false,
		});
		expect(status.kind).toBe("error");
		expect((status as { message: string }).message).toMatch(/didn't finish/);
	});

	it("marks an output-less call as interrupted when the stream went idle — stop() with NO follow-up", () => {
		// The stop-then-idle cell: the user hit stop() and never sent another
		// message, so NO later user message exists (conversationMovedOn false) —
		// but isLoading spans the entire drain, so an idle stream + no output
		// means the result is never coming. Without streamIdle this chip spun
		// until the next message.
		const status = toolChipStatus(part({ state: "input-complete" }), {
			conversationMovedOn: false,
			streamIdle: true,
		});
		expect(status.kind).toBe("error");
		expect((status as { message: string }).message).toMatch(/didn't finish/);
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

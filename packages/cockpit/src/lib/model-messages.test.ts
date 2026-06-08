// Tests for buildModelMessages (DAT-462) — the model/display split chokepoint.
// Pure function, no mocks. Covers: short conversations pass verbatim; older
// turns get tool payloads stubbed while recent turns stay verbatim; and the
// CRITICAL invariant that tool parts are stubbed NEVER dropped (so the
// call/result pairing Anthropic requires survives).

import type { UIMessage } from "@tanstack/ai-react";
import { describe, expect, it } from "vitest";
import { buildModelMessages, STUBBED_TOOL_RESULT } from "./model-messages";

function userMsg(id: string, text: string): UIMessage {
	return {
		id,
		role: "user",
		parts: [{ type: "text", content: text }],
	} as UIMessage;
}

function assistantWithTool(
	id: string,
	callId: string,
	resultContent: string,
): UIMessage {
	return {
		id,
		role: "assistant",
		parts: [
			{ type: "text", content: `narration ${id}` },
			{
				type: "tool-call",
				id: callId,
				name: "look_table",
				arguments: '{"table":"x"}',
				state: "output-available",
			},
			{
				type: "tool-result",
				toolCallId: callId,
				content: resultContent,
				state: "output-available",
			},
		],
	} as UIMessage;
}

function partTypes(m: UIMessage): Array<string> {
	return m.parts.map((p) => p.type);
}

function toolResultContent(m: UIMessage): unknown {
	const part = m.parts.find((p) => p.type === "tool-result");
	return part && "content" in part ? part.content : undefined;
}

describe("buildModelMessages", () => {
	it("returns a short conversation verbatim (≤ recentTurns user turns)", () => {
		const transcript = [
			userMsg("u0", "hi"),
			assistantWithTool("a0", "c0", "BIG RESULT 0"),
			userMsg("u1", "more"),
			assistantWithTool("a1", "c1", "BIG RESULT 1"),
		];
		const out = buildModelMessages(transcript, { recentTurns: 6 });
		expect(out).toEqual(transcript);
	});

	it("windows older turns and keeps recent turns verbatim", () => {
		// 4 user-turns (indices 0,2,4,6); recentTurns=2 → window starts at index 4.
		const transcript = [
			userMsg("u0", "q0"),
			assistantWithTool("a0", "c0", "OLD RESULT 0"),
			userMsg("u1", "q1"),
			assistantWithTool("a1", "c1", "OLD RESULT 1"),
			userMsg("u2", "q2"),
			assistantWithTool("a2", "c2", "RECENT RESULT 2"),
			userMsg("u3", "q3"),
			assistantWithTool("a3", "c3", "RECENT RESULT 3"),
		];
		const out = buildModelMessages(transcript, { recentTurns: 2 });

		// Old assistant turns: tool-result payload stubbed.
		expect(toolResultContent(out[1])).toBe(STUBBED_TOOL_RESULT);
		expect(toolResultContent(out[3])).toBe(STUBBED_TOOL_RESULT);
		// Recent assistant turns: untouched.
		expect(toolResultContent(out[5])).toBe("RECENT RESULT 2");
		expect(toolResultContent(out[7])).toBe("RECENT RESULT 3");
	});

	it("stubs tool parts but never DROPS them (pairing integrity for Anthropic)", () => {
		const transcript = [
			userMsg("u0", "q0"),
			assistantWithTool("a0", "c0", "OLD RESULT"),
			userMsg("u1", "q1"),
			assistantWithTool("a1", "c1", "RECENT"),
		];
		const out = buildModelMessages(transcript, { recentTurns: 1 });
		// The old assistant message still has BOTH the tool-call and tool-result
		// parts (plus its text) — only the payload changed.
		expect(partTypes(out[1])).toEqual(["text", "tool-call", "tool-result"]);
		expect(toolResultContent(out[1])).toBe(STUBBED_TOOL_RESULT);
		// tool-call arguments blanked, but the call survives.
		const call = out[1].parts.find((p) => p.type === "tool-call");
		expect(call && "arguments" in call ? call.arguments : "MISSING").toBe("");
	});

	it("preserves conversational text in older turns", () => {
		const transcript = [
			userMsg("u0", "the important question"),
			assistantWithTool("a0", "c0", "OLD"),
			userMsg("u1", "q1"),
			assistantWithTool("a1", "c1", "RECENT"),
		];
		const out = buildModelMessages(transcript, { recentTurns: 1 });
		// user text kept verbatim
		expect(out[0]).toEqual(transcript[0]);
		// assistant narration text kept; only tool payload changed
		const text = out[1].parts.find((p) => p.type === "text");
		expect(text && "content" in text ? text.content : "").toBe("narration a0");
	});

	it("passes a text-only system row (model-only refs) through untouched", () => {
		const refs = {
			id: "refs-1",
			role: "system",
			parts: [{ type: "text", content: "refs: table_id=abc" }],
		} as UIMessage;
		const transcript = [
			refs,
			userMsg("u0", "q0"),
			assistantWithTool("a0", "c0", "OLD"),
			userMsg("u1", "q1"),
			assistantWithTool("a1", "c1", "RECENT"),
		];
		const out = buildModelMessages(transcript, { recentTurns: 1 });
		expect(out[0]).toEqual(refs);
	});
});

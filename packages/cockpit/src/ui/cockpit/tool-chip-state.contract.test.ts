// SDK contract test for the tool-chip terminal-state mapping (DAT-436).
//
// tool-chip-state.ts encodes UNDOCUMENTED @tanstack/ai internals: the SDK has
// no error-terminal ToolCallState, so an errored server-tool execution parks at
// `state: "input-complete"` with the error riding in `output` + a sibling
// `tool-result` part `state: "error"`. That shape is an implementation detail
// of the SDK's stream pipeline — nothing type-checks it, so an SDK bump can
// silently change it and resurrect the eternal chip spinner.
//
// This test is the contract-catcher: it drives the REAL installed SDK end to
// end (bun.lock owns the version; deps stay "latest" by project convention) —
// the exact production pipeline, no network, no LLM:
//
//   scripted adapter (plays the model: emits the tool call)
//     → chat() server agent loop (REALLY executes the server tool)
//     → toServerSentEventsResponse  (the /api/chat response)
//     → fetchServerSentEvents       (with an in-process fetchClient)
//     → ChatClient / StreamProcessor (what useChat renders from)
//
// and asserts the exact part shapes toolChipStatus() keys on. If a
// `bun update` of @tanstack/ai changes the contract — adds an error terminal
// state, moves the error off `output`, renames the result part — this test
// fails and tool-chip-state.ts must be re-verified.

import {
	chat,
	chatParamsFromRequest,
	toolDefinition,
	toServerSentEventsResponse,
} from "@tanstack/ai";
import { ChatClient, fetchServerSentEvents } from "@tanstack/ai-client";
// What useChat passes (use-chat.js) — the DEFAULT noop bridge in 0.16.1 lacks
// mountWithTools() and crashes sendMessage, so the real factory is part of the
// production wiring this harness mirrors.
import { createChatDevtoolsBridge } from "@tanstack/ai-client/devtools";
import { describe, expect, it, vi } from "vitest";
import { z } from "zod";

import {
	type ToolCallPartLike,
	toolChipStatus,
	toolResultErrorsById,
} from "#/ui/cockpit/tool-chip-state";

// ---------------------------------------------------------------------------
// Scripted adapter — plays the MODEL's role for one chat() run: first
// iteration calls the `workflow_status` tool (the chunk sequence mirrors what
// @tanstack/ai-anthropic emits for a tool_use stop), the follow-up iteration
// (tool result present in the transcript) closes the turn with text + stop.
// ---------------------------------------------------------------------------

const TOOL_CALL_ID = "tc-contract-1";
const TOOL_NAME = "workflow_status";
const TOOL_ARGS = '{"workflow_id":"wf-1"}';

interface ScriptedStreamOptions {
	messages: ReadonlyArray<{ role: string }>;
	threadId?: string;
}

function scriptedAdapter() {
	let call = 0;
	return {
		kind: "text" as const,
		name: "scripted",
		provider: "scripted",
		model: "scripted-model",
		async *chatStream(options: ScriptedStreamOptions) {
			call += 1;
			const runId = `scripted-run-${call}`;
			const threadId = options.threadId ?? "thread-contract";
			const base = { model: "scripted-model", timestamp: Date.now() };
			yield { type: "RUN_STARTED", runId, threadId, ...base };
			const toolResultSeen = options.messages.some((m) => m.role === "tool");
			if (!toolResultSeen) {
				// Model decides to call the tool (mirrors the anthropic adapter's
				// TOOL_CALL_START → ARGS → END(input) → RUN_FINISHED(tool_calls)).
				yield {
					type: "TOOL_CALL_START",
					toolCallId: TOOL_CALL_ID,
					toolCallName: TOOL_NAME,
					toolName: TOOL_NAME,
					index: 0,
					...base,
				};
				yield {
					type: "TOOL_CALL_ARGS",
					toolCallId: TOOL_CALL_ID,
					delta: TOOL_ARGS,
					args: TOOL_ARGS,
					...base,
				};
				yield {
					type: "TOOL_CALL_END",
					toolCallId: TOOL_CALL_ID,
					toolCallName: TOOL_NAME,
					toolName: TOOL_NAME,
					input: JSON.parse(TOOL_ARGS),
					...base,
				};
				yield {
					type: "RUN_FINISHED",
					runId,
					threadId,
					finishReason: "tool_calls",
					...base,
				};
			} else {
				// The tool result is in the transcript — close the turn.
				const messageId = `scripted-msg-${call}`;
				yield {
					type: "TEXT_MESSAGE_START",
					messageId,
					role: "assistant",
					...base,
				};
				yield {
					type: "TEXT_MESSAGE_CONTENT",
					messageId,
					delta: "done",
					...base,
				};
				yield { type: "TEXT_MESSAGE_END", messageId, ...base };
				yield {
					type: "RUN_FINISHED",
					runId,
					threadId,
					finishReason: "stop",
					...base,
				};
			}
		},
	};
}

// ---------------------------------------------------------------------------
// In-process server: the /api/chat handler verbatim (chatParamsFromRequest →
// chat() → toServerSentEventsResponse), handed to fetchServerSentEvents as its
// fetchClient — the full SSE wire format crosses, just without a socket.
// ---------------------------------------------------------------------------

type ChatTools = NonNullable<Parameters<typeof chat>[0]["tools"]>;
type ChatAdapter = Parameters<typeof chat>[0]["adapter"];

function inProcessServer(tools: ChatTools) {
	return async (
		input: string | URL | Request,
		init?: RequestInit,
	): Promise<Response> => {
		const { messages } = await chatParamsFromRequest(new Request(input, init));
		const stream = chat({
			adapter: scriptedAdapter() as unknown as ChatAdapter,
			messages,
			tools,
		});
		return toServerSentEventsResponse(stream);
	};
}

const StatusInput = z.object({ workflow_id: z.string() });

/** The untyped UIMessage part shape the client hands onMessagesChange —
 * structurally a superset of MessageLike's parts (what the rail reads). */
interface TurnPart {
	type: string;
	toolCallId?: string;
	state?: string;
	error?: string;
	content?: unknown;
	[key: string]: unknown;
}
interface TurnMessage {
	role: string;
	parts: TurnPart[];
}

/** Run one full turn through the pipeline; resolves with the final message
 * list once the tool-call part reached a terminal shape (the client resolves
 * the turn at the FIRST RUN_FINISHED and back-fills results via the drain, so
 * the terminal shape can land after sendMessage resolves). */
async function driveTurn(tools: ChatTools): Promise<TurnMessage[]> {
	let messages: TurnMessage[] = [];
	const client = new ChatClient({
		connection: fetchServerSentEvents("http://cockpit.test/api/chat", {
			// Bun's `typeof fetch` carries a `preconnect` member the in-process
			// handler doesn't need — the SDK only ever CALLS it.
			fetchClient: inProcessServer(tools) as unknown as typeof fetch,
		}),
		devtoolsBridgeFactory: createChatDevtoolsBridge,
		onMessagesChange: (m) => {
			messages = m as unknown as TurnMessage[];
		},
	});
	try {
		await client.sendMessage("status of wf-1?");
		await vi.waitFor(
			() => {
				if (soleToolCallPart(messages).output === undefined) {
					throw new Error("tool result not delivered yet");
				}
			},
			{ timeout: 5000 },
		);
	} finally {
		client.unsubscribe();
	}
	return messages;
}

function soleToolCallPart(messages: TurnMessage[]): ToolCallPartLike {
	const parts = messages.flatMap((m) =>
		m.parts.filter((p) => p.type === "tool-call"),
	) as unknown as ToolCallPartLike[];
	if (parts.length !== 1) {
		throw new Error(`expected exactly one tool-call part, got ${parts.length}`);
	}
	return parts[0];
}

function toolResultParts(messages: TurnMessage[]): TurnPart[] {
	return messages.flatMap((m) =>
		m.parts.filter((p) => p.type === "tool-result"),
	);
}

// ---------------------------------------------------------------------------
// The contract.
// ---------------------------------------------------------------------------

describe("@tanstack/ai tool-call part contract (installed version, bun.lock-owned)", () => {
	it("an errored SERVER tool execution parks at input-complete with the error in output + a tool-result part state error", async () => {
		const failing = toolDefinition({
			name: TOOL_NAME,
			description: "scripted: always throws",
			inputSchema: StatusInput,
		}).server(async () => {
			throw new Error("Temporal query failed");
		});

		const messages = await driveTurn([failing] as unknown as ChatTools);
		const part = soleToolCallPart(messages);

		// THE missing-error-terminal contract: the part NEVER reaches an error
		// state — it stays "input-complete" (processor.js maps `output-error` →
		// "input-complete"). `state === "complete"` therefore must not be the
		// rail's only done-condition.
		expect(part.state).toBe("input-complete");

		// The error rides in `output` — pin its EXACT shape so a bump that moves
		// it breaks here. executeServerTool pushes `{ error: message }`,
		// buildToolResultChunks stringifies it onto the wire, and the client's
		// JSON.parse round-trips it back to the object.
		expect(part.output).toEqual({ error: "Temporal query failed" });

		// Sibling tool-result part: state "error" with the extracted error text —
		// the shape toolResultErrorsById collects for the chip tooltip.
		const results = toolResultParts(messages);
		expect(results).toHaveLength(1);
		expect(results[0].state).toBe("error");
		expect(results[0].error).toBe("Temporal query failed");
		expect(toolResultErrorsById(messages).get(TOOL_CALL_ID)).toBe(
			"Temporal query failed",
		);

		// And the production mapping renders it as an explicit failure — an
		// errored call must never spin forever.
		expect(toolChipStatus(part)).toEqual({
			kind: "error",
			message: "Temporal query failed",
		});
	});

	it("a clean SERVER tool execution reaches state complete with the parsed output", async () => {
		const clean = toolDefinition({
			name: TOOL_NAME,
			description: "scripted: succeeds",
			inputSchema: StatusInput,
			outputSchema: z.object({ done: z.boolean(), phase: z.string() }),
		}).server(async () => ({ done: true, phase: "import" }));

		const messages = await driveTurn([clean] as unknown as ChatTools);
		const part = soleToolCallPart(messages);

		// The canonical success terminal: state flips to "complete" and the
		// output is the parsed tool result.
		expect(part.state).toBe("complete");
		expect(part.output).toEqual({ done: true, phase: "import" });

		const results = toolResultParts(messages);
		expect(results).toHaveLength(1);
		expect(results[0].state).toBe("complete");
		expect(toolResultErrorsById(messages).size).toBe(0);

		expect(toolChipStatus(part)).toEqual({ kind: "complete" });
	});
});

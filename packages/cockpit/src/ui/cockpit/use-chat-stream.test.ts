import { describe, expect, it } from "vitest";
import {
	type ChatStreamEvent,
	parseSseFrame,
	readSseStream,
} from "#/ui/cockpit/use-chat-stream";

// Build a ReadableStream that yields the given byte chunks in order, so we can
// split SSE frames across reads at arbitrary boundaries.
function streamOf(chunks: string[]): ReadableStream<Uint8Array> {
	const enc = new TextEncoder();
	let i = 0;
	return new ReadableStream({
		pull(controller) {
			if (i < chunks.length) {
				controller.enqueue(enc.encode(chunks[i++]));
			} else {
				controller.close();
			}
		},
	});
}

describe("parseSseFrame (DAT-347)", () => {
	it("parses each event type", () => {
		expect(parseSseFrame('event: text\ndata: {"text":"hi"}')).toEqual({
			type: "text",
			text: "hi",
		});
		expect(
			parseSseFrame('event: tool_call_start\ndata: {"id":"t1","name":"foo"}'),
		).toEqual({ type: "tool_call_start", id: "t1", name: "foo" });
		expect(
			parseSseFrame(
				'event: tool_result\ndata: {"id":"t1","name":"foo","result":{"ok":true}}',
			),
		).toEqual({
			type: "tool_result",
			id: "t1",
			name: "foo",
			result: { ok: true },
		});
		expect(
			parseSseFrame('event: done\ndata: {"stop_reason":"end_turn"}'),
		).toEqual({ type: "done", stop_reason: "end_turn" });
		expect(parseSseFrame('event: error\ndata: {"message":"nope"}')).toEqual({
			type: "error",
			message: "nope",
		});
	});

	it("skips unknown event names and malformed data", () => {
		expect(parseSseFrame("event: mystery\ndata: {}")).toBeNull();
		expect(parseSseFrame("event: text\ndata: {not json")).toBeNull();
		expect(parseSseFrame("noise")).toBeNull();
	});
});

describe("readSseStream (DAT-347)", () => {
	it("reassembles frames split across reads", async () => {
		// One frame is split mid-JSON across two chunks; two frames share a chunk.
		const chunks = [
			'event: tool_call_start\ndata: {"id":"t1","na',
			'me":"add_source"}\n\nevent: text\ndata: {"text":"hello "}\n\n',
			'event: text\ndata: {"text":"world"}\n\nevent: tool_result\ndata: {"id":"t1","name":"add_source","result":42}\n\n',
			'event: done\ndata: {"stop_reason":"end_turn"}\n\n',
		];
		const events: ChatStreamEvent[] = [];
		await readSseStream(streamOf(chunks), (e) => events.push(e));

		expect(events).toEqual([
			{ type: "tool_call_start", id: "t1", name: "add_source" },
			{ type: "text", text: "hello " },
			{ type: "text", text: "world" },
			{ type: "tool_result", id: "t1", name: "add_source", result: 42 },
			{ type: "done", stop_reason: "end_turn" },
		]);
	});

	it("flushes a trailing frame not terminated by a blank line", async () => {
		const events: ChatStreamEvent[] = [];
		await readSseStream(streamOf(['event: text\ndata: {"text":"tail"}']), (e) =>
			events.push(e),
		);
		expect(events).toEqual([{ type: "text", text: "tail" }]);
	});
});

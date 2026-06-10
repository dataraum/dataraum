import type { StreamChunk } from "@tanstack/ai";
import { describe, expect, it, vi } from "vitest";
import { hasSubscribers, publish, subscribe } from "#/lib/chat-bus";

// A minimal StreamChunk stand-in — the bus is payload-agnostic (it fans the
// opaque chunk through), so a typed cast off a marker object is enough.
function chunk(name: string): StreamChunk {
	return { type: "CUSTOM", name } as unknown as StreamChunk;
}

describe("chat-bus (Phase 2A subscribe transport)", () => {
	it("fans a published chunk out to every subscriber of that conversation", () => {
		const a = { enqueue: vi.fn() };
		const b = { enqueue: vi.fn() };
		subscribe("conv-1", a);
		subscribe("conv-1", b);

		const c = chunk("hello");
		publish("conv-1", c);

		expect(a.enqueue).toHaveBeenCalledWith(c);
		expect(b.enqueue).toHaveBeenCalledWith(c);
	});

	it("isolates conversations — a publish reaches only that conversation's sinks", () => {
		const one = { enqueue: vi.fn() };
		const two = { enqueue: vi.fn() };
		subscribe("conv-1", one);
		subscribe("conv-2", two);

		publish("conv-1", chunk("x"));

		expect(one.enqueue).toHaveBeenCalledOnce();
		expect(two.enqueue).not.toHaveBeenCalled();
	});

	it("stops delivering after unsubscribe and prunes the empty channel", () => {
		const sink = { enqueue: vi.fn() };
		const off = subscribe("conv-prune", sink);
		expect(hasSubscribers("conv-prune")).toBe(true);

		off();
		expect(hasSubscribers("conv-prune")).toBe(false);

		publish("conv-prune", chunk("late"));
		expect(sink.enqueue).not.toHaveBeenCalled();
	});

	it("publish to a conversation with no subscribers is a silent no-op", () => {
		expect(() => publish("conv-empty", chunk("nobody"))).not.toThrow();
		expect(hasSubscribers("conv-empty")).toBe(false);
	});

	it("a throwing sink can't starve the other subscribers (isolated fanout)", () => {
		const bad = {
			enqueue: vi.fn(() => {
				throw new Error("controller closed");
			}),
		};
		const good = { enqueue: vi.fn() };
		subscribe("conv-iso", bad);
		subscribe("conv-iso", good);

		expect(() => publish("conv-iso", chunk("ok"))).not.toThrow();
		expect(good.enqueue).toHaveBeenCalledOnce();
	});

	it("unsubscribe removes only the one sink, leaving siblings live", () => {
		const a = { enqueue: vi.fn() };
		const b = { enqueue: vi.fn() };
		const offA = subscribe("conv-multi", a);
		subscribe("conv-multi", b);

		offA();
		publish("conv-multi", chunk("after"));

		expect(a.enqueue).not.toHaveBeenCalled();
		expect(b.enqueue).toHaveBeenCalledOnce();
		expect(hasSubscribers("conv-multi")).toBe(true);
	});
});

// ALS spike (DAT-528, Phase 0) — the decision gate for run-routing's mechanism.
//
// The risk: does an AsyncLocalStorage store set in the chat handler SURVIVE
// `@tanstack/ai`'s chat() agent loop down into a tool `.server()` handler, where
// recordRun fires? If it doesn't, the foundation can't use ALS and we thread the
// conversationId through the four drivers instead.
//
// This test answers it deterministically — no LLM, no fake adapter — by mirroring
// the EXACT structure of chat()'s tool dispatch (read from the installed SDK,
// activities/chat/tools/tool-calls.js): nested async generators whose innermost
// step does `await tool.execute(args, ctx)`, plus a `Promise.allSettled` of a
// deferred created inside the scope (the loop's `deferredPromises`). If the store
// reaches the simulated tool handler through that, ALS holds for the real loop too
// (the loop adds no thread/worker/detached-queue hop — it's all in-stack awaits).

import { describe, expect, it } from "vitest";
import { currentConversationId, runWithConversation } from "./run-context";

describe("run-context (ALS holder)", () => {
	it("returns null outside any scope", () => {
		expect(currentConversationId()).toBeNull();
	});

	it("exposes the bound id synchronously inside a scope", () => {
		const seen = runWithConversation("conv-a", () => currentConversationId());
		expect(seen).toBe("conv-a");
	});

	it("survives a plain await chain (the recordRun hop)", async () => {
		const seen = await runWithConversation("conv-a", async () => {
			await Promise.resolve();
			// Two driver hops deep, mirroring select → triggerAddSource → recordRun.
			const driver = async () => {
				await Promise.resolve();
				return currentConversationId();
			};
			return driver();
		});
		expect(seen).toBe("conv-a");
	});

	it("survives chat()'s nested-async-generator tool dispatch (THE spike)", async () => {
		// What a tool `.server()` handler sees when it calls currentConversationId().
		let toolSaw: string | null = "UNSET";

		// Mirrors executeTools(): an async generator that, per tool call, AWAITS the
		// handler then yields a result chunk. The deferred + allSettled mirrors the
		// loop's `this.deferredPromises` drain.
		async function* executeToolsLike(): AsyncGenerator<string> {
			const deferred = (async () => {
				await Promise.resolve();
			})();
			await Promise.allSettled([deferred]);
			// THE awaited tool handler — `await tool.execute(args, ctx)` in the SDK.
			toolSaw = await Promise.resolve().then(() => currentConversationId());
			yield "tool-result";
		}

		// Mirrors chat()'s outer stream generator: emit some chunks, then delegate
		// to the tool-execution generator (yield*), all driven by the consumer below.
		async function* chatLike(): AsyncGenerator<string> {
			yield "text";
			await Promise.resolve();
			yield* executeToolsLike();
			yield "done";
		}

		// Mirrors streamAgentTurnToBus: `for await` over the stream INSIDE the scope.
		const chunks: string[] = [];
		await runWithConversation("conv-a", async () => {
			for await (const chunk of chatLike()) chunks.push(chunk);
		});

		expect(chunks).toEqual(["text", "tool-result", "done"]);
		expect(toolSaw).toBe("conv-a");
	});

	it("isolates concurrent scopes — no cross-bleed (multi-user safety)", async () => {
		const results: Array<string | null> = [];
		const turn = (id: string, delay: number) =>
			runWithConversation(id, async () => {
				// Interleave so the two turns' awaits overlap on the event loop.
				await new Promise((r) => setTimeout(r, delay));
				results.push(currentConversationId());
			});

		await Promise.all([turn("conv-a", 5), turn("conv-b", 1)]);

		// Each turn read back its OWN id regardless of interleaving order.
		expect(results.sort()).toEqual(["conv-a", "conv-b"]);
	});
});

// Unit test for the drill Slice menu's Haiku guidance fallback (DAT-673). The
// Haiku call is mocked at the @tanstack/ai boundary (same pattern as
// nav-agent.test.ts) — what's tested is the REAL logic around it: the input
// cap and the never-fabricate-coverage filter on the response.

import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";

const h = vi.hoisted(() => ({
	chat: vi.fn(),
	createAnthropicChat: vi.fn(() => ({})),
}));

vi.mock("@tanstack/ai", () => ({ chat: h.chat }));
vi.mock("@tanstack/ai-anthropic", () => ({
	createAnthropicChat: h.createAnthropicChat,
}));
vi.mock("#/config", () => ({ config: { anthropicApiKey: "sk-test" } }));
// Mode-shared base config (DAT-819) — reached transitively via the
// registry/db seam; parsing the real one needs env this test does not set.
vi.mock("#/config.base", () => ({ baseConfig: {} }));

import { DRILL_GUIDANCE_TIMEOUT_MS, MAX_GUIDANCE_AXES } from "#/duckdb/drill";
import { suggestAxisGuidance } from "#/lib/axis-guidance-agent";

beforeEach(() => {
	h.chat.mockReset();
	h.createAnthropicChat.mockReset();
	h.createAnthropicChat.mockReturnValue({});
});
afterEach(() => vi.restoreAllMocks());

describe("suggestAxisGuidance (DAT-673)", () => {
	it("returns the model's suggestions for the requested axes", async () => {
		h.chat.mockResolvedValue({
			suggestions: [
				{ column: "region", guidance: "See if performance clusters by area." },
				{ column: "channel", guidance: "Compare how channels contribute." },
			],
		});
		const out = await suggestAxisGuidance("revenue", [
			{ column: "region", sliceType: "categorical" },
			{ column: "channel", sliceType: "categorical" },
		]);
		expect(out).toEqual([
			{ column: "region", guidance: "See if performance clusters by area." },
			{ column: "channel", guidance: "Compare how channels contribute." },
		]);
	});

	it("never fabricates coverage of an axis that wasn't asked about — drops any unrecognized column", async () => {
		h.chat.mockResolvedValue({
			suggestions: [
				{ column: "region", guidance: "Real answer." },
				{ column: "made_up_column", guidance: "Should never surface." },
			],
		});
		const out = await suggestAxisGuidance("revenue", [
			{ column: "region", sliceType: "categorical" },
		]);
		expect(out).toEqual([{ column: "region", guidance: "Real answer." }]);
	});

	// Review-round fix: the ORIGINAL version of this test sent MAX+5 axes
	// straight to `suggestAxisGuidance` and asserted the function itself
	// trimmed them — but that scenario is UNREACHABLE in production once the
	// client (drillable-grid.tsx) and the route's zod schema both cap at
	// MAX_GUIDANCE_AXES BEFORE this function ever runs (using the SAME shared
	// constant, duckdb/drill.ts) — a request this large now 400s at the route,
	// never reaching here. The reachable case is the boundary below; the
	// over-sized case is kept too, relabeled as what it actually is: a
	// defensive invariant for a caller that bypasses those two bounds, not
	// evidence the real pipeline ever exercises it.
	it("passes exactly MAX_GUIDANCE_AXES through untouched — the boundary the client/route cap actually produces", async () => {
		h.chat.mockResolvedValue({ suggestions: [] });
		const axes = Array.from({ length: MAX_GUIDANCE_AXES }, (_, i) => ({
			column: `col_${i}`,
			sliceType: "categorical",
		}));
		await suggestAxisGuidance("revenue", axes);
		const call = h.chat.mock.calls[0]?.[0];
		const userMessage = call.messages[0].content as string;
		const mentioned = axes.filter((a) => userMessage.includes(a.column));
		expect(mentioned.length).toBe(MAX_GUIDANCE_AXES);
	});

	it("defensively caps an over-sized input too — belt-and-suspenders only; neither real caller (client or route) ever forwards more than MAX_GUIDANCE_AXES", async () => {
		h.chat.mockResolvedValue({ suggestions: [] });
		const axes = Array.from({ length: MAX_GUIDANCE_AXES + 5 }, (_, i) => ({
			column: `col_${i}`,
			sliceType: "categorical",
		}));
		await suggestAxisGuidance("revenue", axes);
		const call = h.chat.mock.calls[0]?.[0];
		const userMessage = call.messages[0].content as string;
		const mentioned = axes.filter((a) => userMessage.includes(a.column));
		expect(mentioned.length).toBe(MAX_GUIDANCE_AXES);
	});

	// DAT-671 R5 — the cap above is real and routine (a pure-substrate node
	// commonly has more dimensions than MAX_GUIDANCE_AXES), and it is INVISIBLE
	// from here: the route's `.max()` means an over-cap request never arrives, so
	// only the client knows the true count and has to send it. Without the note a
	// model shown eight of twenty-three writes as though it had seen all of them.
	describe("subset disclosure", () => {
		const eight = Array.from({ length: MAX_GUIDANCE_AXES }, (_, i) => ({
			column: `col_${i}`,
			sliceType: "categorical",
		}));
		const userMessage = () =>
			h.chat.mock.calls[0]?.[0].messages[0].content as string;

		it("tells the model when it is seeing a SUBSET of the menu", async () => {
			h.chat.mockResolvedValue({ suggestions: [] });
			await suggestAxisGuidance("revenue", eight, 23);
			expect(userMessage()).toContain(
				`these are ${MAX_GUIDANCE_AXES} of 23 candidate dimensions`,
			);
			// The count alone is not the disclosure — the model must also be told
			// not to speak as though the list were exhaustive.
			expect(userMessage()).toContain("complete set");
		});

		it("says nothing when the menu was sent whole", async () => {
			h.chat.mockResolvedValue({ suggestions: [] });
			await suggestAxisGuidance("revenue", eight, MAX_GUIDANCE_AXES);
			expect(userMessage()).not.toContain("candidate dimensions");
		});

		it("says nothing when the caller does not know the total", async () => {
			// An unstated total is not evidence of completeness — but claiming a
			// cut we cannot size would be its own invention.
			h.chat.mockResolvedValue({ suggestions: [] });
			await suggestAxisGuidance("revenue", eight);
			expect(userMessage()).not.toContain("candidate dimensions");
		});
	});

	it("propagates an LLM failure — the caller keeps the prior menu state rather than showing an unverified result", async () => {
		h.createAnthropicChat.mockImplementation(() => {
			throw new Error("haiku down");
		});
		await expect(
			suggestAxisGuidance("revenue", [
				{ column: "region", sliceType: "categorical" },
			]),
		).rejects.toThrow("haiku down");
	});

	// Fold-in #5: a hung model must not hold the route handler's connection
	// open forever.
	it("passes an AbortController to chat() and aborts it after DRILL_GUIDANCE_TIMEOUT_MS if the call hangs", async () => {
		vi.useFakeTimers();
		let captured: AbortController | undefined;
		h.chat.mockImplementation(
			({ abortController }: { abortController?: AbortController }) => {
				captured = abortController;
				return new Promise(() => {}); // never resolves — simulates a hang
			},
		);
		const promise = suggestAxisGuidance("revenue", [
			{ column: "region", sliceType: "categorical" },
		]);
		// Flush the microtask queue so chat() has run and captured the
		// controller before advancing timers.
		await Promise.resolve();
		expect(captured?.signal.aborted).toBe(false);
		vi.advanceTimersByTime(DRILL_GUIDANCE_TIMEOUT_MS);
		expect(captured?.signal.aborted).toBe(true);
		vi.useRealTimers();
		// Only the abort firing is under test — the still-pending mock promise
		// never settles, so silence its unhandled-rejection potential.
		promise.catch(() => {});
	});
});

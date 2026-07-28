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

import {
	MAX_GUIDANCE_AXES,
	suggestAxisGuidance,
} from "#/lib/axis-guidance-agent";

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

	it("caps the request to MAX_GUIDANCE_AXES — never an unbounded call", async () => {
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
});

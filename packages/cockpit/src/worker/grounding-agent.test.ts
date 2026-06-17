// Unit tests for the grounding-teach agent activity (DAT-551 P3c). Mocks the
// readiness oracle, `@tanstack/ai` chat, and the teach write at the seam (no DB, no
// real LLM). Asserts the DECISION paths: the all-ready fast path skips the LLM; a
// missing key surfaces for judgement without an LLM call; and a real verdict maps
// through. The actual tool-loop (capture-cell applied-count) is exercised by the
// compose smoke, like why-table's synthesizeAnalysis.

import { beforeEach, describe, expect, it, vi } from "vitest";

const h = vi.hoisted(() => ({
	apiKey: "test-key" as string | undefined,
	readiness: [] as Array<Record<string, unknown>>,
	chat: vi.fn(async (_opts: unknown) => ({
		needs_judgement: false,
		judgement_note: null as string | null,
	})),
}));

vi.mock("#/config", () => ({
	get config() {
		return { anthropicApiKey: h.apiKey };
	},
}));
vi.mock("#/db/metadata/grounding-readiness", () => ({
	readGroundingReadiness: vi.fn(async () => h.readiness),
}));
vi.mock("@tanstack/ai", () => ({
	chat: (opts: unknown) => h.chat(opts),
	maxIterations: (n: number) => ({ maxIterations: n }),
	// The constrained tool is built at call time; the mock chat never invokes it,
	// so a stub with a no-op server is enough.
	toolDefinition: () => ({ server: () => ({ __tool: true }) }),
}));
vi.mock("@tanstack/ai-anthropic", () => ({
	createAnthropicChat: () => ({ __adapter: true }),
}));
vi.mock("#/tools/teach", () => ({
	teach: vi.fn(async () => ({ overlay_id: "ov-1", type: "type_pattern" })),
}));

import { assessAndGround } from "./grounding-agent";

function row(
	target: string,
	band: string,
	risk = 0.5,
): Record<string, unknown> {
	return {
		target,
		tableId: "t1",
		columnId: null,
		band,
		worstIntentRisk: risk,
		topDrivers: [
			{ node: "type_fidelity", state: "conflict", impact_delta: 0.4 },
		],
	};
}

beforeEach(() => {
	h.apiKey = "test-key";
	h.readiness = [];
	h.chat.mockClear();
	h.chat.mockResolvedValue({ needs_judgement: false, judgement_note: null });
});

describe("assessAndGround (DAT-551)", () => {
	it("fast-paths to a clean no-op when every target is ready (no LLM call)", async () => {
		h.readiness = [row("column:payments.amount", "ready")];
		const res = await assessAndGround({
			tableIds: ["t1"],
			attemptsRemaining: 3,
		});
		expect(res).toEqual({
			appliedCount: 0,
			needsJudgement: false,
			judgementNote: null,
		});
		expect(h.chat).not.toHaveBeenCalled();
	});

	it("fast-paths when nothing is measured yet (empty readiness, no LLM call)", async () => {
		h.readiness = [];
		const res = await assessAndGround({
			tableIds: ["t1"],
			attemptsRemaining: 3,
		});
		expect(res.appliedCount).toBe(0);
		expect(res.needsJudgement).toBe(false);
		expect(h.chat).not.toHaveBeenCalled();
	});

	it("surfaces for judgement WITHOUT an LLM call when there is no API key", async () => {
		h.apiKey = undefined;
		h.readiness = [row("column:payments.amount", "investigate")];
		const res = await assessAndGround({
			tableIds: ["t1"],
			attemptsRemaining: 3,
		});
		expect(res.needsJudgement).toBe(true);
		expect(res.judgementNote).toMatch(/no ANTHROPIC_API_KEY/i);
		expect(h.chat).not.toHaveBeenCalled();
	});

	it("runs the agent on a gap and maps its verdict through", async () => {
		h.readiness = [
			row("column:payments.amount", "investigate"),
			row("column:payments.method", "ready"), // ready ones are filtered out
		];
		h.chat.mockResolvedValue({
			needs_judgement: true,
			judgement_note: "payments.method needs a concept mapping",
		});
		const res = await assessAndGround({
			tableIds: ["t1"],
			attemptsRemaining: 2,
		});
		expect(h.chat).toHaveBeenCalledTimes(1);
		// appliedCount is 0 here because the mock chat doesn't invoke the tool — the
		// real applied-count wiring is smoke-covered.
		expect(res).toEqual({
			appliedCount: 0,
			needsJudgement: true,
			judgementNote: "payments.method needs a concept mapping",
		});
	});

	it("only sends NON-ready targets to the agent", async () => {
		h.readiness = [
			row("column:payments.amount", "blocked"),
			row("column:payments.id", "ready"),
		];
		await assessAndGround({ tableIds: ["t1"], attemptsRemaining: 3 });
		const opts = h.chat.mock.calls[0][0] as {
			messages: Array<{ content: string }>;
		};
		const userMsg = opts.messages[0].content;
		expect(userMsg).toContain("column:payments.amount");
		expect(userMsg).not.toContain("column:payments.id");
	});
});

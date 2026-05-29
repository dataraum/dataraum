import { describe, expect, it } from "vitest";
import { JOURNEY_STAGES } from "#/journey/stages";
import { getOrchestratorInstructions } from "#/prompts/orchestrator";

describe("orchestrator system prompt", () => {
	it("is byte-stable across calls (the prompt-cache invariant)", () => {
		// cache_control:ephemeral only hits if the system block is identical every
		// turn — so the builder must be a pure constant, no per-call interpolation.
		expect(getOrchestratorInstructions()).toBe(getOrchestratorInstructions());
	});

	it("names every journey stage so the agent can guide the journey", () => {
		const prompt = getOrchestratorInstructions();
		for (const stage of JOURNEY_STAGES) {
			expect(prompt).toContain(stage.id);
		}
	});
});

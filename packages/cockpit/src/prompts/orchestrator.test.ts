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

	// Lockstep with the list_tables/look_table/run_sql projections (DAT-433):
	// table_name is display-only prose, physical_name is the run_sql address, and
	// the content-keyed src_<digest> shape is named as never-echo internal.
	it("teaches the table_name/physical_name split and the src_<digest> rule", () => {
		const prompt = getOrchestratorInstructions();
		expect(prompt).toContain("physical_name");
		expect(prompt).toContain("lake.<layer>.<physical_name>");
		expect(prompt).toContain('"src_" followed by 40 hex characters');
		expect(prompt).toContain("name the FILE");
	});
});

import { describe, expect, it } from "vitest";
import { JOURNEY_STAGES, reEntryCost } from "#/journey/stages";
import { stageColors } from "#/ui/theme";

describe("JOURNEY_STAGES (DAT-347)", () => {
	it("lists the seven engine stages in journey order", () => {
		expect(JOURNEY_STAGES.map((s) => s.id)).toEqual([
			"connect",
			"frame",
			"select",
			"add_source",
			"begin_session",
			"operating_model",
			"answer",
		]);
	});

	it("marks exactly add_source interactive", () => {
		const interactive = JOURNEY_STAGES.filter((s) => s.interactive).map(
			(s) => s.id,
		);
		expect(interactive).toEqual(["add_source"]);
	});

	it("uses only ids that exist as theme stage colors", () => {
		const stageKeys = new Set(Object.keys(stageColors));
		for (const stage of JOURNEY_STAGES) {
			expect(stageKeys.has(stage.id)).toBe(true);
		}
		// And every stage color is covered by the journey (no orphan stage).
		expect(JOURNEY_STAGES).toHaveLength(stageKeys.size);
	});

	it("reEntryCost is a reserved stub that throws until implemented", () => {
		expect(() => reEntryCost("add_source")).toThrow(/not implemented/);
	});
});

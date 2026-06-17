import { describe, expect, it } from "vitest";
import { TEACH_TYPES } from "#/tools/teach.validation";
import { affectedStage } from "#/tools/teach-routing";

describe("affectedStage (DAT-531 teach → stage map)", () => {
	it("routes grounding teaches (incl. concept_property) to add_source", () => {
		for (const t of [
			"type_pattern",
			"null_value",
			"unit",
			"concept",
			"concept_property",
		]) {
			expect(affectedStage(t)).toBe("add_source");
		}
	});

	it("routes relationship + hierarchy to begin_session", () => {
		expect(affectedStage("relationship")).toBe("begin_session");
		expect(affectedStage("hierarchy")).toBe("begin_session");
	});

	it("routes the operating-model families to operating_model", () => {
		for (const t of ["validation", "cycle", "metric"]) {
			expect(affectedStage(t)).toBe("operating_model");
		}
	});

	it("maps EVERY current teach type (born-loud completeness)", () => {
		// The enum changes over time; this proves no live type routes nowhere.
		for (const t of TEACH_TYPES) {
			expect(() => affectedStage(t)).not.toThrow();
		}
	});

	it("throws born-loud on an unmapped type rather than misrouting", () => {
		expect(() => affectedStage("not_a_teach_type")).toThrow(/no stage mapped/);
	});
});

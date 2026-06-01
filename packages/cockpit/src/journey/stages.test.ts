import { describe, expect, it } from "vitest";
import {
	isOnboardingStage,
	JOURNEY_STAGES,
	ONBOARDING_STAGES,
	onboardingReadiness,
	reEntryCost,
} from "#/journey/stages";
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

describe("onboardingReadiness (DAT-378)", () => {
	it("ONBOARDING_STAGES is the connect→add_source prefix of the journey", () => {
		expect([...ONBOARDING_STAGES]).toEqual([
			"connect",
			"frame",
			"select",
			"add_source",
		]);
	});

	it("a new source (null cursor): only connect is ready, rest not_entered", () => {
		expect(onboardingReadiness(null, "connect")).toEqual({ kind: "ready" });
		expect(onboardingReadiness(null, "frame")).toEqual({ kind: "not_entered" });
		expect(onboardingReadiness(null, "select")).toEqual({
			kind: "not_entered",
		});
		expect(onboardingReadiness(null, "add_source")).toEqual({
			kind: "not_entered",
		});
	});

	it("cursor at connect: the next stage (frame) is ready, select not yet", () => {
		expect(onboardingReadiness("connect", "connect")).toEqual({
			kind: "ready",
		});
		expect(onboardingReadiness("connect", "frame")).toEqual({ kind: "ready" });
		expect(onboardingReadiness("connect", "select")).toEqual({
			kind: "not_entered",
		});
	});

	it("cursor at select: connect/frame/select re-enterable, add_source ready", () => {
		expect(onboardingReadiness("select", "connect")).toEqual({ kind: "ready" });
		expect(onboardingReadiness("select", "frame")).toEqual({ kind: "ready" });
		expect(onboardingReadiness("select", "select")).toEqual({ kind: "ready" });
		expect(onboardingReadiness("select", "add_source")).toEqual({
			kind: "ready",
		});
	});

	it("an unknown/forward-compat cursor value can't make a stage spuriously ready", () => {
		// treated as null → only connect ready
		expect(onboardingReadiness("begin_session", "frame")).toEqual({
			kind: "not_entered",
		});
		expect(onboardingReadiness("begin_session", "connect")).toEqual({
			kind: "ready",
		});
	});

	it("isOnboardingStage recognizes exactly the four onboarding cursors", () => {
		expect(isOnboardingStage("connect")).toBe(true);
		expect(isOnboardingStage("add_source")).toBe(true);
		expect(isOnboardingStage("begin_session")).toBe(false);
		expect(isOnboardingStage("nope")).toBe(false);
	});
});

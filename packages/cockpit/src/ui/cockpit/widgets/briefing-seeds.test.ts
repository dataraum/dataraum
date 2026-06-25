// Unit coverage for the briefing chip seeds (DAT-634) — pure.

import { describe, expect, it } from "vitest";

import type { BriefingAction } from "#/db/metadata/briefing";
import { nextActionSeed } from "./briefing-seeds";

function action(over: Partial<BriefingAction>): BriefingAction {
	return {
		kind: "teach",
		label: "x",
		targetChat: "stage",
		priority: 2,
		...over,
	};
}

describe("nextActionSeed", () => {
	it("replay → asks to replay", () => {
		expect(nextActionSeed(action({ kind: "replay" }))).toContain("replay");
	});

	it("teach → folds the label into the request", () => {
		const seed = nextActionSeed(
			action({ kind: "teach", label: "3 columns blocked — teach to unblock" }),
		);
		expect(seed).toContain("3 columns blocked");
	});

	it("begin_session → asks to build the model", () => {
		expect(nextActionSeed(action({ kind: "begin_session" }))).toContain(
			"begin_session",
		);
	});

	it("operating_model → asks to run the operating model", () => {
		expect(nextActionSeed(action({ kind: "operating_model" }))).toContain(
			"operating model",
		);
	});

	it("answer → opens an analysis question", () => {
		expect(nextActionSeed(action({ kind: "answer" }))).toContain("analyze");
	});

	it("review_blocker → folds the note into the request", () => {
		const seed = nextActionSeed(
			action({ kind: "review_blocker", label: "Tell me the NULL token" }),
		);
		expect(seed).toContain("Tell me the NULL token");
	});
});

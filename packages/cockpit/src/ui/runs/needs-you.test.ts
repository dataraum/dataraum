import { describe, expect, it } from "vitest";
import { resolveSeed } from "#/ui/runs/needs-you";

describe("resolveSeed (DAT-553)", () => {
	it("frames the awaitingNote as the user's own resolve request, nudging teach→replay", () => {
		const seed = resolveSeed("columns 'vid' and 'pt' have unclear meaning");
		expect(seed).toContain("columns 'vid' and 'pt' have unclear meaning");
		expect(seed.toLowerCase()).toContain("teach");
		expect(seed.toLowerCase()).toContain("replay");
	});

	it("degrades to a generic-but-actionable prompt when the note is null/blank", () => {
		for (const note of [null, "", "   "]) {
			const seed = resolveSeed(note);
			expect(seed.toLowerCase()).toContain("teach");
			expect(seed.toLowerCase()).toContain("judgement");
			expect(seed).not.toContain("undefined");
		}
	});
});

import { describe, expect, it } from "vitest";
import {
	formatStartedAt,
	stageLabel,
	statusLabel,
	statusTone,
} from "#/ui/runs/run-row";

describe("run-row presentation (DAT-550)", () => {
	it("labels known stages and passes unknown ones through", () => {
		expect(stageLabel("add_source")).toBe("Add source");
		expect(stageLabel("begin_session")).toBe("Begin session");
		expect(stageLabel("operating_model")).toBe("Operating model");
		expect(stageLabel("future_stage")).toBe("future_stage");
	});

	it("maps status to a tone, defaulting unknown to gray", () => {
		expect(statusTone("running")).toBe("blue");
		expect(statusTone("completed")).toBe("green");
		expect(statusTone("failed")).toBe("red");
		// The grounding loop's human-handoff state (DAT-551) — amber, not gray.
		expect(statusTone("awaiting_input")).toBe("yellow");
		// Aged-out-past-retention terminal (DAT-640) — neutral grey.
		expect(statusTone("retired")).toBe("gray");
		expect(statusTone("future_status")).toBe("gray");
	});

	it("labels awaiting_input as a call to action, others verbatim (DAT-551, DAT-640)", () => {
		expect(statusLabel("awaiting_input")).toBe("Needs input");
		// retired stays verbatim, consistent with the other terminal states.
		expect(statusLabel("retired")).toBe("retired");
		expect(statusLabel("running")).toBe("running");
		expect(statusLabel("completed")).toBe("completed");
	});

	it("formats startedAt as stable UTC from a Date or its wire string", () => {
		const iso = "2026-06-17T06:58:27.925Z";
		expect(formatStartedAt(new Date(iso))).toBe("2026-06-17 06:58 UTC");
		// Server-fn loader data may arrive as a string — same result (SSR-stable).
		expect(formatStartedAt(iso)).toBe("2026-06-17 06:58 UTC");
	});
});

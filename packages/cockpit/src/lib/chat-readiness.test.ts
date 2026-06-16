// Unit test for the in-chat readiness mapping (DAT-534). Pure — state→signal,
// no DB; the loader wiring + render are covered by the banner test + smoke.

import { describe, expect, it } from "vitest";
import { chatReadiness } from "#/lib/chat-readiness";

describe("chatReadiness (DAT-534)", () => {
	it("connect is always ready — no banner, regardless of state", () => {
		for (const hasTables of [true, false]) {
			for (const hasActiveRun of [true, false]) {
				expect(
					chatReadiness("connect", { hasTables, hasActiveRun }),
				).toBeNull();
			}
		}
	});

	it("stage/analyse with no data → do-X-first (blocked)", () => {
		for (const kind of ["stage", "analyse"] as const) {
			const r = chatReadiness(kind, { hasTables: false, hasActiveRun: false });
			expect(r?.tone).toBe("blocked");
			expect(r?.message).toMatch(/import/i);
		}
	});

	it("stage/analyse with data + an in-flight run → wait-for-Y (waiting)", () => {
		for (const kind of ["stage", "analyse"] as const) {
			const r = chatReadiness(kind, { hasTables: true, hasActiveRun: true });
			expect(r?.tone).toBe("waiting");
		}
	});

	it("stage/analyse with data and no run → ready (no banner)", () => {
		for (const kind of ["stage", "analyse"] as const) {
			expect(
				chatReadiness(kind, { hasTables: true, hasActiveRun: false }),
			).toBeNull();
		}
	});

	it("no data takes precedence over an in-flight run (import-first is the message)", () => {
		expect(
			chatReadiness("stage", { hasTables: false, hasActiveRun: true })?.tone,
		).toBe("blocked");
	});
});

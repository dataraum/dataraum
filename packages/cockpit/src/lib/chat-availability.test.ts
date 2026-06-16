// Unit test for the chat-type availability mapping (DAT-533, AC2). Pure — the
// state→types decision with no DB; the hasTables read + the loader wiring are
// covered by the live smoke.

import { describe, expect, it } from "vitest";
import { CHAT_KINDS, chatTypesFromState } from "#/lib/chat-availability";

describe("chatTypesFromState (DAT-533)", () => {
	it("connect is always startable, regardless of state", () => {
		for (const hasTables of [true, false]) {
			const connect = chatTypesFromState({ hasTables }).find(
				(t) => t.kind === "connect",
			);
			expect(connect).toEqual({
				kind: "connect",
				available: true,
				reason: null,
			});
		}
	});

	it("stage + analyse are unavailable (with a reason) when there's no data", () => {
		const byKind = new Map(
			chatTypesFromState({ hasTables: false }).map((t) => [t.kind, t]),
		);
		for (const kind of ["stage", "analyse"] as const) {
			expect(byKind.get(kind)?.available).toBe(false);
			expect(byKind.get(kind)?.reason).toBeTruthy(); // a tooltip to show
		}
	});

	it("stage + analyse become startable once data is imported", () => {
		const byKind = new Map(
			chatTypesFromState({ hasTables: true }).map((t) => [t.kind, t]),
		);
		for (const kind of ["stage", "analyse"] as const) {
			expect(byKind.get(kind)).toEqual({ kind, available: true, reason: null });
		}
	});

	it("returns exactly the three kinds in journey order", () => {
		expect(chatTypesFromState({ hasTables: true }).map((t) => t.kind)).toEqual([
			...CHAT_KINDS,
		]);
		expect(CHAT_KINDS).toEqual(["connect", "stage", "analyse"]);
	});
});

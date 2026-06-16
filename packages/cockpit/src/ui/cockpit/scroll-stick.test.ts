// Unit tests for the chat-rail stick-to-bottom decision (DAT-527).

import { describe, expect, it } from "vitest";

import { isNearBottom, STICK_THRESHOLD_PX } from "./scroll-stick";

describe("isNearBottom (DAT-527)", () => {
	it("is true at the exact bottom", () => {
		// scrollTop + clientHeight === scrollHeight → distance 0.
		expect(
			isNearBottom({ scrollTop: 900, scrollHeight: 1000, clientHeight: 100 }),
		).toBe(true);
	});

	it("is true within the slack threshold of the bottom", () => {
		// distance = 1000 - (860 + 100) = 40 ≤ 64.
		expect(
			isNearBottom({ scrollTop: 860, scrollHeight: 1000, clientHeight: 100 }),
		).toBe(true);
	});

	it("is false once scrolled further up than the threshold", () => {
		// distance = 1000 - (700 + 100) = 200 > 64 → reading history, don't yank.
		expect(
			isNearBottom({ scrollTop: 700, scrollHeight: 1000, clientHeight: 100 }),
		).toBe(false);
	});

	it("is true when content is shorter than the viewport (nothing to scroll)", () => {
		expect(
			isNearBottom({ scrollTop: 0, scrollHeight: 80, clientHeight: 100 }),
		).toBe(true);
	});

	it("honors a custom threshold", () => {
		const metrics = { scrollTop: 860, scrollHeight: 1000, clientHeight: 100 };
		// distance 40: within the default 64, outside a tight 10.
		expect(isNearBottom(metrics, STICK_THRESHOLD_PX)).toBe(true);
		expect(isNearBottom(metrics, 10)).toBe(false);
	});
});

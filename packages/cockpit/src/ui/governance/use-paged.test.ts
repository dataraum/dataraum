// Unit coverage for the pure pagination math (DAT-633). The hook itself is a
// thin useState wrapper; the logic lives in `paginate`.

import { describe, expect, it } from "vitest";

import { paginate } from "./use-paged";

const items = Array.from({ length: 53 }, (_, i) => i);

describe("paginate", () => {
	it("returns the first page and the correct total", () => {
		const v = paginate(items, 1, 15);
		expect(v.current).toBe(1);
		expect(v.totalPages).toBe(4); // ceil(53 / 15)
		expect(v.pageItems).toEqual(items.slice(0, 15));
	});

	it("returns a middle page window", () => {
		expect(paginate(items, 2, 15).pageItems).toEqual(items.slice(15, 30));
	});

	it("returns the partial last page (never drops the tail)", () => {
		const v = paginate(items, 4, 15);
		expect(v.pageItems).toEqual(items.slice(45, 53));
		expect(v.pageItems).toHaveLength(8);
	});

	it("clamps an over-large page to the last", () => {
		const v = paginate(items, 99, 15);
		expect(v.current).toBe(4);
		expect(v.pageItems).toEqual(items.slice(45, 53));
	});

	it("clamps a non-positive page to the first", () => {
		expect(paginate(items, 0, 15).current).toBe(1);
	});

	it("an empty list is one page with no items", () => {
		const v = paginate([], 1, 15);
		expect(v.totalPages).toBe(1);
		expect(v.pageItems).toEqual([]);
	});

	it("every item is reachable across the pages — no cap", () => {
		const seen = new Set<number>();
		const v0 = paginate(items, 1, 15);
		for (let p = 1; p <= v0.totalPages; p++) {
			for (const it of paginate(items, p, 15).pageItems) seen.add(it);
		}
		expect(seen.size).toBe(items.length);
	});
});

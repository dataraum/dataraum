import { describe, expect, it, vi } from "vitest";

// `buildConventionsBlock` lazily imports the reader-role metadata client (DAT-789) —
// stub it so the unit test drives a fixed row set (or a forced read error) instead of a
// real Postgres. The `#/` alias is load-bearing: a relative `./db/...` mock silently
// would not intercept.
const mockState = vi.hoisted(() => ({
	rows: [] as Array<Record<string, unknown>>,
	error: null as Error | null,
}));
vi.mock("#/db/metadata/client", () => ({
	metadataDb: {
		select: () => ({
			from: () => ({
				where: async () => {
					if (mockState.error) throw mockState.error;
					return mockState.rows;
				},
			}),
		}),
	},
}));

import { buildConventionsBlock, formatConventionsBlock } from "./conventions";

const conv = (over: Record<string, unknown> = {}) => [
	{
		id: "sign_natural_balance",
		targets: ["extraction", "validation:sign_conventions", "qa"],
		statement:
			"Express every measure in its natural-balance direction so it reads positive.",
		concept_groups: {
			credit_normal: ["revenue", "equity"],
			debit_normal: ["cost_of_goods_sold"],
		},
		...over,
	},
];

describe("formatConventionsBlock", () => {
	it("renders the statement + groups verbatim for a qa-targeted convention", () => {
		const out = formatConventionsBlock(conv());
		expect(out).toContain("<domain_conventions>");
		expect(out).toContain("natural-balance direction");
		expect(out).toContain("credit_normal: revenue, equity");
		expect(out).toContain("debit_normal: cost_of_goods_sold");
		expect(out.endsWith("</domain_conventions>")).toBe(true);
	});

	it("returns '' when no convention targets the consumer", () => {
		expect(formatConventionsBlock(conv({ targets: ["extraction"] }))).toBe("");
	});

	it("routes by the target label", () => {
		// Same convention, asked for the `extraction` consumer instead.
		expect(formatConventionsBlock(conv(), "extraction")).toContain(
			"<domain_conventions>",
		);
	});

	it("returns '' for non-array / empty / no-statement input", () => {
		expect(formatConventionsBlock(null)).toBe("");
		expect(formatConventionsBlock(undefined)).toBe("");
		expect(formatConventionsBlock([])).toBe("");
		expect(formatConventionsBlock([{ targets: ["qa"] }])).toBe(""); // statement missing
	});

	it("narrows untrusted shapes (rule 11) — bad targets/members are skipped", () => {
		// targets not an array → excluded.
		expect(formatConventionsBlock([{ targets: "qa", statement: "x" }])).toBe(
			"",
		);
		// a non-array group is dropped, the rest still renders.
		const out = formatConventionsBlock([
			{
				targets: ["qa"],
				statement: "x",
				concept_groups: { good: ["a"], bad: "nope" },
			},
		]);
		expect(out).toContain("good: a");
		expect(out).not.toContain("bad:");
	});
});

describe("buildConventionsBlock (DAT-789 — reads the typed conventions home)", () => {
	it("renders the active-vertical conventions targeting the consumer", async () => {
		// The DB read shape: rows already scoped to the active vertical + un-superseded
		// by the mirrored view; this fn only routes by label. `concept_groups` arrives
		// under the snake alias the projection selects (`conventionsView.conceptGroups`).
		mockState.error = null;
		mockState.rows = [
			{
				targets: ["extraction", "qa"],
				statement: "sign every measure to read positive",
				concept_groups: { credit_normal: ["revenue"] },
			},
			{
				targets: ["extraction"], // does NOT target qa → omitted
				statement: "extraction-only rule",
				concept_groups: null,
			},
		];
		const out = await buildConventionsBlock("qa");
		expect(out).toContain("<domain_conventions>");
		expect(out).toContain("sign every measure to read positive");
		expect(out).toContain("credit_normal: revenue");
		expect(out).not.toContain("extraction-only rule");
	});

	it("returns '' when no row targets the consumer", async () => {
		mockState.error = null;
		mockState.rows = [
			{ targets: ["extraction"], statement: "x", concept_groups: null },
		];
		expect(await buildConventionsBlock("qa")).toBe("");
	});

	it("never throws on a metadata-read blip — returns '' (an answer must not fail)", async () => {
		mockState.error = new Error("connection reset");
		expect(await buildConventionsBlock("qa")).toBe("");
		mockState.error = null;
	});
});

import { describe, expect, it, vi } from "vitest";

// conventions.ts imports `config` at module load (env-validated at import) — stub
// it so the unit test doesn't boot the real Zod env. The pure formatter under test
// doesn't use config; this just keeps the import side-effect-free.
vi.mock("#/config", () => ({ config: { dataraumConfigPath: "/unused" } }));

import { formatConventionsBlock } from "./conventions";

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

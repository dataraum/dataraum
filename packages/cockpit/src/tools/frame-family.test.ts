// Unit tests for the generic frame-a-family core (DAT-469) — the pure helpers
// that don't need the LLM or the DB: the library-as-seed formatting + the
// nearest-shipped-vertical resolution + the payload sanitizer. `induceNative`
// (the LLM call) and the `frameFamily` write loop are an LLM/DB boundary that a
// mock can only fake — the call SHAPE is pinned by induce-native.contract.test.ts
// and the schemas by induction-schema.contract.test.ts; behaviour is verified by
// the live frame→import smoke (DAT-597), not unit tests.

import { describe, expect, it, vi } from "vitest";

// Importing frame-family.ts transitively boots config.ts + the Postgres metadata
// client (via teach.ts). The pure helpers under test touch neither, so mock both
// so the module imports cleanly (the cockpit unit-test convention).
vi.mock("#/config", () => ({
	config: {
		anthropicApiKey: "sk-ant-test",
		dataraumConfigPath: "/nonexistent",
	},
}));
// Mode-shared base config (DAT-819) — reached transitively via the
// registry/db seam; parsing the real one needs env this test does not set.
vi.mock("#/config.base", () => ({ baseConfig: {} }));
vi.mock("#/db/metadata/client", () => ({
	metadataDb: { insert: vi.fn(() => ({ values: vi.fn() })) },
}));

import {
	formatSeedExamples,
	nearestSeedVertical,
	stripUndefined,
} from "./frame-family";

describe("stripUndefined", () => {
	it("drops undefined-valued keys, keeps null and falsy values", () => {
		expect(
			stripUndefined({ a: 1, b: undefined, c: null, d: 0, e: "" }),
		).toEqual({ a: 1, c: null, d: 0, e: "" });
	});
});

describe("formatSeedExamples (library-as-seed)", () => {
	it("frames the shipped specs as examples / structural / do-not-copy", () => {
		const out = formatSeedExamples(
			[{ validation_id: "trial_balance", check_type: "balance" }],
			{ vertical: "finance", family: "validation" },
		);
		// The framing the AC requires: examples, structural, do-not-copy.
		expect(out).toMatch(/EXAMPLE/);
		expect(out).toMatch(/STRUCTURE|structural/i);
		expect(out).toMatch(/do not (reuse|copy)|not\s+content to copy/i);
		// The seed content is included so the model sees the field shape.
		expect(out).toContain("trial_balance");
		// Tagged by family + the vertical the examples came from.
		expect(out).toContain('<validation_examples vertical="finance">');
	});

	it("emits a no-library note (not the example framing) for an empty seed", () => {
		const out = formatSeedExamples([], {
			vertical: "",
			family: "validation",
		});
		expect(out).toMatch(/No shipped validation library/i);
		expect(out).not.toMatch(/EXAMPLE validation specs/);
	});

	it("frames METRIC seed DAGs as examples / structural / do-not-copy (DAT-471)", () => {
		// The metric family reuses the generic seed helper — the same example /
		// structural / do-not-copy framing the AC requires, tagged by `metric`.
		const out = formatSeedExamples(
			[{ graph_id: "ebitda", dependencies: { op_income: {} } }],
			{ vertical: "finance", family: "metric" },
		);
		expect(out).toMatch(/EXAMPLE/);
		expect(out).toMatch(/STRUCTURE|structural/i);
		expect(out).toMatch(/do not (reuse|copy)|not\s+content to copy/i);
		// The seed DAG content is included so the model sees the dependency shape.
		expect(out).toContain("ebitda");
		expect(out).toContain('<metric_examples vertical="finance">');
	});
});

describe("nearestSeedVertical", () => {
	it("uses the framed vertical's own shipped specs when it ships any", async () => {
		const readSeed = async (v: string) =>
			v === "finance" ? [{ id: "a" }, { id: "b" }] : [];
		const result = await nearestSeedVertical("finance", readSeed, async () => [
			"finance",
			"retail",
		]);
		expect(result.vertical).toBe("finance");
		expect(result.specs).toHaveLength(2);
	});

	it("falls back to the richest OTHER shipped builtin when the framed vertical ships none", async () => {
		// A brand-new framed vertical (`sales`) ships nothing; finance ships the
		// most specs → it's the structural reference.
		const library: Record<string, { id: string }[]> = {
			finance: [{ id: "a" }, { id: "b" }, { id: "c" }],
			retail: [{ id: "x" }],
			sales: [],
		};
		const readSeed = async (v: string) => library[v] ?? [];
		const result = await nearestSeedVertical("sales", readSeed, async () => [
			"finance",
			"retail",
			"sales",
		]);
		expect(result.vertical).toBe("finance");
		expect(result.specs).toHaveLength(3);
	});

	it("skips the framed vertical in the fallback scan and yields empty when nothing ships the family", async () => {
		const readSeed = async () => [] as { id: string }[];
		const result = await nearestSeedVertical("sales", readSeed, async () => [
			"sales",
		]);
		expect(result.vertical).toBe("");
		expect(result.specs).toEqual([]);
	});
});

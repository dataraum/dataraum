// Unit tests for `loadValidationParams` (ADR-0017 / DAT-725 teach-surface
// retire). Guards that the declared judgement params (tolerance/severity) are
// read from the SEEDED reader's typed `tolerance` field directly — not the
// legacy `parameters.tolerance` bag — and that a spec with no declared
// tolerance falls back to `DEFAULT_TOLERANCE`.
//
// `runValidationVerdicts` imports the DuckDB lake node bindings at module
// load, so this file only exercises `loadValidationParams`, which does not
// touch the lake — no lake mock needed.

import { describe, expect, it, vi } from "vitest";

// `validation-verdict-runner.ts` imports `../duckdb/lake`, which imports
// `../config` at module top — config.ts throws immediately at import when its
// env vars are unset, so mock it (the `#/` alias — relative specifiers
// silently don't intercept, house rule) even though this file never touches
// the lake directly (only `loadValidationParams` is exercised here).
vi.mock("#/config", () => ({ config: {} }));
vi.mock("#/tools/teach-validation", () => ({
	readSeededValidations: vi.fn(),
}));

import { readSeededValidations } from "./teach-validation";
import { DEFAULT_TOLERANCE } from "./validation-verdict";
import { loadValidationParams } from "./validation-verdict-runner";

describe("loadValidationParams (DAT-725)", () => {
	it("reads the typed tolerance field directly from the seeded reader", async () => {
		vi.mocked(readSeededValidations).mockResolvedValue([
			{
				validation_id: "trial_balance",
				name: "Trial Balance",
				description: "…",
				check_type: "balance",
				severity: "critical",
				tolerance: 0.01,
				guidance: "Sum debit - credit.",
			},
		]);

		const params = await loadValidationParams("finance");
		expect(params.get("trial_balance")).toEqual({
			tolerance: 0.01,
			severity: "critical",
		});
	});

	it("falls back to DEFAULT_TOLERANCE when a seeded spec declares no tolerance", async () => {
		vi.mocked(readSeededValidations).mockResolvedValue([
			{
				validation_id: "orphan_transactions",
				name: "Orphan Transactions",
				description: "…",
				check_type: "aggregate",
				severity: "warning",
				tolerance: null,
				guidance: null,
			},
		]);

		const params = await loadValidationParams("finance");
		expect(params.get("orphan_transactions")).toEqual({
			tolerance: DEFAULT_TOLERANCE,
			severity: "warning",
		});
	});

	it("returns an empty map when nothing is seeded", async () => {
		vi.mocked(readSeededValidations).mockResolvedValue([]);
		const params = await loadValidationParams("finance");
		expect(params.size).toBe(0);
	});
});

// Unit tests for the why_validation projection (DAT-440 / docs/architecture/grounding.md). Pure — no
// DB; the live read + on-demand verdict path is covered by the operating_model
// integration smoke.
//
// What this guards: the found discriminant, the verbatim (digest-sanitized)
// reason/sql pass-through, the RECOMPUTED verdict (status/passed/message) folded
// over the artifact, and unknown-shape JSON (grounded_against) rendered through
// the shared evidence sanitizer — never assumed, never a stored verdict.

import { describe, expect, it, vi } from "vitest";

// Importing the tool pulls config.ts + the Postgres metadata client; mock both
// (with the `#/` alias — relative specifiers silently don't intercept) so the
// pure projection runs with no env and no connection.
vi.mock("#/config", () => ({ config: {} }));
vi.mock("#/db/metadata/client", () => ({ metadataDb: {} }));

import type { Verdict } from "./validation-verdict";
import type { ValidationParams } from "./validation-verdict-runner";
import {
	projectWhyValidation,
	type WhyValidationArtifactRow,
	type WhyValidationResultRow,
} from "./why-validation";

const executedArtifact: WhyValidationArtifactRow = {
	state: "executed",
	stateReason: null,
	strictness: 0.8,
	groundedAgainst: { tables: ["invoices", "payments"] },
};

const executedResult: WhyValidationResultRow = {
	sqlUsed: "SELECT 12 AS deviation, 100 AS magnitude",
	executedAt: new Date("2026-06-07T12:00:00Z"),
	columnsUsed: ["invoices.id", "payments.invoice_id"],
};

const failedVerdict: Verdict = {
	status: "failed",
	passed: false,
	deviation: 12,
	magnitude: 100,
	message: "validation: deviation 12 (tolerance 0.01)",
};

const errorParams: ValidationParams = { tolerance: 0.01, severity: "error" };

describe("projectWhyValidation (DAT-440 / docs/architecture/grounding.md)", () => {
	it("assembles the executed drill-down: state + recomputed verdict + grounded detail", () => {
		const projected = projectWhyValidation(
			"gl_invoice_match",
			executedArtifact,
			executedResult,
			failedVerdict,
			errorParams,
			2,
		);

		expect(projected).toEqual({
			validation_id: "gl_invoice_match",
			found: true,
			state: "executed",
			state_reason: null,
			strictness: 0.8,
			grounded_against: JSON.stringify({ tables: ["invoices", "payments"] }),
			status: "failed",
			severity: "error",
			passed: false,
			message: "validation: deviation 12 (tolerance 0.01)",
			sql_used: "SELECT 12 AS deviation, 100 AS magnitude",
			executed_at: "2026-06-07T12:00:00.000Z",
			details: JSON.stringify({
				deviation: 12,
				magnitude: 100,
				tolerance: 0.01,
			}),
			columns_used: ["invoices.id", "payments.invoice_id"],
			pending_teaches: 2,
		});
	});

	it("found=false for an unknown validation — all fields null/empty, nothing invented", () => {
		const projected = projectWhyValidation(
			"nope",
			null,
			null,
			undefined,
			undefined,
			0,
		);

		expect(projected.found).toBe(false);
		expect(projected.state).toBeNull();
		expect(projected.state_reason).toBeNull();
		expect(projected.status).toBeNull();
		expect(projected.passed).toBeNull();
		expect(projected.sql_used).toBeNull();
		expect(projected.executed_at).toBeNull();
		expect(projected.grounded_against).toBe("");
		expect(projected.details).toBe("");
	});

	it("a declared-with-reason validation is found, with the reason verbatim", () => {
		const projected = projectWhyValidation(
			"gl_invoice_match",
			{
				state: "declared",
				stateReason: "Missing required tables: journal_entries",
				strictness: null,
				groundedAgainst: null,
			},
			null,
			undefined,
			undefined,
			0,
		);

		expect(projected.found).toBe(true);
		expect(projected.state).toBe("declared");
		expect(projected.state_reason).toBe(
			"Missing required tables: journal_entries",
		);
		// Never grounded → empty grounding render; no verdict fields invented.
		expect(projected.grounded_against).toBe("");
		expect(projected.status).toBeNull();
	});

	it("renders narrow names in reason and SQL; stays digest-free (DAT-639)", () => {
		const projected = projectWhyValidation(
			"balance_check",
			{
				state: "executed",
				stateReason: `bound against orders`,
				strictness: null,
				// `_`-prefixed engine plumbing keys are dropped by the sanitizer.
				groundedAgainst: { _table_name: `orders`, table: "orders" },
			},
			{
				sqlUsed: `SELECT 0 AS deviation, 1 AS magnitude FROM lake.typed.orders`,
				executedAt: null,
				columnsUsed: [`orders.amount`],
			},
			{
				status: "passed",
				passed: true,
				deviation: 0,
				magnitude: 1,
				message: "validation: deviation 0 (tolerance 0.01)",
			},
			{ tolerance: 0.01, severity: null },
			0,
		);

		expect(JSON.stringify(projected)).not.toMatch(/src_[0-9a-f]{40}/);
		expect(projected.passed).toBe(true);
		expect(projected.sql_used).toBe(
			"SELECT 0 AS deviation, 1 AS magnitude FROM lake.typed.orders",
		);
		expect(projected.grounded_against).toBe(
			JSON.stringify({ table: "orders" }),
		);
	});
});

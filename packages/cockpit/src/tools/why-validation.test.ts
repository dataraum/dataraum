// Unit tests for the why_validation projection (DAT-440). Pure — no DB; the
// live read path is covered by the operating_model integration smoke.
//
// What this guards: the found discriminant, the verbatim (digest-sanitized)
// reason/message/sql pass-through, and unknown-shape JSON (grounded_against /
// details) rendering through the shared evidence sanitizer — never assumed.

import { describe, expect, it, vi } from "vitest";

// Importing the tool pulls config.ts + the Postgres metadata client; mock both
// (with the `#/` alias — relative specifiers silently don't intercept) so the
// pure projection runs with no env and no connection.
vi.mock("#/config", () => ({ config: {} }));
vi.mock("#/db/metadata/client", () => ({ metadataDb: {} }));

import {
	projectWhyValidation,
	type WhyValidationArtifactRow,
	type WhyValidationResultRow,
} from "./why-validation";

const D1 = "204bc8e118543a6c35654c1f68c43539a2e226f2";

const executedArtifact: WhyValidationArtifactRow = {
	state: "executed",
	stateReason: null,
	strictness: 0.8,
	groundedAgainst: { tables: ["invoices", "payments"] },
};

const executedResult: WhyValidationResultRow = {
	status: "executed",
	severity: "error",
	passed: false,
	message: "12 invoices have no matching payment",
	sqlUsed: "SELECT i.id FROM invoices i LEFT JOIN payments p ON …",
	executedAt: new Date("2026-06-07T12:00:00Z"),
	details: { failing_rows: 12 },
	columnsUsed: ["invoices.id", "payments.invoice_id"],
};

describe("projectWhyValidation (DAT-440)", () => {
	it("assembles the executed drill-down: state + result + grounded detail", () => {
		const projected = projectWhyValidation(
			"gl_invoice_match",
			executedArtifact,
			executedResult,
			2,
		);

		expect(projected).toEqual({
			validation_id: "gl_invoice_match",
			found: true,
			state: "executed",
			state_reason: null,
			strictness: 0.8,
			grounded_against: JSON.stringify({ tables: ["invoices", "payments"] }),
			status: "executed",
			severity: "error",
			passed: false,
			message: "12 invoices have no matching payment",
			sql_used: "SELECT i.id FROM invoices i LEFT JOIN payments p ON …",
			executed_at: "2026-06-07T12:00:00.000Z",
			details: JSON.stringify({ failing_rows: 12 }),
			columns_used: ["invoices.id", "payments.invoice_id"],
			pending_teaches: 2,
		});
	});

	it("found=false for an unknown validation — all fields null/empty, nothing invented", () => {
		const projected = projectWhyValidation("nope", null, null, 0);

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
			0,
		);

		expect(projected.found).toBe(true);
		expect(projected.state).toBe("declared");
		expect(projected.state_reason).toBe(
			"Missing required tables: journal_entries",
		);
		// Never grounded → empty grounding render; no result fields invented.
		expect(projected.grounded_against).toBe("");
		expect(projected.status).toBeNull();
	});

	it("strips content-keyed digests from reason, message, and SQL; sanitizes JSON blobs", () => {
		const projected = projectWhyValidation(
			"balance_check",
			{
				state: "executed",
				stateReason: `bound against src_${D1}__orders`,
				strictness: null,
				// `_`-prefixed engine plumbing keys are dropped by the sanitizer.
				groundedAgainst: { _table_name: `src_${D1}__orders`, table: "orders" },
			},
			{
				status: "executed",
				severity: null,
				passed: true,
				message: `src_${D1}__orders is balanced`,
				sqlUsed: `SELECT count(*) FROM lake.typed.src_${D1}__orders`,
				executedAt: null,
				details: { table: `src_${D1}__orders` },
				columnsUsed: [`src_${D1}__orders.amount`],
			},
			0,
		);

		expect(JSON.stringify(projected)).not.toMatch(/src_[0-9a-f]{40}/);
		expect(projected.message).toBe("orders is balanced");
		expect(projected.sql_used).toBe("SELECT count(*) FROM lake.typed.orders");
		expect(projected.grounded_against).toBe(
			JSON.stringify({ table: "orders" }),
		);
	});
});

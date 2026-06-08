// Unit tests for teach_validation (DAT-441). Pure — the schema + the shadow
// detection run with no DB and no config tree. The DB-bound write path reuses
// `teach()` (covered by the teach integration smoke); the live config-tree read
// is browser/integration-smoke territory. What this guards:
//   - the spec input is a top-level object whose `check_type` / `severity` are
//     CLOSED enums (no free-text type — the ticket's hard requirement);
//   - the shadow narrowing turns a shipped YAML doc into the summary shape, or
//     null when it isn't a validation spec;
//   - findShadowedSpec is an exact id match → the override flag is honest.

import { describe, expect, it } from "vitest";

import {
	CHECK_TYPES,
	findShadowedSpec,
	narrowShippedSpec,
	SEVERITIES,
	type ShippedValidationSpec,
	ValidationSpecSchema,
} from "./validation-spec";

const MINIMAL = {
	vertical: "finance",
	validation_id: "invoice_reconciliation",
	name: "Invoice Reconciliation",
	description: "Invoice amounts must reconcile with journal lines.",
	category: "financial",
	severity: "warning" as const,
	check_type: "aggregate" as const,
};

describe("ValidationSpecSchema (DAT-441)", () => {
	it("accepts a minimal spec (required fields only)", () => {
		const parsed = ValidationSpecSchema.parse(MINIMAL);
		expect(parsed.validation_id).toBe("invoice_reconciliation");
		expect(parsed.check_type).toBe("aggregate");
		// Optionals stay undefined — the write path strips them.
		expect(parsed.parameters).toBeUndefined();
		expect(parsed.sql_hints).toBeUndefined();
	});

	it("accepts a full spec mirroring the finance YAML shape", () => {
		const parsed = ValidationSpecSchema.parse({
			...MINIMAL,
			validation_id: "trial_balance",
			name: "Trial Balance",
			check_type: "balance",
			severity: "critical",
			parameters: { tolerance: 5.0, asset_types: ["asset", "assets"] },
			sql_hints: "Sum debit - credit per account_type.",
			expected_outcome: "left_side + right_side ≈ 0 within tolerance.",
			tags: ["accounting", "balance-sheet"],
			relevant_cycles: ["journal_entry_cycle"],
		});
		expect(parsed.parameters?.tolerance).toBe(5.0);
		expect(parsed.tags).toEqual(["accounting", "balance-sheet"]);
		expect(parsed.relevant_cycles).toEqual(["journal_entry_cycle"]);
	});

	it.each(CHECK_TYPES)("accepts the closed check_type '%s'", (check_type) => {
		expect(
			ValidationSpecSchema.parse({ ...MINIMAL, check_type }).check_type,
		).toBe(check_type);
	});

	it("REJECTS a free-text check_type (the closed-enum requirement)", () => {
		expect(
			ValidationSpecSchema.safeParse({
				...MINIMAL,
				check_type: "made_up_check",
			}).success,
		).toBe(false);
	});

	it.each(SEVERITIES)("accepts the closed severity '%s'", (severity) => {
		expect(ValidationSpecSchema.parse({ ...MINIMAL, severity }).severity).toBe(
			severity,
		);
	});

	it("REJECTS a free-text severity", () => {
		expect(
			ValidationSpecSchema.safeParse({ ...MINIMAL, severity: "blocker" })
				.success,
		).toBe(false);
	});

	it.each([
		"vertical",
		"validation_id",
		"name",
		"description",
		"category",
		"severity",
		"check_type",
	])("rejects a spec missing required field '%s'", (field) => {
		const incomplete: Record<string, unknown> = { ...MINIMAL };
		delete incomplete[field];
		expect(ValidationSpecSchema.safeParse(incomplete).success).toBe(false);
	});

	it("rejects an empty validation_id / vertical (min length)", () => {
		expect(
			ValidationSpecSchema.safeParse({ ...MINIMAL, validation_id: "" }).success,
		).toBe(false);
		expect(
			ValidationSpecSchema.safeParse({ ...MINIMAL, vertical: "" }).success,
		).toBe(false);
	});
});

describe("narrowShippedSpec (DAT-441)", () => {
	it("narrows a parsed validation YAML to the summary fields", () => {
		const spec = narrowShippedSpec({
			validation_id: "trial_balance",
			name: "Trial Balance (Accounting Equation)",
			description: "Validates the expanded accounting equation.",
			check_type: "balance",
			severity: "critical",
			parameters: { tolerance: 0.01 },
			// extra YAML fields are ignored by the narrowing
			tags: ["accounting"],
		});
		expect(spec).toEqual({
			validation_id: "trial_balance",
			name: "Trial Balance (Accounting Equation)",
			description: "Validates the expanded accounting equation.",
			check_type: "balance",
			severity: "critical",
			parameters: { tolerance: 0.01 },
		});
	});

	it("returns null for a doc with no validation_id (not a spec file)", () => {
		expect(narrowShippedSpec({ description: "no id here" })).toBeNull();
		expect(narrowShippedSpec(null)).toBeNull();
		expect(narrowShippedSpec(undefined)).toBeNull();
	});

	it("coalesces non-string fields to null, non-object parameters to null", () => {
		const spec = narrowShippedSpec({
			validation_id: "x",
			name: 123,
			parameters: "not an object",
		});
		expect(spec).toEqual({
			validation_id: "x",
			name: null,
			description: null,
			check_type: null,
			severity: null,
			parameters: null,
		});
	});
});

describe("findShadowedSpec (DAT-441)", () => {
	const shipped: ShippedValidationSpec[] = [
		{
			validation_id: "trial_balance",
			name: "Trial Balance",
			description: "…",
			check_type: "balance",
			severity: "critical",
			parameters: { tolerance: 0.01 },
		},
		{
			validation_id: "gl_invoice_match",
			name: "GL-Invoice Match",
			description: "…",
			check_type: "aggregate",
			severity: "warning",
			parameters: null,
		},
	];

	it("returns the shipped spec when the id matches (an override)", () => {
		const shadowed = findShadowedSpec(shipped, "trial_balance");
		expect(shadowed?.validation_id).toBe("trial_balance");
		expect(shadowed?.parameters).toEqual({ tolerance: 0.01 });
	});

	it("returns null when the id is new (a fresh declaration)", () => {
		expect(findShadowedSpec(shipped, "invoice_reconciliation")).toBeNull();
	});

	it("returns null against an empty shipped set", () => {
		expect(findShadowedSpec([], "trial_balance")).toBeNull();
	});
});

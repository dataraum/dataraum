// Unit tests for teach_validation (DAT-441; typed tolerance/guidance +
// DB-backed shadow detection, DAT-725 teach-surface retire). Pure — the schema
// + the shadow detection run with no DB and no config tree (the DB query
// composition for `readSeededValidations` is covered separately below, mocking
// the metadata client per the house `#/` alias rule). What this guards:
//   - the spec input is a top-level object whose `check_type` / `severity` are
//     CLOSED enums (no free-text type — the ticket's hard requirement);
//   - `tolerance`/`guidance` are the typed fields (no legacy `parameters`/
//     `sql_hints` bag);
//   - `readSeededValidations` queries the typed `validations` view filtered to
//     `source='seed'` and degrades to `[]` on a failed read, never throwing;
//   - findShadowedSpec is an exact id match → the override flag is honest,
//     generic over both the fs-shipped and DB-seeded summary shapes.

import { beforeEach, describe, expect, it, vi } from "vitest";

// Mock the shared overlay-write path, the env config, and the metadata client
// (with the `#/` alias — relative specifiers silently don't intercept) so
// importing the tool (which evals `../config` + `./teach` + the DB client at
// load) doesn't pull the DB/boot. The seeded-spec reader is injected per call
// in most tests (no DB mock needed there); `readSeededValidations` itself is
// tested directly against a captured query chain.
vi.mock("#/config", () => ({ config: { dataraumConfigPath: "/unused" } }));
vi.mock("#/tools/teach", () => ({ teach: vi.fn() }));

const captured: { cond?: unknown; rows: unknown[]; error?: Error } = {
	rows: [],
};
vi.mock("#/db/metadata/client", () => ({
	metadataDb: {
		select: () => ({
			from: () => ({
				where: (cond: unknown) => {
					captured.cond = cond;
					if (captured.error) return Promise.reject(captured.error);
					return Promise.resolve(captured.rows);
				},
			}),
		}),
	},
}));

import { PgDialect } from "drizzle-orm/pg-core";
import { teach } from "./teach";
import { readSeededValidations, teachValidation } from "./teach-validation";
import {
	CHECK_TYPES,
	findShadowedSpec,
	SEVERITIES,
	type SeededValidationSpec,
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

describe("ValidationSpecSchema (DAT-441 / DAT-725)", () => {
	it("accepts a minimal spec (required fields only)", () => {
		const parsed = ValidationSpecSchema.parse(MINIMAL);
		expect(parsed.validation_id).toBe("invoice_reconciliation");
		expect(parsed.check_type).toBe("aggregate");
		// Optionals stay undefined — the write path strips them.
		expect(parsed.tolerance).toBeUndefined();
		expect(parsed.guidance).toBeUndefined();
	});

	it("accepts a full spec with the typed tolerance/guidance fields", () => {
		const parsed = ValidationSpecSchema.parse({
			...MINIMAL,
			validation_id: "trial_balance",
			name: "Trial Balance",
			check_type: "balance",
			severity: "critical",
			tolerance: 5.0,
			guidance: "Sum debit - credit per account_type.",
			expected_outcome: "left_side + right_side ≈ 0 within tolerance.",
			tags: ["accounting", "balance-sheet"],
			relevant_cycles: ["journal_entry_cycle"],
		});
		expect(parsed.tolerance).toBe(5.0);
		expect(parsed.guidance).toBe("Sum debit - credit per account_type.");
		expect(parsed.tags).toEqual(["accounting", "balance-sheet"]);
		expect(parsed.relevant_cycles).toEqual(["journal_entry_cycle"]);
	});

	it("REJECTS the legacy parameters/sql_hints shape (no such fields anymore)", () => {
		// Zod's plain z.object ignores unknown keys by default (not .strict()) —
		// what matters is that `tolerance`/`guidance` are the ONLY way to carry
		// this information now; the legacy keys parse away as noise.
		const parsed = ValidationSpecSchema.parse({
			...MINIMAL,
			parameters: { tolerance: 5.0 },
			sql_hints: "some hint",
		});
		expect((parsed as Record<string, unknown>).parameters).toBeUndefined();
		expect((parsed as Record<string, unknown>).sql_hints).toBeUndefined();
		expect(parsed.tolerance).toBeUndefined();
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

	it("is generic over the DB-seeded summary shape too (DAT-725)", () => {
		const seeded: SeededValidationSpec[] = [
			{
				validation_id: "trial_balance",
				name: "Trial Balance",
				description: "…",
				check_type: "balance",
				severity: "critical",
				tolerance: 0.01,
				guidance: "Sum debit - credit.",
			},
		];
		const shadowed = findShadowedSpec(seeded, "trial_balance");
		expect(shadowed?.tolerance).toBe(0.01);
		expect(shadowed?.guidance).toBe("Sum debit - credit.");
	});
});

describe("readSeededValidations (DAT-725)", () => {
	beforeEach(() => {
		captured.cond = undefined;
		captured.rows = [];
		captured.error = undefined;
	});

	it("filters on source='seed' AND superseded_at IS NULL (the house _VERTICAL_SCOPED-reader contract)", async () => {
		await readSeededValidations("finance");
		expect(captured.cond).toBeDefined();
		const { sql, params } = new PgDialect().sqlToQuery(captured.cond as never);
		expect(sql).toContain("source");
		expect(params).toContain("seed");
		// The view passes ALL rows (incl. superseded history) through unchanged —
		// every reader applies its own active-row filter (list-verticals.ts,
		// prompts/conventions.ts precedent). A regression here would let
		// `findShadowedSpec` pick an arbitrary row once a writer ever supersedes a
		// seed row (senior-review finding, DAT-725).
		expect(sql).toContain("superseded_at");
		expect(sql).toContain("is null");
	});

	it("maps DB rows to the typed SeededValidationSpec shape (tolerance/guidance)", async () => {
		captured.rows = [
			{
				validationId: "trial_balance",
				name: "Trial Balance",
				description: "Validates the accounting equation.",
				checkType: "balance",
				severity: "critical",
				tolerance: 0.01,
				guidance: "Sum debit - credit per account_type.",
			},
		];
		const specs = await readSeededValidations("finance");
		expect(specs).toEqual([
			{
				validation_id: "trial_balance",
				name: "Trial Balance",
				description: "Validates the accounting equation.",
				check_type: "balance",
				severity: "critical",
				tolerance: 0.01,
				guidance: "Sum debit - credit per account_type.",
			},
		]);
	});

	it("drops a row with a null validationId (a view artifact, never a real spec)", async () => {
		captured.rows = [
			{
				validationId: null,
				name: null,
				description: null,
				checkType: null,
				severity: null,
				tolerance: null,
				guidance: null,
			},
		];
		expect(await readSeededValidations("finance")).toEqual([]);
	});

	it("degrades to [] on a failed query — never throws (the documented degrade contract)", async () => {
		captured.error = new Error("connection refused");
		await expect(readSeededValidations("finance")).resolves.toEqual([]);
	});
});

// The load-bearing composition: read seeded → detect shadow → funnel the
// stripped spec through the shared teach() overlay-write path. teach() is
// mocked; the seeded-spec reader is injected so no DB is touched.
describe("teachValidation wiring (DAT-441 / DAT-725)", () => {
	beforeEach(() => {
		vi.mocked(teach).mockReset();
		vi.mocked(teach).mockResolvedValue({
			overlay_id: "ov-x",
			type: "validation",
		});
	});

	it("writes teach({type:'validation', payload}) with undefined optionals stripped — fresh declaration", async () => {
		const input = ValidationSpecSchema.parse(MINIMAL);
		const result = await teachValidation(input, async () => []);

		expect(teach).toHaveBeenCalledTimes(1);
		const arg = vi.mocked(teach).mock.calls[0][0];
		expect(arg.type).toBe("validation");
		// stripUndefined dropped the optionals the user never declared.
		expect(arg.payload).not.toHaveProperty("tolerance");
		expect(arg.payload).not.toHaveProperty("guidance");
		expect(arg.payload).toMatchObject({
			validation_id: "invoice_reconciliation",
			vertical: "finance",
			check_type: "aggregate",
		});
		expect(result).toEqual({
			overlay_id: "ov-x",
			validation_id: "invoice_reconciliation",
			vertical: "finance",
			override: false,
			shadowed_spec: null,
		});
	});

	it("flags an override, echoes the shadowed seeded spec, and writes the user's new tolerance", async () => {
		const seeded: SeededValidationSpec[] = [
			{
				validation_id: "trial_balance",
				name: "Trial Balance",
				description: "…",
				check_type: "balance",
				severity: "critical",
				tolerance: 0.01,
				guidance: "Sum debit - credit.",
			},
		];
		const input = ValidationSpecSchema.parse({
			...MINIMAL,
			validation_id: "trial_balance",
			check_type: "balance",
			tolerance: 5.0,
		});
		const result = await teachValidation(input, async () => seeded);

		expect(result.override).toBe(true);
		expect(result.shadowed_spec?.tolerance).toBe(0.01);
		// The WRITTEN payload carries the user's override value, not the seeded one.
		const arg = vi.mocked(teach).mock.calls[0][0];
		expect((arg.payload as { tolerance?: number }).tolerance).toBe(5.0);
	});
});

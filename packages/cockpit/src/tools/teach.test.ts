// Per-type validation tests for the teach tool (DAT-343).
//
// Mirror of the engine's per-type appliers — a teach that passes here must
// produce the payload shape the applier in dataraum/core/overlay.py expects.
// The DB-bound `teach(...)` write path is covered by the P8 integration smoke.

import { describe, expect, it } from "vitest";

import {
	AGENT_AUTOAPPLY_TEACH_TYPES,
	AGENT_TEACH_TYPES,
	CONNECT_TEACH_TYPES,
	TEACH_TYPES,
	TeachValidationError,
	validateTeach,
} from "./teach.validation";

describe("validateTeach", () => {
	describe("type_pattern", () => {
		it("accepts name + pattern (minimal)", () => {
			const out = validateTeach({
				type: "type_pattern",
				payload: { name: "iso_date", pattern: "^\\d{4}-\\d{2}-\\d{2}$" },
			});
			expect(out).toMatchObject({
				name: "iso_date",
				pattern: "^\\d{4}-\\d{2}-\\d{2}$",
			});
		});

		it("passes through optional applier fields", () => {
			const out = validateTeach({
				type: "type_pattern",
				payload: {
					name: "us_date",
					pattern: "^\\d{1,2}/\\d{1,2}/\\d{4}$",
					inferred_type: "DATE",
					standardization_expr: "STRPTIME(...)",
					case_sensitive: false,
				},
			});
			expect(out.inferred_type).toBe("DATE");
			expect(out.case_sensitive).toBe(false);
			expect(out.standardization_expr).toBe("STRPTIME(...)");
		});

		it("rejects missing name", () => {
			expect(() =>
				validateTeach({ type: "type_pattern", payload: { pattern: "^x$" } }),
			).toThrow(TeachValidationError);
		});

		it("rejects missing pattern", () => {
			expect(() =>
				validateTeach({ type: "type_pattern", payload: { name: "x" } }),
			).toThrow(TeachValidationError);
		});

		it("rejects empty name (string of length 0)", () => {
			expect(() =>
				validateTeach({
					type: "type_pattern",
					payload: { name: "", pattern: "^x$" },
				}),
			).toThrow(TeachValidationError);
		});
	});

	describe("null_value", () => {
		it("accepts a standard_nulls entry", () => {
			const out = validateTeach({
				type: "null_value",
				payload: {
					category: "standard_nulls",
					value: "TBD",
					description: "to be determined",
				},
			});
			expect(out.category).toBe("standard_nulls");
			expect(out.value).toBe("TBD");
		});

		it.each([
			"standard_nulls",
			"spreadsheet_nulls",
			"placeholder_nulls",
			"missing_indicators",
		])("accepts category '%s'", (category) => {
			const out = validateTeach({
				type: "null_value",
				payload: { category, value: "x" },
			});
			expect(out.category).toBe(category);
		});

		it("rejects an unknown category", () => {
			expect(() =>
				validateTeach({
					type: "null_value",
					payload: { category: "made_up_nulls", value: "x" },
				}),
			).toThrow(TeachValidationError);
		});

		it("rejects missing value", () => {
			expect(() =>
				validateTeach({
					type: "null_value",
					payload: { category: "standard_nulls" },
				}),
			).toThrow(TeachValidationError);
		});
	});

	describe("unit", () => {
		it("accepts {table, column, unit} (DAT-428 column-scoped unit teach)", () => {
			// Engine consumer: _apply_unit keys EXACTLY on payload.{table, column} →
			// overrides.units."<table>.<column>". Identify by NAME, not a column id.
			const out = validateTeach({
				type: "unit",
				payload: { table: "trades", column: "amount", unit: "EUR" },
			});
			expect(out).toEqual({ table: "trades", column: "amount", unit: "EUR" });
		});

		it.each(["table", "column", "unit"])("rejects missing %s", (field) => {
			const payload: Record<string, string> = {
				table: "trades",
				column: "amount",
				unit: "EUR",
			};
			delete payload[field];
			expect(() => validateTeach({ type: "unit", payload })).toThrow(
				TeachValidationError,
			);
		});

		it("rejects an empty column (string of length 0)", () => {
			expect(() =>
				validateTeach({
					type: "unit",
					payload: { table: "trades", column: "", unit: "EUR" },
				}),
			).toThrow(TeachValidationError);
		});
	});

	describe("relationship", () => {
		it("accepts {action, from_column_id, to_column_id} (DAT-409 directional pair)", () => {
			// Engine consumer: load_suppressed_relationship_pairs +
			// materialize-from-overlay (relationships) key on EXACTLY
			// payload.{action, from_column_id, to_column_id}.
			const out = validateTeach({
				type: "relationship",
				payload: {
					action: "reject",
					from_column_id: "col-from",
					to_column_id: "col-to",
				},
			});
			expect(out.action).toBe("reject");
			expect(out.from_column_id).toBe("col-from");
			expect(out.to_column_id).toBe("col-to");
		});

		it.each([
			"confirm",
			"reject",
			"add",
		] as const)("accepts the %s action", (action) => {
			const out = validateTeach({
				type: "relationship",
				payload: { action, from_column_id: "a", to_column_id: "b" },
			});
			expect(out.action).toBe(action);
		});

		it("rejects an unknown action (keep is engine-internal, not a user teach)", () => {
			expect(() =>
				validateTeach({
					type: "relationship",
					payload: { action: "keep", from_column_id: "a", to_column_id: "b" },
				}),
			).toThrow(TeachValidationError);
		});

		it("rejects a missing column id", () => {
			expect(() =>
				validateTeach({
					type: "relationship",
					payload: { action: "reject", from_column_id: "a" },
				}),
			).toThrow(TeachValidationError);
		});
	});

	describe("hierarchy", () => {
		it.each([
			"add",
			"reject",
			"alias",
		] as const)("accepts {action, table_id, members} for the %s action (DAT-537)", (action) => {
			const out = validateTeach({
				type: "hierarchy",
				payload: {
					action,
					table_id: "tbl-1",
					members: ["zip", "city", "state"],
				},
			});
			expect(out.action).toBe(action);
			expect(out.table_id).toBe("tbl-1");
			expect(out.members).toEqual(["zip", "city", "state"]);
		});

		it("rejects an unknown action (confirm is relationship-only)", () => {
			expect(() =>
				validateTeach({
					type: "hierarchy",
					payload: { action: "confirm", table_id: "t", members: ["a", "b"] },
				}),
			).toThrow(TeachValidationError);
		});

		it("rejects an empty members list", () => {
			expect(() =>
				validateTeach({
					type: "hierarchy",
					payload: { action: "add", table_id: "t", members: [] },
				}),
			).toThrow(TeachValidationError);
		});
	});

	// validation/cycle/metric are internal-only dispatch targets (the typed
	// teach_validation/teach_cycle/teach_metric tools write through them with an
	// already-validated payload), so the primitive still passthrough-accepts them.
	describe("internal delegated types (generic passthrough)", () => {
		it.each([
			"validation",
			"cycle",
			"metric",
		] as const)("%s accepts an arbitrary object", (type) => {
			const out = validateTeach({
				type,
				payload: { anything: "goes", here: { nested: true } },
			});
			expect(out).toEqual({ anything: "goes", here: { nested: true } });
		});
	});

	describe("dispatch", () => {
		it("TEACH_TYPES exposes every wired type (incl. internal-only delegated types)", () => {
			// The concept family (concept/concept_property/rebind) was retired in
			// DAT-728 — the concept vocabulary is a typed table the frame stage writes.
			expect(new Set(TEACH_TYPES)).toEqual(
				new Set([
					"type_pattern",
					"null_value",
					"unit",
					"relationship",
					"hierarchy",
					"validation",
					"cycle",
					"metric",
				]),
			);
		});

		it("AGENT_TEACH_TYPES advertises only STAGE's catalogue-grain corrections", () => {
			// STAGE (begin_session) teaches TOPOLOGY (relationship/hierarchy). Column
			// MEANING moved off teach entirely — the concept vocabulary is declared in
			// the frame stage's typed write (DAT-728), rebuilt conversationally by
			// DAT-738. The mechanical typing-grain teaches (type_pattern/null_value/unit)
			// are CONNECT's (add_source replay realizes them, DAT-647); the
			// operating-model declarations (validation/cycle/metric) are owned by the
			// typed teach_* tools. One way to teach each thing; one surface per grain.
			expect(new Set(AGENT_TEACH_TYPES)).toEqual(
				new Set(["relationship", "hierarchy"]),
			);
			// Mechanical typing teaches are CONNECT's, not STAGE's (DAT-647).
			for (const t of ["type_pattern", "null_value", "unit"]) {
				expect(AGENT_TEACH_TYPES as readonly string[]).not.toContain(t);
			}
			for (const t of ["validation", "cycle", "metric", "explanation"]) {
				expect(AGENT_TEACH_TYPES as readonly string[]).not.toContain(t);
			}
			// The retired concept family is no longer a teach type at all.
			for (const t of ["concept", "concept_property", "rebind"]) {
				expect(AGENT_TEACH_TYPES as readonly string[]).not.toContain(t);
			}
		});

		it("AGENT_AUTOAPPLY_TEACH_TYPES is the mechanical grounding subset only (DAT-551)", () => {
			// The agent may AUTO-APPLY only mechanical grounding teaches it can
			// self-verify by re-measuring. Judgement-family types (concept/
			// relationship/hierarchy) are entropy-measurable but stay human-surfaced.
			expect(new Set(AGENT_AUTOAPPLY_TEACH_TYPES)).toEqual(
				new Set(["type_pattern", "null_value", "unit"]),
			);
			// The auto-apply loop is the add_source grounding loop, so its mechanical
			// set is exactly what the CONNECT chat teaches (DAT-647) — not STAGE's
			// catalogue-grain surface.
			for (const t of AGENT_AUTOAPPLY_TEACH_TYPES) {
				expect(CONNECT_TEACH_TYPES as readonly string[]).toContain(t);
			}
			// Judgement-family types are NEVER auto-appliable.
			for (const t of [
				"concept",
				"concept_property",
				"rebind",
				"relationship",
				"hierarchy",
			]) {
				expect(AGENT_AUTOAPPLY_TEACH_TYPES as readonly string[]).not.toContain(
					t,
				);
			}
		});

		it("rejects an unknown type", () => {
			expect(() =>
				validateTeach({
					// biome-ignore lint/suspicious/noExplicitAny: deliberate type smuggling
					type: "made_up_type" as any,
					payload: {},
				}),
			).toThrow(/Unknown teach type/);
		});
	});
});

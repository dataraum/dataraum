// Per-type validation tests for the teach tool (DAT-343).
//
// Mirror of the engine's per-type appliers — a teach that passes here must
// produce the payload shape the applier in dataraum/core/overlay.py expects.
// The DB-bound `teach(...)` write path is covered by the P8 integration smoke.

import { describe, expect, it } from "vitest";

import {
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

	describe("concept", () => {
		it("accepts vertical+name (minimal)", () => {
			const out = validateTeach({
				type: "concept",
				payload: { vertical: "_adhoc", name: "revenue" },
			});
			expect(out.vertical).toBe("_adhoc");
			expect(out.name).toBe("revenue");
		});

		it("accepts the full OntologyConcept shape", () => {
			const out = validateTeach({
				type: "concept",
				payload: {
					vertical: "_adhoc",
					name: "revenue",
					description: "Total income",
					indicators: ["revenue", "sales", "income"],
					exclude_patterns: ["cost", "expense"],
					temporal_behavior: "additive",
					typical_role: "measure",
					typical_values: ["10000", "20000"],
					unit_from_concept: "currency",
					is_unit_dimension: false,
				},
			});
			expect(out.indicators).toEqual(["revenue", "sales", "income"]);
			expect(out.typical_role).toBe("measure");
			expect(out.is_unit_dimension).toBe(false);
		});

		it("rejects missing vertical", () => {
			expect(() =>
				validateTeach({
					type: "concept",
					payload: { name: "revenue" },
				}),
			).toThrow(TeachValidationError);
		});

		it("rejects missing name", () => {
			expect(() =>
				validateTeach({
					type: "concept",
					payload: { vertical: "_adhoc" },
				}),
			).toThrow(TeachValidationError);
		});

		it("rejects empty vertical", () => {
			expect(() =>
				validateTeach({
					type: "concept",
					payload: { vertical: "", name: "revenue" },
				}),
			).toThrow(TeachValidationError);
		});

		it("rejects a non-object payload (must be a dict)", () => {
			expect(() =>
				validateTeach({ type: "concept", payload: "just a string" }),
			).toThrow(TeachValidationError);
		});

		it("passes through extra optional fields", () => {
			// Mirrors passthrough() — the applier can accept new fields without
			// a code change in the cockpit.
			const out = validateTeach({
				type: "concept",
				payload: {
					vertical: "_adhoc",
					name: "revenue",
					some_future_field: "ok",
				},
			});
			expect(out.some_future_field).toBe("ok");
		});
	});

	describe("concept_property", () => {
		it("accepts vertical+concept+property+value", () => {
			const out = validateTeach({
				type: "concept_property",
				payload: {
					vertical: "finance",
					concept: "revenue",
					property: "typical_role",
					value: "measure",
				},
			});
			expect(out.vertical).toBe("finance");
			expect(out.property).toBe("typical_role");
		});

		it("accepts a non-string value (the property is JSON)", () => {
			const out = validateTeach({
				type: "concept_property",
				payload: {
					vertical: "finance",
					concept: "revenue",
					property: "indicators",
					value: ["rev", "revenue", "total_revenue"],
				},
			});
			expect(out.value).toEqual(["rev", "revenue", "total_revenue"]);
		});

		it("rejects missing vertical", () => {
			expect(() =>
				validateTeach({
					type: "concept_property",
					payload: {
						concept: "revenue",
						property: "typical_role",
						value: "measure",
					},
				}),
			).toThrow(TeachValidationError);
		});
	});

	describe("relationship", () => {
		it("accepts source_id+table+target_table (flat shape, not nested under 'parameters')", () => {
			// Engine consumer: entropy/detectors/structural/relations.py reads
			// payload.{source_id, table, target_table}. Verifies we ship the
			// flat shape it expects (legacy 'parameters' nesting is gone).
			const out = validateTeach({
				type: "relationship",
				payload: {
					source_id: "src-1",
					table: "orders",
					target_table: "customers",
					from_column: "customer_id",
				},
			});
			expect(out.source_id).toBe("src-1");
			expect(out.from_column).toBe("customer_id");
		});

		it("rejects missing source_id", () => {
			expect(() =>
				validateTeach({
					type: "relationship",
					payload: { table: "orders", target_table: "customers" },
				}),
			).toThrow(TeachValidationError);
		});
	});

	describe("deferred types (generic passthrough)", () => {
		it.each([
			"validation",
			"cycle",
			"metric",
			"explanation",
		] as const)("%s accepts an arbitrary object", (type) => {
			const out = validateTeach({
				type,
				payload: { anything: "goes", here: { nested: true } },
			});
			expect(out).toEqual({ anything: "goes", here: { nested: true } });
		});
	});

	describe("dispatch", () => {
		it("TEACH_TYPES exposes every wired type", () => {
			expect(new Set(TEACH_TYPES)).toEqual(
				new Set([
					"type_pattern",
					"null_value",
					"concept_property",
					"relationship",
					"concept",
					"validation",
					"cycle",
					"metric",
					"explanation",
				]),
			);
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

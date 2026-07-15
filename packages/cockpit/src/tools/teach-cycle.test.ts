// Unit tests for teach_cycle (DAT-465). Pure — the schema + the shadow detection
// run with no DB and no config tree. The DB-bound write path reuses `teach()`
// (covered by the teach integration smoke); the live config-tree read is
// browser/integration-smoke territory. What this guards:
//   - the spec input is a top-level object whose `name` is FREE-FORM (no closed
//     vocabulary — the cycle counterpart to validation's closed check_type) but
//     whose `business_value` IS a closed enum;
//   - the shadow narrowing turns a shipped cycle_types entry into the summary
//     shape, tolerating a non-object def;
//   - findShadowedCycle is an exact name match → the override flag is honest.

import { beforeEach, describe, expect, it, vi } from "vitest";

import {
	BUSINESS_VALUES,
	CycleSpecSchema,
	findShadowedCycle,
	narrowShippedCycle,
	type ShippedCycleSpec,
} from "./cycle-spec";
import { teach } from "./teach";
import { teachCycle } from "./teach-cycle";

// Mock the shared overlay-write path and the env config so importing the tool
// (which evals `../config` + `./teach` at load) doesn't pull the DB/boot. vitest
// hoists these above the imports above. The shipped-cycle reader is injected per
// call (no fs/bun mock needed).
vi.mock("#/config", () => ({ config: { dataraumConfigPath: "/unused" } }));
vi.mock("#/tools/teach", () => ({ teach: vi.fn() }));

const MINIMAL = {
	vertical: "finance",
	name: "subscription_renewal",
};

describe("CycleSpecSchema (DAT-465)", () => {
	it("accepts a minimal spec (vertical + name only)", () => {
		const parsed = CycleSpecSchema.parse(MINIMAL);
		expect(parsed.name).toBe("subscription_renewal");
		// Optionals stay undefined — the write path strips them.
		expect(parsed.description).toBeUndefined();
		expect(parsed.completion_indicators).toBeUndefined();
		expect(parsed.typical_stages).toBeUndefined();
	});

	it("accepts a full spec mirroring the cycles.yaml entry shape", () => {
		const parsed = CycleSpecSchema.parse({
			...MINIMAL,
			name: "order_to_cash",
			description: "Revenue cycle from order through payment collection.",
			business_value: "high",
			aliases: ["o2c", "revenue_cycle"],
			typical_stages: [
				{ name: "Order Placed", order: 1, indicators: ["ordered", "new"] },
				{ name: "Payment Received", order: 5, indicators: ["paid"] },
			],
			completion_indicators: ["paid", "settled"],
			feeds_into: ["accounts_receivable"],
		});
		expect(parsed.business_value).toBe("high");
		expect(parsed.typical_stages).toHaveLength(2);
		expect(parsed.typical_stages?.[0].indicators).toEqual(["ordered", "new"]);
		expect(parsed.feeds_into).toEqual(["accounts_receivable"]);
	});

	it("accepts a FREE-FORM name (no closed vocabulary — the cycle requirement)", () => {
		// Validation rejects an unknown check_type; a cycle name has no enum, so a
		// novel cycle the user invents must parse.
		expect(
			CycleSpecSchema.parse({ ...MINIMAL, name: "widget_refurbishment_loop" })
				.name,
		).toBe("widget_refurbishment_loop");
	});

	it.each(
		BUSINESS_VALUES,
	)("accepts the closed business_value '%s'", (business_value) => {
		expect(
			CycleSpecSchema.parse({ ...MINIMAL, business_value }).business_value,
		).toBe(business_value);
	});

	it("REJECTS a free-text business_value (the closed-enum line)", () => {
		expect(
			CycleSpecSchema.safeParse({ ...MINIMAL, business_value: "critical" })
				.success,
		).toBe(false);
	});

	it.each([
		"vertical",
		"name",
	])("rejects a spec missing required field '%s'", (field) => {
		const incomplete: Record<string, unknown> = { ...MINIMAL };
		delete incomplete[field];
		expect(CycleSpecSchema.safeParse(incomplete).success).toBe(false);
	});

	it("rejects an empty name / vertical (min length)", () => {
		expect(CycleSpecSchema.safeParse({ ...MINIMAL, name: "" }).success).toBe(
			false,
		);
		expect(
			CycleSpecSchema.safeParse({ ...MINIMAL, vertical: "" }).success,
		).toBe(false);
	});
});

describe("narrowShippedCycle (DAT-465)", () => {
	it("narrows a parsed cycle_types entry to the summary fields", () => {
		const spec = narrowShippedCycle("order_to_cash", {
			description: "Complete revenue cycle from order through collection.",
			business_value: "high",
			completion_indicators: ["paid", "collected", "closed"],
			// extra YAML fields are ignored by the narrowing
			typical_stages: [{ name: "Order Placed", order: 1 }],
		});
		expect(spec).toEqual({
			name: "order_to_cash",
			description: "Complete revenue cycle from order through collection.",
			business_value: "high",
			completion_indicators: ["paid", "collected", "closed"],
		});
	});

	it("returns null for an empty name (not a real cycle_types key)", () => {
		expect(narrowShippedCycle("", {})).toBeNull();
	});

	it("tolerates a non-object def — name-only summary, no throw", () => {
		expect(narrowShippedCycle("weird", "not an object")).toEqual({
			name: "weird",
			description: null,
			business_value: null,
			completion_indicators: null,
		});
	});

	it("coalesces non-string completion_indicators entries away", () => {
		const spec = narrowShippedCycle("x", {
			completion_indicators: ["paid", 42, null, "closed"],
		});
		expect(spec?.completion_indicators).toEqual(["paid", "closed"]);
	});
});

describe("findShadowedCycle (DAT-465)", () => {
	const shipped: ShippedCycleSpec[] = [
		{
			name: "order_to_cash",
			description: "…",
			business_value: "high",
			completion_indicators: ["paid"],
		},
		{
			name: "procure_to_pay",
			description: "…",
			business_value: "high",
			completion_indicators: ["disbursed"],
		},
	];

	it("returns the shipped cycle when the name matches (an override)", () => {
		const shadowed = findShadowedCycle(shipped, "order_to_cash");
		expect(shadowed?.name).toBe("order_to_cash");
		expect(shadowed?.completion_indicators).toEqual(["paid"]);
	});

	it("returns null when the name is new (a fresh declaration)", () => {
		expect(findShadowedCycle(shipped, "subscription_renewal")).toBeNull();
	});

	it("returns null against an empty shipped set", () => {
		expect(findShadowedCycle([], "order_to_cash")).toBeNull();
	});
});

// The load-bearing composition: read shipped → detect shadow → funnel the
// stripped spec through the shared teach() overlay-write path. teach() is mocked;
// the shipped-cycle reader is injected so no config tree / fs is touched.
describe("teachCycle wiring (DAT-465)", () => {
	beforeEach(() => {
		vi.mocked(teach).mockReset();
		vi.mocked(teach).mockResolvedValue({
			overlay_id: "ov-c",
			type: "cycle",
		});
	});

	it("writes teach({type:'cycle', payload}) with undefined optionals stripped — fresh declaration", async () => {
		const input = CycleSpecSchema.parse({
			...MINIMAL,
			description: "Renewal cycle.",
			business_value: "medium" as const,
		});
		const result = await teachCycle(input, async () => []);

		expect(teach).toHaveBeenCalledTimes(1);
		const arg = vi.mocked(teach).mock.calls[0][0];
		expect(arg.type).toBe("cycle");
		// stripUndefined dropped the optionals the user never declared.
		expect(arg.payload).not.toHaveProperty("aliases");
		expect(arg.payload).not.toHaveProperty("typical_stages");
		expect(arg.payload).toMatchObject({
			name: "subscription_renewal",
			vertical: "finance",
			business_value: "medium",
		});
		expect(result).toEqual({
			overlay_id: "ov-c",
			name: "subscription_renewal",
			vertical: "finance",
			override: false,
			shadowed_spec: null,
		});
	});

	it("flags an override, echoes the shadowed shipped cycle, and writes the user's new indicators", async () => {
		const shipped: ShippedCycleSpec[] = [
			{
				name: "order_to_cash",
				description: "Ships with default completion.",
				business_value: "high",
				completion_indicators: ["paid", "closed"],
			},
		];
		const input = CycleSpecSchema.parse({
			...MINIMAL,
			name: "order_to_cash",
			completion_indicators: ["settled"],
		});
		const result = await teachCycle(input, async () => shipped);

		expect(result.override).toBe(true);
		expect(result.shadowed_spec?.completion_indicators).toEqual([
			"paid",
			"closed",
		]);
		// The WRITTEN payload carries the user's override value, not the shipped one.
		const arg = vi.mocked(teach).mock.calls[0][0];
		expect(
			(arg.payload as { completion_indicators?: string[] })
				.completion_indicators,
		).toEqual(["settled"]);
	});
});

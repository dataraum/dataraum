// Constrained-decoding BUDGET contract for the two reshaped induction schemas
// (DAT-807).
//
// `InducedMetrics` and `InducedValidations` exist only to be compiled into a
// decoding grammar by Anthropic. That compiler enforces hard limits the type
// system knows nothing about, and every one of them fails at REQUEST time on a
// live call — the one place this repo never exercises (the LLM is stubbed in
// every test). So the limits are asserted statically here instead, against the
// JSON Schema the adapter actually sends.
//
// Caps and keyword support are from Anthropic's structured-outputs reference:
//   - 24 optional properties, 16 union-typed properties, per request
//   - no recursive schemas
//   - `additionalProperties: false` REQUIRED on every object
//   - supported: anyOf / allOf / enum / const / $ref. NOT oneOf.
//   - array `minItems` is the ONE numeric constraint, and only with value 0 or 1
//
// Three of these were live defects caught by this audit before any call was
// made, which is why it is a permanent test and not a one-off script:
//   - `z.discriminatedUnion` renders `oneOf`, which is not accepted -> `z.union`
//   - `z.int()` renders safe-integer `minimum`/`maximum` -> `z.number()`
//   - the adapter's normalizer does NOT descend into union branches, so objects
//     nested in one never got `additionalProperties: false` -> `z.strictObject`

import { describe, expect, it } from "vitest";

import { InducedMetrics } from "./metric-induction";
import { InducedValidations } from "./validation-induction";

// The adapter-internal converter that builds `output_config.format.schema`.
// `@tanstack/ai`'s export map does not expose it, so it is resolved off the
// package entry point at runtime rather than re-implemented — the audit must
// run against the REAL conversion or it proves nothing. A dep bump that moves
// this module fails the suite loudly, which is the correct outcome.
// `import.meta.resolve` honours the package's "import" condition (the only one
// @tanstack/ai declares) and returns the entry's file URL; the converter sits
// alongside it in the same dist tree.
const converterUrl = new URL(
	"activities/chat/tools/schema-converter.js",
	import.meta.resolve("@tanstack/ai"),
).href;
const { convertSchemaForStructuredOutput } = (await import(
	/* @vite-ignore */ converterUrl
)) as {
	convertSchemaForStructuredOutput: (s: unknown) => { jsonSchema: unknown };
};

const OPTIONAL_CAP = 24;
const UNION_CAP = 16;

/** Keywords the grammar compiler rejects outright. */
const BANNED = [
	"maxItems",
	"minLength",
	"maxLength",
	"minimum",
	"maximum",
	"exclusiveMinimum",
	"exclusiveMaximum",
	"multipleOf",
	"pattern",
	"uniqueItems",
] as const;

interface Audit {
	optional: string[];
	union: string[];
	banned: string[];
	looseObjects: string[];
	oneOf: string[];
	cycle: boolean;
}

// biome-ignore lint/suspicious/noExplicitAny: walks raw JSON Schema
type Node = any;

function walk(node: Node, path: string, a: Audit, seen: string[]): void {
	if (!node || typeof node !== "object") return;

	for (const k of BANNED) {
		if (k in node) a.banned.push(`${path}.${k}`);
	}
	if ("minItems" in node && node.minItems !== 0 && node.minItems !== 1) {
		a.banned.push(`${path}.minItems=${node.minItems}`);
	}
	if (Array.isArray(node.oneOf)) a.oneOf.push(path);

	if (node.$ref) {
		const ref = String(node.$ref);
		if (seen.includes(ref)) {
			a.cycle = true;
			return;
		}
		seen = [...seen, ref];
	}

	const isObject =
		node.type === "object" ||
		(Array.isArray(node.type) && node.type.includes("object")) ||
		("properties" in node && !node.type);

	if (isObject && node.properties) {
		if (node.additionalProperties !== false) a.looseObjects.push(path);
		const required: string[] = node.required ?? [];
		for (const [name, prop] of Object.entries<Node>(node.properties)) {
			const p = `${path}.${name}`;
			// An optional survives conversion as a REQUIRED NULLABLE
			// (`type: [T, "null"]`), so it spends from both budgets — count it as
			// optional either way.
			const nullable = Array.isArray(prop?.type) && prop.type.includes("null");
			if (!required.includes(name) || nullable) a.optional.push(p);
			const items = prop?.items;
			// Conservative reading of "parameters that use anyOf or type arrays":
			// an array whose ITEMS are a union counts too.
			if (
				prop?.anyOf ||
				prop?.oneOf ||
				Array.isArray(prop?.type) ||
				items?.anyOf ||
				items?.oneOf ||
				Array.isArray(items?.type)
			) {
				a.union.push(p);
			}
			walk(prop, p, a, seen);
		}
	}

	if (node.items) walk(node.items, `${path}[]`, a, seen);
	for (const key of ["anyOf", "oneOf", "allOf"]) {
		if (Array.isArray(node[key])) {
			node[key].forEach((b: Node, i: number) => {
				walk(b, `${path}|${key}${i}`, a, seen);
			});
		}
	}
	for (const bag of ["$defs", "definitions"]) {
		if (node[bag]) {
			for (const [n, d] of Object.entries<Node>(node[bag])) {
				walk(d, `#/${bag}/${n}`, a, seen);
			}
		}
	}
}

function audit(schema: unknown): Audit {
	// biome-ignore lint/suspicious/noExplicitAny: converter takes a SchemaInput
	const { jsonSchema } = convertSchemaForStructuredOutput(schema as any);
	const a: Audit = {
		optional: [],
		union: [],
		banned: [],
		looseObjects: [],
		oneOf: [],
		cycle: false,
	};
	walk(jsonSchema, "$", a, []);
	return a;
}

describe.each([
	["InducedMetrics", InducedMetrics, 2],
	["InducedValidations", InducedValidations, 1],
] as const)("%s — constrained-decoding budget", (_label, schema, unions) => {
	const a = audit(schema);

	it("has ZERO optional properties", () => {
		// The lead's DAT-807 ruling: an optional field is usually a modelling
		// mistake. Every field here is required with a documented "" / [] sentinel,
		// so the 24-cap is not merely met — it is unspent.
		expect(a.optional).toEqual([]);
	});

	it("stays under the union cap, at the expected count", () => {
		// Pinned exactly, not just `<= cap`: a new union appearing silently is the
		// drift this test exists to catch.
		expect(a.union).toHaveLength(unions);
		expect(a.union.length).toBeLessThanOrEqual(UNION_CAP);
		expect(a.optional.length).toBeLessThanOrEqual(OPTIONAL_CAP);
	});

	it("is not recursive", () => {
		expect(a.cycle).toBe(false);
	});

	it("uses anyOf, never oneOf", () => {
		// `z.discriminatedUnion` renders `oneOf`, which is NOT in the accepted
		// keyword set — `z.union` renders `anyOf` and keeps the `const`
		// discriminator. This is the assertion that catches a revert.
		expect(a.oneOf).toEqual([]);
	});

	it("sets additionalProperties:false on EVERY object, including inside unions", () => {
		// The adapter's normalizer only descends into object/array nodes, never
		// into union branches — so objects nested in a union depend on Zod emitting
		// this directly (`z.strictObject`).
		expect(a.looseObjects).toEqual([]);
	});

	it("carries no rejected JSON Schema keywords", () => {
		// `minItems: 1` on the output step's checks is deliberately NOT banned —
		// it is the one array constraint the API documents as supported.
		expect(a.banned).toEqual([]);
	});
});

describe("the output step's mandatory check", () => {
	it("renders as minItems:1 on every output-step variant", () => {
		const { jsonSchema } = convertSchemaForStructuredOutput(
			// biome-ignore lint/suspicious/noExplicitAny: converter takes a SchemaInput
			InducedMetrics as any,
		);
		const variants = (jsonSchema as Node).properties.metrics.items.properties
			.output_step.anyOf as Node[];

		expect(variants.length).toBeGreaterThan(0);
		for (const v of variants) {
			expect(v.properties.checks.minItems).toBe(1);
		}

		// ...and NOT on the dependency steps, which may legitimately carry none.
		const steps = (jsonSchema as Node).properties.metrics.items.properties.steps
			.items.anyOf as Node[];
		for (const v of steps) {
			expect(v.properties.checks.minItems).toBeUndefined();
		}
	});
});

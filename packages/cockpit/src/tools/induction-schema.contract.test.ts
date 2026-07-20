// Constrained-decoding BUDGET contract for EVERY schema the cockpit sends as
// `output_config.format` (DAT-807).
//
// These schemas exist to be compiled into a decoding grammar by Anthropic. That
// compiler enforces hard limits the type system knows nothing about, and every
// one of them fails at REQUEST time on a live call — the one place this repo
// never exercises (the LLM is stubbed in every test). So the limits are asserted
// statically here instead, against the JSON Schema the adapter actually sends.
//
// The list below is the WHOLE set, deliberately: this guard first covered only
// the two reshaped induction schemas, and the four sites migrated before it
// existed went unaudited — which is exactly where the live 400s came from. The
// only `chat({ outputSchema })` calls not listed are the four that pass an inline
// single-field object (`{analysis}` in why_column / why_relationship / why_table,
// `{summary}` in the report-summary agent, `{kind}` in the nav agent): one
// required scalar each, no optionals, no unions, nothing to drift.
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

import { describe, expect, it, vi } from "vitest";
import { z } from "zod";

// `frame.ts` reaches config/db at module scope (it owns the overlay writes), so
// the same server-only stubs the sibling induce-native contract test uses are
// needed to import its SCHEMAS. The schemas themselves are pure Zod.
vi.mock("#/config", () => ({
	get config() {
		return { anthropicApiKey: "test-key", dataraumConfigPath: "/nonexistent" };
	},
}));
vi.mock("#/config.base", () => ({ baseConfig: {} }));
vi.mock("#/db/cockpit/registry", () => ({
	setActiveWorkspaceVertical: () => {},
}));
vi.mock("#/db/metadata/client", () => ({
	metadataDb: {},
	metadataWriteDb: {},
}));

import { AuthoredChartSchema } from "../charts/chart-config";
import { VerdictSchema } from "../worker/grounding-agent";
import { InducedCycles, InducedFrame } from "./frame";
import { InducedMetrics } from "./metric-induction";
import { QueryDraftSchema } from "./query";
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
	convertSchemaForStructuredOutput: (s: z.ZodType) => { jsonSchema: Node };
};

const OPTIONAL_CAP = 24;
const UNION_CAP = 16;

/** Keywords the grammar compiler rejects outright. `propertyNames` /
 * `patternProperties` are how Zod renders a `z.record` — the OPEN MAP this
 * whole design exists to avoid — so they are banned by name as well as caught
 * by the `additionalProperties` check below. */
// Only what is PROVEN rejected by the live API. The docs list several keywords
// as "not supported", but unsupported turns out to mean IGNORED for some of
// them: concept induction shipped `name.minLength` in its converted schema and
// the API accepted the request (DAT-807, live). Banning those was an inherited
// assumption, and a guard stricter than reality just costs us expressiveness
// for nothing.
//
// Proven rejected, each by an observed 400:
//   minimum/maximum on an integer — "For 'integer' type, properties maximum,
//     minimum are not supported" (z.int() renders safe-integer bounds)
//   oneOf — not in the accepted keyword set (z.discriminatedUnion renders it)
//   an open map — propertyNames/patternProperties are how Zod renders z.record,
//     which additionalProperties:false forbids by construction
const BANNED = [
	"minimum",
	"maximum",
	"exclusiveMinimum",
	"exclusiveMaximum",
	"multipleOf",
	"maxItems",
	"uniqueItems",
	"propertyNames",
	"patternProperties",
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

/** Follow a local `#/$defs/Name` (or `definitions`) pointer to its target. */
function resolveRef(root: Node, ref: string): Node | undefined {
	const m = /^#\/(\$defs|definitions)\/(.+)$/.exec(ref);
	return m ? root?.[m[1] as string]?.[m[2] as string] : undefined;
}

function walk(
	node: Node,
	path: string,
	a: Audit,
	seen: string[],
	root: Node,
): void {
	if (!node || typeof node !== "object") return;

	for (const k of BANNED) {
		if (k in node) a.banned.push(`${path}.${k}`);
	}
	if ("minItems" in node && node.minItems !== 0 && node.minItems !== 1) {
		a.banned.push(`${path}.minItems=${node.minItems}`);
	}
	if (Array.isArray(node.oneOf)) a.oneOf.push(path);

	// Resolve the ref against the root's $defs and keep walking — a $ref node
	// has no children of its own, so without this the cycle check could never
	// accumulate two entries on one path and would be vacuously true.
	if (node.$ref) {
		const ref = String(node.$ref);
		if (seen.includes(ref)) {
			a.cycle = true;
			return;
		}
		const target = resolveRef(root, ref);
		if (target) walk(target, path, a, [...seen, ref], root);
		return;
	}

	const isObject =
		node.type === "object" ||
		(Array.isArray(node.type) && node.type.includes("object")) ||
		("properties" in node && !node.type);

	// NOT gated on `node.properties`: a `z.record` renders as an object with
	// `additionalProperties: <schema>` and NO `properties` at all, so gating the
	// check on properties made the one shape this design exists to forbid
	// invisible to the audit. The API requires `additionalProperties: false` on
	// every object, full stop.
	if (isObject && node.additionalProperties !== false)
		a.looseObjects.push(path);

	if (isObject && node.properties) {
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
			walk(prop, p, a, seen, root);
		}
	}

	if (node.items) walk(node.items, `${path}[]`, a, seen, root);
	for (const key of ["anyOf", "oneOf", "allOf"]) {
		if (Array.isArray(node[key])) {
			node[key].forEach((b: Node, i: number) => {
				walk(b, `${path}|${key}${i}`, a, seen, root);
			});
		}
	}
	for (const bag of ["$defs", "definitions"]) {
		if (node[bag]) {
			for (const [n, d] of Object.entries<Node>(node[bag])) {
				walk(d, `#/${bag}/${n}`, a, seen, root);
			}
		}
	}
}

function blank(): Audit {
	return {
		optional: [],
		union: [],
		banned: [],
		looseObjects: [],
		oneOf: [],
		cycle: false,
	};
}

function audit(schema: z.ZodType): Audit {
	// Everything except optionality is read from the CONVERTED schema — that is
	// the literal bytes the adapter puts in `output_config.format.schema`.
	const { jsonSchema } = convertSchemaForStructuredOutput(schema);
	const converted = blank();
	walk(jsonSchema, "$", converted, [], jsonSchema);

	// Optionality must be read from the RAW schema instead. The adapter force-adds
	// EVERY property to `required`, and only null-widens the ones carrying a
	// scalar/object/array `type`; a property that is a bare `anyOf` (i.e. a union,
	// which is exactly what `steps` / `output_step` / `parameters` are) or a `$ref`
	// matches no branch, so an `.optional()` on one silently becomes required with
	// no `null` in sight — invisible in the converted output. Zod's own rendering
	// reports `required` honestly, so the "zero optionals" claim is made there.
	const raw = z.toJSONSchema(schema, { io: "input" });
	const rawAudit = blank();
	walk(raw, "$", rawAudit, [], raw);

	return { ...converted, optional: rawAudit.optional };
}

// EVERY schema sent as `output_config.format`, not just the two this file was
// written for. The four sites migrated first (concepts, cycles, chart author,
// query draft) were NOT audited, and three live 400s came out of that gap on the
// first real induction calls — an optional enum, an integer carrying
// minimum/maximum, then the compiled grammar being too large. Statically
// checkable, so checked statically (DAT-807).
//
// The union count is the expected number of union-typed properties. Every one of
// these is now ZERO: with no optionals left, the only unions that remain are the
// deliberate ones — the metric DAG's step variants and the validation parameter
// kinds, which are genuinely either/or and carry their own `const` discriminator.
describe.each([
	["InducedMetrics", InducedMetrics, 1],
	["InducedValidations", InducedValidations, 1],
	["InducedFrame", InducedFrame, 0],
	["InducedCycles", InducedCycles, 0],
	["AuthoredChartSchema", AuthoredChartSchema, 0],
	["QueryDraftSchema", QueryDraftSchema, 0],
	["VerdictSchema", VerdictSchema, 0],
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

// The UNDOCUMENTED compiled-grammar size limit, which only `InducedMetrics` is
// big enough to reach. Measured against the live compiler (DAT-807): five
// step-shaped union branches — three dependency-step variants plus a
// structurally separate two-variant `output_step` — was rejected with "The
// compiled grammar is too large"; removing either union made it fit. Stripping
// every description (11108 bytes -> 4091) did NOT, which is what identifies
// branches rather than bytes as the cost.
//
// So this is a budget, not a style rule, and it is the one limit with no error
// message until a real request: a fourth step type, or restoring the output-step
// union, breaks metric induction in production while every test stays green.
describe("the metric schema's union-branch budget", () => {
	it("keeps the step union to the three branches the grammar affords", () => {
		const { jsonSchema } = convertSchemaForStructuredOutput(
			// biome-ignore lint/suspicious/noExplicitAny: converter takes a SchemaInput
			InducedMetrics as any,
		);
		const steps = (jsonSchema as Node).properties.metrics.items.properties.steps
			.items.anyOf as Node[];

		expect(steps).toHaveLength(3);
		expect(steps.map((v) => v.properties.type.const).sort()).toEqual([
			"constant",
			"extract",
			"formula",
		]);
	});

	it("names the output step by id instead of restating its shape", () => {
		const { jsonSchema } = convertSchemaForStructuredOutput(
			// biome-ignore lint/suspicious/noExplicitAny: converter takes a SchemaInput
			InducedMetrics as any,
		);
		const props = (jsonSchema as Node).properties.metrics.items.properties;

		expect(props.output_step_id.type).toBe("string");
		expect(props.output_step).toBeUndefined();
	});
});

// A guard that has never been shown to fail on a bad input is not a guard. Each
// case below is a shape the API rejects; the audit must SEE it. The first two
// were live blind spots — the walker reported them clean.
describe("the audit itself catches the shapes it exists to forbid", () => {
	it("flags an open map (z.record) — the shape this design exists to avoid", () => {
		// Was invisible: the `additionalProperties` check used to be gated on
		// `node.properties`, and a record has none. It renders as
		// `{type:"object", propertyNames:{...}, additionalProperties:{...}}`.
		const a = audit(
			z.strictObject({
				deps: z.record(z.string(), z.strictObject({ x: z.string() })),
			}),
		);

		expect(a.looseObjects.length).toBeGreaterThan(0);
		expect(a.banned.some((b) => b.includes("propertyNames"))).toBe(true);
	});

	it("flags an OPTIONAL union property", () => {
		// Was invisible: the adapter force-adds every property to `required` and
		// only null-widens ones carrying a concrete `type`, so an optional union
		// silently became mandatory. Read from the raw schema, it shows up.
		const branch = z.union([
			z.strictObject({ k: z.literal("a"), x: z.string() }),
			z.strictObject({ k: z.literal("b"), y: z.string() }),
		]);
		const a = audit(
			z.strictObject({ maybe: branch.optional(), always: z.string() }),
		);

		expect(a.optional).toEqual(["$.maybe"]);
	});

	it("flags a plain optional scalar", () => {
		expect(
			audit(z.strictObject({ maybe: z.string().optional() })).optional,
		).toEqual(["$.maybe"]);
	});

	it("flags oneOf from a discriminated union", () => {
		const a = audit(
			z.strictObject({
				u: z.discriminatedUnion("k", [
					z.strictObject({ k: z.literal("a"), x: z.string() }),
					z.strictObject({ k: z.literal("b"), y: z.string() }),
				]),
			}),
		);

		expect(a.oneOf.length).toBeGreaterThan(0);
	});

	it("flags the numeric bounds z.int() emits", () => {
		const a = audit(z.strictObject({ n: z.int() }));

		expect(a.banned.some((b) => b.includes("minimum"))).toBe(true);
	});

	it("flags a NON-strict object nested inside a union branch", () => {
		// The adapter's normalizer does not descend into union branches, so this
		// object never receives `additionalProperties: false` from it.
		const a = audit(
			z.strictObject({
				u: z.union([
					z.strictObject({
						k: z.literal("a"),
						loose: z.object({ x: z.string() }),
					}),
					z.strictObject({ k: z.literal("b"), y: z.string() }),
				]),
			}),
		);

		expect(a.looseObjects.length).toBeGreaterThan(0);
	});

	it("passes a schema that is actually clean", () => {
		const a = audit(z.strictObject({ x: z.string(), xs: z.array(z.string()) }));

		expect(a).toMatchObject({
			optional: [],
			banned: [],
			looseObjects: [],
			oneOf: [],
			cycle: false,
		});
	});
});

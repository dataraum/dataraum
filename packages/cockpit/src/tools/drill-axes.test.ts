// The per-node axis resolver (DAT-672, re-cut DAT-703): the pure halves
// (dag→fields, slice-row→axis narrowing, substrate union, driver ordering)
// plus the full `resolveDrillAxes` orchestration through a mocked
// `#/db/metadata/client` — the join logic (extract parts → relation → fact
// table → curated ∪ substrate) is where a silent shape mismatch would produce
// zero axes, so it gets pinned with fake rows.

import { beforeEach, describe, expect, it, vi } from "vitest";

vi.mock("#/config", () => ({
	config: { dataraumWorkspaceId: "ws-test" },
}));

// A thenable fluent stub: every drizzle builder method returns the same
// object, and awaiting it yields the rows registered for the FROM table.
//
// A table's registered value may be a FUNCTION instead of an array, in which
// case it receives every string literal found in the query's WHERE arguments.
// That is what makes a TARGET-AWARE double possible: the verdict read is issued
// once per drill target (the metric, then each of its carrier measures), and a
// static row set would answer all of them identically — which is exactly the
// case the carrier gate exists to distinguish. Keyed on the target's own name,
// not on call ORDER, so the double does not silently encode the resolver's
// current call sequence.
// biome-ignore lint/suspicious/noExplicitAny: test double for the fluent builder
type RowsFor = any[] | ((whereLiterals: string[]) => any[]);
const rowsByTable = new Map<unknown, RowsFor>();

/** The BOUND VALUES of a where clause — drizzle nests its conditions as
 *  `queryChunks`, with each bound literal wrapped in a `Param` carrying `.value`.
 *  Reading those two shapes keeps this cheap and total; a blind walk of the
 *  object graph re-traverses drizzle's cyclic table/column back-references and
 *  costs seconds per query. */
function whereLiterals(node: unknown, out: string[]): string[] {
	if (Array.isArray(node)) {
		for (const child of node) whereLiterals(child, out);
		return out;
	}
	if (node === null || typeof node !== "object") return out;
	const obj = node as Record<string, unknown>;
	if (typeof obj.value === "string") out.push(obj.value);
	if (Array.isArray(obj.queryChunks)) whereLiterals(obj.queryChunks, out);
	return out;
}

function fluent(rowsFor: RowsFor) {
	const literals: string[] = [];
	// biome-ignore lint/suspicious/noExplicitAny: test double for the fluent builder
	const q: any = {
		where: (...args: unknown[]) => {
			whereLiterals(args, literals);
			return q;
		},
		orderBy: () => q,
		limit: () => q,
		leftJoin: () => q,
		// biome-ignore lint/suspicious/noThenProperty: drizzle query builders ARE thenables — the double must be awaitable mid-chain
		then: (
			resolve: (v: unknown[]) => unknown,
			reject?: (e: unknown) => unknown,
		) =>
			Promise.resolve(
				typeof rowsFor === "function" ? rowsFor(literals) : rowsFor,
			).then(resolve, reject),
	};
	return q;
}
vi.mock("#/db/metadata/client", () => ({
	metadataDb: {
		select: () => ({
			from: (table: unknown) => fluent(rowsByTable.get(table) ?? []),
		}),
	},
}));

// The IDENTITY read is a GRAPH_TABLE MATCH (integration-tested in
// concept-target.integration.test.ts); here it is a controllable double so the
// TARGET-SHAPE logic — measure vs composed, and what withholds — can be pinned
// as a unit. `sqlEquivalent` stays REAL (it parses through the shared in-memory
// DuckDB), because comparing the declared expression to the classified one IS
// the behaviour under test.
const groundedConcepts = new Map<
	string,
	{ concept: string; selectExpr: string }
>();
vi.mock("./concept-target", () => ({
	resolveGroundedConcepts: async (ids: readonly string[]) =>
		new Map(
			ids.flatMap((id) => {
				const hit = groundedConcepts.get(id);
				return hit ? [[id, hit] as const] : [];
			}),
		),
}));

// The AST read (real DuckDB) is integration-tested in sql-ast.integration.test;
// here a thin regex stub extracts the aggregated column so the GATE logic is
// tested in this pure-metadata unit.
vi.mock("#/duckdb/sql-ast", () => ({
	aggregatedColumns: async (expr: string) => {
		// Mirror the real fail-closed-on-WINDOW: the aggregate walk can't read a
		// WINDOW node, so a windowed aggregate yields NO columns (sql-ast.ts).
		if (/\bOVER\s*\(/i.test(expr)) return new Set<string>();
		const cols = new Set<string>();
		for (const m of expr.matchAll(
			/\b(?:SUM|COUNT|AVG|MIN|MAX)\s*\(\s*(\w+)/gi,
		)) {
			if (m[1]) cols.add(m[1]);
		}
		return cols;
	},
}));

import {
	columns,
	currentDimensionHierarchies,
	currentDriverRankings,
	currentEnrichedViews,
	currentLifecycleArtifacts,
	currentMetricAxisAdditivity,
	currentSliceDefinitions,
	sqlSnippets,
} from "#/db/metadata/schema";
import type { DrillAxis } from "#/duckdb/drill";
import { grainPresetsFrom } from "#/duckdb/grain";
import {
	ALREADY_AT_GRAIN_REASON,
	applyHierarchyDescent,
	applyTemporalKinds,
	axesFromSliceRows,
	buildTargetAdditivity,
	coarsestGrain,
	composedVerdict,
	decideTimeAxis,
	demoteWithheldDateAxes,
	describeTimeWithhold,
	describeUnitGate,
	driverGains,
	hierarchyDescentMap,
	markAlreadyInResult,
	measureFieldsFromDag,
	orderAxesByDrivers,
	resolveAnswerTarget,
	resolveAxisVerdict,
	resolveDrillAxes,
	temporalKindsFromColumns,
	unionSubstrateAxes,
	unitGate,
} from "./drill-axes";

describe("measureFieldsFromDag", () => {
	it("collects extract-step standard fields, deduped, ignoring formula/constant steps", () => {
		const dag = {
			dependencies: {
				rev: { type: "extract", source: { standard_field: "revenue" } },
				rev2: { source: { standard_field: "revenue" } }, // type defaults to extract
				cogs: { type: "extract", source: { standard_field: "cogs" } },
				margin: {
					type: "formula",
					expression: "rev - cogs",
					depends_on: ["rev", "cogs"],
				},
				days: { type: "constant", parameter: "period_days", value: 365 },
				broken: { type: "extract" }, // no source → no field
			},
			output: { unit: "currency" },
		};
		expect(measureFieldsFromDag(dag)).toEqual(["revenue", "cogs"]);
	});

	it("yields nothing for an unparseable dag", () => {
		expect(measureFieldsFromDag(null)).toEqual([]);
		expect(measureFieldsFromDag({ dependencies: {} })).toEqual([]);
		expect(measureFieldsFromDag("nope")).toEqual([]);
	});
});

describe("axesFromSliceRows", () => {
	it("narrows nullable view rows, dedupes by column, and ranks by curation order", () => {
		// DAT-879: order comes from (interest tier, measured relevance desc, name)
		// and ARRAY ORDER is the ranking — deliberately NOT the raw row order, so
		// a judged axis leads regardless of how the rows arrive.
		const axes = axesFromSliceRows([
			{
				tableId: "fact1",
				columnName: "booking_month",
				sliceRelevance: 0.99, // measures best, but was never judged
				sliceInterest: null,
				sliceType: null,
				distinctValues: "not-an-array",
				valueCount: 12,
				businessContext: null,
			},
			{
				tableId: "fact1",
				columnName: "customer__region",
				sliceRelevance: 0.4,
				sliceInterest: "primary",
				sliceType: "categorical",
				distinctValues: ["EU", "US", 7, null],
				valueCount: 2,
				businessContext: "sales region",
			},
			// Same dimension cataloged on a second fact — deduped away; the
			// better-ordered row wins, so the enrichment here never surfaces.
			{
				tableId: "fact2",
				columnName: "customer__region",
				sliceRelevance: 0.1,
				sliceInterest: "supporting",
				sliceType: "categorical",
				distinctValues: [],
				valueCount: null,
				businessContext: null,
			},
			{
				tableId: "fact1",
				columnName: null, // stale row without a name → dropped
				sliceRelevance: 0.5,
				sliceInterest: "primary",
				sliceType: null,
				distinctValues: null,
				valueCount: null,
				businessContext: null,
			},
		]);
		expect(axes).toEqual([
			{
				column: "customer__region",
				sliceType: "categorical",
				values: ["EU", "US"],
				valueCount: 2,
				businessContext: "sales region",
				temporal: null,
				// DAT-673: carried through from the winning (fact1, relevance 0.4,
				// primary) row — the deduped fact2 row's 0.1/supporting never surfaces.
				sliceRelevance: 0.4,
				sliceInterest: "primary",
				driverGain: null,
				hierarchyNext: null,
				disabledReason: null,
			},
			{
				column: "booking_month",
				sliceType: "categorical",
				values: [],
				valueCount: 12,
				businessContext: null,
				temporal: null,
				sliceRelevance: 0.99,
				sliceInterest: null,
				driverGain: null,
				hierarchyNext: null,
				disabledReason: null,
			},
		]);
	});

	it("keeps un-measured axes behind measured ones within a tier", () => {
		// A null relevance must not read as zero-and-therefore-best.
		const row = (columnName: string, sliceRelevance: number | null) => ({
			tableId: "fact1",
			columnName,
			sliceRelevance,
			sliceInterest: "primary",
			sliceType: "categorical",
			distinctValues: [],
			valueCount: null,
			businessContext: null,
		});
		const axes = axesFromSliceRows([
			row("unmeasured", null),
			row("measured", 0.1),
		]);
		expect(axes.map((a) => a.column)).toEqual(["measured", "unmeasured"]);
	});
});

/** A minimal curated axis for the pure-function tests. */
const axis = (column: string): DrillAxis => ({
	column,
	sliceType: "categorical",
	values: [],
	valueCount: null,
	businessContext: null,
	temporal: null,
	driverGain: null,
	sliceRelevance: null,
	sliceInterest: null,
	hierarchyNext: null,
	disabledReason: null,
});

describe("markAlreadyInResult (DAT-671 slice-menu curation)", () => {
	it("stamps the disabled reason on a matching axis, case-insensitively, by RESULT spelling", () => {
		const axes = [axis("Account_Id__Name"), axis("region_id__name")];
		const out = markAlreadyInResult(
			axes,
			new Set(["account_id__name"]), // structural read's own (lowercased-agnostic) spelling
		);
		expect(out[0].disabledReason).toBe(ALREADY_AT_GRAIN_REASON);
		expect(out[0].column).toBe("Account_Id__Name"); // the axis keeps ITS spelling
		expect(out[1].disabledReason).toBeNull();
	});

	// One case, not two, since DAT-671 R5: "the structural read couldn't decide"
	// and "nothing is already sliced" both arrive here as an EMPTY set, because
	// `alreadyInResult` absorbs `existingIdentifierColumns`'s null into the union
	// it returns. They were always the same behaviour — pass everything through,
	// never guess — and they are now the same value too, so this parameter is no
	// longer nullable.
	it("passes axes through unchanged when nothing is known to be already sliced", () => {
		const axes = [axis("account_id__name")];
		expect(markAlreadyInResult(axes, new Set())).toBe(axes);
	});

	it("never overwrites an axis that's already disabled for another reason", () => {
		const already = {
			...axis("account_id__name"),
			disabledReason: "some other reason",
		};
		const out = markAlreadyInResult([already], new Set(["account_id__name"]));
		expect(out[0].disabledReason).toBe("some other reason");
	});
});

describe("unionSubstrateAxes", () => {
	it("appends uncataloged substrate dims below curated axes, skipping covered columns", () => {
		const out = unionSubstrateAxes(
			[axis("customer__region")],
			["customer__region", "customer__segment"],
		);
		expect(out.map((a) => a.column)).toEqual([
			"customer__region",
			"customer__segment",
		]);
		// The curated row is untouched; the substrate row carries no curation.
		expect(out[1]).toEqual({
			column: "customer__segment",
			sliceType: "categorical",
			values: [],
			valueCount: null,
			businessContext: null,
			temporal: null,
			sliceRelevance: null,
			sliceInterest: null,
			driverGain: null,
			hierarchyNext: null,
			disabledReason: null,
		});
	});
});

describe("temporalKindsFromColumns", () => {
	const viewIds = new Set(["vt1"]);

	it("maps DATE/TIMESTAMP resolved types, ignoring everything else", () => {
		const kinds = temporalKindsFromColumns(
			[
				{ tableId: "vt1", columnName: "entry__date", resolvedType: "DATE" },
				{ tableId: "vt1", columnName: "created", resolvedType: "TIMESTAMP" },
				{ tableId: "vt1", columnName: "name", resolvedType: "VARCHAR" },
				{ tableId: "vt1", columnName: null, resolvedType: "DATE" },
			],
			viewIds,
		);
		expect(kinds.get("entry__date")).toBe("date");
		expect(kinds.get("created")).toBe("timestamp");
		expect(kinds.has("name")).toBe(false);
	});

	it("is first-wins across view rows — a shared name can't flip between loads", () => {
		const kinds = temporalKindsFromColumns(
			[
				{ tableId: "vt1", columnName: "shared", resolvedType: "DATE" },
				{ tableId: "vt2", columnName: "shared", resolvedType: "VARCHAR" },
			],
			new Set(["vt1", "vt2"]),
		);
		expect(kinds.get("shared")).toBe("date");
	});

	it("lets a view row decide over a same-named fact row — including 'not temporal'", () => {
		const kinds = temporalKindsFromColumns(
			[
				// The fact stores the raw value as VARCHAR; the view projects DATE.
				{ tableId: "fact1", columnName: "booked", resolvedType: "VARCHAR" },
				{ tableId: "vt1", columnName: "booked", resolvedType: "DATE" },
				// The view says VARCHAR — the fact's DATE must not leak through.
				{ tableId: "vt1", columnName: "label", resolvedType: "VARCHAR" },
				{ tableId: "fact1", columnName: "label", resolvedType: "DATE" },
				// No view row → the fact type fills in (bare fact columns).
				{ tableId: "fact1", columnName: "paid_at", resolvedType: "TIMESTAMP" },
			],
			viewIds,
		);
		expect(kinds.get("booked")).toBe("date");
		expect(kinds.has("label")).toBe(false);
		expect(kinds.get("paid_at")).toBe("timestamp");
	});
});

describe("applyTemporalKinds", () => {
	it("stamps resolved kinds onto matching axes only", () => {
		const out = applyTemporalKinds(
			[axis("entry__date"), axis("region")],
			new Map([["entry__date", "date" as const]]),
		);
		expect(out.map((a) => a.temporal)).toEqual(["date", null]);
	});
});

describe("unitGate (DAT-731 — cross-unit aggregation flag)", () => {
	it("flags a measure whose unit column carries more than one distinct unit", () => {
		const offending = unitGate(
			[{ tableId: "f1", column: "amount" }],
			[
				{
					tableId: "f1",
					column: "amount",
					unitSource: "currency",
					distinctCount: null,
				},
				{
					tableId: "f1",
					column: "currency",
					unitSource: null,
					distinctCount: 4,
				},
			],
		);
		expect(offending).toEqual([
			{ measure: "amount", unitColumn: "currency", unitCount: 4 },
		]);
	});

	it("stays silent for a single-unit column — the clean finance corpus (all USD)", () => {
		expect(
			unitGate(
				[{ tableId: "f1", column: "amount" }],
				[
					{
						tableId: "f1",
						column: "amount",
						unitSource: "currency",
						distinctCount: null,
					},
					{
						tableId: "f1",
						column: "currency",
						unitSource: null,
						distinctCount: 1,
					},
				],
			),
		).toEqual([]);
	});

	it("resolves each measure's unit column IN ITS OWN FACT — one fact's clean unit column cannot mask another's mixed one (the multi-fact masking fix)", () => {
		// f1.amount is measured_in a SINGLE-currency f1.currency; f2.cost is
		// measured_in a 5-currency f2.currency. Both unit columns are named
		// `currency` — a bare-name fold would let f1's clean count shadow f2's and
		// MASK the real mixing. Per-fact resolution flags exactly f2.cost.
		const offending = unitGate(
			[
				{ tableId: "f1", column: "amount" },
				{ tableId: "f2", column: "cost" },
			],
			[
				{
					tableId: "f1",
					column: "amount",
					unitSource: "currency",
					distinctCount: null,
				},
				{
					tableId: "f1",
					column: "currency",
					unitSource: null,
					distinctCount: 1,
				},
				{
					tableId: "f2",
					column: "cost",
					unitSource: "currency",
					distinctCount: null,
				},
				{
					tableId: "f2",
					column: "currency",
					unitSource: null,
					distinctCount: 5,
				},
			],
		);
		expect(offending).toEqual([
			{ measure: "cost", unitColumn: "currency", unitCount: 5 },
		]);
	});

	it("does not gate a dimensionless measure, or a qualified cross-table pointer (deferred to the engine edge)", () => {
		// dimensionless → never a unit to mix.
		expect(
			unitGate(
				[{ tableId: "f1", column: "ratio" }],
				[
					{
						tableId: "f1",
						column: "ratio",
						unitSource: "dimensionless",
						distinctCount: null,
					},
					{
						tableId: "f1",
						column: "currency",
						unitSource: null,
						distinctCount: 4,
					},
				],
			),
		).toEqual([]);
		// A qualified `table.column` pointer resolves in another table — the cockpit
		// defers cross-table resolution to the engine's measured_in edge (v1 flags
		// same-table units only), so it does NOT gate even when a same-named unit
		// column happens to sit on the measure's own fact.
		expect(
			unitGate(
				[{ tableId: "f1", column: "fee" }],
				[
					{
						tableId: "f1",
						column: "fee",
						unitSource: "book.currency",
						distinctCount: null,
					},
					{
						tableId: "f1",
						column: "currency",
						unitSource: null,
						distinctCount: 4,
					},
				],
			),
		).toEqual([]);
	});

	it("describeUnitGate names the measure, the count, and the unit column", () => {
		const msg = describeUnitGate([
			{ measure: "amount", unitColumn: "currency", unitCount: 4 },
		]);
		expect(msg).toContain("amount");
		expect(msg).toContain("4");
		expect(msg).toContain("currency");
		expect(msg).toContain("conversion");
	});
});

describe("driver ordering", () => {
	it("takes the max gain per dimension across rankings, ignoring malformed entries", () => {
		const gains = driverGains([
			{
				status: "measured",
				rankedDimensions: [
					{ dimension: "region", gain: 0.2 },
					{ dimension: "channel", gain: 0.5 },
					{ dimension: 7, gain: 0.9 },
					"junk",
				],
			},
			{
				status: "measured",
				rankedDimensions: [{ dimension: "region", gain: 0.4 }],
			},
			{ status: "measured", rankedDimensions: null },
		]);
		expect([...gains.entries()]).toEqual([
			["region", 0.4],
			["channel", 0.5],
		]);
	});

	// DAT-859: an abstained ranking's rankedDimensions never contributes, even if
	// it somehow carried entries — align with the same read-side convention as
	// look_drivers/formatDrivers (defense in depth over the engine's own invariant).
	it("ignores an abstained ranking's dimensions regardless of content", () => {
		const gains = driverGains([
			{
				status: "abstained",
				rankedDimensions: [{ dimension: "region", gain: 0.9 }],
			},
			{
				status: "measured",
				rankedDimensions: [{ dimension: "channel", gain: 0.3 }],
			},
		]);
		expect([...gains.entries()]).toEqual([["channel", 0.3]]);
	});

	it("puts measured drivers first by gain and keeps the rest in incoming order", () => {
		const out = orderAxesByDrivers(
			[axis("a"), axis("b"), axis("c"), axis("d")],
			new Map([
				["c", 0.1],
				["b", 0.6],
			]),
		);
		expect(out.map((a) => a.column)).toEqual(["b", "c", "a", "d"]);
	});

	// DAT-673: the gain used to decide order was thrown away before the axis
	// reached the wire — the chip had no way to disclose WHY a driver led.
	it("stamps the SAME gain it ordered by onto each axis, null everywhere else", () => {
		const out = orderAxesByDrivers(
			[axis("a"), axis("b"), axis("c")],
			new Map([
				["c", 0.1],
				["b", 0.6],
			]),
		);
		expect(out.map((a) => [a.column, a.driverGain])).toEqual([
			["b", 0.6],
			["c", 0.1],
			["a", null],
		]);
	});
});

describe("hierarchyDescentMap (DAT-673 hierarchy descent)", () => {
	it("maps a CONFIRMED drill-down chain's members to their next-finer neighbor, ordered by level not array position", () => {
		const next = hierarchyDescentMap([
			{
				tableId: "fact1",
				kind: "drilldown",
				needsConfirmation: false,
				// Deliberately out of level order — level must decide, not index.
				members: [
					{ column_name: "account_id__account_type", level: 0 },
					{ column_name: "account_id__account_subtype", level: 1 },
					{ column_name: "account_id__account_name", level: 2 },
				],
			},
		]);
		expect(next.get("account_id__account_type")).toBe(
			"account_id__account_subtype",
		);
		expect(next.get("account_id__account_subtype")).toBe(
			"account_id__account_name",
		);
		// The finest level has no next — absent from the map, never a phantom null.
		expect(next.has("account_id__account_name")).toBe(false);
	});

	it("excludes an UNCONFIRMED drill-down chain — same caution DAT-762 gives unconfirmed aliases", () => {
		const next = hierarchyDescentMap([
			{
				tableId: "fact1",
				kind: "drilldown",
				needsConfirmation: true,
				members: [
					{ column_name: "a", level: 0 },
					{ column_name: "b", level: 1 },
				],
			},
		]);
		expect(next.size).toBe(0);
	});

	it("excludes alias and role kinds — neither is an ordered descent chain", () => {
		const rows = [
			{
				tableId: "fact1",
				kind: "alias",
				needsConfirmation: false,
				members: [
					{ column_name: "a", level: 0 },
					{ column_name: "b", level: 1 },
				],
			},
			{
				tableId: "fact1",
				kind: "role",
				needsConfirmation: false,
				members: [
					{ column_name: "c", level: 0 },
					{ column_name: "d", level: 1 },
				],
			},
		];
		expect(hierarchyDescentMap(rows).size).toBe(0);
	});

	it("first occurrence wins when a column appears in more than one qualifying hierarchy", () => {
		const next = hierarchyDescentMap([
			{
				tableId: "fact1",
				kind: "drilldown",
				needsConfirmation: false,
				members: [
					{ column_name: "a", level: 0 },
					{ column_name: "b", level: 1 },
				],
			},
			{
				tableId: "fact1",
				kind: "drilldown",
				needsConfirmation: false,
				members: [
					{ column_name: "a", level: 0 },
					{ column_name: "z", level: 1 },
				],
			},
		]);
		expect(next.get("a")).toBe("b");
	});

	it("falls back to array index only when a member's level is absent, and ignores malformed members", () => {
		const next = hierarchyDescentMap([
			{
				tableId: "fact1",
				kind: "drilldown",
				needsConfirmation: false,
				members: [{ column_name: "a" }, { column_name: "b" }, "junk", null],
			},
		]);
		expect(next.get("a")).toBe("b");
	});

	it("yields nothing for a non-array members value", () => {
		expect(
			hierarchyDescentMap([
				{
					tableId: "fact1",
					kind: "drilldown",
					needsConfirmation: false,
					members: "not-an-array",
				},
			]).size,
		).toBe(0);
	});
});

describe("applyHierarchyDescent", () => {
	it("stamps hierarchyNext only when the suggested column is among the resolved axes", () => {
		const out = applyHierarchyDescent(
			[axis("a"), axis("b")],
			new Map([
				["a", "b"], // b IS among the resolved axes → kept
				["b", "phantom"], // phantom is NOT → dropped, never a dead reference
			]),
		);
		expect(out.map((a) => a.hierarchyNext)).toEqual(["b", null]);
	});
});

/** The engine-persisted parts shape — grounding is `from[0]` since DAT-703. */
const partsJson = (relation: string) => ({
	select: [{ expr: "SUM(amount)", alias: "value" }],
	from: [relation],
	where: [],
});

const seed = () => {
	rowsByTable.clear();
	rowsByTable.set(currentLifecycleArtifacts, [
		{
			dag: {
				dependencies: {
					rev: { type: "extract", source: { standard_field: "revenue" } },
					cogs: { type: "extract", source: { standard_field: "cogs" } },
					margin: { type: "formula", expression: "rev - cogs" },
				},
			},
		},
	]);
	rowsByTable.set(sqlSnippets, [
		// Grounded: its parts name the enriched view directly.
		{
			standardField: "revenue",
			parts: partsJson("enriched_invoices"),
			failureCount: 0,
		},
		// Failed extract → ungrounded → contributes no fact table.
		{
			standardField: "cogs",
			parts: partsJson("enriched_purchases"),
			failureCount: 2,
		},
		// A field the metric does not reference → filtered out up front.
		{
			standardField: "cash",
			parts: partsJson("enriched_bank"),
			failureCount: 0,
		},
	]);
	rowsByTable.set(currentEnrichedViews, [
		{
			viewName: "enriched_invoices",
			viewTableId: "vt1",
			factTableId: "fact1",
			// The grain-verified substrate: one column the catalog also curates
			// (stays curated) and one it doesn't (offered bare, after curated).
			dimensionColumns: ["customer__region", "customer__segment"],
			isGrainVerified: true,
		},
		{
			viewName: "enriched_purchases",
			viewTableId: "vt2",
			factTableId: "fact2",
			dimensionColumns: ["supplier__country"],
			isGrainVerified: true,
		},
		{ viewName: "enriched_bank", viewTableId: "vt3", factTableId: "fact3" },
	]);
	rowsByTable.set(currentSliceDefinitions, [
		{
			tableId: "fact1",
			columnName: "customer__region",
			sliceRelevance: 0.9,
			sliceInterest: "primary",
			sliceType: "categorical",
			distinctValues: ["EU", "US"],
			valueCount: 2,
			businessContext: null,
		},
	]);
	// The catalog types behind temporal detection: the VIEW table (vt1) carries
	// the FK-projected dims; customer__segment is a DATE there.
	rowsByTable.set(columns, [
		{ tableId: "vt1", columnName: "customer__region", resolvedType: "VARCHAR" },
		{ tableId: "vt1", columnName: "customer__segment", resolvedType: "DATE" },
		{
			tableId: "fact1",
			columnName: "amount",
			resolvedType: "DOUBLE",
		},
	]);
	// The baseline fixture represents an already-classified metric (DAT-725):
	// the engine's additivity phase has run and says time-additive, so tests
	// NOT about the time gate itself don't need to think about it. Tests that
	// exercise the gate override this explicitly (VERDICT-STOCK clears/flips
	// it, VERDICT-MISSING/WITHHELD empties the table to simulate no verdict).
	rowsByTable.set(currentMetricAxisAdditivity, [
		{
			axisKind: "time",
			axisKey: "*",
			status: "classified",
			verdict: "additive",
			reason: null,
			abstainReason: null,
			bucketGrain: null,
		},
		{
			axisKind: "categorical",
			axisKey: "*",
			status: "classified",
			verdict: "additive",
			reason: null,
			abstainReason: null,
			bucketGrain: null,
		},
	]);
};

describe("resolveDrillAxes (mocked metadata client)", () => {
	it("joins dag → accepted parts → fact table → curated ∪ substrate axes", async () => {
		seed();
		const { axes } = await resolveDrillAxes({ metricKey: "gross_margin" });
		expect(axes).toEqual([
			{
				column: "customer__region",
				sliceType: "categorical",
				values: ["EU", "US"],
				valueCount: 2,
				businessContext: null,
				temporal: null,
				sliceRelevance: 0.9,
				sliceInterest: "primary",
				driverGain: null,
				hierarchyNext: null,
				disabledReason: null,
			},
			// Substrate-only: the view exposes it, the catalog never curated it.
			// supplier__country stays absent — its fact (cogs) never grounded.
			// Its DATE type on the view table makes it the temporal axis.
			{
				column: "customer__segment",
				sliceType: "categorical",
				values: [],
				valueCount: null,
				businessContext: null,
				temporal: "date",
				sliceRelevance: null,
				sliceInterest: null,
				driverGain: null,
				hierarchyNext: null,
				disabledReason: null,
			},
		]);
	});

	it("offers no substrate from a view that is not grain-verified", async () => {
		seed();
		rowsByTable.set(currentEnrichedViews, [
			{
				viewName: "enriched_invoices",
				viewTableId: "vt1",
				factTableId: "fact1",
				dimensionColumns: ["customer__region", "customer__segment"],
				isGrainVerified: false,
			},
		]);
		const { axes } = await resolveDrillAxes({ standardField: "revenue" });
		expect(axes.map((a) => a.column)).toEqual(["customer__region"]);
	});

	it("puts a measured driver ahead of curation order", async () => {
		seed();
		rowsByTable.set(currentDriverRankings, [
			{
				status: "measured",
				rankedDimensions: [{ dimension: "customer__segment", gain: 0.31 }],
			},
		]);
		const { axes } = await resolveDrillAxes({ standardField: "revenue" });
		expect(axes.map((a) => a.column)).toEqual([
			"customer__segment",
			"customer__region",
		]);
	});

	// Critical review-round finding: this DB-read → descent wiring had ZERO
	// coverage — the mutation `hierarchyDescentMap(hierarchyRows)` →
	// `hierarchyDescentMap([])` left every other test in this file green,
	// because none of them ever registered a row for
	// `currentDimensionHierarchies` (the fluent mock silently returns `[]` for
	// any unseeded table). This seeds a CONFIRMED drilldown chain over the
	// fixture's own fact1 axes and asserts the wiring actually reaches the
	// resolved axis, end to end through resolveDrillAxes.
	it("wires a CONFIRMED drilldown hierarchy's next level onto the resolved axis (DAT-673 hierarchy descent)", async () => {
		seed();
		rowsByTable.set(currentDimensionHierarchies, [
			{
				tableId: "fact1",
				kind: "drilldown",
				needsConfirmation: false,
				members: [
					{ column_name: "customer__region", level: 0 },
					{ column_name: "customer__segment", level: 1 },
				],
			},
		]);
		const { axes } = await resolveDrillAxes({ metricKey: "gross_margin" });
		const region = axes.find((a) => a.column === "customer__region");
		expect(region?.hierarchyNext).toBe("customer__segment");
		// The finest level has nothing further to descend to.
		const segment = axes.find((a) => a.column === "customer__segment");
		expect(segment?.hierarchyNext).toBeNull();
	});

	it("resolves a single measure by standard field without the lifecycle read", async () => {
		seed();
		rowsByTable.delete(currentLifecycleArtifacts);
		const { axes } = await resolveDrillAxes({ standardField: "revenue" });
		expect(axes.map((a) => a.column)).toEqual([
			"customer__region",
			"customer__segment",
		]);
	});

	it("yields no axes for an unknown metric or a fully ungrounded one", async () => {
		seed();
		rowsByTable.set(currentLifecycleArtifacts, []);
		expect((await resolveDrillAxes({ metricKey: "nope" })).axes).toEqual([]);
		seed();
		expect((await resolveDrillAxes({ standardField: "cogs" })).axes).toEqual(
			[],
		);
	});

	// One `metric_axis_additivity` row as the view serves it.
	const verdictRow = (
		axisKind: string,
		over: Partial<{
			axisKey: string;
			status: string;
			verdict: string | null;
			reason: string | null;
			abstainReason: string | null;
			bucketGrain: string | null;
		}> = {},
	) => ({
		axisKind,
		axisKey: "*",
		status: "classified",
		verdict: "additive",
		reason: null,
		abstainReason: null,
		bucketGrain: null,
		...over,
	});

	it("VERDICT-RECOMPUTE (DAT-857): a ratio over ADDITIVE carriers KEEPS its grain — the pinned gross-margin case", async () => {
		seed();
		// The engine says the target is a ratio: recomputed per bucket, not summed.
		// Its carrier measures are additive flows, so bucketing IS honest — the
		// composer sums each carrier per bucket and re-evaluates the formula there.
		// The old boolean gate stripped the grain here and left a raw 365-row date
		// slice in its place: the meaningful ask withheld, the misleading one kept.
		rowsByTable.set(currentMetricAxisAdditivity, [
			verdictRow("time", {
				verdict: "non_additive_recompute",
				reason: "ratio",
			}),
			verdictRow("categorical", {
				verdict: "non_additive_recompute",
				reason: "ratio",
			}),
		]);
		const res = await resolveDrillAxes({ standardField: "revenue" });
		const dateAxis = res.axes.find((a) => a.column === "customer__segment");
		expect(dateAxis?.temporal).toBe("date"); // OFFERED, with per-bucket recompute
		expect(res.temporalGateReason).toBeUndefined();
		expect(res.temporalGateSource).toBe("engine-verdict");
		// ...and the drilled total must not claim the buckets add up to it.
		expect(res.reconciles).toEqual({ time: false, categorical: false });
	});

	it("VERDICT-SEMI-ADDITIVE (DAT-857): a stock withholds its grain and says the buckets are meaningful but unsummable", async () => {
		seed();
		rowsByTable.set(currentMetricAxisAdditivity, [
			verdictRow("time", { verdict: "semi_additive", reason: "stock" }),
			verdictRow("categorical"),
		]);
		const res = await resolveDrillAxes({ standardField: "revenue" });
		const dateAxis = res.axes.find((a) => a.column === "customer__segment");
		expect(dateAxis?.temporal).toBeNull(); // withheld: the composer can only SUM
		expect(res.temporalGateReason).toContain(
			"Each period on its own is meaningful",
		);
		expect(res.temporalGateSource).toBe("engine-verdict");
		// The categorical breakdown still reconciles — a balance sums across accounts.
		expect(res.reconciles).toEqual({ time: false, categorical: true });
	});

	it("VERDICT-FLOW (DAT-857): an additive flow keeps its grain and reconciles on both axes", async () => {
		seed();
		rowsByTable.set(currentMetricAxisAdditivity, [
			verdictRow("time"),
			verdictRow("categorical"),
		]);
		const res = await resolveDrillAxes({ standardField: "revenue" });
		const dateAxis = res.axes.find((a) => a.column === "customer__segment");
		expect(dateAxis?.temporal).toBe("date"); // grain KEPT
		expect(res.temporalGateReason).toBeUndefined();
		expect(res.temporalGateSource).toBe("engine-verdict");
		expect(res.reconciles).toEqual({ time: true, categorical: true });
	});

	it("VERDICT-ABSTAINED (DAT-868): a typed abstention withholds the grain in the ENGINE's words, not as a flat refusal", async () => {
		seed();
		rowsByTable.set(currentMetricAxisAdditivity, [
			verdictRow("time", {
				status: "abstained",
				verdict: null,
				abstainReason: "unknown_temporal",
			}),
			verdictRow("categorical"),
		]);
		const res = await resolveDrillAxes({ standardField: "revenue" });
		expect(
			res.axes.find((a) => a.column === "customer__segment")?.temporal,
		).toBeNull();
		expect(res.temporalGateReason).toContain("no stock/flow classification");
		// An abstention is NOT "we judged it non-additive" — the source still says a
		// verdict row was found, and the reason names the gap.
		expect(res.temporalGateSource).toBe("engine-verdict");
	});

	it("VERDICT-MISSING / WITHHELD (DAT-725): no persisted verdict → grain withheld with a visible reason, never silently recomputed from a local heuristic", async () => {
		seed();
		rowsByTable.set(currentMetricAxisAdditivity, []); // no rows at all
		const res = await resolveDrillAxes({ standardField: "revenue" });
		const dateAxis = res.axes.find((a) => a.column === "customer__segment");
		expect(dateAxis).toBeDefined();
		expect(dateAxis?.temporal).toBeNull(); // grain WITHHELD, not silently kept
		expect(dateAxis?.temporalWithheldReason).toBeDefined(); // ...and it says why
		expect(res.temporalGateSource).toBe("withheld-no-verdict");
		expect(res.temporalGateReason).toContain("has not classified this target");
	});

	it("BUCKET GRAIN (DAT-857/730): a refining per-axis row carries the axis's observed cadence onto the axis", async () => {
		seed();
		rowsByTable.set(currentMetricAxisAdditivity, [
			verdictRow("time"),
			verdictRow("categorical"),
			// The engine refined THIS column: monthly data, so no day buckets.
			verdictRow("time", {
				axisKey: "customer__segment",
				bucketGrain: "month",
			}),
		]);
		const res = await resolveDrillAxes({ standardField: "revenue" });
		const dateAxis = res.axes.find((a) => a.column === "customer__segment");
		expect(dateAxis?.temporal).toBe("date");
		expect(dateAxis?.bucketGrain).toBe("month");
	});

	it("AC WIRING (DAT-857): a bucketable axis ranks ABOVE a withheld raw date, and the floored presets reach the axis", async () => {
		seed();
		// TWO date columns on the same fact: the engine refined `customer__segment`
		// (monthly cadence, additive → bucketable) and abstained on `due_date`.
		rowsByTable.set(columns, [
			{
				tableId: "vt1",
				columnName: "customer__region",
				resolvedType: "VARCHAR",
			},
			{ tableId: "vt1", columnName: "customer__segment", resolvedType: "DATE" },
			{ tableId: "vt1", columnName: "due_date", resolvedType: "DATE" },
			{ tableId: "fact1", columnName: "amount", resolvedType: "DOUBLE" },
		]);
		rowsByTable.set(currentEnrichedViews, [
			{
				viewName: "enriched_invoices",
				viewTableId: "vt1",
				factTableId: "fact1",
				dimensionColumns: ["customer__region", "customer__segment", "due_date"],
				isGrainVerified: true,
			},
		]);
		rowsByTable.set(currentMetricAxisAdditivity, [
			verdictRow("categorical"),
			verdictRow("time"),
			verdictRow("time", {
				axisKey: "customer__segment",
				bucketGrain: "month",
			}),
			verdictRow("time", {
				axisKey: "due_date",
				status: "abstained",
				verdict: null,
				abstainReason: "unknown_temporal",
			}),
		]);

		const res = await resolveDrillAxes({ standardField: "revenue" });
		const bucketable = res.axes.find((a) => a.column === "customer__segment");
		const withheld = res.axes.find((a) => a.column === "due_date");
		expect(bucketable?.temporal).toBe("date");
		expect(withheld?.temporal).toBeNull();
		expect(withheld?.temporalWithheldReason).toContain(
			"no stock/flow classification",
		);

		// DEMOTION: the raw-date slice ranks LAST — below every axis that can
		// actually be bucketed or broken out.
		expect(res.axes.at(-1)?.column).toBe("due_date");
		expect(
			res.axes.findIndex((a) => a.column === "customer__segment"),
		).toBeLessThan(res.axes.findIndex((a) => a.column === "due_date"));

		// ...and the served cadence reaches the grain menu as a FLOOR: a monthly
		// axis is not offered day buckets it has no data to fill.
		expect(
			grainPresetsFrom(
				bucketable?.temporal ?? "date",
				bucketable?.bucketGrain,
			).map((g) => g.token),
		).toEqual(["1M", "1q", "1y"]);
	});

	it("AC WIRING (DAT-857): a METRIC target reads its CARRIERS' verdicts — the pinned gross-margin case", async () => {
		seed();
		// The target-aware double: the metric is a recompute (a ratio), and the
		// answer for its two carriers differs. `revenue` sums; `cogs` does not.
		const additive = (axisKind: string) => verdictRow(axisKind);
		const ratio = (axisKind: string) =>
			verdictRow(axisKind, {
				verdict: "non_additive_recompute",
				reason: "ratio",
			});
		rowsByTable.set(currentMetricAxisAdditivity, (literals: string[]) => {
			if (literals.includes("cogs")) {
				return [
					verdictRow("time", { verdict: "semi_additive", reason: "stock" }),
					additive("categorical"),
				];
			}
			if (literals.includes("revenue")) {
				return [additive("time"), additive("categorical")];
			}
			return [ratio("time"), ratio("categorical")]; // the metric itself
		});

		// The metric's DAG names revenue + cogs as its extracts (see seed()).
		const res = await resolveDrillAxes({ metricKey: "margin" });
		const dateAxis = res.axes.find((a) => a.column === "customer__segment");
		// A ratio recomputes per bucket from its carriers — but `cogs` does not sum
		// across periods, so the recomputed number would be wrong. Withheld, and
		// the reason NAMES the carrier that blocks it.
		expect(dateAxis?.temporal).toBeNull();
		expect(dateAxis?.temporalWithheldReason).toContain("cogs");
		expect(res.reconciles).toEqual({ time: false, categorical: false });

		// Flip the blocking carrier to additive and the SAME metric is offered.
		rowsByTable.set(currentMetricAxisAdditivity, (literals: string[]) =>
			literals.includes("revenue") || literals.includes("cogs")
				? [additive("time"), additive("categorical")]
				: [ratio("time"), ratio("categorical")],
		);
		const offered = await resolveDrillAxes({ metricKey: "margin" });
		expect(
			offered.axes.find((a) => a.column === "customer__segment")?.temporal,
		).toBe("date");
		// ...and its total still must not claim the buckets add up to it.
		expect(offered.reconciles).toEqual({ time: false, categorical: false });
	});

	it("UNIT GATE (DAT-731): a measure measured_in a MULTI-valued unit column flags a cross-unit aggregation", async () => {
		seed();
		// `amount` (the aggregated measure) is measured_in `currency`, which carries
		// 4 distinct units on the fact → summing across the population mixes units.
		rowsByTable.set(columns, [
			{ tableId: "vt1", columnName: "customer__segment", resolvedType: "DATE" },
			{
				tableId: "fact1",
				columnName: "amount",
				resolvedType: "DOUBLE",
				unitSourceColumn: "currency",
			},
			{
				tableId: "fact1",
				columnName: "currency",
				resolvedType: "VARCHAR",
				distinctCount: 4,
			},
		]);
		const res = await resolveDrillAxes({ standardField: "revenue" });
		expect(res.unitGateReason).toContain("currency");
		expect(res.unitGateReason).toContain("4");
		expect(res.unitGateReason).toContain("conversion");
		// The time gate is orthogonal to the unit gate — seed()'s time-additive
		// verdict keeps the grain regardless of the cross-unit flag.
		expect(
			res.axes.find((a) => a.column === "customer__segment")?.temporal,
		).toBe("date");
	});

	it("UNIT GATE (DAT-731): a SINGLE-currency measure is NOT flagged (the clean corpus stays quiet)", async () => {
		seed();
		rowsByTable.set(columns, [
			{ tableId: "vt1", columnName: "customer__segment", resolvedType: "DATE" },
			{
				tableId: "fact1",
				columnName: "amount",
				resolvedType: "DOUBLE",
				unitSourceColumn: "currency",
			},
			{
				tableId: "fact1",
				columnName: "currency",
				resolvedType: "VARCHAR",
				distinctCount: 1,
			},
		]);
		const res = await resolveDrillAxes({ standardField: "revenue" });
		expect(res.unitGateReason).toBeUndefined();
	});

	it("UNIT GATE (DAT-731): a MULTI-FACT metric flags the mixed-currency fact — one fact's clean `currency` does NOT mask another's (the masking regression)", async () => {
		seed();
		// gross_margin = revenue(fact1) − cogs(fact2), BOTH grounded. fact1.amount is
		// measured_in a single-currency fact1.currency; fact2.cost is measured_in a
		// 5-currency fact2.currency. Both unit columns are named `currency`. A
		// bare-name fold would let fact1's clean count shadow fact2's and return
		// undefined; per-fact resolution flags exactly cost.
		rowsByTable.set(sqlSnippets, [
			{
				standardField: "revenue",
				parts: {
					select: [{ expr: "SUM(amount)", alias: "value" }],
					from: ["enriched_invoices"],
					where: [],
				},
				failureCount: 0,
			},
			{
				standardField: "cogs",
				parts: {
					select: [{ expr: "SUM(cost)", alias: "value" }],
					from: ["enriched_purchases"],
					where: [],
				},
				failureCount: 0,
			},
		]);
		rowsByTable.set(columns, [
			{
				tableId: "fact1",
				columnName: "amount",
				resolvedType: "DOUBLE",
				unitSourceColumn: "currency",
			},
			{
				tableId: "fact1",
				columnName: "currency",
				resolvedType: "VARCHAR",
				distinctCount: 1,
			},
			{
				tableId: "fact2",
				columnName: "cost",
				resolvedType: "DOUBLE",
				unitSourceColumn: "currency",
			},
			{
				tableId: "fact2",
				columnName: "currency",
				resolvedType: "VARCHAR",
				distinctCount: 5,
			},
		]);
		const res = await resolveDrillAxes({ metricKey: "gross_margin" });
		expect(res.unitGateReason).toBeDefined();
		expect(res.unitGateReason).toContain("cost");
		expect(res.unitGateReason).toContain("5");
	});
});

describe("resolveDrillAxes empty-result reasons", () => {
	it("names WHY axes are empty for each class", async () => {
		// Unknown metric → no extracts in its definition.
		seed();
		rowsByTable.set(currentLifecycleArtifacts, []);
		const unknown = await resolveDrillAxes({ metricKey: "nope" });
		expect(unknown.reason).toContain("names no measure extracts");

		// Failed extract → nothing accepted to resolve from.
		seed();
		const failed = await resolveDrillAxes({ standardField: "cogs" });
		expect(failed.reason).toContain("No accepted extract");

		// A pre-parts accepted snippet (no narrowable parts) → same class: the
		// re-injected corpus is the substrate; an old row resolves nothing.
		seed();
		rowsByTable.set(sqlSnippets, [
			{ standardField: "revenue", parts: null, failureCount: 0 },
		]);
		const preParts = await resolveDrillAxes({ standardField: "revenue" });
		expect(preParts.reason).toContain("No accepted extract");

		// Accepted parts reading a NON-current relation (cross-lineage / stale
		// snippet) → the reason names exactly what it reads.
		seed();
		rowsByTable.set(sqlSnippets, [
			{
				standardField: "revenue",
				parts: partsJson("enriched_master_txn_table"),
				failureCount: 0,
			},
		]);
		const stale = await resolveDrillAxes({ standardField: "revenue" });
		expect(stale.axes).toEqual([]);
		expect(stale.reason).toContain("enriched_master_txn_table");
	});
});

describe("resolveDrillAxes bare-catalog reason", () => {
	it("names the bare catalogs when the fact resolves but neither source offers a dimension", async () => {
		seed();
		rowsByTable.set(currentSliceDefinitions, []);
		rowsByTable.set(currentEnrichedViews, [
			{
				viewName: "enriched_invoices",
				viewTableId: "vt1",
				factTableId: "fact1",
				dimensionColumns: [],
				isGrainVerified: true,
			},
		]);
		const result = await resolveDrillAxes({ standardField: "revenue" });
		expect(result.axes).toEqual([]);
		expect(result.reason).toContain("No dimensions available");
	});

	it("still resolves axes from the substrate alone when the slice catalog is empty", async () => {
		seed();
		rowsByTable.set(currentSliceDefinitions, []);
		const { axes } = await resolveDrillAxes({ standardField: "revenue" });
		expect(axes.map((a) => a.column)).toEqual([
			"customer__region",
			"customer__segment",
		]);
	});
});

describe("decideTimeAxis (DAT-857 composition rule)", () => {
	// Built through the SAME constructor the DB read uses, so the test cannot
	// drift from the key format.
	const target = (
		verdict: string,
		reason: string | null,
		bucketGrain: string | null = null,
	) =>
		buildTargetAdditivity([
			{
				axisKind: "time",
				axisKey: "*",
				status: "classified",
				verdict,
				reason,
				abstainReason: null,
				bucketGrain,
			},
		]);
	const additiveCarrier = () => target("additive", null);

	it("offers an additive axis", () => {
		const got = decideTimeAxis("booked_on", {
			target: target("additive", null),
			carriers: new Map(),
		});
		expect(got).toEqual({ offer: true, bucketGrain: null });
	});

	it("offers a recompute target when EVERY carrier is additive", () => {
		const got = decideTimeAxis("booked_on", {
			target: target("non_additive_recompute", "ratio"),
			carriers: new Map([
				["revenue", additiveCarrier()],
				["cost_of_goods_sold", additiveCarrier()],
			]),
		});
		expect(got).toEqual({ offer: true, bucketGrain: null });
	});

	it("REFUSES a recompute target whose carrier does not sum, and names the carrier", () => {
		// The composer sums carriers per bucket before re-evaluating the formula,
		// so a semi-additive carrier would silently corrupt the recomputed value.
		const got = decideTimeAxis("booked_on", {
			target: target("non_additive_recompute", "ratio"),
			carriers: new Map([
				["revenue", additiveCarrier()],
				["closing_balance", target("semi_additive", "stock")],
			]),
		});
		expect(got.offer).toBe(false);
		if (got.offer === false) expect(got.reason).toContain("closing_balance");
	});

	it("REFUSES a recompute target whose carrier was never judged", () => {
		const got = decideTimeAxis("booked_on", {
			target: target("non_additive_recompute", "ratio"),
			carriers: new Map([["revenue", null]]),
		});
		expect(got.offer).toBe(false);
	});

	it("prefers a refining per-axis row over the class row", () => {
		const withRefinement = buildTargetAdditivity([
			{
				axisKind: "time",
				axisKey: "*",
				status: "classified",
				verdict: "additive",
				reason: null,
				abstainReason: null,
				bucketGrain: null,
			},
			{
				axisKind: "time",
				axisKey: "booked_on",
				status: "classified",
				verdict: "additive",
				reason: null,
				abstainReason: null,
				bucketGrain: "month",
			},
		]);
		expect(
			decideTimeAxis("booked_on", {
				target: withRefinement,
				carriers: new Map(),
			}),
		).toEqual({ offer: true, bucketGrain: "month" });
		// A column the engine did NOT refine still resolves — via the class row.
		expect(
			decideTimeAxis("due_date", {
				target: withRefinement,
				carriers: new Map(),
			}),
		).toEqual({ offer: true, bucketGrain: null });
	});
});

describe("describeTimeWithhold", () => {
	it("distinguishes never-judged from abstained from semi-additive", () => {
		const missing = describeTimeWithhold(null);
		const abstained = describeTimeWithhold({
			status: "abstained",
			verdict: null,
			reason: null,
			abstainReason: "unknown_temporal",
			bucketGrain: null,
		});
		const semi = describeTimeWithhold({
			status: "classified",
			verdict: "semi_additive",
			reason: "stock",
			abstainReason: null,
			bucketGrain: null,
		});
		expect(missing).toContain("has not classified");
		expect(abstained).toContain("no stock/flow classification");
		expect(semi).toContain("Each period on its own is meaningful");
		// Three different facts must not collapse to one sentence.
		expect(new Set([missing, abstained, semi]).size).toBe(3);
	});
});

describe("demoteWithheldDateAxes (DAT-857)", () => {
	const axis = (column: string, over: Partial<DrillAxis> = {}): DrillAxis => ({
		column,
		sliceType: "categorical",
		values: [],
		valueCount: null,
		businessContext: null,
		temporal: null,
		driverGain: null,
		sliceRelevance: null,
		sliceInterest: null,
		hierarchyNext: null,
		disabledReason: null,
		...over,
	});

	it("sinks a withheld raw-date slice below everything once ANY axis is bucketable", () => {
		const got = demoteWithheldDateAxes([
			axis("due_date", { temporalWithheldReason: "nope" }),
			axis("region"),
			axis("booked_on", { temporal: "date" }),
		]);
		expect(got.map((a) => a.column)).toEqual([
			"region",
			"booked_on",
			"due_date",
		]);
	});

	it("leaves the order alone when NOTHING is bucketable — there is no better option to promote", () => {
		const input = [
			axis("due_date", { temporalWithheldReason: "nope" }),
			axis("region"),
		];
		expect(demoteWithheldDateAxes(input)).toBe(input);
	});
});

describe("the COMPOSED answer target (DAT-671 R2)", () => {
	// Built through the same constructor the DB read uses, so these cannot drift
	// from the key format.
	const carrier = (verdict: string, bucketGrain: string | null = null) =>
		buildTargetAdditivity([
			{
				axisKind: "time",
				axisKey: "*",
				status: "classified",
				verdict,
				reason: null,
				abstainReason: null,
				bucketGrain,
			},
			{
				axisKind: "categorical",
				axisKey: "*",
				status: "classified",
				verdict,
				reason: null,
				abstainReason: null,
				bucketGrain: null,
			},
		]);

	describe("coarsestGrain", () => {
		// COARSEST, not finest: a formula re-evaluated per bucket is only as fine
		// as its least frequent input. Bucketing monthly data by day would print a
		// value in one bucket and a dash in the other thirty.
		it("takes the coarsest rung across the carriers", () => {
			expect(coarsestGrain(["day", "month"])).toBe("month");
			expect(coarsestGrain(["quarter", "month", "day"])).toBe("quarter");
			expect(coarsestGrain(["year"])).toBe("year");
		});

		it("makes NO claim when any carrier makes none", () => {
			// Not out-votable: one carrier with no observed cadence means the
			// composition has no floor to state, and the menu offers every preset.
			expect(coarsestGrain(["month", null])).toBeNull();
			expect(coarsestGrain([])).toBeNull();
		});

		it("treats a token outside the mirrored ladder as no claim", () => {
			// The ladder mirrors additivity_db_models.BUCKET_GRAINS. If the engine
			// adds a rung, inventing a position for it here would claim a cadence
			// nobody served — so an unknown token degrades to "no claim", loudly
			// enough to notice and never wrong.
			expect(coarsestGrain(["fortnight"])).toBeNull();
			expect(coarsestGrain(["month", "fortnight"])).toBeNull();
		});
	});

	describe("composedVerdict", () => {
		it("treats the combination as a RECOMPUTE, floored at the coarsest carrier", () => {
			const composed = composedVerdict(
				new Map([
					["revenue", carrier("additive", "month")],
					["cogs", carrier("additive", "day")],
				]),
			);
			// The verdict the answer's own arithmetic implies — the same one a ratio
			// metric carries, so decideTimeAxis decides it through one rule.
			expect(
				decideTimeAxis("entry_date", {
					target: composed,
					carriers: new Map([
						["revenue", carrier("additive", "month")],
						["cogs", carrier("additive", "day")],
					]),
				}),
			).toEqual({ offer: true, bucketGrain: "month" });
		});

		it("never claims additivity — a composed total is always recomputed", () => {
			// The conservative direction, deliberately: the arithmetic MIGHT be a
			// plain difference whose parts sum, and understating that costs a label.
			// Overstating it would present a ratio's buckets as if they added up.
			const composed = composedVerdict(
				new Map([["revenue", carrier("additive", "month")]]),
			);
			for (const axisKind of ["time", "categorical"]) {
				expect(resolveAxisVerdict(composed, axisKind, "*")?.verdict).toBe(
					"non_additive_recompute",
				);
			}
		});

		it("withholds when a carrier does not sum — naming it", () => {
			const carriers = new Map([
				["revenue", carrier("additive", "month")],
				["closing_balance", carrier("semi_additive", "month")],
			]);
			const got = decideTimeAxis("entry_date", {
				target: composedVerdict(carriers),
				carriers,
			});
			expect(got.offer).toBe(false);
			if (got.offer === false) expect(got.reason).toContain("closing_balance");
		});
	});
});

describe("resolveAnswerTarget (DAT-671 R2 — the identity spine)", () => {
	const REVENUE = "CASE WHEN COUNT(*) = 0 THEN NULL ELSE SUM(credit) END";
	const source = (snippetId: string | null, selectExpr = REVENUE) => ({
		snippetId,
		selectExpr,
	});

	beforeEach(() => {
		groundedConcepts.clear();
		groundedConcepts.set("snip_rev", {
			concept: "revenue",
			selectExpr: REVENUE,
		});
		groundedConcepts.set("snip_rev_alt", {
			concept: "revenue",
			selectExpr: REVENUE,
		});
		groundedConcepts.set("snip_cogs", {
			concept: "cogs",
			selectExpr: "SUM(debit)",
		});
	});

	it("ONE source IS the measure — its own persisted verdict governs", async () => {
		expect(await resolveAnswerTarget([source("snip_rev")])).toEqual({
			kind: "measure",
			key: "revenue",
		});
	});

	it("several concepts are carriers of a COMPOSED target", async () => {
		expect(
			await resolveAnswerTarget([
				source("snip_rev"),
				source("snip_cogs", "SUM(debit)"),
			]),
		).toEqual({ kind: "composed", carriers: ["revenue", "cogs"] });
	});

	// The branch is on how many operands the ANSWER combined, never on how many
	// distinct concepts they resolved to. A period-over-period change of ONE
	// measure — (revenue_this - revenue_prior) / revenue_prior — resolves to a
	// single concept and is emphatically not that concept: as a `measure` target
	// it would inherit revenue's own `additive` verdict and print a percentage
	// as though the monthly rows summed to it.
	it("TWO sources of the SAME concept still compose — never collapse to the measure", async () => {
		expect(
			await resolveAnswerTarget([source("snip_rev"), source("snip_rev_alt")]),
		).toEqual({ kind: "composed", carriers: ["revenue"] });
	});

	it("withholds when any source names no grounding", async () => {
		expect(await resolveAnswerTarget([source(null)])).toBeNull();
		// All-or-nothing: one unjudged operand poisons a composed bucket, and
		// saying so once beats naming a carrier the practitioner never saw.
		expect(
			await resolveAnswerTarget([source("snip_rev"), source(null)]),
		).toBeNull();
	});

	it("withholds when a grounding does not resolve", async () => {
		// Hallucinated, retired, or retained-FAILED (the graph read drops those).
		expect(await resolveAnswerTarget([source("snip_ghost")])).toBeNull();
	});

	// The identity is a claim the answer makes, and this is what verifies it.
	it("withholds when the declared expression is no longer the classified one", async () => {
		// The sanctioned ADAPT (a tighter filter, a narrower period) lives in
		// `where` and leaves this untouched. Adapting the ARITHMETIC makes the
		// answer a different measure than the one whose additivity was judged —
		// the verdict would then license a bucketing nobody ruled on.
		expect(
			await resolveAnswerTarget([source("snip_rev", "AVG(credit)")]),
		).toBeNull();
		expect(
			await resolveAnswerTarget([source("snip_rev", "SUM(credit) * 2")]),
		).toBeNull();
	});

	it("accepts a differently-SPELLED but identical expression", async () => {
		// Canonicalized through the same AST comparison the reuse classifier
		// uses, so quoting and whitespace never cost a legitimate reuse.
		groundedConcepts.set("snip_plain", {
			concept: "revenue",
			selectExpr: "SUM(credit)",
		});
		expect(
			await resolveAnswerTarget([source("snip_plain", 'SUM( "credit" )')]),
		).toEqual({ kind: "measure", key: "revenue" });
	});
});

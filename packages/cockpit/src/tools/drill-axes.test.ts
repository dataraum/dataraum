// The per-node axis resolver (DAT-672, re-cut DAT-703): the pure halves
// (dag→fields, slice-row→axis narrowing, substrate union, driver ordering)
// plus the full `resolveDrillAxes` orchestration through a mocked
// `#/db/metadata/client` — the join logic (extract parts → relation → fact
// table → curated ∪ substrate) is where a silent shape mismatch would produce
// zero axes, so it gets pinned with fake rows.

import { describe, expect, it, vi } from "vitest";

vi.mock("#/config", () => ({
	config: { dataraumWorkspaceId: "ws-test" },
}));

// A thenable fluent stub: every drizzle builder method returns the same
// object, and awaiting it yields the rows registered for the FROM table.
// biome-ignore lint/suspicious/noExplicitAny: test double for the fluent builder
const rowsByTable = new Map<unknown, any[]>();
function fluent(rows: unknown[]) {
	// biome-ignore lint/suspicious/noExplicitAny: test double for the fluent builder
	const q: any = {
		where: () => q,
		orderBy: () => q,
		limit: () => q,
		leftJoin: () => q,
		// biome-ignore lint/suspicious/noThenProperty: drizzle query builders ARE thenables — the double must be awaitable mid-chain
		then: (
			resolve: (v: unknown[]) => unknown,
			reject?: (e: unknown) => unknown,
		) => Promise.resolve(rows).then(resolve, reject),
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

// The AST read (real DuckDB) is integration-tested in sql-ast.integration.test;
// here a thin regex stub extracts the aggregated column so the GATE logic is
// tested in this pure-metadata unit.
vi.mock("#/duckdb/sql-ast", () => ({
	aggregatedColumns: async (expr: string) => {
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
	currentDriverRankings,
	currentEnrichedViews,
	currentLifecycleArtifacts,
	currentSliceDefinitions,
	sqlSnippets,
} from "#/db/metadata/schema";
import type { DrillAxis } from "#/duckdb/drill";
import {
	applyTemporalKinds,
	axesFromSliceRows,
	driverGains,
	measureFieldsFromDag,
	orderAxesByDrivers,
	resolveDrillAxes,
	type TemporalBehavior,
	temporalGate,
	temporalKindsFromColumns,
	unionSubstrateAxes,
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
	it("narrows nullable view rows and dedupes by column keeping first (best priority)", () => {
		const axes = axesFromSliceRows([
			{
				tableId: "fact1",
				columnName: "customer__region",
				slicePriority: 1,
				sliceType: "categorical",
				distinctValues: ["EU", "US", 7, null],
				valueCount: 2,
				businessContext: "sales region",
			},
			// Same dimension cataloged on a second fact — lower priority, dropped.
			{
				tableId: "fact2",
				columnName: "customer__region",
				slicePriority: 3,
				sliceType: "categorical",
				distinctValues: [],
				valueCount: null,
				businessContext: null,
			},
			{
				tableId: "fact1",
				columnName: null, // stale row without a name → dropped
				slicePriority: 2,
				sliceType: null,
				distinctValues: null,
				valueCount: null,
				businessContext: null,
			},
			{
				tableId: "fact1",
				columnName: "booking_month",
				slicePriority: null,
				sliceType: null,
				distinctValues: "not-an-array",
				valueCount: 12,
				businessContext: null,
			},
		]);
		expect(axes).toEqual([
			{
				column: "customer__region",
				priority: 1,
				sliceType: "categorical",
				values: ["EU", "US"],
				valueCount: 2,
				businessContext: "sales region",
				temporal: null,
			},
			{
				column: "booking_month",
				priority: Number.MAX_SAFE_INTEGER,
				sliceType: "categorical",
				values: [],
				valueCount: 12,
				businessContext: null,
				temporal: null,
			},
		]);
	});
});

/** A minimal curated axis for the pure-function tests. */
const axis = (column: string, priority = 1): DrillAxis => ({
	column,
	priority,
	sliceType: "categorical",
	values: [],
	valueCount: null,
	businessContext: null,
	temporal: null,
});

describe("unionSubstrateAxes", () => {
	it("appends uncataloged substrate dims below curated axes, skipping covered columns", () => {
		const out = unionSubstrateAxes(
			[axis("customer__region", 1)],
			["customer__region", "customer__segment"],
		);
		expect(out.map((a) => a.column)).toEqual([
			"customer__region",
			"customer__segment",
		]);
		// The curated row is untouched; the substrate row carries no curation.
		expect(out[0]?.priority).toBe(1);
		expect(out[1]).toEqual({
			column: "customer__segment",
			priority: Number.MAX_SAFE_INTEGER,
			sliceType: "categorical",
			values: [],
			valueCount: null,
			businessContext: null,
			temporal: null,
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

describe("temporalGate (DAT-673)", () => {
	const behavior = (
		entries: [string, string | null, boolean][],
	): Map<string, TemporalBehavior> =>
		new Map(
			entries.map(([col, b, contested]) => [col, { behavior: b, contested }]),
		);

	it("passes a plain additive (uncontested) flow — grain stays", () => {
		const gate = temporalGate(
			new Set(["credit"]),
			behavior([["credit", "additive", false]]),
		);
		expect(gate).toEqual({ safe: true, offending: [] });
	});

	it("flags a contested additive column as stock — grain stripped", () => {
		const gate = temporalGate(
			new Set(["credit"]),
			behavior([["credit", "additive", true]]),
		);
		expect(gate).toEqual({ safe: false, offending: ["credit"] });
	});

	it("flags a point_in_time balance and an unclassified (missing) column", () => {
		const gate = temporalGate(
			new Set(["balance", "mystery"]),
			behavior([["balance", "point_in_time", false]]),
		);
		expect(gate.safe).toBe(false);
		expect(gate.offending).toEqual(["balance", "mystery"]);
	});

	it("is safe only when EVERY aggregated column is a clean flow", () => {
		expect(
			temporalGate(
				new Set(["credit", "debit"]),
				behavior([
					["credit", "additive", false],
					["debit", "additive", false],
				]),
			),
		).toEqual({ safe: true, offending: [] });
		expect(
			temporalGate(
				new Set(["credit", "debit"]),
				behavior([
					["credit", "additive", false],
					["debit", "additive", true],
				]),
			),
		).toEqual({ safe: false, offending: ["debit"] });
	});

	it("fails closed when the aggregated set is empty (unparseable expr)", () => {
		expect(
			temporalGate(new Set(), behavior([["credit", "additive", false]])),
		).toEqual({
			safe: false,
			offending: [],
		});
	});
});

describe("driver ordering", () => {
	it("takes the max gain per dimension across rankings, ignoring malformed entries", () => {
		const gains = driverGains([
			{
				rankedDimensions: [
					{ dimension: "region", gain: 0.2 },
					{ dimension: "channel", gain: 0.5 },
					{ dimension: 7, gain: 0.9 },
					"junk",
				],
			},
			{ rankedDimensions: [{ dimension: "region", gain: 0.4 }] },
			{ rankedDimensions: null },
		]);
		expect([...gains.entries()]).toEqual([
			["region", 0.4],
			["channel", 0.5],
		]);
	});

	it("puts measured drivers first by gain and keeps the rest in incoming order", () => {
		const out = orderAxesByDrivers(
			[axis("a", 1), axis("b", 2), axis("c", 3), axis("d", 4)],
			new Map([
				["c", 0.1],
				["b", 0.6],
			]),
		);
		expect(out.map((a) => a.column)).toEqual(["b", "c", "a", "d"]);
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
			slicePriority: 1,
			sliceType: "categorical",
			distinctValues: ["EU", "US"],
			valueCount: 2,
			businessContext: null,
		},
	]);
	// The catalog types behind temporal detection: the VIEW table (vt1) carries
	// the FK-projected dims; customer__segment is a DATE there. The FACT (fact1)
	// carries the aggregated measure `amount` — additive AND uncontested (a clean
	// flow), so the flow gate leaves the temporal grain on.
	rowsByTable.set(columns, [
		{ tableId: "vt1", columnName: "customer__region", resolvedType: "VARCHAR" },
		{ tableId: "vt1", columnName: "customer__segment", resolvedType: "DATE" },
		{
			tableId: "fact1",
			columnName: "amount",
			resolvedType: "DOUBLE",
			temporalBehavior: "additive",
			temporalBehaviorContested: false,
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
				priority: 1,
				sliceType: "categorical",
				values: ["EU", "US"],
				valueCount: 2,
				businessContext: null,
				temporal: null,
			},
			// Substrate-only: the view exposes it, the catalog never curated it.
			// supplier__country stays absent — its fact (cogs) never grounded.
			// Its DATE type on the view table makes it the temporal axis.
			{
				column: "customer__segment",
				priority: Number.MAX_SAFE_INTEGER,
				sliceType: "categorical",
				values: [],
				valueCount: null,
				businessContext: null,
				temporal: "date",
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

	it("puts a measured driver ahead of curated priority", async () => {
		seed();
		rowsByTable.set(currentDriverRankings, [
			{
				rankedDimensions: [{ dimension: "customer__segment", gain: 0.31 }],
			},
		]);
		const { axes } = await resolveDrillAxes({ standardField: "revenue" });
		expect(axes.map((a) => a.column)).toEqual([
			"customer__segment",
			"customer__region",
		]);
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

	it("FLOW GATE: a stock measure keeps the date axis but loses its grain (DAT-673)", async () => {
		seed();
		// The extract sums a BALANCE column (point-in-time), not a flow.
		rowsByTable.set(sqlSnippets, [
			{
				standardField: "revenue",
				parts: {
					select: [{ expr: "SUM(debit_balance)", alias: "value" }],
					from: ["enriched_invoices"],
					where: [],
				},
				failureCount: 0,
			},
		]);
		rowsByTable.set(columns, [
			{ tableId: "vt1", columnName: "customer__segment", resolvedType: "DATE" },
			{
				tableId: "fact1",
				columnName: "debit_balance",
				resolvedType: "DOUBLE",
				temporalBehavior: "point_in_time",
			},
		]);
		const res = await resolveDrillAxes({ standardField: "revenue" });
		const dateAxis = res.axes.find((a) => a.column === "customer__segment");
		// Still offered as a raw slice…
		expect(dateAxis).toBeDefined();
		// …but the grain is gated off, with a surfaced reason.
		expect(dateAxis?.temporal).toBeNull();
		expect(res.temporalGateReason).toContain("debit_balance");
		expect(res.temporalGateReason).toContain("balance");
	});

	it("FLOW GATE: an unclassified measure fails closed (no grain)", async () => {
		seed();
		rowsByTable.set(sqlSnippets, [
			{
				standardField: "revenue",
				parts: {
					select: [{ expr: "SUM(mystery)", alias: "value" }],
					from: ["enriched_invoices"],
					where: [],
				},
				failureCount: 0,
			},
		]);
		// `mystery` has no column_concept → temporalBehavior null → fail closed.
		rowsByTable.set(columns, [
			{ tableId: "vt1", columnName: "customer__segment", resolvedType: "DATE" },
			{
				tableId: "fact1",
				columnName: "mystery",
				resolvedType: "DOUBLE",
				temporalBehavior: null,
			},
		]);
		const res = await resolveDrillAxes({ standardField: "revenue" });
		expect(
			res.axes.find((a) => a.column === "customer__segment")?.temporal,
		).toBeNull();
		expect(res.temporalGateReason).toContain("mystery");
	});

	it("FLOW GATE: a CONTESTED additive measure counts as stock — grain stripped (DAT-673)", async () => {
		seed();
		rowsByTable.set(sqlSnippets, [
			{
				standardField: "revenue",
				parts: {
					select: [{ expr: "SUM(net_position)", alias: "value" }],
					from: ["enriched_invoices"],
					where: [],
				},
				failureCount: 0,
			},
		]);
		// `net_position` is additive, but the detectors CONTESTED it — we can't
		// stand behind "summable flow", so the grain must fail closed.
		rowsByTable.set(columns, [
			{ tableId: "vt1", columnName: "customer__segment", resolvedType: "DATE" },
			{
				tableId: "fact1",
				columnName: "net_position",
				resolvedType: "DOUBLE",
				temporalBehavior: "additive",
				temporalBehaviorContested: true,
			},
		]);
		const res = await resolveDrillAxes({ standardField: "revenue" });
		const dateAxis = res.axes.find((a) => a.column === "customer__segment");
		// Still offered as a raw slice, but the grain is gated off.
		expect(dateAxis).toBeDefined();
		expect(dateAxis?.temporal).toBeNull();
		expect(res.temporalGateReason).toContain("net_position");
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

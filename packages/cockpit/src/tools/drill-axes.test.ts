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
	currentDriverRankings,
	currentEnrichedViews,
	currentLifecycleArtifacts,
	currentMetricAdditivity,
	currentSliceDefinitions,
	sqlSnippets,
} from "#/db/metadata/schema";
import type { DrillAxis } from "#/duckdb/drill";
import {
	applyTemporalKinds,
	axesFromSliceRows,
	describeEngineTimeVerdict,
	describeTemporalGate,
	describeUnitGate,
	driverGains,
	measureFieldsFromDag,
	orderAxesByDrivers,
	resolveDrillAxes,
	type TemporalBehavior,
	temporalGate,
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

describe("temporalGate (DAT-673, contested handling reversed by DAT-786)", () => {
	const behavior = (
		entries: [string, string | null][],
	): Map<string, TemporalBehavior> =>
		new Map(entries.map(([col, b]) => [col, { behavior: b }]));

	it("passes a plain additive flow — grain stays", () => {
		const gate = temporalGate(
			new Set(["credit"]),
			behavior([["credit", "additive"]]),
		);
		expect(gate).toEqual({ safe: true, offending: [] });
	});

	it("trusts a reconciled additive verdict at face value — no contested gate (DAT-786 reversal of DAT-673)", () => {
		// DAT-673 used to treat a "contested" additive verdict as stock (fail
		// closed). DAT-786 removed the contested flag entirely — the stock/flow
		// resolve pass already adjudicates the LLM claim vs the structural
		// witness, so the reconciled `additive` verdict is trusted outright.
		const gate = temporalGate(
			new Set(["credit"]),
			behavior([["credit", "additive"]]),
		);
		expect(gate).toEqual({ safe: true, offending: [] });
	});

	it("distinguishes stock (point_in_time) from unclassified (missing)", () => {
		const gate = temporalGate(
			new Set(["balance", "mystery"]),
			behavior([["balance", "point_in_time"]]),
		);
		expect(gate.safe).toBe(false);
		expect(gate.offending).toEqual([
			{ column: "balance", cause: "stock" },
			{ column: "mystery", cause: "unclassified" },
		]);
	});

	it("reports a present-but-null behavior as unclassified", () => {
		const gate = temporalGate(new Set(["x"]), behavior([["x", null]]));
		expect(gate.offending).toEqual([{ column: "x", cause: "unclassified" }]);
	});

	it("is safe only when EVERY aggregated column is a clean flow", () => {
		expect(
			temporalGate(
				new Set(["credit", "debit"]),
				behavior([
					["credit", "additive"],
					["debit", "additive"],
				]),
			),
		).toEqual({ safe: true, offending: [] });
		expect(
			temporalGate(
				new Set(["credit", "debit"]),
				behavior([
					["credit", "additive"],
					["debit", "point_in_time"],
				]),
			),
		).toEqual({
			safe: false,
			offending: [{ column: "debit", cause: "stock" }],
		});
	});

	it("fails closed when the aggregated set is empty (unparseable expr)", () => {
		expect(temporalGate(new Set(), behavior([["credit", "additive"]]))).toEqual(
			{
				safe: false,
				offending: [],
			},
		);
	});
});

describe("describeTemporalGate (DAT-673)", () => {
	it("phrases each cause honestly — a balance, a gap", () => {
		expect(
			describeTemporalGate([{ column: "debit_balance", cause: "stock" }]),
		).toMatch(/debit_balance.*balance.*not a flow/);
		expect(
			describeTemporalGate([{ column: "mystery", cause: "unclassified" }]),
		).toMatch(/mystery.*no stock\/flow classification/);
	});

	it("does NOT call an unclassified column a balance", () => {
		expect(
			describeTemporalGate([{ column: "mystery", cause: "unclassified" }]),
		).not.toContain("balance");
	});

	it("joins multiple offenders and covers the empty (couldn't-confirm) case", () => {
		const msg = describeTemporalGate([
			{ column: "a", cause: "stock" },
			{ column: "b", cause: "unclassified" },
		]);
		expect(msg).toContain("a");
		expect(msg).toContain("b");
		expect(describeTemporalGate([])).toContain("couldn't confirm");
	});
});

describe("describeEngineTimeVerdict (DAT-731 — the engine verdict's richer reasons)", () => {
	it("phrases each engine reason distinctly — the DAG-aware causes the local gate can't see", () => {
		expect(describeEngineTimeVerdict("stock")).toContain("balance");
		expect(describeEngineTimeVerdict("snapshot_count")).toContain("snapshot");
		expect(describeEngineTimeVerdict("ratio")).toContain("ratio");
		expect(describeEngineTimeVerdict("average")).toContain("average");
		expect(describeEngineTimeVerdict("distinct_count")).toContain("distinct");
		expect(describeEngineTimeVerdict("min_max")).toContain("min/max");
		expect(describeEngineTimeVerdict("unknown_temporal")).toContain(
			"no stock/flow classification",
		);
	});

	it("falls back to the honest 'couldn't confirm' for a null / unrecognized reason", () => {
		expect(describeEngineTimeVerdict(null)).toContain("couldn't");
		expect(describeEngineTimeVerdict("some_future_code")).toContain("couldn't");
	});
});

describe("unitGate (DAT-731 — cross-unit aggregation flag)", () => {
	it("flags a measure whose unit column carries more than one distinct unit", () => {
		const offending = unitGate(
			new Set(["amount"]),
			new Map([["amount", "currency"]]),
			new Map([["currency", 4]]),
		);
		expect(offending).toEqual([
			{ measure: "amount", unitColumn: "currency", unitCount: 4 },
		]);
	});

	it("stays silent for a single-unit column — the clean finance corpus (all USD)", () => {
		expect(
			unitGate(
				new Set(["amount"]),
				new Map([["amount", "currency"]]),
				new Map([["currency", 1]]),
			),
		).toEqual([]);
	});

	it("does not gate a dimensionless measure, or one whose unit column can't be resolved", () => {
		// dimensionless → never a unit to mix.
		expect(
			unitGate(
				new Set(["ratio"]),
				new Map([["ratio", "dimensionless"]]),
				new Map([["currency", 4]]),
			),
		).toEqual([]);
		// A qualified pointer whose unit column is outside the node's facts (no
		// cardinality) → conservative: no FALSE flag.
		expect(
			unitGate(
				new Set(["fee"]),
				new Map([["fee", "other_table.ccy"]]),
				new Map([["currency", 4]]),
			),
		).toEqual([]);
	});

	it("resolves the column part of a qualified table.column unit pointer", () => {
		const offending = unitGate(
			new Set(["fee"]),
			new Map([["fee", "book.currency"]]),
			new Map([["currency", 3]]),
		);
		expect(offending).toEqual([
			{ measure: "fee", unitColumn: "currency", unitCount: 3 },
		]);
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
	// carries the aggregated measure `amount` — additive (a clean flow), so the
	// flow gate leaves the temporal grain on.
	rowsByTable.set(columns, [
		{ tableId: "vt1", columnName: "customer__region", resolvedType: "VARCHAR" },
		{ tableId: "vt1", columnName: "customer__segment", resolvedType: "DATE" },
		{
			tableId: "fact1",
			columnName: "amount",
			resolvedType: "DOUBLE",
			temporalBehavior: "additive",
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
		// Accurate cause: an unclassified column is NOT called a balance.
		expect(res.temporalGateReason).toContain("no stock/flow classification");
		expect(res.temporalGateReason).not.toContain("balance");
	});

	it("FLOW GATE: a reconciled additive measure keeps its grain — no contested second-guessing (DAT-786 reversal of DAT-673)", async () => {
		// DAT-673 used to fail this closed when the detectors "contested" the
		// additive verdict. DAT-786 removed that flag: the stock/flow resolve
		// pass already adjudicated the LLM claim vs the structural witness, so
		// the reconciled `additive` verdict on `net_position` is now trusted
		// outright and the time-grain slice is NOT withheld.
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
		rowsByTable.set(columns, [
			{ tableId: "vt1", columnName: "customer__segment", resolvedType: "DATE" },
			{
				tableId: "fact1",
				columnName: "net_position",
				resolvedType: "DOUBLE",
				temporalBehavior: "additive",
			},
		]);
		const res = await resolveDrillAxes({ standardField: "revenue" });
		const dateAxis = res.axes.find((a) => a.column === "customer__segment");
		expect(dateAxis).toBeDefined();
		expect(dateAxis?.temporal).toBe("date");
		expect(res.temporalGateReason).toBeUndefined();
	});

	it("FLOW GATE: a stale multi-measure snippet does NOT strip a safe measure's grain (scope leak, DAT-673)", async () => {
		seed();
		// The metric `gross_margin` names two extracts: `revenue` grounds to a
		// promoted view and aggregates an additive FLOW; `cogs` is ACCEPTED but
		// reads an UNPROMOTED relation (not in currentEnrichedViews) and sums a
		// stock. The stale snippet's `debit_balance` must never reach the gate —
		// otherwise it would strip grain from the whole node, including the
		// genuinely-safe `revenue` measure.
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
					select: [{ expr: "SUM(debit_balance)", alias: "value" }],
					from: ["enriched_stale_ledger"], // not a promoted view → ungrounded
					where: [],
				},
				failureCount: 0,
			},
		]);
		const res = await resolveDrillAxes({ metricKey: "gross_margin" });
		const dateAxis = res.axes.find((a) => a.column === "customer__segment");
		// The safe measure keeps its grain: the stale snippet contributed nothing.
		expect(dateAxis?.temporal).toBe("date");
		expect(res.temporalGateReason).toBeUndefined();
	});

	it("FLOW GATE: a windowed measure among flows fails the WHOLE gate closed (F3 multi-expr, DAT-673)", async () => {
		seed();
		// Two GROUNDED measures on the same node: `revenue` is a normal additive
		// FLOW; `cogs` is a WINDOW aggregate the AST read can't parse → empty
		// aggregated set. The windowed measure's stock/flow status can't be
		// confirmed, so the whole gate must fail closed — even though the flow
		// sibling makes the aggregated-column total non-empty (the fail-OPEN the
		// single-expr window guard alone missed).
		rowsByTable.set(sqlSnippets, [
			{
				standardField: "revenue",
				parts: {
					select: [{ expr: "SUM(credit)", alias: "value" }],
					from: ["enriched_invoices"],
					where: [],
				},
				failureCount: 0,
			},
			{
				standardField: "cogs",
				parts: {
					select: [
						{
							expr: "SUM(running_balance) OVER (PARTITION BY period)",
							alias: "value",
						},
					],
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
				columnName: "credit",
				resolvedType: "DOUBLE",
				temporalBehavior: "additive",
			},
		]);
		const res = await resolveDrillAxes({ metricKey: "gross_margin" });
		const dateAxis = res.axes.find((a) => a.column === "customer__segment");
		// Grain stripped despite the additive-flow sibling — couldn't confirm.
		expect(dateAxis).toBeDefined();
		expect(dateAxis?.temporal).toBeNull();
		expect(res.temporalGateReason).toContain("couldn't confirm");
	});

	it("ENGINE VERDICT (DAT-731): a persisted time_additive=false strips grain — and WINS over an additive-flow sibling", async () => {
		seed();
		// The local heuristic would PASS (seed's `amount` is additive), but the
		// engine's DAG-aware verdict says the metric is a RATIO (non-additive on
		// every axis) — the engine verdict is authoritative and strips the grain.
		rowsByTable.set(currentMetricAdditivity, [
			{
				timeAdditive: false,
				timeReason: "ratio",
				categoricalAdditive: false,
				categoricalReason: "ratio",
			},
		]);
		const res = await resolveDrillAxes({ standardField: "revenue" });
		const dateAxis = res.axes.find((a) => a.column === "customer__segment");
		expect(dateAxis).toBeDefined();
		expect(dateAxis?.temporal).toBeNull(); // grain gated off by the engine verdict
		expect(res.temporalGateReason).toContain("ratio");
		expect(res.temporalGateSource).toBe("engine-verdict");
	});

	it("ENGINE VERDICT (DAT-731): a persisted time_additive=true keeps grain — and WINS over a stock sibling the heuristic would strip", async () => {
		seed();
		// The extract sums a BALANCE (the local heuristic would strip the grain),
		// but the engine's rolled-up verdict says time_additive=true — the engine
		// wins and the grain stays. This is the user-visible richer-verdict path.
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
		rowsByTable.set(currentMetricAdditivity, [
			{
				timeAdditive: true,
				timeReason: null,
				categoricalAdditive: true,
				categoricalReason: null,
			},
		]);
		const res = await resolveDrillAxes({ standardField: "revenue" });
		const dateAxis = res.axes.find((a) => a.column === "customer__segment");
		expect(dateAxis?.temporal).toBe("date"); // grain KEPT — the engine verdict wins
		expect(res.temporalGateReason).toBeUndefined();
		expect(res.temporalGateSource).toBe("engine-verdict");
	});

	it("FAIL-OPEN (DAT-731): no persisted verdict → the local heuristic runs, STAMPED heuristic-fallback (never silent)", async () => {
		seed(); // no currentMetricAdditivity row → resolveTargetAdditivity returns null
		const res = await resolveDrillAxes({ standardField: "revenue" });
		const dateAxis = res.axes.find((a) => a.column === "customer__segment");
		// seed's `amount` is additive → the fallback gate passes, grain kept…
		expect(dateAxis?.temporal).toBe("date");
		// …and the fallback is STAMPED so its use is visible, not silent.
		expect(res.temporalGateSource).toBe("heuristic-fallback");
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
				temporalBehavior: "additive",
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
		// The time grain is orthogonal — `amount` is a clean flow, so it stays.
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
				temporalBehavior: "additive",
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

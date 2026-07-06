// The metric-path axis resolver (DAT-672): the pure halves (dag→fields,
// slice-row→axis narrowing) plus the full `resolveDrillAxes` orchestration
// through a mocked `#/db/metadata/client` — the join logic (extracts →
// resolveGrounding → fact table → slice definitions) is where a silent shape
// mismatch would produce zero axes, so it gets pinned with fake rows.

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

import {
	currentEnrichedViews,
	currentLifecycleArtifacts,
	currentSliceDefinitions,
	currentTables,
	sqlSnippets,
} from "#/db/metadata/schema";
import {
	axesFromSliceRows,
	measureFieldsFromDag,
	resolveDrillAxes,
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
		const sources = new Map([["fact1", ["orders", "enriched_orders"]]]);
		const axes = axesFromSliceRows(
			[
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
			],
			sources,
		);
		expect(axes).toEqual([
			{
				column: "customer__region",
				sourceRelations: ["orders", "enriched_orders"],
				priority: 1,
				sliceType: "categorical",
				values: ["EU", "US"],
				valueCount: 2,
				businessContext: "sales region",
			},
			{
				column: "booking_month",
				sourceRelations: ["orders", "enriched_orders"],
				priority: Number.MAX_SAFE_INTEGER,
				sliceType: "categorical",
				values: [],
				valueCount: 12,
				businessContext: null,
			},
		]);
	});
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
		// Grounded: names its enriched view in the SQL (resolveGrounding match).
		{
			standardField: "revenue",
			sql: "SELECT SUM(amount) AS value FROM enriched_invoices",
			failureCount: 0,
		},
		// Failed extract → ungrounded → contributes no fact table.
		{
			standardField: "cogs",
			sql: "SELECT SUM(cost) AS value FROM enriched_purchases",
			failureCount: 2,
		},
		// A field the metric does not reference → filtered out up front.
		{
			standardField: "cash",
			sql: "SELECT 1 FROM enriched_bank",
			failureCount: 0,
		},
	]);
	rowsByTable.set(currentEnrichedViews, [
		{
			viewName: "enriched_invoices",
			viewTableId: "vt1",
			factTableId: "fact1",
		},
		{
			viewName: "enriched_purchases",
			viewTableId: "vt2",
			factTableId: "fact2",
		},
		{ viewName: "enriched_bank", viewTableId: "vt3", factTableId: "fact3" },
	]);
	rowsByTable.set(currentTables, [{ tableId: "fact1", tableName: "invoices" }]);
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
};

describe("resolveDrillAxes (mocked metadata client)", () => {
	it("joins dag → grounded extracts → fact table → slice definitions", async () => {
		seed();
		const { axes } = await resolveDrillAxes({ metricKey: "gross_margin" });
		expect(axes).toEqual([
			{
				column: "customer__region",
				sourceRelations: ["invoices", "enriched_invoices"],
				priority: 1,
				sliceType: "categorical",
				values: ["EU", "US"],
				valueCount: 2,
				businessContext: null,
			},
		]);
	});

	it("resolves a single measure by standard field without the lifecycle read", async () => {
		seed();
		rowsByTable.delete(currentLifecycleArtifacts);
		const { axes } = await resolveDrillAxes({ standardField: "revenue" });
		expect(axes.map((a) => a.column)).toEqual(["customer__region"]);
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

		// Accepted extract reading a NON-current relation (cross-lineage /
		// stale snippet) → the reason names exactly what it reads.
		seed();
		rowsByTable.set(sqlSnippets, [
			{
				standardField: "revenue",
				sql: "SELECT SUM(x) AS value FROM enriched_master_txn_table",
				failureCount: 0,
			},
		]);
		const stale = await resolveDrillAxes({ standardField: "revenue" });
		expect(stale.axes).toEqual([]);
		expect(stale.reason).toContain("enriched_master_txn_table");
	});
});

describe("resolveDrillAxes bare-catalog reason", () => {
	it("names the bare catalog when the fact resolves but has no slice definitions", async () => {
		seed();
		rowsByTable.set(currentSliceDefinitions, []);
		const result = await resolveDrillAxes({ standardField: "revenue" });
		expect(result.axes).toEqual([]);
		expect(result.reason).toContain("No dimensions cataloged");
	});
});

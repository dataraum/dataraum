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
		const axes = axesFromSliceRows([
			{
				columnName: "customer__region",
				slicePriority: 1,
				sliceType: "categorical",
				distinctValues: ["EU", "US", 7, null],
				valueCount: 2,
				businessContext: "sales region",
			},
			// Same dimension cataloged on a second fact — lower priority, dropped.
			{
				columnName: "customer__region",
				slicePriority: 3,
				sliceType: "categorical",
				distinctValues: [],
				valueCount: null,
				businessContext: null,
			},
			{
				columnName: null, // stale row without a name → dropped
				slicePriority: 2,
				sliceType: null,
				distinctValues: null,
				valueCount: null,
				businessContext: null,
			},
			{
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
			},
			{
				column: "booking_month",
				priority: Number.MAX_SAFE_INTEGER,
				sliceType: "categorical",
				values: [],
				valueCount: 12,
				businessContext: null,
			},
		]);
	});
});

describe("resolveDrillAxes (mocked metadata client)", () => {
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
				columnMappings: null,
				failureCount: 0,
			},
			// Failed extract → ungrounded → contributes no fact table.
			{
				standardField: "cogs",
				sql: "SELECT SUM(cost) AS value FROM enriched_purchases",
				columnMappings: null,
				failureCount: 2,
			},
			// A field the metric does not reference → filtered out up front.
			{
				standardField: "cash",
				sql: "SELECT 1 FROM enriched_bank",
				columnMappings: null,
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
		rowsByTable.set(currentSliceDefinitions, [
			{
				columnName: "customer__region",
				slicePriority: 1,
				sliceType: "categorical",
				distinctValues: ["EU", "US"],
				valueCount: 2,
				businessContext: null,
			},
		]);
	};

	it("joins dag → grounded extracts → fact table → slice definitions", async () => {
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

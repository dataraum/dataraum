// Node-drill part resolution (DAT-702/703) through a mocked
// `#/db/metadata/client` — the newest-accepted-snippet pick, the parts
// narrowing at the boundary, and the hole-is-not-a-refusal contract are where
// a silent shape mismatch would starve the composer, so they get pinned with
// fake rows.

import { beforeEach, describe, expect, it, vi } from "vitest";

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

import { currentLifecycleArtifacts, sqlSnippets } from "#/db/metadata/schema";
import { resolveNodeSteps } from "./drill-metric";

const DSO_DAG = {
	dependencies: {
		accounts_receivable: {
			type: "extract",
			source: { standard_field: "accounts_receivable" },
			aggregation: "sum",
		},
		revenue: {
			type: "extract",
			source: { standard_field: "revenue" },
			aggregation: "sum",
		},
		days_in_period: {
			type: "constant",
			parameter: "days_in_period",
			default: 30,
		},
		dso: {
			type: "formula",
			expression: "(accounts_receivable / revenue) * days_in_period",
			depends_on: ["accounts_receivable", "revenue", "days_in_period"],
			output_step: true,
		},
	},
	output: { unit: "days" },
};

/** The engine-persisted parts shape (`extract_parts_dict`). */
const partsJson = (expr: string, where: string[]) => ({
	select: [{ expr, alias: "value" }],
	from: ["enriched_txn"],
	where,
});

beforeEach(() => rowsByTable.clear());

describe("resolveNodeSteps — metric", () => {
	it("attaches the newest ACCEPTED snippet's narrowed parts per field; a newer failing row is a hole", async () => {
		rowsByTable.set(currentLifecycleArtifacts, [{ dag: DSO_DAG }]);
		// Rows arrive newest-first (the query orders by updatedAt desc):
		// revenue's newest row FAILS → hole, no silent fall-back to the older
		// accepted one; accounts_receivable's newest is accepted.
		rowsByTable.set(sqlSnippets, [
			{
				standardField: "revenue",
				parts: partsJson("SUM(bad)", []),
				aggregation: "sum",
				failureCount: 2,
			},
			{
				standardField: "revenue",
				parts: partsJson("SUM(credit)", ["kind = 'rev'"]),
				aggregation: "sum",
				failureCount: 0,
			},
			{
				standardField: "accounts_receivable",
				parts: partsJson("SUM(open_balance)", ["kind = 'ar'"]),
				aggregation: "sum",
				failureCount: 0,
			},
		]);
		const resolved = await resolveNodeSteps({ metricKey: "dso" });
		if ("missing" in resolved) throw new Error(resolved.missing);
		const byId = new Map(resolved.steps.map((s) => [s.stepId, s]));
		expect(byId.get("accounts_receivable")).toMatchObject({
			kind: "extract",
			aggregation: "sum",
			parts: {
				selectExpr: "SUM(open_balance)",
				relation: "enriched_txn",
				where: ["kind = 'ar'"],
			},
		});
		expect(byId.get("revenue")).toMatchObject({ kind: "extract", parts: null });
		expect(byId.get("days_in_period")).toMatchObject({
			kind: "constant",
			value: "30",
		});
		expect(byId.get("dso")).toMatchObject({
			kind: "formula",
			expression: "(accounts_receivable / revenue) * days_in_period",
			outputStep: true,
		});
	});

	it("an accepted snippet WITHOUT narrowable parts (pre-parts row) is a hole", async () => {
		rowsByTable.set(currentLifecycleArtifacts, [{ dag: DSO_DAG }]);
		rowsByTable.set(sqlSnippets, [
			{
				standardField: "revenue",
				parts: null,
				aggregation: "sum",
				failureCount: 0,
			},
		]);
		const resolved = await resolveNodeSteps({ metricKey: "dso" });
		if ("missing" in resolved) throw new Error(resolved.missing);
		const byId = new Map(resolved.steps.map((s) => [s.stepId, s]));
		expect(byId.get("revenue")).toMatchObject({ parts: null });
	});

	it("reports a missing metric definition", async () => {
		rowsByTable.set(currentLifecycleArtifacts, []);
		expect(await resolveNodeSteps({ metricKey: "nope" })).toEqual({
			missing: "no metric definition found for 'nope'",
		});
	});

	it("reports an unparseable definition as missing", async () => {
		rowsByTable.set(currentLifecycleArtifacts, [{ dag: { dependencies: {} } }]);
		expect(await resolveNodeSteps({ metricKey: "empty" })).toEqual({
			missing: "no metric definition found for 'empty'",
		});
	});
});

describe("resolveNodeSteps — measure", () => {
	it("resolves a bare measure to the single-extract output step", async () => {
		rowsByTable.set(sqlSnippets, [
			{
				standardField: "revenue",
				parts: partsJson("SUM(credit)", ["kind = 'rev'"]),
				aggregation: "sum",
				failureCount: 0,
			},
		]);
		const resolved = await resolveNodeSteps({ standardField: "revenue" });
		if ("missing" in resolved) throw new Error(resolved.missing);
		expect(resolved.steps).toEqual([
			{
				stepId: "revenue",
				kind: "extract",
				parts: {
					selectExpr: "SUM(credit)",
					relation: "enriched_txn",
					where: ["kind = 'rev'"],
				},
				aggregation: "sum",
				expression: null,
				value: null,
				dependsOn: [],
				outputStep: true,
			},
		]);
	});

	it("a failing newest measure snippet is a hole (composer refuses by name), not a fall-back", async () => {
		rowsByTable.set(sqlSnippets, [
			{
				standardField: "revenue",
				parts: partsJson("SUM(bad)", []),
				aggregation: "sum",
				failureCount: 1,
			},
			{
				standardField: "revenue",
				parts: partsJson("SUM(credit)", []),
				aggregation: "sum",
				failureCount: 0,
			},
		]);
		const resolved = await resolveNodeSteps({ standardField: "revenue" });
		if ("missing" in resolved) throw new Error(resolved.missing);
		expect(resolved.steps[0]).toMatchObject({ parts: null });
	});

	it("reports a measure with no snippet at all as missing", async () => {
		rowsByTable.set(sqlSnippets, []);
		expect(await resolveNodeSteps({ standardField: "ghost" })).toEqual({
			missing: "no graph extract snippet found for 'ghost'",
		});
	});
});

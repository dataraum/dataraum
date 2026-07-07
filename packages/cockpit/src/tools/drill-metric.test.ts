// Metric-drill part resolution (DAT-702) through a mocked
// `#/db/metadata/client` — the newest-accepted-snippet pick and the
// hole-is-not-a-refusal contract are where a silent shape mismatch would
// starve the composer, so they get pinned with fake rows.

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
import { resolveMetricDrillSteps } from "./drill-metric";

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

beforeEach(() => rowsByTable.clear());

describe("resolveMetricDrillSteps", () => {
	it("attaches the newest ACCEPTED snippet per field; a newer failing row is a hole", async () => {
		rowsByTable.set(currentLifecycleArtifacts, [{ dag: DSO_DAG }]);
		// Rows arrive newest-first (the query orders by updatedAt desc):
		// revenue's newest row FAILS → hole, no silent fall-back to the older
		// accepted one; accounts_receivable's newest is accepted.
		rowsByTable.set(sqlSnippets, [
			{ standardField: "revenue", sql: "SELECT 1 AS value", failureCount: 2 },
			{ standardField: "revenue", sql: "SELECT 2 AS value", failureCount: 0 },
			{
				standardField: "accounts_receivable",
				sql: "SELECT 3 AS value",
				failureCount: 0,
			},
		]);
		const parts = await resolveMetricDrillSteps("dso");
		if ("missing" in parts) throw new Error(parts.missing);
		const byId = new Map(parts.steps.map((s) => [s.stepId, s]));
		expect(byId.get("accounts_receivable")).toMatchObject({
			kind: "extract",
			sql: "SELECT 3 AS value",
		});
		expect(byId.get("revenue")).toMatchObject({ kind: "extract", sql: null });
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

	it("reports a missing metric definition", async () => {
		rowsByTable.set(currentLifecycleArtifacts, []);
		expect(await resolveMetricDrillSteps("nope")).toEqual({
			missing: "no metric definition found for 'nope'",
		});
	});

	it("reports an unparseable definition as missing", async () => {
		rowsByTable.set(currentLifecycleArtifacts, [{ dag: { dependencies: {} } }]);
		expect(await resolveMetricDrillSteps("empty")).toEqual({
			missing: "no metric definition found for 'empty'",
		});
	});
});

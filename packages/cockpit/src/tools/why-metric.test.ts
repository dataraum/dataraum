// Unit tests for the why_metric projection (DAT-466). Pure — no DB; the live
// read path is covered by the operating_model integration smoke.
//
// What this guards: the found discriminant is the DECLARED artifact (NOT the
// cross-session-durable snippets), the per-step evidence labels + digest-
// sanitized SQL, grounded_against rendering, and the MAX_STEPS bound (rule 15).

import { describe, expect, it, vi } from "vitest";

// Importing the tool pulls config.ts + the Postgres metadata client; mock both
// (with the `#/` alias — relative specifiers silently don't intercept) so the
// pure projection runs with no env and no connection.
vi.mock("#/config", () => ({ config: {} }));
vi.mock("#/db/metadata/client", () => ({ metadataDb: {} }));

import {
	type MetricSnippetRow,
	projectWhyMetric,
	type WhyMetricArtifactRow,
} from "./why-metric";

const executedArtifact: WhyMetricArtifactRow = {
	state: "executed",
	stateReason: null,
	strictness: 0.8,
	groundedAgainst: { detect: "run-7", typing: "run-3" },
};

const extractSnippet: MetricSnippetRow = {
	snippetId: "snip-extract",
	snippetType: "extract",
	standardField: "revenue",
	statement: "income_statement",
	aggregation: "sum",
	normalizedExpression: null,
	sql: "SELECT sum(amount) FROM lake.typed.income WHERE field = 'revenue'",
	description: "Total revenue",
	executionCount: 4,
	failureCount: 0,
};

const formulaSnippet: MetricSnippetRow = {
	snippetId: "snip-formula",
	snippetType: "formula",
	standardField: null,
	statement: null,
	aggregation: null,
	normalizedExpression: "revenue - cost_of_goods_sold",
	sql: "SELECT revenue - cogs AS result",
	description: "",
	executionCount: 4,
	failureCount: 1,
};

describe("projectWhyMetric (DAT-466)", () => {
	it("assembles the executed drill-down: state + grounding + per-step evidence", () => {
		const projected = projectWhyMetric(
			"gross_margin",
			executedArtifact,
			[extractSnippet, formulaSnippet],
			2,
		);

		expect(projected).toEqual({
			graph_id: "gross_margin",
			found: true,
			state: "executed",
			state_reason: null,
			strictness: 0.8,
			grounded_against: JSON.stringify({ detect: "run-7", typing: "run-3" }),
			snippet_count: 2,
			steps: [
				{
					snippet_id: "snip-extract",
					type: "extract",
					label: "revenue",
					sql: "SELECT sum(amount) FROM lake.typed.income WHERE field = 'revenue'",
					description: "Total revenue",
					execution_count: 4,
					failure_count: 0,
				},
				{
					snippet_id: "snip-formula",
					type: "formula",
					label: "revenue - cost_of_goods_sold",
					sql: "SELECT revenue - cogs AS result",
					// empty description coalesces to null
					description: null,
					execution_count: 4,
					failure_count: 1,
				},
			],
			pending_teaches: 2,
		});
	});

	it("found=false for an unknown metric — snippets do NOT make it found", () => {
		// A metric not declared in THIS run but with durable snippets from another
		// session: found is the artifact, not the (cross-session) snippets.
		const projected = projectWhyMetric("nope", null, [extractSnippet], 0);

		expect(projected.found).toBe(false);
		expect(projected.state).toBeNull();
		expect(projected.state_reason).toBeNull();
		expect(projected.grounded_against).toBe("");
		// The snippets still surface as evidence (the widget gates on `found`).
		expect(projected.snippet_count).toBe(1);
	});

	it("an ungroundable metric is found, with the reason verbatim and no steps", () => {
		const projected = projectWhyMetric(
			"dso",
			{
				state: "declared",
				stateReason: "ungroundable: required field mappings missing",
				strictness: null,
				groundedAgainst: null,
			},
			[],
			0,
		);

		expect(projected.found).toBe(true);
		expect(projected.state).toBe("declared");
		expect(projected.state_reason).toBe(
			"ungroundable: required field mappings missing",
		);
		// Never grounded → empty grounding render; no steps.
		expect(projected.grounded_against).toBe("");
		expect(projected.steps).toEqual([]);
		expect(projected.snippet_count).toBe(0);
	});

	it("labels a step by type when neither field nor expression is present", () => {
		const constantSnippet: MetricSnippetRow = {
			snippetId: "snip-const",
			snippetType: "constant",
			standardField: null,
			statement: null,
			aggregation: null,
			normalizedExpression: null,
			sql: "SELECT 365",
			description: null,
			executionCount: 1,
			failureCount: 0,
		};
		const projected = projectWhyMetric(
			"x",
			executedArtifact,
			[constantSnippet],
			0,
		);
		expect(projected.steps[0].label).toBe("(constant)");
	});

	it("renders narrow names in SQL, description, and labels; stays digest-free (DAT-639)", () => {
		const dirty: MetricSnippetRow = {
			snippetId: "snip-dirty",
			snippetType: "extract",
			standardField: `revenue`,
			statement: null,
			aggregation: "sum",
			normalizedExpression: null,
			sql: `SELECT sum(x) FROM lake.typed.income`,
			description: `pulls revenue`,
			executionCount: 0,
			failureCount: 0,
		};
		const projected = projectWhyMetric(
			"m",
			{
				state: "executed",
				stateReason: `measured income`,
				strictness: null,
				groundedAgainst: { detect: "run-9" },
			},
			[dirty],
			0,
		);

		expect(JSON.stringify(projected)).not.toMatch(/src_[0-9a-f]{40}/);
		expect(projected.state_reason).toBe("measured income");
		expect(projected.steps[0].label).toBe("revenue");
		expect(projected.steps[0].sql).toBe("SELECT sum(x) FROM lake.typed.income");
		expect(projected.steps[0].description).toBe("pulls revenue");
		expect(projected.grounded_against).toBe(
			JSON.stringify({ detect: "run-9" }),
		);
	});

	it("bounds the steps rendered to MAX_STEPS (rule 15)", () => {
		const many: MetricSnippetRow[] = Array.from({ length: 60 }, (_, i) => ({
			...extractSnippet,
			standardField: `field_${i}`,
		}));
		const projected = projectWhyMetric("big", executedArtifact, many, 0);
		// Capped at 40 rendered, but the true count is reported.
		expect(projected.steps).toHaveLength(40);
		expect(projected.snippet_count).toBe(60);
	});
});

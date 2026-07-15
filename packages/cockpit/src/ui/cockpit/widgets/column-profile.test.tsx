// @vitest-environment jsdom
//
// Render tests for the ColumnProfileWidget (DAT-475). Plain Mantine layout (no
// virtualization) → blocks render under jsdom. Asserts the populated-block
// rendering (semantic / stats / type candidates / quality / temporal / derived),
// the not-found state, and the unprofiled (every-block-null) state. The live
// reads are smoke-covered; the JSONB parse + caps are unit-tested in look-profile.test.ts.

import { MantineProvider } from "@mantine/core";
import { cleanup, render, screen } from "@testing-library/react";
import { afterEach, describe, expect, it } from "vitest";

import type { LookProfileResult } from "#/tools/look-profile";
import { ColumnProfileWidget } from "#/ui/cockpit/widgets/column-profile";
import { theme } from "#/ui/theme";

function renderWidget(profile: LookProfileResult) {
	render(
		<MantineProvider theme={theme} env="test">
			<ColumnProfileWidget state={{ kind: "column-profile", profile }} />
		</MantineProvider>,
	);
}

const EMPTY: LookProfileResult = {
	found: true,
	column_id: "c_amount",
	column_name: "amount",
	table_name: "orders",
	resolved_type: "DECIMAL(18,2)",
	semantic: null,
	stats: null,
	type_candidates: [],
	type_decision: null,
	quality: null,
	temporal: null,
	derived: [],
};

const full: LookProfileResult = {
	...EMPTY,
	semantic: {
		meaning: "Total order amount",
		semantic_role: "measure",
		business_name: "Order Amount",
		entity_type: null,
		temporal_behavior: null,
		unit_source_column: "currency",
	},
	stats: {
		total_count: 100,
		null_count: 5,
		distinct_count: 80,
		null_ratio: 0.05,
		cardinality_ratio: 0.8,
		is_unique: false,
		is_numeric: true,
		numeric_stats: {
			min_value: 0,
			max_value: 1000,
			mean: 42.5,
			stddev: 10.1,
			skewness: 0.2,
			kurtosis: 3.1,
			cv: 0.24,
			mad: 8,
			robust_cv: 0.2,
			percentiles: { p50: 40 },
		},
		string_stats: null,
		histogram: [{ bucket_min: 0, bucket_max: 100, count: 30 }],
		top_values: [{ value: "USD", count: 50, percentage: 0.5 }],
	},
	type_candidates: [
		{
			data_type: "DECIMAL",
			confidence: 0.9,
			parse_success_rate: 0.99,
			detected_pattern: null,
			pattern_match_rate: null,
			detected_unit: "USD",
			unit_confidence: 0.8,
			quarantine_rate: 0.01,
		},
	],
	type_decision: {
		decided_type: "DECIMAL(18,2)",
		decision_source: "detector",
		decision_reason: "high parse rate",
		previous_type: "VARCHAR",
	},
	quality: {
		has_outliers: true,
		iqr_outlier_ratio: 0.02,
		zscore_outlier_ratio: 0.01,
		benford_compliant: false,
		benford: {
			chi_square: 12.3,
			p_value: 0.04,
			is_compliant: false,
			interpretation: "non-compliant",
		},
		outlier_samples: [999, 1000],
	},
	temporal: {
		min_timestamp: "2020-01-01T00:00:00.000Z",
		max_timestamp: "2021-12-31T00:00:00.000Z",
		span_days: 730,
		granularity: "day",
		granularity_confidence: 0.9,
		completeness: 0.97,
		expected_periods: 731,
		actual_periods: 709,
		gap_count: 3,
		largest_gap_days: 12,
		gaps: [],
		is_stale: false,
	},
	derived: [
		{ derivation_type: "arithmetic", formula: "qty * price", match_rate: 0.98 },
	],
};

describe("ColumnProfileWidget (DAT-475)", () => {
	afterEach(() => cleanup());

	it("renders the not-found state for an unknown column", () => {
		renderWidget({ ...EMPTY, found: false });
		expect(screen.getByTestId("canvas-column-profile-notfound")).toBeTruthy();
	});

	it("renders the unprofiled note when every block is null/empty", () => {
		renderWidget(EMPTY);
		expect(screen.getByTestId("canvas-column-profile-unprofiled")).toBeTruthy();
		// Header (name + display table) still shows.
		expect(screen.getByText("amount")).toBeTruthy();
	});

	it("renders every populated block", () => {
		renderWidget(full);
		expect(screen.getByTestId("canvas-column-profile")).toBeTruthy();
		expect(screen.getByTestId("canvas-column-profile-semantic")).toBeTruthy();
		expect(screen.getByTestId("canvas-column-profile-stats")).toBeTruthy();
		expect(screen.getByTestId("canvas-column-profile-numeric")).toBeTruthy();
		expect(screen.getByTestId("canvas-column-profile-topvalues")).toBeTruthy();
		expect(screen.getByTestId("canvas-column-profile-histogram")).toBeTruthy();
		expect(screen.getByTestId("canvas-column-profile-candidates")).toBeTruthy();
		expect(screen.getByTestId("canvas-column-profile-decision")).toBeTruthy();
		expect(screen.getByTestId("canvas-column-profile-quality")).toBeTruthy();
		expect(screen.getByTestId("canvas-column-profile-temporal")).toBeTruthy();
		expect(screen.getByTestId("canvas-column-profile-derived")).toBeTruthy();
		// The unprofiled note is gone when blocks are present.
		expect(screen.queryByTestId("canvas-column-profile-unprofiled")).toBeNull();
		// A few load-bearing values surface. "non-compliant" shows twice (the
		// Benford verdict field + its interpretation line) — assert on count.
		expect(screen.getByText("Total order amount")).toBeTruthy();
		expect(screen.getAllByText("non-compliant").length).toBeGreaterThan(0);
		expect(screen.getByText("qty * price")).toBeTruthy();
		// The histogram bucket renders its edges + count (0–100 · 30). The badge
		// holds several text fragments, so assert on the block's text content.
		expect(
			screen.getByTestId("canvas-column-profile-histogram").textContent,
		).toContain("0–100");
		expect(
			screen.getByTestId("canvas-column-profile-histogram").textContent,
		).toContain("30");
	});
});

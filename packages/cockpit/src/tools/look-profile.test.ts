// Unit tests for look_profile's pure row→shape projection (DAT-475). No DB — the
// Drizzle joins are smoke-covered; here we pin the JSONB parsing (numeric/string
// stats, histogram, top_values, Benford, outlier samples), the null-degradation
// of an absent block, the not-found shell, the caps (top_values ≤10, outlier
// samples ≤10), the confidence-desc candidate sort + `failed_examples` omission,
// the 0/1-int → bool coercion, the timestamp ISO serialization, and the
// display-form table_name (no `src_<digest>__`).

import { describe, expect, it, vi } from "vitest";

// Importing the tool transitively pulls config.ts + the Postgres metadata client.
// Mock both so this pure-helper test needs no env and opens no connection — and,
// per registry.test.ts, set NO process.env (which would leak across files in a
// reused worker and un-skip the gated integration tests).
vi.mock("#/config", () => ({ config: {} }));
vi.mock("#/db/metadata/client", () => ({ metadataDb: {} }));

import {
	type ColumnProfileRows,
	projectColumnProfile,
	type StatsRow,
	type TypeCandidateRow,
} from "./look-profile";

const EMPTY_ROWS: ColumnProfileRows = {
	semantic: null,
	stats: null,
	typeCandidates: [],
	typeDecision: null,
	quality: null,
	temporal: null,
	derived: [],
};

function statsRow(profileData: unknown): StatsRow {
	return {
		totalCount: 100,
		nullCount: 5,
		distinctCount: 80,
		nullRatio: 0.05,
		cardinalityRatio: 0.8,
		isUnique: 0,
		isNumeric: 1,
		profileData,
	};
}

function candidate(
	overrides: Partial<TypeCandidateRow> = {},
): TypeCandidateRow {
	return {
		dataType: "INTEGER",
		confidence: 0.5,
		parseSuccessRate: 0.99,
		detectedPattern: null,
		patternMatchRate: null,
		detectedUnit: null,
		unitConfidence: null,
		quarantineRate: 0.01,
		...overrides,
	};
}

describe("projectColumnProfile — identity + not-found (DAT-475)", () => {
	it("renders the narrow table_name; column_id stays the round-trip key (DAT-639)", () => {
		const out = projectColumnProfile(
			"c_1",
			"amount",
			`orders`,
			"DECIMAL(18,2)",
			EMPTY_ROWS,
		);
		expect(out.found).toBe(true);
		expect(out.column_id).toBe("c_1");
		expect(out.column_name).toBe("amount");
		expect(out.table_name).toBe("orders");
		expect(out.resolved_type).toBe("DECIMAL(18,2)");
		// The digest appears NOWHERE in the projection.
		expect(JSON.stringify(out)).not.toMatch(/src_[0-9a-f]{40}/);
	});

	it("degrades every block to null/empty when no stage has a promoted row", () => {
		const out = projectColumnProfile(
			"c_1",
			"amount",
			"orders",
			null,
			EMPTY_ROWS,
		);
		expect(out.semantic).toBeNull();
		expect(out.stats).toBeNull();
		expect(out.type_candidates).toEqual([]);
		expect(out.type_decision).toBeNull();
		expect(out.quality).toBeNull();
		expect(out.temporal).toBeNull();
		expect(out.derived).toEqual([]);
	});
});

describe("projectColumnProfile — semantic + type decision", () => {
	it("projects the semantic annotation", () => {
		const out = projectColumnProfile("c_1", "amount", "orders", null, {
			...EMPTY_ROWS,
			semantic: {
				meaning: "Total order amount",
				semanticRole: "measure",
				businessName: "Order Amount",
				entityType: null,
				temporalBehavior: null,
				unitSourceColumn: "currency",
			},
		});
		expect(out.semantic).toEqual({
			meaning: "Total order amount",
			semantic_role: "measure",
			business_name: "Order Amount",
			entity_type: null,
			temporal_behavior: null,
			unit_source_column: "currency",
		});
	});

	it("projects the type decision", () => {
		const out = projectColumnProfile("c_1", "amount", "orders", null, {
			...EMPTY_ROWS,
			typeDecision: {
				decidedType: "DECIMAL(18,2)",
				decisionSource: "detector",
				decisionReason: "high parse rate",
				previousType: "VARCHAR",
			},
		});
		expect(out.type_decision).toEqual({
			decided_type: "DECIMAL(18,2)",
			decision_source: "detector",
			decision_reason: "high parse rate",
			previous_type: "VARCHAR",
		});
	});
});

describe("projectColumnProfile — stats JSONB parse + caps", () => {
	it("parses numeric/string stats, histogram, and top_values; coerces 0/1 flags", () => {
		const out = projectColumnProfile("c_1", "amount", "orders", null, {
			...EMPTY_ROWS,
			stats: statsRow({
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
					percentiles: { p50: 40, p90: 200 },
				},
				string_stats: { min_length: 1, max_length: 12, avg_length: 6.5 },
				histogram: [{ bucket_min: 0, bucket_max: 100, count: 30 }],
				top_values: [{ value: "USD", count: 50, percentage: 0.5 }],
			}),
		});
		expect(out.stats?.total_count).toBe(100);
		expect(out.stats?.is_unique).toBe(false);
		expect(out.stats?.is_numeric).toBe(true);
		expect(out.stats?.numeric_stats?.mean).toBe(42.5);
		expect(out.stats?.numeric_stats?.percentiles).toEqual({
			p50: 40,
			p90: 200,
		});
		expect(out.stats?.string_stats?.avg_length).toBe(6.5);
		expect(out.stats?.histogram).toEqual([
			{ bucket_min: 0, bucket_max: 100, count: 30 },
		]);
		expect(out.stats?.top_values).toEqual([
			{ value: "USD", count: 50, percentage: 0.5 },
		]);
	});

	it("keeps the whole stats block when a percentile leaf is null", () => {
		// The engine wraps each percentile in `_finite_or_none` (profiler.py) → a
		// degenerate/NaN column persists `{"p01": null, "p50": 40, ...}`. A
		// non-nullable percentile value would fail the whole ProfileData parse and
		// SILENTLY drop numeric_stats + string_stats + histogram + top_values.
		const out = projectColumnProfile("c_1", "amount", "orders", null, {
			...EMPTY_ROWS,
			stats: statsRow({
				numeric_stats: {
					min_value: 40,
					max_value: 40,
					mean: 40,
					stddev: 0,
					skewness: null,
					kurtosis: null,
					cv: null,
					mad: 0,
					robust_cv: null,
					percentiles: { p01: null, p25: 40, p50: 40, p75: 40, p99: null },
				},
				string_stats: { min_length: 2, max_length: 2, avg_length: 2 },
				histogram: [{ bucket_min: 0, bucket_max: 100, count: 30 }],
				top_values: [{ value: 40, count: 100, percentage: 1 }],
			}),
		});
		// numeric_stats survives with the null leaves preserved.
		expect(out.stats?.numeric_stats?.mean).toBe(40);
		expect(out.stats?.numeric_stats?.percentiles).toEqual({
			p01: null,
			p25: 40,
			p50: 40,
			p75: 40,
			p99: null,
		});
		// The sibling sub-blocks are NOT dropped.
		expect(out.stats?.string_stats?.avg_length).toBe(2);
		expect(out.stats?.histogram).toEqual([
			{ bucket_min: 0, bucket_max: 100, count: 30 },
		]);
		expect(out.stats?.top_values).toEqual([
			{ value: 40, count: 100, percentage: 1 },
		]);
	});

	it("keeps the stats block when a histogram bucket edge is categorical (string)", () => {
		// The engine types bucket_min/bucket_max `float | str` (models.py) — a
		// string bucket must not be rejected and nuke the whole stats block.
		const out = projectColumnProfile("c_1", "category", "orders", null, {
			...EMPTY_ROWS,
			stats: statsRow({
				histogram: [
					{ bucket_min: "A", bucket_max: "A", count: 12 },
					{ bucket_min: "B", bucket_max: "B", count: 8 },
				],
				top_values: [{ value: "A", count: 12, percentage: 0.6 }],
			}),
		});
		expect(out.stats?.histogram).toEqual([
			{ bucket_min: "A", bucket_max: "A", count: 12 },
			{ bucket_min: "B", bucket_max: "B", count: 8 },
		]);
		expect(out.stats?.top_values).toEqual([
			{ value: "A", count: 12, percentage: 0.6 },
		]);
	});

	it("caps top_values at 10", () => {
		const many = Array.from({ length: 25 }, (_, i) => ({
			value: `v${i}`,
			count: 25 - i,
			percentage: (25 - i) / 100,
		}));
		const out = projectColumnProfile("c_1", "amount", "orders", null, {
			...EMPTY_ROWS,
			stats: statsRow({ top_values: many }),
		});
		expect(out.stats?.top_values).toHaveLength(10);
		expect(out.stats?.top_values[0]?.value).toBe("v0");
	});

	it("degrades a malformed profile_data blob to scalar-only stats (no throw)", () => {
		const out = projectColumnProfile("c_1", "amount", "orders", null, {
			...EMPTY_ROWS,
			stats: statsRow("garbage-not-an-object"),
		});
		// The scalar columns still come through; the JSONB-derived blocks degrade.
		expect(out.stats?.total_count).toBe(100);
		expect(out.stats?.numeric_stats).toBeNull();
		expect(out.stats?.string_stats).toBeNull();
		expect(out.stats?.histogram).toEqual([]);
		expect(out.stats?.top_values).toEqual([]);
	});

	it("handles an absent numeric_stats sub-block (string-only column)", () => {
		const out = projectColumnProfile("c_1", "name", "orders", null, {
			...EMPTY_ROWS,
			stats: statsRow({
				string_stats: { min_length: 2, max_length: 40, avg_length: 12 },
			}),
		});
		expect(out.stats?.numeric_stats).toBeNull();
		expect(out.stats?.string_stats?.max_length).toBe(40);
		expect(out.stats?.histogram).toEqual([]);
		expect(out.stats?.top_values).toEqual([]);
	});
});

describe("projectColumnProfile — type candidates", () => {
	it("preserves the DB confidence-desc order and omits failed_examples", () => {
		// Rows arrive already confidence-desc ordered from the DB
		// (loadTypeCandidates `orderBy(desc(confidence))`); the projection trusts
		// that order and does NOT re-sort.
		const out = projectColumnProfile("c_1", "amount", "orders", null, {
			...EMPTY_ROWS,
			typeCandidates: [
				candidate({ dataType: "DECIMAL", confidence: 0.9 }),
				candidate({ dataType: "INTEGER", confidence: 0.6 }),
				candidate({ dataType: "VARCHAR", confidence: 0.3 }),
			],
		});
		expect(out.type_candidates.map((c) => c.data_type)).toEqual([
			"DECIMAL",
			"INTEGER",
			"VARCHAR",
		]);
		// `failed_examples` is omitted — never present on the projected shape.
		expect(out.type_candidates[0]).not.toHaveProperty("failed_examples");
	});
});

describe("projectColumnProfile — quality JSONB parse + caps", () => {
	it("parses Benford + outlier samples and coerces 0/1 flags", () => {
		const out = projectColumnProfile("c_1", "amount", "orders", null, {
			...EMPTY_ROWS,
			quality: {
				hasOutliers: 1,
				iqrOutlierRatio: 0.02,
				zscoreOutlierRatio: 0.01,
				benfordCompliant: 0,
				benfordStatus: "violating",
				qualityData: {
					benford_analysis: {
						status: "violating",
						chi_square: 12.3,
						p_value: 0.04,
						interpretation: "non-compliant",
					},
					outlier_detection: { outlier_samples: [999, 1000, 1001] },
				},
			},
		});
		expect(out.quality?.has_outliers).toBe(true);
		expect(out.quality?.benford_compliant).toBe(false);
		expect(out.quality?.benford).toEqual({
			status: "violating",
			chi_square: 12.3,
			p_value: 0.04,
			interpretation: "non-compliant",
		});
		expect(out.quality?.outlier_samples).toEqual([999, 1000, 1001]);
	});

	it("renders a not-applicable Benford as its typed status, not compliant/violating (DAT-853)", () => {
		// Benford is mathematically undefined for a column whose values span under
		// ~one order of magnitude. The engine sets benford_compliant NULL and
		// benford_status='not_applicable'; the reader must surface the status so it
		// reads "not applicable", never a bare "—" (which conflates with not-computed)
		// and never compliant/violating.
		const out = projectColumnProfile("c_1", "flag", "orders", null, {
			...EMPTY_ROWS,
			quality: {
				hasOutliers: 0,
				iqrOutlierRatio: 0,
				zscoreOutlierRatio: 0,
				benfordCompliant: null,
				benfordStatus: "not_applicable",
				qualityData: {
					benford_analysis: {
						status: "not_applicable",
						chi_square: null,
						p_value: null,
						interpretation: "Values span under one order of magnitude.",
					},
				},
			},
		});
		expect(out.quality?.benford_compliant).toBeNull();
		expect(out.quality?.benford?.status).toBe("not_applicable");
		expect(out.quality?.benford?.chi_square).toBeNull();
	});

	it("caps outlier_samples at 10", () => {
		const samples = Array.from({ length: 30 }, (_, i) => i);
		const out = projectColumnProfile("c_1", "amount", "orders", null, {
			...EMPTY_ROWS,
			quality: {
				hasOutliers: 1,
				iqrOutlierRatio: null,
				zscoreOutlierRatio: null,
				benfordCompliant: null,
				benfordStatus: null,
				qualityData: { outlier_detection: { outlier_samples: samples } },
			},
		});
		expect(out.quality?.outlier_samples).toHaveLength(10);
	});

	it("degrades a malformed quality_data blob to scalar-only quality (no throw)", () => {
		const out = projectColumnProfile("c_1", "amount", "orders", null, {
			...EMPTY_ROWS,
			quality: {
				hasOutliers: 0,
				iqrOutlierRatio: 0,
				zscoreOutlierRatio: 0,
				benfordCompliant: 1,
				benfordStatus: "compliant",
				qualityData: 42,
			},
		});
		expect(out.quality?.has_outliers).toBe(false);
		expect(out.quality?.benford).toBeNull();
		expect(out.quality?.outlier_samples).toEqual([]);
	});
});

describe("projectColumnProfile — temporal + derived", () => {
	it("serializes timestamps + coverage facts and caps gaps", () => {
		const min = new Date("2020-01-01T00:00:00.000Z");
		const max = new Date("2021-12-31T00:00:00.000Z");
		// More gaps than MAX_SAMPLE (10) — the projection must cap the surface.
		const gaps = Array.from({ length: 25 }, (_, i) => ({
			gap_start: "2020-06-01T00:00:00",
			gap_end: "2020-06-05T00:00:00",
			gap_length_days: 4 + i,
			missing_periods: 3,
			severity: "moderate",
		}));
		const out = projectColumnProfile("c_1", "order_date", "orders", null, {
			...EMPTY_ROWS,
			temporal: {
				minTimestamp: min,
				maxTimestamp: max,
				spanDays: 730,
				detectedGranularity: "day",
				granularityConfidence: 0.9,
				completenessRatio: 0.97,
				expectedPeriods: 731,
				actualPeriods: 709,
				gapCount: 25,
				largestGapDays: 28,
				gaps,
				isStale: false,
			},
		});
		expect(out.temporal?.min_timestamp).toBe("2020-01-01T00:00:00.000Z");
		expect(out.temporal?.max_timestamp).toBe("2021-12-31T00:00:00.000Z");
		expect(out.temporal?.span_days).toBe(730);
		expect(out.temporal?.granularity).toBe("day");
		expect(out.temporal?.granularity_confidence).toBe(0.9);
		expect(out.temporal?.completeness).toBe(0.97);
		expect(out.temporal?.expected_periods).toBe(731);
		expect(out.temporal?.actual_periods).toBe(709);
		expect(out.temporal?.gap_count).toBe(25);
		expect(out.temporal?.largest_gap_days).toBe(28);
		// gap_count stays the TRUE count; the gaps sample is bounded.
		expect(out.temporal?.gaps.length).toBe(10);
		expect(out.temporal?.gaps[0]?.severity).toBe("moderate");
		expect(out.temporal?.is_stale).toBe(false);
	});

	it("tolerates a null timestamp and a non-array gaps blob", () => {
		const out = projectColumnProfile("c_1", "order_date", "orders", null, {
			...EMPTY_ROWS,
			temporal: {
				minTimestamp: null,
				maxTimestamp: null,
				spanDays: null,
				detectedGranularity: null,
				granularityConfidence: null,
				completenessRatio: null,
				expectedPeriods: null,
				actualPeriods: null,
				gapCount: null,
				largestGapDays: null,
				gaps: null,
				isStale: null,
			},
		});
		expect(out.temporal?.min_timestamp).toBeNull();
		expect(out.temporal?.max_timestamp).toBeNull();
		expect(out.temporal?.gaps).toEqual([]);
	});

	it("projects derived columns (formula + match rate)", () => {
		const out = projectColumnProfile("c_1", "total", "orders", null, {
			...EMPTY_ROWS,
			derived: [
				{
					derivationType: "arithmetic",
					formula: "qty * price",
					matchRate: 0.98,
				},
			],
		});
		expect(out.derived).toEqual([
			{
				derivation_type: "arithmetic",
				formula: "qty * price",
				match_rate: 0.98,
			},
		]);
	});
});

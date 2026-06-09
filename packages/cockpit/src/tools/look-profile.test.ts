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
	it("strips the digest prefix to a display table_name; column_id stays the round-trip key", () => {
		const DIGEST = "204bc8e118543a6c35654c1f68c43539a2e226f2";
		const out = projectColumnProfile(
			"c_1",
			"amount",
			`src_${DIGEST}__orders`,
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
				businessConcept: "revenue",
				semanticRole: "measure",
				businessName: "Order Amount",
				entityType: null,
				temporalBehavior: null,
				unitSourceColumn: "currency",
			},
		});
		expect(out.semantic).toEqual({
			business_concept: "revenue",
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
	it("sorts by confidence desc and omits failed_examples", () => {
		const out = projectColumnProfile("c_1", "amount", "orders", null, {
			...EMPTY_ROWS,
			typeCandidates: [
				candidate({ dataType: "VARCHAR", confidence: 0.3 }),
				candidate({ dataType: "DECIMAL", confidence: 0.9 }),
				candidate({ dataType: "INTEGER", confidence: 0.6 }),
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

	it("sinks a null-confidence candidate to the bottom", () => {
		const out = projectColumnProfile("c_1", "amount", "orders", null, {
			...EMPTY_ROWS,
			typeCandidates: [
				candidate({ dataType: "UNKNOWN", confidence: null }),
				candidate({ dataType: "DECIMAL", confidence: 0.9 }),
			],
		});
		expect(out.type_candidates.map((c) => c.data_type)).toEqual([
			"DECIMAL",
			"UNKNOWN",
		]);
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
				qualityData: {
					benford_analysis: {
						chi_square: 12.3,
						p_value: 0.04,
						is_compliant: false,
						interpretation: "non-compliant",
					},
					outlier_detection: { outlier_samples: [999, 1000, 1001] },
				},
			},
		});
		expect(out.quality?.has_outliers).toBe(true);
		expect(out.quality?.benford_compliant).toBe(false);
		expect(out.quality?.benford).toEqual({
			chi_square: 12.3,
			p_value: 0.04,
			is_compliant: false,
			interpretation: "non-compliant",
		});
		expect(out.quality?.outlier_samples).toEqual([999, 1000, 1001]);
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
				qualityData: 42,
			},
		});
		expect(out.quality?.has_outliers).toBe(false);
		expect(out.quality?.benford).toBeNull();
		expect(out.quality?.outlier_samples).toEqual([]);
	});
});

describe("projectColumnProfile — temporal + derived", () => {
	it("serializes timestamps to ISO strings", () => {
		const min = new Date("2020-01-01T00:00:00.000Z");
		const max = new Date("2021-12-31T00:00:00.000Z");
		const out = projectColumnProfile("c_1", "order_date", "orders", null, {
			...EMPTY_ROWS,
			temporal: {
				minTimestamp: min,
				maxTimestamp: max,
				detectedGranularity: "day",
				completenessRatio: 0.97,
				hasSeasonality: true,
				hasTrend: false,
				isStale: false,
			},
		});
		expect(out.temporal?.min_timestamp).toBe("2020-01-01T00:00:00.000Z");
		expect(out.temporal?.max_timestamp).toBe("2021-12-31T00:00:00.000Z");
		expect(out.temporal?.granularity).toBe("day");
		expect(out.temporal?.completeness).toBe(0.97);
		expect(out.temporal?.has_seasonality).toBe(true);
	});

	it("tolerates a null timestamp", () => {
		const out = projectColumnProfile("c_1", "order_date", "orders", null, {
			...EMPTY_ROWS,
			temporal: {
				minTimestamp: null,
				maxTimestamp: null,
				detectedGranularity: null,
				completenessRatio: null,
				hasSeasonality: null,
				hasTrend: null,
				isStale: null,
			},
		});
		expect(out.temporal?.min_timestamp).toBeNull();
		expect(out.temporal?.max_timestamp).toBeNull();
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

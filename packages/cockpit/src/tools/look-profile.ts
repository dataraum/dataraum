// look_profile tool (DAT-475) — one column's heavy descriptive profile.
//
// The cockpit analog of the MCP `look(target="table.column")`: the per-column
// deep dive that aggregates every promoted per-column artifact the engine
// persists — semantic annotation, the statistical profile (numeric / string /
// histogram / top values), the type candidates + decision, the statistical
// quality metrics (outliers + Benford), the temporal profile, and any derived
// columns that point AT this column. Each block reads from a `current_*` view,
// so it reflects the latest promoted run — the view's head-join lives in the
// database (ADR-0008/DAT-453); no run plumbing here.
//
// Read-only → no approval. A column with no promoted artifact for a given stage
// gets a null/empty block for it; an unknown column_id returns the empty shell
// with `found:false` (the look_table not-found signal — NOT the `{error}`
// envelope). The heavy JSONB blobs (`profileData`, `qualityData`) are parsed
// LENIENTLY (zod `safeParse`): a malformed/absent blob degrades to null/empty,
// never throws. The DB joins are smoke-covered; the pure `projectColumnProfile`
// is unit-tested directly here.

import { toolDefinition } from "@tanstack/ai";
import { desc, eq } from "drizzle-orm";
import { z } from "zod";

import { metadataDb } from "../db/metadata/client";
import {
	columns,
	currentColumnConcepts,
	currentDerivedColumns,
	currentSemanticAnnotations,
	currentStatisticalProfiles,
	currentStatisticalQualityMetrics,
	currentTemporalColumnProfiles,
	currentTypeCandidates,
	currentTypeDecisions,
	tables,
} from "../db/metadata/schema";
import { displayTableName } from "../lib/display-names";

// How many entries of an unbounded sample list to surface — caps keep the heavy
// profile from dumping an arbitrarily long list into the agent's context.
const MAX_SAMPLE = 10;

// --- The lenient JSONB grammars. Each `safeParse`d at the projection so a
// malformed/absent blob degrades to null/empty rather than throwing.

const NumericStats = z.object({
	min_value: z.number().nullable().optional(),
	max_value: z.number().nullable().optional(),
	mean: z.number().nullable().optional(),
	stddev: z.number().nullable().optional(),
	skewness: z.number().nullable().optional(),
	kurtosis: z.number().nullable().optional(),
	cv: z.number().nullable().optional(),
	mad: z.number().nullable().optional(),
	robust_cv: z.number().nullable().optional(),
	// Percentile VALUES are nullable: the engine wraps each in `_finite_or_none`
	// (profiler.py) → null on a NaN/Inf/degenerate column. A non-nullable value
	// would fail the whole ProfileData parse and silently drop the entire stats
	// block (numeric/string/histogram/top_values all empty).
	percentiles: z
		.record(z.string(), z.number().nullable())
		.nullable()
		.optional(),
});

const StringStats = z.object({
	min_length: z.number().nullable().optional(),
	max_length: z.number().nullable().optional(),
	avg_length: z.number().nullable().optional(),
});

const HistogramBucket = z.object({
	// The engine types these `float | str` (HistogramBucket, models.py) — numeric
	// buckets carry edges, categorical buckets carry the category label. A
	// number-only schema would reject a string bucket and silently drop the whole
	// stats block, exactly like the nullable-percentile bug.
	bucket_min: z.union([z.number(), z.string()]).nullable().optional(),
	bucket_max: z.union([z.number(), z.string()]).nullable().optional(),
	count: z.number().nullable().optional(),
});

const TopValue = z.object({
	value: z.unknown(),
	count: z.number().nullable().optional(),
	percentage: z.number().nullable().optional(),
});

// The whole `statistical_profiles.profile_data` blob — every block optional so a
// numeric-only or string-only column parses cleanly.
const ProfileData = z.object({
	numeric_stats: NumericStats.nullable().optional(),
	string_stats: StringStats.nullable().optional(),
	histogram: z.array(HistogramBucket).nullable().optional(),
	top_values: z.array(TopValue).nullable().optional(),
});

const BenfordAnalysis = z.object({
	chi_square: z.number().nullable().optional(),
	p_value: z.number().nullable().optional(),
	is_compliant: z.boolean().nullable().optional(),
	interpretation: z.string().nullable().optional(),
});

const OutlierSample = z.unknown();

// The `statistical_quality_metrics.quality_data` blob — the Benford analysis +
// outlier samples live under their detector sub-objects.
const QualityData = z.object({
	benford_analysis: BenfordAnalysis.nullable().optional(),
	outlier_detection: z
		.object({ outlier_samples: z.array(OutlierSample).nullable().optional() })
		.nullable()
		.optional(),
});

// --- The tool output shape.

const Semantic = z.object({
	business_concept: z.string().nullable(),
	semantic_role: z.string().nullable(),
	business_name: z.string().nullable(),
	entity_type: z.string().nullable(),
	temporal_behavior: z.string().nullable(),
	unit_source_column: z.string().nullable(),
});

const Stats = z.object({
	total_count: z.number().nullable(),
	null_count: z.number().nullable(),
	distinct_count: z.number().nullable(),
	null_ratio: z.number().nullable(),
	cardinality_ratio: z.number().nullable(),
	is_unique: z.boolean().nullable(),
	is_numeric: z.boolean().nullable(),
	numeric_stats: z
		.object({
			min_value: z.number().nullable(),
			max_value: z.number().nullable(),
			mean: z.number().nullable(),
			stddev: z.number().nullable(),
			skewness: z.number().nullable(),
			kurtosis: z.number().nullable(),
			cv: z.number().nullable(),
			mad: z.number().nullable(),
			robust_cv: z.number().nullable(),
			// Values nullable to match the input grammar — a null percentile leaf
			// must survive @tanstack/ai outputSchema validation, not blow up.
			percentiles: z.record(z.string(), z.number().nullable()).nullable(),
		})
		.nullable(),
	string_stats: z
		.object({
			min_length: z.number().nullable(),
			max_length: z.number().nullable(),
			avg_length: z.number().nullable(),
		})
		.nullable(),
	histogram: z.array(
		z.object({
			// number-or-string to match the input grammar (categorical buckets).
			bucket_min: z.union([z.number(), z.string()]).nullable(),
			bucket_max: z.union([z.number(), z.string()]).nullable(),
			count: z.number().nullable(),
		}),
	),
	top_values: z.array(
		z.object({
			value: z.unknown(),
			count: z.number().nullable(),
			percentage: z.number().nullable(),
		}),
	),
});
export type ProfileStats = z.infer<typeof Stats>;

const TypeCandidate = z.object({
	data_type: z.string().nullable(),
	confidence: z.number().nullable(),
	parse_success_rate: z.number().nullable(),
	detected_pattern: z.string().nullable(),
	pattern_match_rate: z.number().nullable(),
	detected_unit: z.string().nullable(),
	unit_confidence: z.number().nullable(),
	quarantine_rate: z.number().nullable(),
});
export type ProfileTypeCandidate = z.infer<typeof TypeCandidate>;

const TypeDecision = z.object({
	decided_type: z.string().nullable(),
	decision_source: z.string().nullable(),
	decision_reason: z.string().nullable(),
	previous_type: z.string().nullable(),
});

const Quality = z.object({
	has_outliers: z.boolean().nullable(),
	iqr_outlier_ratio: z.number().nullable(),
	zscore_outlier_ratio: z.number().nullable(),
	benford_compliant: z.boolean().nullable(),
	benford: z
		.object({
			chi_square: z.number().nullable(),
			p_value: z.number().nullable(),
			is_compliant: z.boolean().nullable(),
			interpretation: z.string().nullable(),
		})
		.nullable(),
	outlier_samples: z.array(z.unknown()),
});
export type ProfileQuality = z.infer<typeof Quality>;

const Temporal = z.object({
	// ISO strings — the engine persists these as timestamps; serialized at the edge.
	min_timestamp: z.string().nullable(),
	max_timestamp: z.string().nullable(),
	granularity: z.string().nullable(),
	completeness: z.number().nullable(),
	is_stale: z.boolean().nullable(),
});

const Derived = z.object({
	derivation_type: z.string().nullable(),
	formula: z.string().nullable(),
	match_rate: z.number().nullable(),
});
export type ProfileDerived = z.infer<typeof Derived>;

const LookProfileResult = z.object({
	// False when column_id matched no column — return the empty shell (the
	// look_table not-found signal, NOT the `{error}` envelope).
	found: z.boolean(),
	column_id: z.string(),
	column_name: z.string(),
	// Display form (`src_<digest>__` prefix stripped, DAT-433) — for prose. The
	// round-trip key stays column_id; never surface the raw physical name here.
	table_name: z.string(),
	resolved_type: z.string().nullable(),
	semantic: Semantic.nullable(),
	stats: Stats.nullable(),
	type_candidates: z.array(TypeCandidate),
	type_decision: TypeDecision.nullable(),
	quality: Quality.nullable(),
	temporal: Temporal.nullable(),
	derived: z.array(Derived),
});
export type LookProfileResult = z.infer<typeof LookProfileResult>;

// --- The raw Drizzle row shapes (one per `current_*` read).

export interface SemanticRow {
	businessConcept: string | null;
	semanticRole: string | null;
	businessName: string | null;
	entityType: string | null;
	temporalBehavior: string | null;
	unitSourceColumn: string | null;
}

export interface StatsRow {
	totalCount: number | null;
	nullCount: number | null;
	distinctCount: number | null;
	nullRatio: number | null;
	cardinalityRatio: number | null;
	isUnique: number | null;
	isNumeric: number | null;
	profileData: unknown;
}

export interface TypeCandidateRow {
	dataType: string | null;
	confidence: number | null;
	parseSuccessRate: number | null;
	detectedPattern: string | null;
	patternMatchRate: number | null;
	detectedUnit: string | null;
	unitConfidence: number | null;
	quarantineRate: number | null;
}

export interface TypeDecisionRow {
	decidedType: string | null;
	decisionSource: string | null;
	decisionReason: string | null;
	previousType: string | null;
}

export interface QualityRow {
	hasOutliers: number | null;
	iqrOutlierRatio: number | null;
	zscoreOutlierRatio: number | null;
	benfordCompliant: number | null;
	qualityData: unknown;
}

export interface TemporalRow {
	minTimestamp: Date | string | null;
	maxTimestamp: Date | string | null;
	detectedGranularity: string | null;
	completenessRatio: number | null;
	isStale: boolean | null;
}

export interface DerivedRow {
	derivationType: string | null;
	formula: string | null;
	matchRate: number | null;
}

/** The seven per-column reads gathered for one column (any may be absent). */
export interface ColumnProfileRows {
	semantic: SemanticRow | null;
	stats: StatsRow | null;
	typeCandidates: TypeCandidateRow[];
	typeDecision: TypeDecisionRow | null;
	quality: QualityRow | null;
	temporal: TemporalRow | null;
	derived: DerivedRow[];
}

/** Coerce the engine's 0/1 integer flags (DuckDB has no bool in these views) to
 * a tri-state boolean; null stays null. */
function intToBool(v: number | null | undefined): boolean | null {
	return v === null || v === undefined ? null : v !== 0;
}

/** Serialize a timestamp value (Drizzle hands back a Date for `timestamp`
 * columns; a raw string can slip through) to an ISO string; null stays null. */
function toIso(v: Date | string | null | undefined): string | null {
	if (v === null || v === undefined) return null;
	if (v instanceof Date) return v.toISOString();
	const d = new Date(v);
	return Number.isNaN(d.getTime()) ? String(v) : d.toISOString();
}

function projectSemantic(
	row: SemanticRow | null,
): z.infer<typeof Semantic> | null {
	if (!row) return null;
	return {
		business_concept: row.businessConcept ?? null,
		semantic_role: row.semanticRole ?? null,
		business_name: row.businessName ?? null,
		entity_type: row.entityType ?? null,
		temporal_behavior: row.temporalBehavior ?? null,
		unit_source_column: row.unitSourceColumn ?? null,
	};
}

function projectStats(row: StatsRow | null): ProfileStats | null {
	if (!row) return null;
	const parsed = ProfileData.safeParse(row.profileData);
	const data = parsed.success ? parsed.data : {};

	const n = data.numeric_stats;
	const s = data.string_stats;
	const histogram = Array.isArray(data.histogram) ? data.histogram : [];
	const top = Array.isArray(data.top_values) ? data.top_values : [];

	return {
		total_count: row.totalCount ?? null,
		null_count: row.nullCount ?? null,
		distinct_count: row.distinctCount ?? null,
		null_ratio: row.nullRatio ?? null,
		cardinality_ratio: row.cardinalityRatio ?? null,
		is_unique: intToBool(row.isUnique),
		is_numeric: intToBool(row.isNumeric),
		numeric_stats: n
			? {
					min_value: n.min_value ?? null,
					max_value: n.max_value ?? null,
					mean: n.mean ?? null,
					stddev: n.stddev ?? null,
					skewness: n.skewness ?? null,
					kurtosis: n.kurtosis ?? null,
					cv: n.cv ?? null,
					mad: n.mad ?? null,
					robust_cv: n.robust_cv ?? null,
					percentiles: n.percentiles ?? null,
				}
			: null,
		string_stats: s
			? {
					min_length: s.min_length ?? null,
					max_length: s.max_length ?? null,
					avg_length: s.avg_length ?? null,
				}
			: null,
		histogram: histogram.map((b) => ({
			bucket_min: b.bucket_min ?? null,
			bucket_max: b.bucket_max ?? null,
			count: b.count ?? null,
		})),
		// Cap the top values at MAX_SAMPLE — an unbounded distinct-value list would
		// flood the agent's context.
		top_values: top.slice(0, MAX_SAMPLE).map((v) => ({
			value: v.value ?? null,
			count: v.count ?? null,
			percentage: v.percentage ?? null,
		})),
	};
}

function projectTypeCandidates(
	rows: TypeCandidateRow[],
): ProfileTypeCandidate[] {
	// Rows arrive already confidence-desc ordered from the DB (`orderBy(desc(confidence))`
	// in loadTypeCandidates) — trust that order, don't re-sort. `failed_examples` is
	// deliberately omitted — it's noisy raw input the agent doesn't need.
	return rows.map((r) => ({
		data_type: r.dataType ?? null,
		confidence: r.confidence ?? null,
		parse_success_rate: r.parseSuccessRate ?? null,
		detected_pattern: r.detectedPattern ?? null,
		pattern_match_rate: r.patternMatchRate ?? null,
		detected_unit: r.detectedUnit ?? null,
		unit_confidence: r.unitConfidence ?? null,
		quarantine_rate: r.quarantineRate ?? null,
	}));
}

function projectTypeDecision(
	row: TypeDecisionRow | null,
): z.infer<typeof TypeDecision> | null {
	if (!row) return null;
	return {
		decided_type: row.decidedType ?? null,
		decision_source: row.decisionSource ?? null,
		decision_reason: row.decisionReason ?? null,
		previous_type: row.previousType ?? null,
	};
}

function projectQuality(row: QualityRow | null): ProfileQuality | null {
	if (!row) return null;
	const parsed = QualityData.safeParse(row.qualityData);
	const data = parsed.success ? parsed.data : {};
	const benford = data.benford_analysis;
	const samples = Array.isArray(data.outlier_detection?.outlier_samples)
		? data.outlier_detection.outlier_samples
		: [];
	return {
		has_outliers: intToBool(row.hasOutliers),
		iqr_outlier_ratio: row.iqrOutlierRatio ?? null,
		zscore_outlier_ratio: row.zscoreOutlierRatio ?? null,
		benford_compliant: intToBool(row.benfordCompliant),
		benford: benford
			? {
					chi_square: benford.chi_square ?? null,
					p_value: benford.p_value ?? null,
					is_compliant: benford.is_compliant ?? null,
					interpretation: benford.interpretation ?? null,
				}
			: null,
		// Cap the outlier samples at MAX_SAMPLE.
		outlier_samples: samples.slice(0, MAX_SAMPLE),
	};
}

function projectTemporal(
	row: TemporalRow | null,
): z.infer<typeof Temporal> | null {
	if (!row) return null;
	return {
		min_timestamp: toIso(row.minTimestamp),
		max_timestamp: toIso(row.maxTimestamp),
		granularity: row.detectedGranularity ?? null,
		completeness: row.completenessRatio ?? null,
		is_stale: row.isStale ?? null,
	};
}

function projectDerived(rows: DerivedRow[]): ProfileDerived[] {
	return rows.map((r) => ({
		derivation_type: r.derivationType ?? null,
		formula: r.formula ?? null,
		match_rate: r.matchRate ?? null,
	}));
}

/**
 * Assemble the full profile from the resolved pieces. Pure (no DB) so the
 * JSONB-parse + null-degradation + cap logic is unit-testable without a live
 * schema. Every block degrades to null/empty when its stage has no promoted row;
 * a malformed JSONB blob degrades to null/empty rather than throwing. `rawTableName`
 * is the physical name — stripped to display form (DAT-433); the round-trip key
 * stays column_id.
 */
export function projectColumnProfile(
	columnId: string,
	columnName: string,
	rawTableName: string | null,
	resolvedType: string | null,
	rows: ColumnProfileRows,
): LookProfileResult {
	return {
		found: true,
		column_id: columnId,
		column_name: columnName,
		table_name: rawTableName === null ? "" : displayTableName(rawTableName),
		resolved_type: resolvedType,
		semantic: projectSemantic(rows.semantic),
		stats: projectStats(rows.stats),
		type_candidates: projectTypeCandidates(rows.typeCandidates),
		type_decision: projectTypeDecision(rows.typeDecision),
		quality: projectQuality(rows.quality),
		temporal: projectTemporal(rows.temporal),
		derived: projectDerived(rows.derived),
	};
}

/** The empty not-found shell for an unknown column_id (NOT the `{error}`
 * envelope — the look_table `found:false` signal). */
function notFound(columnId: string): LookProfileResult {
	return {
		found: false,
		column_id: columnId,
		column_name: "",
		table_name: "",
		resolved_type: null,
		semantic: null,
		stats: null,
		type_candidates: [],
		type_decision: null,
		quality: null,
		temporal: null,
		derived: [],
	};
}

/** The heavy per-column descriptive profile: every promoted per-column artifact
 * for one column, aggregated. */
export async function lookProfile(input: {
	column_id: string;
}): Promise<LookProfileResult> {
	// Resolve the column + its table first (mirror look_table's identity read) —
	// even an unprofiled column has a name and a resolved type.
	const [col] = await metadataDb
		.select({
			columnId: columns.columnId,
			columnName: columns.columnName,
			resolvedType: columns.resolvedType,
			tableName: tables.tableName,
		})
		.from(columns)
		.innerJoin(tables, eq(tables.tableId, columns.tableId))
		.where(eq(columns.columnId, input.column_id))
		.limit(1);

	if (!col) {
		// Unknown column id — return the empty shell, not an error, so the agent can
		// say "no such column" cleanly rather than surfacing a tool failure.
		return notFound(input.column_id);
	}

	// The seven per-column reads share no input once the column is known — fan
	// them out. Each `current_*` view IS the promoted run (the head join lives in
	// the database, ADR-0008/DAT-453), so a plain by-columnId read suffices; a
	// stage with no promoted row simply returns nothing → a null/empty block.
	const [
		semantic,
		stats,
		typeCandidates,
		typeDecision,
		quality,
		temporal,
		derived,
	] = await Promise.all([
		loadSemantic(input.column_id),
		loadStats(input.column_id),
		loadTypeCandidates(input.column_id),
		loadTypeDecision(input.column_id),
		loadQuality(input.column_id),
		loadTemporal(input.column_id),
		loadDerived(input.column_id),
	]);

	return projectColumnProfile(
		col.columnId ?? input.column_id,
		col.columnName ?? "",
		col.tableName ?? null,
		col.resolvedType ?? null,
		{
			semantic,
			stats,
			typeCandidates,
			typeDecision,
			quality,
			temporal,
			derived,
		},
	);
}

async function loadSemantic(columnId: string): Promise<SemanticRow | null> {
	const [row] = await metadataDb
		.select({
			businessConcept: currentColumnConcepts.businessConcept,
			semanticRole: currentSemanticAnnotations.semanticRole,
			businessName: currentSemanticAnnotations.businessName,
			entityType: currentSemanticAnnotations.entityType,
			temporalBehavior: currentColumnConcepts.temporalBehavior,
			unitSourceColumn: currentColumnConcepts.unitSourceColumn,
		})
		.from(currentSemanticAnnotations)
		.leftJoin(
			currentColumnConcepts,
			eq(currentColumnConcepts.columnId, currentSemanticAnnotations.columnId),
		)
		.where(eq(currentSemanticAnnotations.columnId, columnId))
		.limit(1);
	return row ?? null;
}

async function loadStats(columnId: string): Promise<StatsRow | null> {
	const [row] = await metadataDb
		.select({
			totalCount: currentStatisticalProfiles.totalCount,
			nullCount: currentStatisticalProfiles.nullCount,
			distinctCount: currentStatisticalProfiles.distinctCount,
			nullRatio: currentStatisticalProfiles.nullRatio,
			cardinalityRatio: currentStatisticalProfiles.cardinalityRatio,
			isUnique: currentStatisticalProfiles.isUnique,
			isNumeric: currentStatisticalProfiles.isNumeric,
			profileData: currentStatisticalProfiles.profileData,
		})
		.from(currentStatisticalProfiles)
		.where(eq(currentStatisticalProfiles.columnId, columnId))
		.limit(1);
	return row ?? null;
}

async function loadTypeCandidates(
	columnId: string,
): Promise<TypeCandidateRow[]> {
	// `.limit(MAX_SAMPLE)` caps at the query level (defense-in-depth, like
	// top_values/outlier_samples) so a pathological column can't dump an unbounded
	// candidate list into context; `.orderBy(desc(confidence))` keeps the cap meaningful.
	return metadataDb
		.select({
			dataType: currentTypeCandidates.dataType,
			confidence: currentTypeCandidates.confidence,
			parseSuccessRate: currentTypeCandidates.parseSuccessRate,
			detectedPattern: currentTypeCandidates.detectedPattern,
			patternMatchRate: currentTypeCandidates.patternMatchRate,
			detectedUnit: currentTypeCandidates.detectedUnit,
			unitConfidence: currentTypeCandidates.unitConfidence,
			quarantineRate: currentTypeCandidates.quarantineRate,
		})
		.from(currentTypeCandidates)
		.where(eq(currentTypeCandidates.columnId, columnId))
		.orderBy(desc(currentTypeCandidates.confidence))
		.limit(MAX_SAMPLE);
}

async function loadTypeDecision(
	columnId: string,
): Promise<TypeDecisionRow | null> {
	const [row] = await metadataDb
		.select({
			decidedType: currentTypeDecisions.decidedType,
			decisionSource: currentTypeDecisions.decisionSource,
			decisionReason: currentTypeDecisions.decisionReason,
			previousType: currentTypeDecisions.previousType,
		})
		.from(currentTypeDecisions)
		.where(eq(currentTypeDecisions.columnId, columnId))
		.limit(1);
	return row ?? null;
}

async function loadQuality(columnId: string): Promise<QualityRow | null> {
	const [row] = await metadataDb
		.select({
			hasOutliers: currentStatisticalQualityMetrics.hasOutliers,
			iqrOutlierRatio: currentStatisticalQualityMetrics.iqrOutlierRatio,
			zscoreOutlierRatio: currentStatisticalQualityMetrics.zscoreOutlierRatio,
			benfordCompliant: currentStatisticalQualityMetrics.benfordCompliant,
			qualityData: currentStatisticalQualityMetrics.qualityData,
		})
		.from(currentStatisticalQualityMetrics)
		.where(eq(currentStatisticalQualityMetrics.columnId, columnId))
		.limit(1);
	return row ?? null;
}

async function loadTemporal(columnId: string): Promise<TemporalRow | null> {
	const [row] = await metadataDb
		.select({
			minTimestamp: currentTemporalColumnProfiles.minTimestamp,
			maxTimestamp: currentTemporalColumnProfiles.maxTimestamp,
			detectedGranularity: currentTemporalColumnProfiles.detectedGranularity,
			completenessRatio: currentTemporalColumnProfiles.completenessRatio,
			isStale: currentTemporalColumnProfiles.isStale,
		})
		.from(currentTemporalColumnProfiles)
		.where(eq(currentTemporalColumnProfiles.columnId, columnId))
		.limit(1);
	return row ?? null;
}

async function loadDerived(columnId: string): Promise<DerivedRow[]> {
	// `derived_column_id` is the column this derived column IS — i.e. the rows
	// where THIS column was synthesized from others (formula + match rate).
	//
	// `current_derived_columns` is SESSION-grain (head-joined on `session:{id}`),
	// so across multiple sessions the same derived column yields several rows.
	// Order by `computed_at` desc so the read is DETERMINISTIC (latest first, not
	// arbitrary), and cap at MAX_SAMPLE so it stays bounded. (Cross-lane guard —
	// DAT-476/477/478 share this session-grain class.)
	return metadataDb
		.select({
			derivationType: currentDerivedColumns.derivationType,
			formula: currentDerivedColumns.formula,
			matchRate: currentDerivedColumns.matchRate,
		})
		.from(currentDerivedColumns)
		.where(eq(currentDerivedColumns.derivedColumnId, columnId))
		.orderBy(desc(currentDerivedColumns.computedAt))
		.limit(MAX_SAMPLE);
}

export const lookProfileTool = toolDefinition({
	name: "look_profile",
	description:
		"Show ONE column's full descriptive profile — its semantic annotation, " +
		"statistical profile (counts, null/cardinality ratios, numeric/string stats, " +
		"histogram, top values), type candidates + the type decision, statistical " +
		"quality (outliers + Benford), the temporal profile (if it's a time column), " +
		"and any derived-column formula targeting it. Read-only; reflects the latest " +
		"promoted analysis. Identify the column by its column_id (from look_table). " +
		"A block is null/empty when that stage hasn't run for the column. Use " +
		"look_table for the at-a-glance readiness grid and why_column to EXPLAIN a " +
		"band; look_profile is the raw per-column descriptive deep-dive.",
	inputSchema: z.object({
		column_id: z
			.string()
			.describe("The column to profile (a column_id from look_table)."),
	}),
	outputSchema: LookProfileResult,
}).server((input) => lookProfile(input));

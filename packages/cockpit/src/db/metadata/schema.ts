import { sql } from "drizzle-orm";
import {
	boolean,
	date,
	doublePrecision,
	integer,
	json,
	jsonb,
	pgSchema,
	text,
	timestamp,
	varchar,
} from "drizzle-orm/pg-core";

export const metadataSchema = pgSchema(
	"ws_00000000_0000_0000_0000_000000000001_read",
);

export const columns = metadataSchema
	.view("columns", {
		columnId: varchar("column_id"),
		tableId: varchar("table_id"),
		columnName: varchar("column_name"),
		originalName: varchar("original_name"),
		columnPosition: integer("column_position"),
		rawType: varchar("raw_type"),
		resolvedType: varchar("resolved_type"),
	})
	.as(
		sql`SELECT column_id, table_id, column_name, original_name, column_position, raw_type, resolved_type FROM ws_00000000_0000_0000_0000_000000000001.columns`,
	);

export const configOverlay = metadataSchema
	.view("config_overlay", {
		overlayId: varchar("overlay_id"),
		sessionId: varchar("session_id"),
		type: varchar(),
		payload: json(),
		createdAt: timestamp("created_at"),
		supersededAt: timestamp("superseded_at"),
	})
	.as(
		sql`SELECT overlay_id, session_id, type, payload, created_at, superseded_at FROM ws_00000000_0000_0000_0000_000000000001.config_overlay`,
	);

export const currentClaimWitnesses = metadataSchema
	.view("current_claim_witnesses", {
		claimWitnessId: varchar("claim_witness_id"),
		sessionId: varchar("session_id"),
		tableId: varchar("table_id"),
		columnId: varchar("column_id"),
		runId: varchar("run_id"),
		target: varchar(),
		claimField: varchar("claim_field"),
		witnessId: varchar("witness_id"),
		distribution: jsonb(),
		reliability: doublePrecision(),
		detectorId: varchar("detector_id"),
		computedAt: timestamp("computed_at"),
		viaTableHead: boolean("via_table_head"),
		viaSessionHead: boolean("via_session_head"),
		viaOperatingModelHead: boolean("via_operating_model_head"),
	})
	.as(
		sql`SELECT claim_witness_id, session_id, table_id, column_id, run_id, target, claim_field, witness_id, distribution, reliability, detector_id, computed_at, (EXISTS ( SELECT 1 FROM ws_00000000_0000_0000_0000_000000000001.metadata_snapshot_head h WHERE h.stage::text = 'detect'::text AND h.run_id::text = r.run_id::text AND h.target::text = ('table:'::text || r.table_id::text))) AS via_table_head, (EXISTS ( SELECT 1 FROM ws_00000000_0000_0000_0000_000000000001.metadata_snapshot_head h WHERE h.stage::text = 'detect'::text AND h.run_id::text = r.run_id::text AND h.target::text = ('session:'::text || r.session_id::text))) AS via_session_head, (EXISTS ( SELECT 1 FROM ws_00000000_0000_0000_0000_000000000001.metadata_snapshot_head h WHERE h.stage::text = 'operating_model'::text AND h.run_id::text = r.run_id::text AND h.target::text = ('session:'::text || r.session_id::text))) AS via_operating_model_head FROM ws_00000000_0000_0000_0000_000000000001.claim_witnesses r WHERE (EXISTS ( SELECT 1 FROM ws_00000000_0000_0000_0000_000000000001.metadata_snapshot_head h WHERE h.run_id::text = r.run_id::text AND (h.stage::text = 'detect'::text AND (h.target::text = ('table:'::text || r.table_id::text) OR h.target::text = ('session:'::text || r.session_id::text)) OR h.stage::text = 'operating_model'::text AND h.target::text = ('session:'::text || r.session_id::text))))`,
	);

export const currentColumnDriftSummaries = metadataSchema
	.view("current_column_drift_summaries", {
		id: varchar(),
		sessionId: varchar("session_id"),
		runId: varchar("run_id"),
		sliceTableName: varchar("slice_table_name", { length: 255 }),
		columnName: varchar("column_name", { length: 255 }),
		timeColumn: varchar("time_column", { length: 255 }),
		maxJsDivergence: doublePrecision("max_js_divergence"),
		meanJsDivergence: doublePrecision("mean_js_divergence"),
		driftDivergence: doublePrecision("drift_divergence"),
		periodsAnalyzed: integer("periods_analyzed"),
		periodsWithDrift: integer("periods_with_drift"),
		driftEvidenceJson: json("drift_evidence_json"),
		createdAt: timestamp("created_at"),
	})
	.as(
		sql`SELECT id, session_id, run_id, slice_table_name, column_name, time_column, max_js_divergence, mean_js_divergence, drift_divergence, periods_analyzed, periods_with_drift, drift_evidence_json, created_at FROM ws_00000000_0000_0000_0000_000000000001.column_drift_summaries r WHERE (EXISTS ( SELECT 1 FROM ws_00000000_0000_0000_0000_000000000001.metadata_snapshot_head h WHERE h.target::text = ('session:'::text || r.session_id::text) AND h.stage::text = 'detect'::text AND h.run_id::text = r.run_id::text))`,
	);

export const currentColumnEligibility = metadataSchema
	.view("current_column_eligibility", {
		eligibilityId: varchar("eligibility_id", { length: 36 }),
		sessionId: varchar("session_id"),
		columnId: varchar("column_id", { length: 36 }),
		tableId: varchar("table_id"),
		sourceId: varchar("source_id"),
		runId: varchar("run_id"),
		columnName: varchar("column_name"),
		tableName: varchar("table_name"),
		resolvedType: varchar("resolved_type"),
		status: varchar({ length: 20 }),
		triggeredRule: varchar("triggered_rule", { length: 50 }),
		reason: text(),
		metricsSnapshot: json("metrics_snapshot"),
		configVersion: varchar("config_version", { length: 20 }),
		evaluatedAt: timestamp("evaluated_at"),
	})
	.as(
		sql`SELECT eligibility_id, session_id, column_id, table_id, source_id, run_id, column_name, table_name, resolved_type, status, triggered_rule, reason, metrics_snapshot, config_version, evaluated_at FROM ws_00000000_0000_0000_0000_000000000001.column_eligibility r WHERE (EXISTS ( SELECT 1 FROM ws_00000000_0000_0000_0000_000000000001.metadata_snapshot_head h WHERE h.target::text = ('table:'::text || r.table_id::text) AND h.stage::text = 'column_eligibility'::text AND h.run_id::text = r.run_id::text))`,
	);

export const currentDerivedColumns = metadataSchema
	.view("current_derived_columns", {
		derivedId: varchar("derived_id"),
		sessionId: varchar("session_id"),
		runId: varchar("run_id"),
		tableId: varchar("table_id"),
		derivedColumnId: varchar("derived_column_id"),
		sourceColumnIds: json("source_column_ids"),
		derivationType: varchar("derivation_type"),
		formula: varchar(),
		matchRate: doublePrecision("match_rate"),
		computedAt: timestamp("computed_at"),
		totalRows: integer("total_rows"),
		matchingRows: integer("matching_rows"),
		mismatchExamples: json("mismatch_examples"),
	})
	.as(
		sql`SELECT derived_id, session_id, run_id, table_id, derived_column_id, source_column_ids, derivation_type, formula, match_rate, computed_at, total_rows, matching_rows, mismatch_examples FROM ws_00000000_0000_0000_0000_000000000001.derived_columns r WHERE (EXISTS ( SELECT 1 FROM ws_00000000_0000_0000_0000_000000000001.metadata_snapshot_head h WHERE h.target::text = ('session:'::text || r.session_id::text) AND h.stage::text = 'detect'::text AND h.run_id::text = r.run_id::text))`,
	);

export const currentDetectedBusinessCycles = metadataSchema
	.view("current_detected_business_cycles", {
		cycleId: varchar("cycle_id"),
		sessionId: varchar("session_id"),
		runId: varchar("run_id"),
		cycleName: varchar("cycle_name"),
		cycleType: varchar("cycle_type"),
		canonicalType: varchar("canonical_type"),
		isKnownType: boolean("is_known_type"),
		description: text(),
		businessValue: varchar("business_value"),
		confidence: doublePrecision(),
		tablesInvolved: json("tables_involved"),
		stages: json(),
		entityFlows: json("entity_flows"),
		statusTable: varchar("status_table"),
		statusColumn: varchar("status_column"),
		completionValue: varchar("completion_value"),
		totalRecords: integer("total_records"),
		completedCycles: integer("completed_cycles"),
		completionRate: doublePrecision("completion_rate"),
		evidence: json(),
		detectedAt: timestamp("detected_at"),
	})
	.as(
		sql`SELECT cycle_id, session_id, run_id, cycle_name, cycle_type, canonical_type, is_known_type, description, business_value, confidence, tables_involved, stages, entity_flows, status_table, status_column, completion_value, total_records, completed_cycles, completion_rate, evidence, detected_at FROM ws_00000000_0000_0000_0000_000000000001.detected_business_cycles r WHERE (EXISTS ( SELECT 1 FROM ws_00000000_0000_0000_0000_000000000001.metadata_snapshot_head h WHERE h.target::text = ('session:'::text || r.session_id::text) AND h.stage::text = 'operating_model'::text AND h.run_id::text = r.run_id::text))`,
	);

export const currentEnrichedViews = metadataSchema
	.view("current_enriched_views", {
		viewId: varchar("view_id"),
		sessionId: varchar("session_id"),
		factTableId: varchar("fact_table_id"),
		viewTableId: varchar("view_table_id"),
		viewName: varchar("view_name"),
		runId: varchar("run_id"),
		relationshipIds: json("relationship_ids"),
		dimensionTableIds: json("dimension_table_ids"),
		dimensionColumns: json("dimension_columns"),
		isGrainVerified: boolean("is_grain_verified"),
		evidence: json(),
		createdAt: timestamp("created_at"),
	})
	.as(
		sql`SELECT view_id, session_id, fact_table_id, view_table_id, view_name, run_id, relationship_ids, dimension_table_ids, dimension_columns, is_grain_verified, evidence, created_at FROM ws_00000000_0000_0000_0000_000000000001.enriched_views r WHERE (EXISTS ( SELECT 1 FROM ws_00000000_0000_0000_0000_000000000001.metadata_snapshot_head h WHERE h.target::text = ('session:'::text || r.session_id::text) AND h.stage::text = 'detect'::text AND h.run_id::text = r.run_id::text))`,
	);

export const currentEntropyObjects = metadataSchema
	.view("current_entropy_objects", {
		objectId: varchar("object_id"),
		sessionId: varchar("session_id"),
		layer: varchar(),
		dimension: varchar(),
		subDimension: varchar("sub_dimension"),
		target: varchar(),
		tableId: varchar("table_id"),
		columnId: varchar("column_id"),
		runId: varchar("run_id"),
		score: doublePrecision(),
		evidence: jsonb(),
		detectorId: varchar("detector_id"),
		sourceAnalysisIds: jsonb("source_analysis_ids"),
		computedAt: timestamp("computed_at"),
		viaTableHead: boolean("via_table_head"),
		viaSessionHead: boolean("via_session_head"),
		viaOperatingModelHead: boolean("via_operating_model_head"),
	})
	.as(
		sql`SELECT object_id, session_id, layer, dimension, sub_dimension, target, table_id, column_id, run_id, score, evidence, detector_id, source_analysis_ids, computed_at, (EXISTS ( SELECT 1 FROM ws_00000000_0000_0000_0000_000000000001.metadata_snapshot_head h WHERE h.stage::text = 'detect'::text AND h.run_id::text = r.run_id::text AND h.target::text = ('table:'::text || r.table_id::text))) AS via_table_head, (EXISTS ( SELECT 1 FROM ws_00000000_0000_0000_0000_000000000001.metadata_snapshot_head h WHERE h.stage::text = 'detect'::text AND h.run_id::text = r.run_id::text AND h.target::text = ('session:'::text || r.session_id::text))) AS via_session_head, (EXISTS ( SELECT 1 FROM ws_00000000_0000_0000_0000_000000000001.metadata_snapshot_head h WHERE h.stage::text = 'operating_model'::text AND h.run_id::text = r.run_id::text AND h.target::text = ('session:'::text || r.session_id::text))) AS via_operating_model_head FROM ws_00000000_0000_0000_0000_000000000001.entropy_objects r WHERE (EXISTS ( SELECT 1 FROM ws_00000000_0000_0000_0000_000000000001.metadata_snapshot_head h WHERE h.run_id::text = r.run_id::text AND (h.stage::text = 'detect'::text AND (h.target::text = ('table:'::text || r.table_id::text) OR h.target::text = ('session:'::text || r.session_id::text)) OR h.stage::text = 'operating_model'::text AND h.target::text = ('session:'::text || r.session_id::text))))`,
	);

export const currentEntropyReadiness = metadataSchema
	.view("current_entropy_readiness", {
		readinessId: varchar("readiness_id"),
		sessionId: varchar("session_id"),
		target: varchar(),
		tableId: varchar("table_id"),
		columnId: varchar("column_id"),
		runId: varchar("run_id"),
		band: varchar(),
		worstIntentRisk: doublePrecision("worst_intent_risk"),
		intents: jsonb(),
		topDrivers: jsonb("top_drivers"),
		computedAt: timestamp("computed_at"),
		viaTableHead: boolean("via_table_head"),
		viaSessionHead: boolean("via_session_head"),
		viaOperatingModelHead: boolean("via_operating_model_head"),
	})
	.as(
		sql`SELECT readiness_id, session_id, target, table_id, column_id, run_id, band, worst_intent_risk, intents, top_drivers, computed_at, (EXISTS ( SELECT 1 FROM ws_00000000_0000_0000_0000_000000000001.metadata_snapshot_head h WHERE h.stage::text = 'detect'::text AND h.run_id::text = r.run_id::text AND h.target::text = ('table:'::text || r.table_id::text))) AS via_table_head, (EXISTS ( SELECT 1 FROM ws_00000000_0000_0000_0000_000000000001.metadata_snapshot_head h WHERE h.stage::text = 'detect'::text AND h.run_id::text = r.run_id::text AND h.target::text = ('session:'::text || r.session_id::text))) AS via_session_head, (EXISTS ( SELECT 1 FROM ws_00000000_0000_0000_0000_000000000001.metadata_snapshot_head h WHERE h.stage::text = 'operating_model'::text AND h.run_id::text = r.run_id::text AND h.target::text = ('session:'::text || r.session_id::text))) AS via_operating_model_head FROM ws_00000000_0000_0000_0000_000000000001.entropy_readiness r WHERE (EXISTS ( SELECT 1 FROM ws_00000000_0000_0000_0000_000000000001.metadata_snapshot_head h WHERE h.run_id::text = r.run_id::text AND (h.stage::text = 'detect'::text AND (h.target::text = ('table:'::text || r.table_id::text) OR h.target::text = ('session:'::text || r.session_id::text)) OR h.stage::text = 'operating_model'::text AND h.target::text = ('session:'::text || r.session_id::text)))) AND (NOT (EXISTS ( SELECT 1 FROM ws_00000000_0000_0000_0000_000000000001.metadata_snapshot_head h3 WHERE h3.run_id::text = r.run_id::text AND h3.target::text = ('session:'::text || r.session_id::text) AND (h3.stage::text = ANY (ARRAY['detect'::character varying, 'operating_model'::character varying]::text[])))) OR NOT (EXISTS ( SELECT 1 FROM ws_00000000_0000_0000_0000_000000000001.entropy_readiness r2 JOIN ws_00000000_0000_0000_0000_000000000001.metadata_snapshot_head h2 ON h2.run_id::text = r2.run_id::text AND h2.target::text = ('session:'::text || r2.session_id::text) AND (h2.stage::text = ANY (ARRAY['detect'::character varying, 'operating_model'::character varying]::text[])) WHERE r2.session_id::text = r.session_id::text AND r2.target::text = r.target::text AND r2.run_id::text <> r.run_id::text AND h2.promoted_at > (( SELECT max(h3.promoted_at) AS max FROM ws_00000000_0000_0000_0000_000000000001.metadata_snapshot_head h3 WHERE h3.run_id::text = r.run_id::text AND h3.target::text = ('session:'::text || r.session_id::text) AND (h3.stage::text = ANY (ARRAY['detect'::character varying, 'operating_model'::character varying]::text[])))))))`,
	);

export const currentLifecycleArtifacts = metadataSchema
	.view("current_lifecycle_artifacts", {
		artifactId: varchar("artifact_id"),
		sessionId: varchar("session_id"),
		artifactType: varchar("artifact_type"),
		artifactKey: varchar("artifact_key"),
		runId: varchar("run_id"),
		state: varchar(),
		stateReason: text("state_reason"),
		stage: varchar(),
		strictness: doublePrecision(),
		groundedAgainst: json("grounded_against"),
		teaches: json(),
		createdAt: timestamp("created_at"),
		stateChangedAt: timestamp("state_changed_at"),
	})
	.as(
		sql`SELECT artifact_id, session_id, artifact_type, artifact_key, run_id, state, state_reason, stage, strictness, grounded_against, teaches, created_at, state_changed_at FROM ws_00000000_0000_0000_0000_000000000001.lifecycle_artifacts r WHERE (EXISTS ( SELECT 1 FROM ws_00000000_0000_0000_0000_000000000001.metadata_snapshot_head h WHERE h.target::text = ('session:'::text || r.session_id::text) AND h.stage::text = 'operating_model'::text AND h.run_id::text = r.run_id::text))`,
	);

export const currentMaterializationRecipes = metadataSchema
	.view("current_materialization_recipes", {
		recipeId: varchar("recipe_id"),
		sessionId: varchar("session_id"),
		tableId: varchar("table_id"),
		layer: varchar(),
		runId: varchar("run_id"),
		targetFqn: varchar("target_fqn"),
		ddl: varchar(),
		dependsOn: json("depends_on"),
		createdAt: timestamp("created_at"),
	})
	.as(
		sql`SELECT recipe_id, session_id, table_id, layer, run_id, target_fqn, ddl, depends_on, created_at FROM ws_00000000_0000_0000_0000_000000000001.materialization_recipes r WHERE (EXISTS ( SELECT 1 FROM ws_00000000_0000_0000_0000_000000000001.metadata_snapshot_head h WHERE h.target::text = ('table:'::text || r.table_id::text) AND h.stage::text = 'typing'::text AND h.run_id::text = r.run_id::text))`,
	);

export const currentMeasureAggregationLineage = metadataSchema
	.view("current_measure_aggregation_lineage", {
		lineageId: varchar("lineage_id"),
		sessionId: varchar("session_id"),
		runId: varchar("run_id"),
		measureTableId: varchar("measure_table_id"),
		measureColumnId: varchar("measure_column_id"),
		eventTableId: varchar("event_table_id"),
		sliceDimension: varchar("slice_dimension"),
		conventionSql: text("convention_sql"),
		periodGrain: varchar("period_grain"),
		pattern: varchar(),
		matchRate: doublePrecision("match_rate"),
		rFlowMedian: doublePrecision("r_flow_median"),
		rStockMedian: doublePrecision("r_stock_median"),
		nEntities: integer("n_entities"),
		nEntitiesFired: integer("n_entities_fired"),
		createdAt: timestamp("created_at", { withTimezone: true }),
	})
	.as(
		sql`SELECT lineage_id, session_id, run_id, measure_table_id, measure_column_id, event_table_id, slice_dimension, convention_sql, period_grain, pattern, match_rate, r_flow_median, r_stock_median, n_entities, n_entities_fired, created_at FROM ws_00000000_0000_0000_0000_000000000001.measure_aggregation_lineage r WHERE (EXISTS ( SELECT 1 FROM ws_00000000_0000_0000_0000_000000000001.metadata_snapshot_head h WHERE h.target::text = ('session:'::text || r.session_id::text) AND h.stage::text = 'detect'::text AND h.run_id::text = r.run_id::text))`,
	);

export const currentRelationships = metadataSchema
	.view("current_relationships", {
		relationshipId: varchar("relationship_id"),
		sessionId: varchar("session_id"),
		runId: varchar("run_id"),
		fromTableId: varchar("from_table_id"),
		fromColumnId: varchar("from_column_id"),
		toTableId: varchar("to_table_id"),
		toColumnId: varchar("to_column_id"),
		relationshipType: varchar("relationship_type"),
		cardinality: varchar(),
		confidence: doublePrecision(),
		detectionMethod: varchar("detection_method"),
		evidence: json(),
		isConfirmed: boolean("is_confirmed"),
		confirmedAt: timestamp("confirmed_at"),
		confirmedBy: varchar("confirmed_by"),
		detectedAt: timestamp("detected_at"),
	})
	.as(
		sql`SELECT relationship_id, session_id, run_id, from_table_id, from_column_id, to_table_id, to_column_id, relationship_type, cardinality, confidence, detection_method, evidence, is_confirmed, confirmed_at, confirmed_by, detected_at FROM ws_00000000_0000_0000_0000_000000000001.relationships r WHERE (EXISTS ( SELECT 1 FROM ws_00000000_0000_0000_0000_000000000001.metadata_snapshot_head h WHERE h.target::text = ('session:'::text || r.session_id::text) AND h.stage::text = 'detect'::text AND h.run_id::text = r.run_id::text))`,
	);

export const currentSemanticAnnotations = metadataSchema
	.view("current_semantic_annotations", {
		annotationId: varchar("annotation_id"),
		sessionId: varchar("session_id"),
		columnId: varchar("column_id"),
		runId: varchar("run_id"),
		semanticRole: varchar("semantic_role"),
		entityType: varchar("entity_type"),
		businessName: varchar("business_name"),
		businessDescription: text("business_description"),
		businessConcept: varchar("business_concept"),
		temporalBehavior: varchar("temporal_behavior"),
		temporalBehaviorClaim: varchar("temporal_behavior_claim"),
		temporalBehaviorClaimConfidence: doublePrecision(
			"temporal_behavior_claim_confidence",
		),
		temporalBehaviorContested: boolean("temporal_behavior_contested"),
		derivedFormulaHypothesis: varchar("derived_formula_hypothesis"),
		derivedFormulaConfidence: doublePrecision("derived_formula_confidence"),
		unitSourceColumn: varchar("unit_source_column"),
		nullTokens: json("null_tokens"),
		annotationSource: varchar("annotation_source"),
		annotatedAt: timestamp("annotated_at"),
		annotatedBy: varchar("annotated_by"),
		confidence: doublePrecision(),
	})
	.as(
		sql`SELECT annotation_id, session_id, column_id, run_id, semantic_role, entity_type, business_name, business_description, business_concept, temporal_behavior, temporal_behavior_claim, temporal_behavior_claim_confidence, temporal_behavior_contested, derived_formula_hypothesis, derived_formula_confidence, unit_source_column, null_tokens, annotation_source, annotated_at, annotated_by, confidence FROM ws_00000000_0000_0000_0000_000000000001.semantic_annotations r WHERE (EXISTS ( SELECT 1 FROM ws_00000000_0000_0000_0000_000000000001.columns c JOIN ws_00000000_0000_0000_0000_000000000001.metadata_snapshot_head h ON h.target::text = ('table:'::text || c.table_id::text) WHERE c.column_id::text = r.column_id::text AND h.stage::text = 'semantic_per_column'::text AND h.run_id::text = r.run_id::text))`,
	);

export const currentSliceDefinitions = metadataSchema
	.view("current_slice_definitions", {
		sliceId: varchar("slice_id"),
		sessionId: varchar("session_id"),
		runId: varchar("run_id"),
		tableId: varchar("table_id"),
		columnId: varchar("column_id"),
		columnName: varchar("column_name"),
		slicePriority: integer("slice_priority"),
		sliceType: varchar("slice_type"),
		distinctValues: json("distinct_values"),
		valueCount: integer("value_count"),
		reasoning: text(),
		businessContext: text("business_context"),
		confidence: doublePrecision(),
		sqlTemplate: text("sql_template"),
		detectionSource: varchar("detection_source"),
		createdAt: timestamp("created_at"),
	})
	.as(
		sql`SELECT slice_id, session_id, run_id, table_id, column_id, column_name, slice_priority, slice_type, distinct_values, value_count, reasoning, business_context, confidence, sql_template, detection_source, created_at FROM ws_00000000_0000_0000_0000_000000000001.slice_definitions r WHERE (EXISTS ( SELECT 1 FROM ws_00000000_0000_0000_0000_000000000001.metadata_snapshot_head h WHERE h.target::text = ('session:'::text || r.session_id::text) AND h.stage::text = 'detect'::text AND h.run_id::text = r.run_id::text))`,
	);

export const currentSlicingViews = metadataSchema
	.view("current_slicing_views", {
		viewId: varchar("view_id"),
		sessionId: varchar("session_id"),
		factTableId: varchar("fact_table_id"),
		viewName: varchar("view_name"),
		runId: varchar("run_id"),
		sliceDefinitionIds: json("slice_definition_ids"),
		sliceColumns: json("slice_columns"),
		isGrainVerified: boolean("is_grain_verified"),
		createdAt: timestamp("created_at"),
	})
	.as(
		sql`SELECT view_id, session_id, fact_table_id, view_name, run_id, slice_definition_ids, slice_columns, is_grain_verified, created_at FROM ws_00000000_0000_0000_0000_000000000001.slicing_views r WHERE (EXISTS ( SELECT 1 FROM ws_00000000_0000_0000_0000_000000000001.metadata_snapshot_head h WHERE h.target::text = ('session:'::text || r.session_id::text) AND h.stage::text = 'detect'::text AND h.run_id::text = r.run_id::text))`,
	);

export const currentStatisticalProfiles = metadataSchema
	.view("current_statistical_profiles", {
		profileId: varchar("profile_id"),
		sessionId: varchar("session_id"),
		columnId: varchar("column_id"),
		runId: varchar("run_id"),
		profiledAt: timestamp("profiled_at"),
		layer: varchar(),
		totalCount: integer("total_count"),
		nullCount: integer("null_count"),
		distinctCount: integer("distinct_count"),
		nullRatio: doublePrecision("null_ratio"),
		cardinalityRatio: doublePrecision("cardinality_ratio"),
		isUnique: integer("is_unique"),
		isNumeric: integer("is_numeric"),
		profileData: json("profile_data"),
	})
	.as(
		sql`SELECT profile_id, session_id, column_id, run_id, profiled_at, layer, total_count, null_count, distinct_count, null_ratio, cardinality_ratio, is_unique, is_numeric, profile_data FROM ws_00000000_0000_0000_0000_000000000001.statistical_profiles r WHERE (EXISTS ( SELECT 1 FROM ws_00000000_0000_0000_0000_000000000001.columns c JOIN ws_00000000_0000_0000_0000_000000000001.metadata_snapshot_head h ON h.target::text = ('table:'::text || c.table_id::text) WHERE c.column_id::text = r.column_id::text AND h.stage::text = 'statistics'::text AND h.run_id::text = r.run_id::text))`,
	);

export const currentStatisticalQualityMetrics = metadataSchema
	.view("current_statistical_quality_metrics", {
		metricId: varchar("metric_id"),
		sessionId: varchar("session_id"),
		columnId: varchar("column_id"),
		runId: varchar("run_id"),
		computedAt: timestamp("computed_at"),
		benfordCompliant: integer("benford_compliant"),
		hasOutliers: integer("has_outliers"),
		iqrOutlierRatio: doublePrecision("iqr_outlier_ratio"),
		zscoreOutlierRatio: doublePrecision("zscore_outlier_ratio"),
		qualityData: json("quality_data"),
	})
	.as(
		sql`SELECT metric_id, session_id, column_id, run_id, computed_at, benford_compliant, has_outliers, iqr_outlier_ratio, zscore_outlier_ratio, quality_data FROM ws_00000000_0000_0000_0000_000000000001.statistical_quality_metrics r WHERE (EXISTS ( SELECT 1 FROM ws_00000000_0000_0000_0000_000000000001.columns c JOIN ws_00000000_0000_0000_0000_000000000001.metadata_snapshot_head h ON h.target::text = ('table:'::text || c.table_id::text) WHERE c.column_id::text = r.column_id::text AND h.stage::text = 'statistical_quality'::text AND h.run_id::text = r.run_id::text))`,
	);

export const currentTableEntities = metadataSchema
	.view("current_table_entities", {
		entityId: varchar("entity_id"),
		sessionId: varchar("session_id"),
		tableId: varchar("table_id"),
		runId: varchar("run_id"),
		detectedEntityType: varchar("detected_entity_type"),
		description: text(),
		confidence: doublePrecision(),
		evidence: json(),
		grainColumns: json("grain_columns"),
		isFactTable: boolean("is_fact_table"),
		isDimensionTable: boolean("is_dimension_table"),
		timeColumn: varchar("time_column"),
		detectionSource: varchar("detection_source"),
		detectedAt: timestamp("detected_at"),
	})
	.as(
		sql`SELECT entity_id, session_id, table_id, run_id, detected_entity_type, description, confidence, evidence, grain_columns, is_fact_table, is_dimension_table, time_column, detection_source, detected_at FROM ws_00000000_0000_0000_0000_000000000001.table_entities r WHERE (EXISTS ( SELECT 1 FROM ws_00000000_0000_0000_0000_000000000001.metadata_snapshot_head h WHERE h.target::text = ('session:'::text || r.session_id::text) AND h.stage::text = 'detect'::text AND h.run_id::text = r.run_id::text))`,
	);

export const currentTemporalColumnProfiles = metadataSchema
	.view("current_temporal_column_profiles", {
		profileId: varchar("profile_id"),
		sessionId: varchar("session_id"),
		columnId: varchar("column_id"),
		runId: varchar("run_id"),
		profiledAt: timestamp("profiled_at"),
		minTimestamp: timestamp("min_timestamp"),
		maxTimestamp: timestamp("max_timestamp"),
		detectedGranularity: varchar("detected_granularity"),
		completenessRatio: doublePrecision("completeness_ratio"),
		hasSeasonality: boolean("has_seasonality"),
		hasTrend: boolean("has_trend"),
		isStale: boolean("is_stale"),
		profileData: json("profile_data"),
	})
	.as(
		sql`SELECT profile_id, session_id, column_id, run_id, profiled_at, min_timestamp, max_timestamp, detected_granularity, completeness_ratio, has_seasonality, has_trend, is_stale, profile_data FROM ws_00000000_0000_0000_0000_000000000001.temporal_column_profiles r WHERE (EXISTS ( SELECT 1 FROM ws_00000000_0000_0000_0000_000000000001.columns c JOIN ws_00000000_0000_0000_0000_000000000001.metadata_snapshot_head h ON h.target::text = ('table:'::text || c.table_id::text) WHERE c.column_id::text = r.column_id::text AND h.stage::text = 'temporal'::text AND h.run_id::text = r.run_id::text))`,
	);

export const currentTemporalSliceAnalyses = metadataSchema
	.view("current_temporal_slice_analyses", {
		id: varchar(),
		sessionId: varchar("session_id"),
		runId: varchar("run_id"),
		sliceTableName: varchar("slice_table_name", { length: 255 }),
		timeColumn: varchar("time_column", { length: 255 }),
		periodLabel: varchar("period_label", { length: 50 }),
		periodStart: date("period_start"),
		periodEnd: date("period_end"),
		rowCount: integer("row_count"),
		expectedDays: integer("expected_days"),
		observedDays: integer("observed_days"),
		coverageRatio: doublePrecision("coverage_ratio"),
		isComplete: integer("is_complete"),
		hasEarlyCutoff: integer("has_early_cutoff"),
		daysMissingAtEnd: integer("days_missing_at_end"),
		lastDayRatio: doublePrecision("last_day_ratio"),
		columnSums: json("column_sums"),
		zScore: doublePrecision("z_score"),
		rollingAvg: doublePrecision("rolling_avg"),
		rollingStd: doublePrecision("rolling_std"),
		isVolumeAnomaly: integer("is_volume_anomaly"),
		anomalyType: varchar("anomaly_type", { length: 20 }),
		periodOverPeriodChange: doublePrecision("period_over_period_change"),
		issuesJson: json("issues_json"),
		createdAt: timestamp("created_at"),
	})
	.as(
		sql`SELECT id, session_id, run_id, slice_table_name, time_column, period_label, period_start, period_end, row_count, expected_days, observed_days, coverage_ratio, is_complete, has_early_cutoff, days_missing_at_end, last_day_ratio, column_sums, z_score, rolling_avg, rolling_std, is_volume_anomaly, anomaly_type, period_over_period_change, issues_json, created_at FROM ws_00000000_0000_0000_0000_000000000001.temporal_slice_analyses r WHERE (EXISTS ( SELECT 1 FROM ws_00000000_0000_0000_0000_000000000001.metadata_snapshot_head h WHERE h.target::text = ('session:'::text || r.session_id::text) AND h.stage::text = 'detect'::text AND h.run_id::text = r.run_id::text))`,
	);

export const currentTypeCandidates = metadataSchema
	.view("current_type_candidates", {
		candidateId: varchar("candidate_id"),
		sessionId: varchar("session_id"),
		columnId: varchar("column_id"),
		runId: varchar("run_id"),
		detectedAt: timestamp("detected_at"),
		dataType: varchar("data_type"),
		confidence: doublePrecision(),
		parseSuccessRate: doublePrecision("parse_success_rate"),
		failedExamples: json("failed_examples"),
		detectedPattern: varchar("detected_pattern"),
		patternMatchRate: doublePrecision("pattern_match_rate"),
		detectedUnit: varchar("detected_unit"),
		unitConfidence: doublePrecision("unit_confidence"),
		quarantineCount: integer("quarantine_count"),
		quarantineRate: doublePrecision("quarantine_rate"),
	})
	.as(
		sql`SELECT candidate_id, session_id, column_id, run_id, detected_at, data_type, confidence, parse_success_rate, failed_examples, detected_pattern, pattern_match_rate, detected_unit, unit_confidence, quarantine_count, quarantine_rate FROM ws_00000000_0000_0000_0000_000000000001.type_candidates r WHERE (EXISTS ( SELECT 1 FROM ws_00000000_0000_0000_0000_000000000001.columns c JOIN ws_00000000_0000_0000_0000_000000000001.metadata_snapshot_head h ON h.target::text = ('table:'::text || c.table_id::text) WHERE c.column_id::text = r.column_id::text AND h.stage::text = 'typing'::text AND h.run_id::text = r.run_id::text))`,
	);

export const currentTypeDecisions = metadataSchema
	.view("current_type_decisions", {
		decisionId: varchar("decision_id"),
		sessionId: varchar("session_id"),
		columnId: varchar("column_id"),
		runId: varchar("run_id"),
		decidedType: varchar("decided_type"),
		decisionSource: varchar("decision_source"),
		decidedAt: timestamp("decided_at"),
		decidedBy: varchar("decided_by"),
		previousType: varchar("previous_type"),
		decisionReason: varchar("decision_reason"),
	})
	.as(
		sql`SELECT decision_id, session_id, column_id, run_id, decided_type, decision_source, decided_at, decided_by, previous_type, decision_reason FROM ws_00000000_0000_0000_0000_000000000001.type_decisions r WHERE (EXISTS ( SELECT 1 FROM ws_00000000_0000_0000_0000_000000000001.columns c JOIN ws_00000000_0000_0000_0000_000000000001.metadata_snapshot_head h ON h.target::text = ('table:'::text || c.table_id::text) WHERE c.column_id::text = r.column_id::text AND h.stage::text = 'typing'::text AND h.run_id::text = r.run_id::text))`,
	);

export const currentValidationResults = metadataSchema
	.view("current_validation_results", {
		resultId: varchar("result_id"),
		sessionId: varchar("session_id"),
		runId: varchar("run_id"),
		validationId: varchar("validation_id"),
		tableIds: json("table_ids"),
		columnsUsed: json("columns_used"),
		status: varchar(),
		severity: varchar(),
		passed: boolean(),
		message: text(),
		executedAt: timestamp("executed_at"),
		sqlUsed: text("sql_used"),
		details: json(),
	})
	.as(
		sql`SELECT result_id, session_id, run_id, validation_id, table_ids, columns_used, status, severity, passed, message, executed_at, sql_used, details FROM ws_00000000_0000_0000_0000_000000000001.validation_results r WHERE (EXISTS ( SELECT 1 FROM ws_00000000_0000_0000_0000_000000000001.metadata_snapshot_head h WHERE h.target::text = ('session:'::text || r.session_id::text) AND h.stage::text = 'operating_model'::text AND h.run_id::text = r.run_id::text))`,
	);

export const fixLedger = metadataSchema
	.view("fix_ledger", {
		fixId: varchar("fix_id"),
		sessionId: varchar("session_id"),
		sourceId: varchar("source_id"),
		actionName: varchar("action_name"),
		tableName: varchar("table_name"),
		columnName: varchar("column_name"),
		userInput: varchar("user_input"),
		interpretation: varchar(),
		status: varchar(),
		createdAt: timestamp("created_at"),
		supersededAt: timestamp("superseded_at"),
		supersededBy: varchar("superseded_by"),
	})
	.as(
		sql`SELECT fix_id, session_id, source_id, action_name, table_name, column_name, user_input, interpretation, status, created_at, superseded_at, superseded_by FROM ws_00000000_0000_0000_0000_000000000001.fix_ledger`,
	);

export const investigationSessions = metadataSchema
	.view("investigation_sessions", {
		sessionId: varchar("session_id"),
		status: varchar(),
		startedAt: timestamp("started_at"),
		endedAt: timestamp("ended_at"),
		durationSeconds: doublePrecision("duration_seconds"),
		intent: varchar(),
		contract: varchar(),
		vertical: varchar(),
		outcomeSummary: varchar("outcome_summary"),
		outcomePayload: json("outcome_payload"),
		stepCount: integer("step_count"),
	})
	.as(
		sql`SELECT session_id, status, started_at, ended_at, duration_seconds, intent, contract, vertical, outcome_summary, outcome_payload, step_count FROM ws_00000000_0000_0000_0000_000000000001.investigation_sessions`,
	);

export const investigationSteps = metadataSchema
	.view("investigation_steps", {
		stepId: varchar("step_id"),
		sessionId: varchar("session_id"),
		ordinal: integer(),
		toolName: varchar("tool_name"),
		arguments: json(),
		status: varchar(),
		resultSummary: varchar("result_summary"),
		error: varchar(),
		startedAt: timestamp("started_at"),
		durationSeconds: doublePrecision("duration_seconds"),
		target: varchar(),
		dimension: varchar(),
	})
	.as(
		sql`SELECT step_id, session_id, ordinal, tool_name, arguments, status, result_summary, error, started_at, duration_seconds, target, dimension FROM ws_00000000_0000_0000_0000_000000000001.investigation_steps`,
	);

export const metadataSnapshotHead = metadataSchema
	.view("metadata_snapshot_head", {
		headId: varchar("head_id"),
		target: varchar(),
		stage: varchar(),
		runId: varchar("run_id"),
		promotedAt: timestamp("promoted_at"),
	})
	.as(
		sql`SELECT head_id, target, stage, run_id, promoted_at FROM ws_00000000_0000_0000_0000_000000000001.metadata_snapshot_head`,
	);

export const sessionTables = metadataSchema
	.view("session_tables", {
		sessionId: varchar("session_id"),
		tableId: varchar("table_id"),
	})
	.as(
		sql`SELECT session_id, table_id FROM ws_00000000_0000_0000_0000_000000000001.session_tables`,
	);

export const snippetUsage = metadataSchema
	.view("snippet_usage", {
		usageId: varchar("usage_id"),
		sessionId: varchar("session_id"),
		executionId: varchar("execution_id"),
		executionType: varchar("execution_type"),
		snippetId: varchar("snippet_id"),
		usageType: varchar("usage_type"),
		matchConfidence: doublePrecision("match_confidence"),
		sqlMatchRatio: doublePrecision("sql_match_ratio"),
		stepId: varchar("step_id"),
		createdAt: timestamp("created_at"),
	})
	.as(
		sql`SELECT usage_id, session_id, execution_id, execution_type, snippet_id, usage_type, match_confidence, sql_match_ratio, step_id, created_at FROM ws_00000000_0000_0000_0000_000000000001.snippet_usage`,
	);

export const sources = metadataSchema
	.view("sources", {
		sourceId: varchar("source_id"),
		name: varchar(),
		sourceType: varchar("source_type"),
		connectionConfig: json("connection_config"),
		createdAt: timestamp("created_at"),
		updatedAt: timestamp("updated_at"),
		status: varchar(),
		stage: varchar(),
		backend: varchar(),
		discoveredSchema: json("discovered_schema"),
		archivedAt: timestamp("archived_at"),
	})
	.as(
		sql`SELECT source_id, name, source_type, connection_config, created_at, updated_at, status, stage, backend, discovered_schema, archived_at FROM ws_00000000_0000_0000_0000_000000000001.sources`,
	);

export const sqlSnippets = metadataSchema
	.view("sql_snippets", {
		snippetId: varchar("snippet_id"),
		sessionId: varchar("session_id"),
		snippetType: varchar("snippet_type"),
		standardField: varchar("standard_field"),
		statement: varchar(),
		aggregation: varchar(),
		schemaMappingId: varchar("schema_mapping_id"),
		parameterValue: varchar("parameter_value"),
		normalizedExpression: varchar("normalized_expression"),
		inputFields: json("input_fields"),
		sql: text(),
		description: text(),
		columnMappings: json("column_mappings"),
		source: varchar(),
		llmModel: varchar("llm_model"),
		provenance: json(),
		executionCount: integer("execution_count"),
		failureCount: integer("failure_count"),
		lastUsedAt: timestamp("last_used_at"),
		columnHash: varchar("column_hash"),
		createdAt: timestamp("created_at"),
		updatedAt: timestamp("updated_at"),
	})
	.as(
		sql`SELECT snippet_id, session_id, snippet_type, standard_field, statement, aggregation, schema_mapping_id, parameter_value, normalized_expression, input_fields, sql, description, column_mappings, source, llm_model, provenance, execution_count, failure_count, last_used_at, column_hash, created_at, updated_at FROM ws_00000000_0000_0000_0000_000000000001.sql_snippets`,
	);

export const tables = metadataSchema
	.view("tables", {
		tableId: varchar("table_id"),
		sourceId: varchar("source_id"),
		tableName: varchar("table_name"),
		layer: varchar(),
		duckdbPath: varchar("duckdb_path"),
		rowCount: integer("row_count"),
		createdAt: timestamp("created_at"),
		lastProfiledAt: timestamp("last_profiled_at"),
	})
	.as(
		sql`SELECT table_id, source_id, table_name, layer, duckdb_path, row_count, created_at, last_profiled_at FROM ws_00000000_0000_0000_0000_000000000001.tables`,
	);

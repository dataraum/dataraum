import { sql } from "drizzle-orm";
import {
	boolean,
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

export const conceptEdges = metadataSchema
	.view("concept_edges", {
		edgeId: varchar("edge_id"),
		vertical: varchar(),
		predicate: varchar(),
		fromConcept: varchar("from_concept"),
		toConcept: varchar("to_concept"),
		tolerance: doublePrecision(),
		source: varchar(),
		createdAt: timestamp("created_at"),
		supersededAt: timestamp("superseded_at"),
	})
	.as(
		sql`SELECT edge_id, vertical, predicate, from_concept, to_concept, tolerance, source, created_at, superseded_at FROM ws_00000000_0000_0000_0000_000000000001.concept_edges`,
	);

export const concepts = metadataSchema
	.view("concepts", {
		conceptId: varchar("concept_id"),
		vertical: varchar(),
		name: varchar(),
		kind: varchar(),
		description: text(),
		indicators: json(),
		excludePatterns: json("exclude_patterns"),
		unitFromConcept: varchar("unit_from_concept"),
		source: varchar(),
		createdAt: timestamp("created_at"),
		supersededAt: timestamp("superseded_at"),
	})
	.as(
		sql`SELECT concept_id, vertical, name, kind, description, indicators, exclude_patterns, unit_from_concept, source, created_at, superseded_at FROM ws_00000000_0000_0000_0000_000000000001.concepts`,
	);

export const configOverlay = metadataSchema
	.view("config_overlay", {
		overlayId: varchar("overlay_id"),
		type: varchar(),
		payload: json(),
		createdAt: timestamp("created_at"),
		supersededAt: timestamp("superseded_at"),
	})
	.as(
		sql`SELECT overlay_id, type, payload, created_at, superseded_at FROM ws_00000000_0000_0000_0000_000000000001.config_overlay`,
	);

export const currentClaimWitnesses = metadataSchema
	.view("current_claim_witnesses", {
		claimWitnessId: varchar("claim_witness_id"),
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
		viaCatalogHead: boolean("via_catalog_head"),
		viaOperatingModelHead: boolean("via_operating_model_head"),
	})
	.as(
		sql`SELECT claim_witness_id, table_id, column_id, run_id, target, claim_field, witness_id, distribution, reliability, detector_id, computed_at, (EXISTS ( SELECT 1 FROM ws_00000000_0000_0000_0000_000000000001.metadata_snapshot_head h WHERE h.stage::text = 'generation'::text AND h.run_id::text = r.run_id::text AND h.target::text = ('table:'::text || r.table_id::text))) AS via_table_head, (EXISTS ( SELECT 1 FROM ws_00000000_0000_0000_0000_000000000001.metadata_snapshot_head h WHERE h.stage::text = 'catalog'::text AND h.run_id::text = r.run_id::text AND h.target::text = 'catalog'::text)) AS via_catalog_head, (EXISTS ( SELECT 1 FROM ws_00000000_0000_0000_0000_000000000001.metadata_snapshot_head h WHERE h.stage::text = 'operating_model'::text AND h.run_id::text = r.run_id::text AND h.target::text = 'catalog'::text)) AS via_operating_model_head FROM ws_00000000_0000_0000_0000_000000000001.claim_witnesses r WHERE (EXISTS ( SELECT 1 FROM ws_00000000_0000_0000_0000_000000000001.metadata_snapshot_head h WHERE h.run_id::text = r.run_id::text AND (h.stage::text = 'generation'::text AND h.target::text = ('table:'::text || r.table_id::text) OR h.stage::text = 'catalog'::text AND h.target::text = 'catalog'::text OR h.stage::text = 'operating_model'::text AND h.target::text = 'catalog'::text)))`,
	);

export const currentColumnConcepts = metadataSchema
	.view("current_column_concepts", {
		conceptId: varchar("concept_id"),
		columnId: varchar("column_id"),
		runId: varchar("run_id"),
		meaning: text(),
		temporalBehavior: varchar("temporal_behavior"),
		unitSourceColumn: varchar("unit_source_column"),
		derivedFormulaHypothesis: varchar("derived_formula_hypothesis"),
		derivedFormulaConfidence: doublePrecision("derived_formula_confidence"),
		annotationSource: varchar("annotation_source"),
		annotatedAt: timestamp("annotated_at"),
		annotatedBy: varchar("annotated_by"),
		confidence: doublePrecision(),
	})
	.as(
		sql`SELECT concept_id, column_id, run_id, meaning, temporal_behavior, unit_source_column, derived_formula_hypothesis, derived_formula_confidence, annotation_source, annotated_at, annotated_by, confidence FROM ws_00000000_0000_0000_0000_000000000001.column_concepts r WHERE (EXISTS ( SELECT 1 FROM ws_00000000_0000_0000_0000_000000000001.metadata_snapshot_head h WHERE h.target::text = 'catalog'::text AND h.stage::text = 'catalog'::text AND h.run_id::text = r.run_id::text))`,
	);

export const currentColumnEligibility = metadataSchema
	.view("current_column_eligibility", {
		eligibilityId: varchar("eligibility_id", { length: 36 }),
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
		sql`SELECT eligibility_id, column_id, table_id, source_id, run_id, column_name, table_name, resolved_type, status, triggered_rule, reason, metrics_snapshot, config_version, evaluated_at FROM ws_00000000_0000_0000_0000_000000000001.column_eligibility r WHERE (EXISTS ( SELECT 1 FROM ws_00000000_0000_0000_0000_000000000001.metadata_snapshot_head h WHERE h.target::text = ('table:'::text || r.table_id::text) AND h.stage::text = 'generation'::text AND h.run_id::text = r.run_id::text))`,
	);

export const currentColumns = metadataSchema
	.view("current_columns", {
		columnId: varchar("column_id"),
		tableId: varchar("table_id"),
		columnName: varchar("column_name"),
		originalName: varchar("original_name"),
		columnPosition: integer("column_position"),
		rawType: varchar("raw_type"),
		resolvedType: varchar("resolved_type"),
	})
	.as(
		sql`SELECT column_id, table_id, column_name, original_name, column_position, raw_type, resolved_type FROM ws_00000000_0000_0000_0000_000000000001.columns c WHERE (EXISTS ( SELECT 1 FROM ws_00000000_0000_0000_0000_000000000001.tables t JOIN ws_00000000_0000_0000_0000_000000000001.metadata_snapshot_head h ON h.target::text = ('table:'::text || t.table_id::text) AND h.stage::text = 'generation'::text WHERE t.table_id::text = c.table_id::text AND t.layer::text = 'typed'::text))`,
	);

export const currentDerivedColumns = metadataSchema
	.view("current_derived_columns", {
		derivedId: varchar("derived_id"),
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
		sql`SELECT derived_id, run_id, table_id, derived_column_id, source_column_ids, derivation_type, formula, match_rate, computed_at, total_rows, matching_rows, mismatch_examples FROM ws_00000000_0000_0000_0000_000000000001.derived_columns r WHERE (EXISTS ( SELECT 1 FROM ws_00000000_0000_0000_0000_000000000001.metadata_snapshot_head h WHERE h.target::text = 'catalog'::text AND h.stage::text = 'catalog'::text AND h.run_id::text = r.run_id::text))`,
	);

export const currentDetectedBusinessCycles = metadataSchema
	.view("current_detected_business_cycles", {
		cycleId: varchar("cycle_id"),
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
		sql`SELECT cycle_id, run_id, cycle_name, cycle_type, canonical_type, is_known_type, description, business_value, confidence, tables_involved, stages, entity_flows, status_table, status_column, completion_value, total_records, completed_cycles, completion_rate, evidence, detected_at FROM ws_00000000_0000_0000_0000_000000000001.detected_business_cycles r WHERE (EXISTS ( SELECT 1 FROM ws_00000000_0000_0000_0000_000000000001.metadata_snapshot_head h WHERE h.target::text = 'catalog'::text AND h.stage::text = 'operating_model'::text AND h.run_id::text = r.run_id::text))`,
	);

export const currentDimensionHierarchies = metadataSchema
	.view("current_dimension_hierarchies", {
		hierarchyId: varchar("hierarchy_id"),
		runId: varchar("run_id"),
		tableId: varchar("table_id"),
		kind: varchar(),
		members: json(),
		canonicalLabel: varchar("canonical_label"),
		signature: varchar(),
		g3: doublePrecision(),
		roleVerdict: varchar("role_verdict"),
		roleEvidence: json("role_evidence"),
		detectionSource: varchar("detection_source"),
		needsConfirmation: boolean("needs_confirmation"),
		createdAt: timestamp("created_at", { withTimezone: true }),
	})
	.as(
		sql`SELECT hierarchy_id, run_id, table_id, kind, members, canonical_label, signature, g3, role_verdict, role_evidence, detection_source, needs_confirmation, created_at FROM ws_00000000_0000_0000_0000_000000000001.dimension_hierarchies r WHERE (EXISTS ( SELECT 1 FROM ws_00000000_0000_0000_0000_000000000001.metadata_snapshot_head h WHERE h.target::text = 'catalog'::text AND h.stage::text = 'catalog'::text AND h.run_id::text = r.run_id::text))`,
	);

export const currentDriverRankings = metadataSchema
	.view("current_driver_rankings", {
		rankingId: varchar("ranking_id"),
		runId: varchar("run_id"),
		measureTableId: varchar("measure_table_id"),
		measureColumnId: varchar("measure_column_id"),
		measureLabel: varchar("measure_label"),
		targetType: varchar("target_type"),
		grain: varchar(),
		entity: varchar(),
		nRows: integer("n_rows"),
		rankedDimensions: json("ranked_dimensions"),
		driverPaths: json("driver_paths"),
		interestingSlices: json("interesting_slices"),
		secondaryDimensions: json("secondary_dimensions"),
		createdAt: timestamp("created_at", { withTimezone: true }),
	})
	.as(
		sql`SELECT ranking_id, run_id, measure_table_id, measure_column_id, measure_label, target_type, grain, entity, n_rows, ranked_dimensions, driver_paths, interesting_slices, secondary_dimensions, created_at FROM ws_00000000_0000_0000_0000_000000000001.driver_rankings r WHERE (EXISTS ( SELECT 1 FROM ws_00000000_0000_0000_0000_000000000001.metadata_snapshot_head h WHERE h.target::text = 'catalog'::text AND h.stage::text = 'catalog'::text AND h.run_id::text = r.run_id::text))`,
	);

export const currentEnrichedViews = metadataSchema
	.view("current_enriched_views", {
		viewId: varchar("view_id"),
		factTableId: varchar("fact_table_id"),
		viewTableId: varchar("view_table_id"),
		viewName: varchar("view_name"),
		runId: varchar("run_id"),
		relationshipIds: json("relationship_ids"),
		consideredRelationshipPairs: json("considered_relationship_pairs"),
		exposedDimensionJoins: json("exposed_dimension_joins"),
		dimensionTableIds: json("dimension_table_ids"),
		dimensionColumns: json("dimension_columns"),
		isGrainVerified: boolean("is_grain_verified"),
		evidence: json(),
		createdAt: timestamp("created_at"),
	})
	.as(
		sql`SELECT view_id, fact_table_id, view_table_id, view_name, run_id, relationship_ids, considered_relationship_pairs, exposed_dimension_joins, dimension_table_ids, dimension_columns, is_grain_verified, evidence, created_at FROM ws_00000000_0000_0000_0000_000000000001.enriched_views r WHERE (EXISTS ( SELECT 1 FROM ws_00000000_0000_0000_0000_000000000001.metadata_snapshot_head h WHERE h.target::text = 'catalog'::text AND h.stage::text = 'catalog'::text AND h.run_id::text = r.run_id::text))`,
	);

export const currentEntropyObjects = metadataSchema
	.view("current_entropy_objects", {
		objectId: varchar("object_id"),
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
		viaCatalogHead: boolean("via_catalog_head"),
		viaOperatingModelHead: boolean("via_operating_model_head"),
	})
	.as(
		sql`SELECT object_id, layer, dimension, sub_dimension, target, table_id, column_id, run_id, score, evidence, detector_id, source_analysis_ids, computed_at, (EXISTS ( SELECT 1 FROM ws_00000000_0000_0000_0000_000000000001.metadata_snapshot_head h WHERE h.stage::text = 'generation'::text AND h.run_id::text = r.run_id::text AND h.target::text = ('table:'::text || r.table_id::text))) AS via_table_head, (EXISTS ( SELECT 1 FROM ws_00000000_0000_0000_0000_000000000001.metadata_snapshot_head h WHERE h.stage::text = 'catalog'::text AND h.run_id::text = r.run_id::text AND h.target::text = 'catalog'::text)) AS via_catalog_head, (EXISTS ( SELECT 1 FROM ws_00000000_0000_0000_0000_000000000001.metadata_snapshot_head h WHERE h.stage::text = 'operating_model'::text AND h.run_id::text = r.run_id::text AND h.target::text = 'catalog'::text)) AS via_operating_model_head FROM ws_00000000_0000_0000_0000_000000000001.entropy_objects r WHERE (EXISTS ( SELECT 1 FROM ws_00000000_0000_0000_0000_000000000001.metadata_snapshot_head h WHERE h.run_id::text = r.run_id::text AND (h.stage::text = 'generation'::text AND h.target::text = ('table:'::text || r.table_id::text) OR h.stage::text = 'catalog'::text AND h.target::text = 'catalog'::text OR h.stage::text = 'operating_model'::text AND h.target::text = 'catalog'::text)))`,
	);

export const currentEntropyReadiness = metadataSchema
	.view("current_entropy_readiness", {
		readinessId: varchar("readiness_id"),
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
		viaCatalogHead: boolean("via_catalog_head"),
		viaOperatingModelHead: boolean("via_operating_model_head"),
	})
	.as(
		sql`SELECT readiness_id, target, table_id, column_id, run_id, band, worst_intent_risk, intents, top_drivers, computed_at, (EXISTS ( SELECT 1 FROM ws_00000000_0000_0000_0000_000000000001.metadata_snapshot_head h WHERE h.stage::text = 'generation'::text AND h.run_id::text = r.run_id::text AND h.target::text = ('table:'::text || r.table_id::text))) AS via_table_head, (EXISTS ( SELECT 1 FROM ws_00000000_0000_0000_0000_000000000001.metadata_snapshot_head h WHERE h.stage::text = 'catalog'::text AND h.run_id::text = r.run_id::text AND h.target::text = 'catalog'::text)) AS via_catalog_head, (EXISTS ( SELECT 1 FROM ws_00000000_0000_0000_0000_000000000001.metadata_snapshot_head h WHERE h.stage::text = 'operating_model'::text AND h.run_id::text = r.run_id::text AND h.target::text = 'catalog'::text)) AS via_operating_model_head FROM ws_00000000_0000_0000_0000_000000000001.entropy_readiness r WHERE (EXISTS ( SELECT 1 FROM ws_00000000_0000_0000_0000_000000000001.metadata_snapshot_head h WHERE h.run_id::text = r.run_id::text AND (h.stage::text = 'generation'::text AND h.target::text = ('table:'::text || r.table_id::text) OR h.stage::text = 'catalog'::text AND h.target::text = 'catalog'::text OR h.stage::text = 'operating_model'::text AND h.target::text = 'catalog'::text))) AND (NOT (EXISTS ( SELECT 1 FROM ws_00000000_0000_0000_0000_000000000001.metadata_snapshot_head h3 WHERE h3.run_id::text = r.run_id::text AND h3.target::text = 'catalog'::text AND (h3.stage::text = ANY (ARRAY['catalog'::character varying, 'operating_model'::character varying]::text[])))) OR NOT (EXISTS ( SELECT 1 FROM ws_00000000_0000_0000_0000_000000000001.entropy_readiness r2 JOIN ws_00000000_0000_0000_0000_000000000001.metadata_snapshot_head h2 ON h2.run_id::text = r2.run_id::text AND h2.target::text = 'catalog'::text AND (h2.stage::text = ANY (ARRAY['catalog'::character varying, 'operating_model'::character varying]::text[])) WHERE r2.target::text = r.target::text AND r2.run_id::text <> r.run_id::text AND h2.promoted_at > (( SELECT max(h3.promoted_at) AS max FROM ws_00000000_0000_0000_0000_000000000001.metadata_snapshot_head h3 WHERE h3.run_id::text = r.run_id::text AND h3.target::text = 'catalog'::text AND (h3.stage::text = ANY (ARRAY['catalog'::character varying, 'operating_model'::character varying]::text[])))))))`,
	);

export const currentLifecycleArtifacts = metadataSchema
	.view("current_lifecycle_artifacts", {
		artifactId: varchar("artifact_id"),
		artifactType: varchar("artifact_type"),
		artifactKey: varchar("artifact_key"),
		runId: varchar("run_id"),
		state: varchar(),
		stateReason: text("state_reason"),
		stage: varchar(),
		strictness: doublePrecision(),
		groundedAgainst: json("grounded_against"),
		teaches: json(),
		graphDefinition: json("graph_definition"),
		createdAt: timestamp("created_at"),
		stateChangedAt: timestamp("state_changed_at"),
	})
	.as(
		sql`SELECT artifact_id, artifact_type, artifact_key, run_id, state, state_reason, stage, strictness, grounded_against, teaches, graph_definition, created_at, state_changed_at FROM ws_00000000_0000_0000_0000_000000000001.lifecycle_artifacts r WHERE (EXISTS ( SELECT 1 FROM ws_00000000_0000_0000_0000_000000000001.metadata_snapshot_head h WHERE h.target::text = 'catalog'::text AND h.stage::text = 'operating_model'::text AND h.run_id::text = r.run_id::text))`,
	);

export const currentMaterializationRecipes = metadataSchema
	.view("current_materialization_recipes", {
		recipeId: varchar("recipe_id"),
		tableId: varchar("table_id"),
		layer: varchar(),
		runId: varchar("run_id"),
		targetFqn: varchar("target_fqn"),
		ddl: varchar(),
		dependsOn: json("depends_on"),
		createdAt: timestamp("created_at"),
	})
	.as(
		sql`SELECT recipe_id, table_id, layer, run_id, target_fqn, ddl, depends_on, created_at FROM ws_00000000_0000_0000_0000_000000000001.materialization_recipes r WHERE (EXISTS ( SELECT 1 FROM ws_00000000_0000_0000_0000_000000000001.metadata_snapshot_head h WHERE h.target::text = ('table:'::text || r.table_id::text) AND h.stage::text = 'generation'::text AND h.run_id::text = r.run_id::text))`,
	);

export const currentMeasureAggregationLineage = metadataSchema
	.view("current_measure_aggregation_lineage", {
		lineageId: varchar("lineage_id"),
		runId: varchar("run_id"),
		measureTableId: varchar("measure_table_id"),
		measureColumnId: varchar("measure_column_id"),
		eventTableId: varchar("event_table_id"),
		measureTimeAxisColumn: varchar("measure_time_axis_column"),
		measureTimeAxisColumnId: varchar("measure_time_axis_column_id"),
		eventTimeAxisColumn: varchar("event_time_axis_column"),
		eventTimeAxisColumnId: varchar("event_time_axis_column_id"),
		measureSliceColumnId: varchar("measure_slice_column_id"),
		eventSliceColumnId: varchar("event_slice_column_id"),
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
		sql`SELECT lineage_id, run_id, measure_table_id, measure_column_id, event_table_id, measure_time_axis_column, measure_time_axis_column_id, event_time_axis_column, event_time_axis_column_id, measure_slice_column_id, event_slice_column_id, slice_dimension, convention_sql, period_grain, pattern, match_rate, r_flow_median, r_stock_median, n_entities, n_entities_fired, created_at FROM ws_00000000_0000_0000_0000_000000000001.measure_aggregation_lineage r WHERE (EXISTS ( SELECT 1 FROM ws_00000000_0000_0000_0000_000000000001.metadata_snapshot_head h WHERE h.target::text = 'catalog'::text AND h.stage::text = 'catalog'::text AND h.run_id::text = r.run_id::text))`,
	);

export const currentMetricAdditivity = metadataSchema
	.view("current_metric_additivity", {
		additivityId: varchar("additivity_id"),
		runId: varchar("run_id"),
		targetKind: varchar("target_kind"),
		targetKey: varchar("target_key"),
		categoricalAdditive: boolean("categorical_additive"),
		timeAdditive: boolean("time_additive"),
		categoricalReason: varchar("categorical_reason"),
		timeReason: varchar("time_reason"),
		createdAt: timestamp("created_at", { withTimezone: true }),
	})
	.as(
		sql`SELECT additivity_id, run_id, target_kind, target_key, categorical_additive, time_additive, categorical_reason, time_reason, created_at FROM ws_00000000_0000_0000_0000_000000000001.metric_additivity r WHERE (EXISTS ( SELECT 1 FROM ws_00000000_0000_0000_0000_000000000001.metadata_snapshot_head h WHERE h.target::text = 'catalog'::text AND h.stage::text = 'operating_model'::text AND h.run_id::text = r.run_id::text))`,
	);

export const currentRelationships = metadataSchema
	.view("current_relationships", {
		relationshipId: varchar("relationship_id"),
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
		confirmationSource: varchar("confirmation_source"),
		detectedAt: timestamp("detected_at"),
	})
	.as(
		sql`SELECT relationship_id, run_id, from_table_id, from_column_id, to_table_id, to_column_id, relationship_type, cardinality, confidence, detection_method, evidence, confirmation_source, detected_at FROM ws_00000000_0000_0000_0000_000000000001.relationships r WHERE (EXISTS ( SELECT 1 FROM ws_00000000_0000_0000_0000_000000000001.metadata_snapshot_head h WHERE h.target::text = 'catalog'::text AND h.stage::text = 'catalog'::text AND h.run_id::text = r.run_id::text))`,
	);

export const currentSemanticAnnotations = metadataSchema
	.view("current_semantic_annotations", {
		annotationId: varchar("annotation_id"),
		columnId: varchar("column_id"),
		runId: varchar("run_id"),
		semanticRole: varchar("semantic_role"),
		entityType: varchar("entity_type"),
		businessName: varchar("business_name"),
		businessDescription: text("business_description"),
		temporalBehaviorClaim: varchar("temporal_behavior_claim"),
		temporalBehaviorClaimConfidence: doublePrecision(
			"temporal_behavior_claim_confidence",
		),
		nullTokens: json("null_tokens"),
		annotationSource: varchar("annotation_source"),
		annotatedAt: timestamp("annotated_at"),
		annotatedBy: varchar("annotated_by"),
		confidence: doublePrecision(),
	})
	.as(
		sql`SELECT annotation_id, column_id, run_id, semantic_role, entity_type, business_name, business_description, temporal_behavior_claim, temporal_behavior_claim_confidence, null_tokens, annotation_source, annotated_at, annotated_by, confidence FROM ws_00000000_0000_0000_0000_000000000001.semantic_annotations r WHERE (EXISTS ( SELECT 1 FROM ws_00000000_0000_0000_0000_000000000001.columns c JOIN ws_00000000_0000_0000_0000_000000000001.metadata_snapshot_head h ON h.target::text = ('table:'::text || c.table_id::text) WHERE c.column_id::text = r.column_id::text AND h.stage::text = 'generation'::text AND h.run_id::text = r.run_id::text))`,
	);

export const currentSliceDefinitions = metadataSchema
	.view("current_slice_definitions", {
		sliceId: varchar("slice_id"),
		runId: varchar("run_id"),
		tableId: varchar("table_id"),
		columnId: varchar("column_id"),
		columnName: varchar("column_name"),
		dimensionTableId: varchar("dimension_table_id"),
		dimensionAttribute: varchar("dimension_attribute"),
		fkRole: varchar("fk_role"),
		slicePriority: integer("slice_priority"),
		sliceType: varchar("slice_type"),
		distinctValues: json("distinct_values"),
		valueCount: integer("value_count"),
		reasoning: text(),
		businessContext: text("business_context"),
		confidence: doublePrecision(),
		detectionSource: varchar("detection_source"),
		createdAt: timestamp("created_at"),
	})
	.as(
		sql`SELECT slice_id, run_id, table_id, column_id, column_name, dimension_table_id, dimension_attribute, fk_role, slice_priority, slice_type, distinct_values, value_count, reasoning, business_context, confidence, detection_source, created_at FROM ws_00000000_0000_0000_0000_000000000001.slice_definitions r WHERE (EXISTS ( SELECT 1 FROM ws_00000000_0000_0000_0000_000000000001.metadata_snapshot_head h WHERE h.target::text = 'catalog'::text AND h.stage::text = 'catalog'::text AND h.run_id::text = r.run_id::text))`,
	);

export const currentStatisticalProfiles = metadataSchema
	.view("current_statistical_profiles", {
		profileId: varchar("profile_id"),
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
		sql`SELECT profile_id, column_id, run_id, profiled_at, layer, total_count, null_count, distinct_count, null_ratio, cardinality_ratio, is_unique, is_numeric, profile_data FROM ws_00000000_0000_0000_0000_000000000001.statistical_profiles r WHERE (EXISTS ( SELECT 1 FROM ws_00000000_0000_0000_0000_000000000001.columns c JOIN ws_00000000_0000_0000_0000_000000000001.metadata_snapshot_head h ON h.target::text = ('table:'::text || c.table_id::text) WHERE c.column_id::text = r.column_id::text AND h.stage::text = 'generation'::text AND h.run_id::text = r.run_id::text))`,
	);

export const currentStatisticalQualityMetrics = metadataSchema
	.view("current_statistical_quality_metrics", {
		metricId: varchar("metric_id"),
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
		sql`SELECT metric_id, column_id, run_id, computed_at, benford_compliant, has_outliers, iqr_outlier_ratio, zscore_outlier_ratio, quality_data FROM ws_00000000_0000_0000_0000_000000000001.statistical_quality_metrics r WHERE (EXISTS ( SELECT 1 FROM ws_00000000_0000_0000_0000_000000000001.columns c JOIN ws_00000000_0000_0000_0000_000000000001.metadata_snapshot_head h ON h.target::text = ('table:'::text || c.table_id::text) WHERE c.column_id::text = r.column_id::text AND h.stage::text = 'generation'::text AND h.run_id::text = r.run_id::text))`,
	);

export const currentSurrogateKeyIntents = metadataSchema
	.view("current_surrogate_key_intents", {
		intentId: varchar("intent_id"),
		runId: varchar("run_id"),
		intentDigest: varchar("intent_digest"),
		status: varchar(),
		fromTableId: varchar("from_table_id"),
		toTableId: varchar("to_table_id"),
		columnPairs: json("column_pairs"),
		cardinality: varchar(),
		confidence: doublePrecision(),
		reasoning: varchar(),
		detectedAt: timestamp("detected_at"),
	})
	.as(
		sql`SELECT intent_id, run_id, intent_digest, status, from_table_id, to_table_id, column_pairs, cardinality, confidence, reasoning, detected_at FROM ws_00000000_0000_0000_0000_000000000001.surrogate_key_intents r WHERE (EXISTS ( SELECT 1 FROM ws_00000000_0000_0000_0000_000000000001.metadata_snapshot_head h WHERE h.target::text = 'catalog'::text AND h.stage::text = 'catalog'::text AND h.run_id::text = r.run_id::text))`,
	);

export const currentTableEntities = metadataSchema
	.view("current_table_entities", {
		entityId: varchar("entity_id"),
		tableId: varchar("table_id"),
		runId: varchar("run_id"),
		detectedEntityType: varchar("detected_entity_type"),
		description: text(),
		grainColumns: json("grain_columns"),
		tableRole: varchar("table_role"),
		timeColumns: json("time_columns"),
		identityColumns: json("identity_columns"),
		detectionSource: varchar("detection_source"),
		detectedAt: timestamp("detected_at"),
	})
	.as(
		sql`SELECT entity_id, table_id, run_id, detected_entity_type, description, grain_columns, table_role, time_columns, identity_columns, detection_source, detected_at FROM ws_00000000_0000_0000_0000_000000000001.table_entities r WHERE (EXISTS ( SELECT 1 FROM ws_00000000_0000_0000_0000_000000000001.metadata_snapshot_head h WHERE h.target::text = 'catalog'::text AND h.stage::text = 'catalog'::text AND h.run_id::text = r.run_id::text))`,
	);

export const currentTables = metadataSchema
	.view("current_tables", {
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
		sql`SELECT table_id, source_id, table_name, layer, duckdb_path, row_count, created_at, last_profiled_at FROM ws_00000000_0000_0000_0000_000000000001.tables t WHERE layer::text = 'typed'::text AND (EXISTS ( SELECT 1 FROM ws_00000000_0000_0000_0000_000000000001.metadata_snapshot_head h WHERE h.target::text = ('table:'::text || t.table_id::text) AND h.stage::text = 'generation'::text))`,
	);

export const currentTemporalColumnProfiles = metadataSchema
	.view("current_temporal_column_profiles", {
		profileId: varchar("profile_id"),
		columnId: varchar("column_id"),
		runId: varchar("run_id"),
		profiledAt: timestamp("profiled_at"),
		minTimestamp: timestamp("min_timestamp"),
		maxTimestamp: timestamp("max_timestamp"),
		spanDays: doublePrecision("span_days"),
		detectedGranularity: varchar("detected_granularity"),
		granularityConfidence: doublePrecision("granularity_confidence"),
		completenessRatio: doublePrecision("completeness_ratio"),
		expectedPeriods: integer("expected_periods"),
		actualPeriods: integer("actual_periods"),
		gapCount: integer("gap_count"),
		largestGapDays: doublePrecision("largest_gap_days"),
		isStale: boolean("is_stale"),
		gaps: json(),
	})
	.as(
		sql`SELECT profile_id, column_id, run_id, profiled_at, min_timestamp, max_timestamp, span_days, detected_granularity, granularity_confidence, completeness_ratio, expected_periods, actual_periods, gap_count, largest_gap_days, is_stale, gaps FROM ws_00000000_0000_0000_0000_000000000001.temporal_column_profiles r WHERE (EXISTS ( SELECT 1 FROM ws_00000000_0000_0000_0000_000000000001.columns c JOIN ws_00000000_0000_0000_0000_000000000001.metadata_snapshot_head h ON h.target::text = ('table:'::text || c.table_id::text) WHERE c.column_id::text = r.column_id::text AND h.stage::text = 'generation'::text AND h.run_id::text = r.run_id::text))`,
	);

export const currentTypeCandidates = metadataSchema
	.view("current_type_candidates", {
		candidateId: varchar("candidate_id"),
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
		sql`SELECT candidate_id, column_id, run_id, detected_at, data_type, confidence, parse_success_rate, failed_examples, detected_pattern, pattern_match_rate, detected_unit, unit_confidence, quarantine_count, quarantine_rate FROM ws_00000000_0000_0000_0000_000000000001.type_candidates r WHERE (EXISTS ( SELECT 1 FROM ws_00000000_0000_0000_0000_000000000001.columns c JOIN ws_00000000_0000_0000_0000_000000000001.metadata_snapshot_head h ON h.target::text = ('table:'::text || c.table_id::text) WHERE c.column_id::text = r.column_id::text AND h.stage::text = 'generation'::text AND h.run_id::text = r.run_id::text))`,
	);

export const currentTypeDecisions = metadataSchema
	.view("current_type_decisions", {
		decisionId: varchar("decision_id"),
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
		sql`SELECT decision_id, column_id, run_id, decided_type, decision_source, decided_at, decided_by, previous_type, decision_reason FROM ws_00000000_0000_0000_0000_000000000001.type_decisions r WHERE (EXISTS ( SELECT 1 FROM ws_00000000_0000_0000_0000_000000000001.columns c JOIN ws_00000000_0000_0000_0000_000000000001.metadata_snapshot_head h ON h.target::text = ('table:'::text || c.table_id::text) WHERE c.column_id::text = r.column_id::text AND h.stage::text = 'generation'::text AND h.run_id::text = r.run_id::text))`,
	);

export const currentValidationResults = metadataSchema
	.view("current_validation_results", {
		resultId: varchar("result_id"),
		runId: varchar("run_id"),
		validationId: varchar("validation_id"),
		tableIds: json("table_ids"),
		columnsUsed: json("columns_used"),
		sqlUsed: text("sql_used"),
		executedAt: timestamp("executed_at"),
	})
	.as(
		sql`SELECT result_id, run_id, validation_id, table_ids, columns_used, sql_used, executed_at FROM ws_00000000_0000_0000_0000_000000000001.validation_results r WHERE (EXISTS ( SELECT 1 FROM ws_00000000_0000_0000_0000_000000000001.metadata_snapshot_head h WHERE h.target::text = 'catalog'::text AND h.stage::text = 'operating_model'::text AND h.run_id::text = r.run_id::text))`,
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

export const runTables = metadataSchema
	.view("run_tables", {
		runId: varchar("run_id"),
		tableId: varchar("table_id"),
	})
	.as(
		sql`SELECT run_id, table_id FROM ws_00000000_0000_0000_0000_000000000001.run_tables`,
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
		workspaceId: varchar("workspace_id"),
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
		source: varchar(),
		provenance: json(),
		parts: json(),
		executionCount: integer("execution_count"),
		failureCount: integer("failure_count"),
		createdAt: timestamp("created_at"),
		updatedAt: timestamp("updated_at"),
	})
	.as(
		sql`SELECT snippet_id, workspace_id, snippet_type, standard_field, statement, aggregation, schema_mapping_id, parameter_value, normalized_expression, input_fields, sql, description, source, provenance, parts, execution_count, failure_count, created_at, updated_at FROM ws_00000000_0000_0000_0000_000000000001.sql_snippets`,
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

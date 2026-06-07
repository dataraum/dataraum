import {
	boolean,
	date,
	doublePrecision,
	foreignKey,
	index,
	integer,
	json,
	jsonb,
	pgSchema,
	primaryKey,
	text,
	timestamp,
	unique,
	varchar,
} from "drizzle-orm/pg-core";

export const metadataSchema = pgSchema(
	"ws_00000000_0000_0000_0000_000000000001",
);

export const columnDriftSummaries = metadataSchema.table(
	"column_drift_summaries",
	{
		id: varchar().primaryKey(),
		sessionId: varchar("session_id")
			.notNull()
			.references(() => investigationSessions.sessionId),
		runId: varchar("run_id"),
		sliceTableName: varchar("slice_table_name", { length: 255 }).notNull(),
		columnName: varchar("column_name", { length: 255 }).notNull(),
		timeColumn: varchar("time_column", { length: 255 }).notNull(),
		maxJsDivergence: doublePrecision("max_js_divergence").notNull(),
		meanJsDivergence: doublePrecision("mean_js_divergence").notNull(),
		periodsAnalyzed: integer("periods_analyzed").notNull(),
		periodsWithDrift: integer("periods_with_drift").notNull(),
		driftEvidenceJson: json("drift_evidence_json"),
		createdAt: timestamp("created_at").notNull(),
	},
	(table) => [
		index("ix_column_drift_summaries_column_name").using(
			"btree",
			table.columnName.asc().nullsLast(),
		),
		index("ix_column_drift_summaries_session_id").using(
			"btree",
			table.sessionId.asc().nullsLast(),
		),
		index("ix_column_drift_summaries_slice_table_name").using(
			"btree",
			table.sliceTableName.asc().nullsLast(),
		),
		unique("uq_drift_slice_column_run").on(
			table.sliceTableName,
			table.columnName,
			table.runId,
		),
	],
);

export const columnEligibility = metadataSchema.table(
	"column_eligibility",
	{
		eligibilityId: varchar("eligibility_id", { length: 36 }).primaryKey(),
		sessionId: varchar("session_id")
			.notNull()
			.references(() => investigationSessions.sessionId),
		columnId: varchar("column_id", { length: 36 }).notNull(),
		tableId: varchar("table_id")
			.notNull()
			.references(() => tables.tableId, { onDelete: "cascade" }),
		sourceId: varchar("source_id")
			.notNull()
			.references(() => sources.sourceId, { onDelete: "cascade" }),
		runId: varchar("run_id"),
		columnName: varchar("column_name").notNull(),
		tableName: varchar("table_name").notNull(),
		resolvedType: varchar("resolved_type"),
		status: varchar({ length: 20 }).notNull(),
		triggeredRule: varchar("triggered_rule", { length: 50 }),
		reason: text(),
		metricsSnapshot: json("metrics_snapshot").notNull(),
		configVersion: varchar("config_version", { length: 20 }).notNull(),
		evaluatedAt: timestamp("evaluated_at").notNull(),
	},
	(table) => [
		index("idx_eligibility_column").using(
			"btree",
			table.columnId.asc().nullsLast(),
		),
		index("idx_eligibility_source").using(
			"btree",
			table.sourceId.asc().nullsLast(),
		),
		index("idx_eligibility_status").using(
			"btree",
			table.status.asc().nullsLast(),
		),
		index("idx_eligibility_table").using(
			"btree",
			table.tableId.asc().nullsLast(),
		),
		index("ix_column_eligibility_session_id").using(
			"btree",
			table.sessionId.asc().nullsLast(),
		),
		unique("uq_column_eligibility_column_run").on(table.columnId, table.runId),
	],
);

export const columnSliceProfiles = metadataSchema.table(
	"column_slice_profiles",
	{
		profileId: varchar("profile_id").primaryKey(),
		sessionId: varchar("session_id")
			.notNull()
			.references(() => investigationSessions.sessionId),
		sourceColumnId: varchar("source_column_id")
			.notNull()
			.references(() => columns.columnId, { onDelete: "cascade" }),
		sliceColumnId: varchar("slice_column_id")
			.notNull()
			.references(() => columns.columnId, { onDelete: "cascade" }),
		sourceTableName: varchar("source_table_name").notNull(),
		columnName: varchar("column_name").notNull(),
		sliceColumnName: varchar("slice_column_name").notNull(),
		sliceValue: varchar("slice_value").notNull(),
		rowCount: integer("row_count").notNull(),
		nullRatio: doublePrecision("null_ratio"),
		distinctCount: integer("distinct_count"),
		qualityScore: doublePrecision("quality_score"),
		hasIssues: integer("has_issues").notNull(),
		issueCount: integer("issue_count").notNull(),
		profileData: json("profile_data"),
		createdAt: timestamp("created_at").notNull(),
	},
	(table) => [
		index("idx_slice_profiles_lookup").using(
			"btree",
			table.sourceColumnId.asc().nullsLast(),
			table.sliceColumnId.asc().nullsLast(),
			table.sliceValue.asc().nullsLast(),
		),
		index("idx_slice_profiles_slice_column").using(
			"btree",
			table.sliceColumnId.asc().nullsLast(),
		),
		index("idx_slice_profiles_source_column").using(
			"btree",
			table.sourceColumnId.asc().nullsLast(),
		),
		index("ix_column_slice_profiles_session_id").using(
			"btree",
			table.sessionId.asc().nullsLast(),
		),
	],
);

export const columns = metadataSchema.table(
	"columns",
	{
		columnId: varchar("column_id").primaryKey(),
		tableId: varchar("table_id")
			.notNull()
			.references(() => tables.tableId, { onDelete: "cascade" }),
		columnName: varchar("column_name").notNull(),
		originalName: varchar("original_name"),
		columnPosition: integer("column_position").notNull(),
		rawType: varchar("raw_type"),
		resolvedType: varchar("resolved_type"),
	},
	(table) => [
		index("idx_columns_table").using("btree", table.tableId.asc().nullsLast()),
		unique("uq_table_column").on(table.tableId, table.columnName),
	],
);

export const configOverlay = metadataSchema.table(
	"config_overlay",
	{
		overlayId: varchar("overlay_id").primaryKey(),
		sessionId: varchar("session_id"),
		type: varchar().notNull(),
		payload: json().notNull(),
		createdAt: timestamp("created_at").notNull(),
		supersededAt: timestamp("superseded_at"),
	},
	(table) => [
		index("idx_config_overlay_active").using(
			"btree",
			table.supersededAt.asc().nullsLast(),
			table.type.asc().nullsLast(),
		),
	],
);

export const derivedColumns = metadataSchema.table(
	"derived_columns",
	{
		derivedId: varchar("derived_id").primaryKey(),
		sessionId: varchar("session_id")
			.notNull()
			.references(() => investigationSessions.sessionId),
		runId: varchar("run_id"),
		tableId: varchar("table_id")
			.notNull()
			.references(() => tables.tableId, { onDelete: "cascade" }),
		derivedColumnId: varchar("derived_column_id")
			.notNull()
			.references(() => columns.columnId, { onDelete: "cascade" }),
		sourceColumnIds: json("source_column_ids").notNull(),
		derivationType: varchar("derivation_type").notNull(),
		formula: varchar().notNull(),
		matchRate: doublePrecision("match_rate").notNull(),
		computedAt: timestamp("computed_at").notNull(),
		totalRows: integer("total_rows").notNull(),
		matchingRows: integer("matching_rows").notNull(),
		mismatchExamples: json("mismatch_examples"),
	},
	(table) => [
		index("idx_derived_column").using(
			"btree",
			table.derivedColumnId.asc().nullsLast(),
		),
		index("idx_derived_table").using("btree", table.tableId.asc().nullsLast()),
		index("ix_derived_columns_session_id").using(
			"btree",
			table.sessionId.asc().nullsLast(),
		),
	],
);

export const detectedBusinessCycles = metadataSchema.table(
	"detected_business_cycles",
	{
		cycleId: varchar("cycle_id").primaryKey(),
		sessionId: varchar("session_id")
			.notNull()
			.references(() => investigationSessions.sessionId),
		sourceId: varchar("source_id")
			.notNull()
			.references(() => sources.sourceId, { onDelete: "cascade" }),
		cycleName: varchar("cycle_name").notNull(),
		cycleType: varchar("cycle_type").notNull(),
		canonicalType: varchar("canonical_type"),
		isKnownType: boolean("is_known_type").notNull(),
		description: text(),
		businessValue: varchar("business_value").notNull(),
		confidence: doublePrecision().notNull(),
		tablesInvolved: json("tables_involved").notNull(),
		stages: json().notNull(),
		entityFlows: json("entity_flows").notNull(),
		statusTable: varchar("status_table"),
		statusColumn: varchar("status_column"),
		completionValue: varchar("completion_value"),
		totalRecords: integer("total_records"),
		completedCycles: integer("completed_cycles"),
		completionRate: doublePrecision("completion_rate"),
		evidence: json().notNull(),
		detectedAt: timestamp("detected_at").notNull(),
	},
	(table) => [
		index("idx_detected_cycles_source").using(
			"btree",
			table.sourceId.asc().nullsLast(),
		),
		index("ix_detected_business_cycles_session_id").using(
			"btree",
			table.sessionId.asc().nullsLast(),
		),
	],
);

export const enrichedViews = metadataSchema.table(
	"enriched_views",
	{
		viewId: varchar("view_id").primaryKey(),
		sessionId: varchar("session_id")
			.notNull()
			.references(() => investigationSessions.sessionId),
		factTableId: varchar("fact_table_id")
			.notNull()
			.references(() => tables.tableId, { onDelete: "cascade" }),
		viewTableId: varchar("view_table_id").references(() => tables.tableId, {
			onDelete: "set null",
		}),
		viewName: varchar("view_name").notNull(),
		runId: varchar("run_id"),
		relationshipIds: json("relationship_ids"),
		dimensionTableIds: json("dimension_table_ids"),
		dimensionColumns: json("dimension_columns"),
		isGrainVerified: boolean("is_grain_verified").notNull(),
		evidence: json(),
		createdAt: timestamp("created_at").notNull(),
	},
	(table) => [
		index("ix_enriched_views_run_id").using(
			"btree",
			table.runId.asc().nullsLast(),
		),
		index("ix_enriched_views_session_id").using(
			"btree",
			table.sessionId.asc().nullsLast(),
		),
		unique("uq_enriched_view_fact_table").on(table.factTableId),
	],
);

export const entropyObjects = metadataSchema.table(
	"entropy_objects",
	{
		objectId: varchar("object_id").primaryKey(),
		sessionId: varchar("session_id")
			.notNull()
			.references(() => investigationSessions.sessionId),
		layer: varchar().notNull(),
		dimension: varchar().notNull(),
		subDimension: varchar("sub_dimension").notNull(),
		target: varchar().notNull(),
		tableId: varchar("table_id").references(() => tables.tableId, {
			onDelete: "cascade",
		}),
		columnId: varchar("column_id").references(() => columns.columnId, {
			onDelete: "cascade",
		}),
		runId: varchar("run_id"),
		score: doublePrecision().notNull(),
		evidence: jsonb(),
		detectorId: varchar("detector_id").notNull(),
		sourceAnalysisIds: jsonb("source_analysis_ids"),
		computedAt: timestamp("computed_at").notNull(),
	},
	(table) => [
		index("idx_entropy_column").using(
			"btree",
			table.columnId.asc().nullsLast(),
		),
		index("idx_entropy_layer_dimension").using(
			"btree",
			table.layer.asc().nullsLast(),
			table.dimension.asc().nullsLast(),
		),
		index("idx_entropy_score").using("btree", table.score.asc().nullsLast()),
		index("idx_entropy_table").using("btree", table.tableId.asc().nullsLast()),
		index("idx_entropy_target").using("btree", table.target.asc().nullsLast()),
		index("ix_entropy_objects_session_id").using(
			"btree",
			table.sessionId.asc().nullsLast(),
		),
	],
);

export const entropyReadiness = metadataSchema.table(
	"entropy_readiness",
	{
		readinessId: varchar("readiness_id").primaryKey(),
		sessionId: varchar("session_id")
			.notNull()
			.references(() => investigationSessions.sessionId),
		target: varchar().notNull(),
		tableId: varchar("table_id").references(() => tables.tableId, {
			onDelete: "cascade",
		}),
		columnId: varchar("column_id").references(() => columns.columnId, {
			onDelete: "cascade",
		}),
		runId: varchar("run_id"),
		band: varchar().notNull(),
		worstIntentRisk: doublePrecision("worst_intent_risk").notNull(),
		intents: jsonb(),
		topDrivers: jsonb("top_drivers"),
		computedAt: timestamp("computed_at").notNull(),
	},
	(table) => [
		index("idx_readiness_column").using(
			"btree",
			table.columnId.asc().nullsLast(),
		),
		index("idx_readiness_table").using(
			"btree",
			table.tableId.asc().nullsLast(),
		),
		index("idx_readiness_target").using(
			"btree",
			table.target.asc().nullsLast(),
		),
		index("ix_entropy_readiness_session_id").using(
			"btree",
			table.sessionId.asc().nullsLast(),
		),
	],
);

export const fixLedger = metadataSchema.table(
	"fix_ledger",
	{
		fixId: varchar("fix_id").primaryKey(),
		sessionId: varchar("session_id")
			.notNull()
			.references(() => investigationSessions.sessionId),
		sourceId: varchar("source_id")
			.notNull()
			.references(() => sources.sourceId),
		actionName: varchar("action_name").notNull(),
		tableName: varchar("table_name").notNull(),
		columnName: varchar("column_name"),
		userInput: varchar("user_input").notNull(),
		interpretation: varchar().notNull(),
		status: varchar().notNull(),
		createdAt: timestamp("created_at").notNull(),
		supersededAt: timestamp("superseded_at"),
		supersededBy: varchar("superseded_by"),
	},
	(table) => [
		foreignKey({
			columns: [table.supersededBy],
			foreignColumns: [table.fixId],
			name: "fk_fix_ledger_superseded_by_fix_ledger",
		}),
		index("idx_fix_ledger_scope").using(
			"btree",
			table.sourceId.asc().nullsLast(),
			table.actionName.asc().nullsLast(),
			table.tableName.asc().nullsLast(),
			table.columnName.asc().nullsLast(),
		),
		index("idx_fix_ledger_source").using(
			"btree",
			table.sourceId.asc().nullsLast(),
		),
		index("ix_fix_ledger_session_id").using(
			"btree",
			table.sessionId.asc().nullsLast(),
		),
	],
);

export const investigationSessions = metadataSchema.table(
	"investigation_sessions",
	{
		sessionId: varchar("session_id").primaryKey(),
		status: varchar().notNull(),
		startedAt: timestamp("started_at").notNull(),
		endedAt: timestamp("ended_at"),
		durationSeconds: doublePrecision("duration_seconds"),
		intent: varchar().notNull(),
		contract: varchar(),
		vertical: varchar(),
		outcomeSummary: varchar("outcome_summary"),
		outcomePayload: json("outcome_payload"),
		stepCount: integer("step_count").notNull(),
	},
);

export const investigationSteps = metadataSchema.table(
	"investigation_steps",
	{
		stepId: varchar("step_id").primaryKey(),
		sessionId: varchar("session_id")
			.notNull()
			.references(() => investigationSessions.sessionId, {
				onDelete: "cascade",
			}),
		ordinal: integer().notNull(),
		toolName: varchar("tool_name").notNull(),
		arguments: json().notNull(),
		status: varchar().notNull(),
		resultSummary: varchar("result_summary"),
		error: varchar(),
		startedAt: timestamp("started_at").notNull(),
		durationSeconds: doublePrecision("duration_seconds").notNull(),
		target: varchar(),
		dimension: varchar(),
	},
	(table) => [
		index("idx_inv_step_target").using("btree", table.target.asc().nullsLast()),
		index("idx_inv_step_tool").using("btree", table.toolName.asc().nullsLast()),
		index("ix_investigation_steps_session_id").using(
			"btree",
			table.sessionId.asc().nullsLast(),
		),
	],
);

export const materializationRecipes = metadataSchema.table(
	"materialization_recipes",
	{
		recipeId: varchar("recipe_id").primaryKey(),
		sessionId: varchar("session_id")
			.notNull()
			.references(() => investigationSessions.sessionId),
		tableId: varchar("table_id")
			.notNull()
			.references(() => tables.tableId, { onDelete: "cascade" }),
		layer: varchar().notNull(),
		runId: varchar("run_id"),
		targetFqn: varchar("target_fqn").notNull(),
		ddl: varchar().notNull(),
		dependsOn: json("depends_on"),
		createdAt: timestamp("created_at").notNull(),
	},
	(table) => [
		index("idx_materialization_recipes_table").using(
			"btree",
			table.tableId.asc().nullsLast(),
		),
		index("ix_materialization_recipes_session_id").using(
			"btree",
			table.sessionId.asc().nullsLast(),
		),
		unique("uq_materialization_recipe_table_layer_run").on(
			table.tableId,
			table.layer,
			table.runId,
		),
	],
);

export const metadataSnapshotHead = metadataSchema.table(
	"metadata_snapshot_head",
	{
		headId: varchar("head_id").primaryKey(),
		target: varchar().notNull(),
		stage: varchar().notNull(),
		runId: varchar("run_id").notNull(),
		promotedAt: timestamp("promoted_at").notNull(),
		version: integer().notNull(),
	},
	(table) => [
		unique("uq_snapshot_head_target_stage").on(table.target, table.stage),
	],
);

export const queryExecutions = metadataSchema.table(
	"query_executions",
	{
		executionId: varchar("execution_id").primaryKey(),
		sessionId: varchar("session_id")
			.notNull()
			.references(() => investigationSessions.sessionId),
		sourceId: varchar("source_id")
			.notNull()
			.references(() => sources.sourceId, { onDelete: "cascade" }),
		question: text().notNull(),
		sqlExecuted: text("sql_executed").notNull(),
		executedAt: timestamp("executed_at").notNull(),
		success: boolean().notNull(),
		rowCount: integer("row_count"),
		errorMessage: text("error_message"),
		confidenceLevel: varchar("confidence_level").notNull(),
		contractName: varchar("contract_name"),
		entropyAction: varchar("entropy_action"),
	},
	(table) => [
		index("ix_query_executions_session_id").using(
			"btree",
			table.sessionId.asc().nullsLast(),
		),
		index("ix_query_executions_source_id").using(
			"btree",
			table.sourceId.asc().nullsLast(),
		),
	],
);

export const relationships = metadataSchema.table(
	"relationships",
	{
		relationshipId: varchar("relationship_id").primaryKey(),
		sessionId: varchar("session_id")
			.notNull()
			.references(() => investigationSessions.sessionId),
		runId: varchar("run_id"),
		fromTableId: varchar("from_table_id")
			.notNull()
			.references(() => tables.tableId, { onDelete: "cascade" }),
		fromColumnId: varchar("from_column_id")
			.notNull()
			.references(() => columns.columnId, { onDelete: "cascade" }),
		toTableId: varchar("to_table_id")
			.notNull()
			.references(() => tables.tableId, { onDelete: "cascade" }),
		toColumnId: varchar("to_column_id")
			.notNull()
			.references(() => columns.columnId, { onDelete: "cascade" }),
		relationshipType: varchar("relationship_type").notNull(),
		cardinality: varchar(),
		confidence: doublePrecision().notNull(),
		detectionMethod: varchar("detection_method"),
		evidence: json(),
		isConfirmed: boolean("is_confirmed").notNull(),
		confirmedAt: timestamp("confirmed_at"),
		confirmedBy: varchar("confirmed_by"),
		detectedAt: timestamp("detected_at").notNull(),
	},
	(table) => [
		index("idx_relationships_from").using(
			"btree",
			table.fromTableId.asc().nullsLast(),
		),
		index("idx_relationships_from_column").using(
			"btree",
			table.fromColumnId.asc().nullsLast(),
		),
		index("idx_relationships_from_table_column").using(
			"btree",
			table.fromTableId.asc().nullsLast(),
			table.fromColumnId.asc().nullsLast(),
		),
		index("idx_relationships_to").using(
			"btree",
			table.toTableId.asc().nullsLast(),
		),
		index("idx_relationships_to_column").using(
			"btree",
			table.toColumnId.asc().nullsLast(),
		),
		index("idx_relationships_to_table_column").using(
			"btree",
			table.toTableId.asc().nullsLast(),
			table.toColumnId.asc().nullsLast(),
		),
		index("ix_relationships_run_id").using(
			"btree",
			table.runId.asc().nullsLast(),
		),
		index("ix_relationships_session_id").using(
			"btree",
			table.sessionId.asc().nullsLast(),
		),
		unique("uq_relationship_columns_method").on(
			table.sessionId,
			table.runId,
			table.fromColumnId,
			table.toColumnId,
			table.detectionMethod,
		),
	],
);

export const semanticAnnotations = metadataSchema.table(
	"semantic_annotations",
	{
		annotationId: varchar("annotation_id").primaryKey(),
		sessionId: varchar("session_id")
			.notNull()
			.references(() => investigationSessions.sessionId),
		columnId: varchar("column_id")
			.notNull()
			.references(() => columns.columnId, { onDelete: "cascade" }),
		runId: varchar("run_id"),
		semanticRole: varchar("semantic_role"),
		entityType: varchar("entity_type"),
		businessName: varchar("business_name"),
		businessDescription: text("business_description"),
		businessConcept: varchar("business_concept"),
		temporalBehavior: varchar("temporal_behavior"),
		unitSourceColumn: varchar("unit_source_column"),
		annotationSource: varchar("annotation_source"),
		annotatedAt: timestamp("annotated_at").notNull(),
		annotatedBy: varchar("annotated_by"),
		confidence: doublePrecision(),
	},
	(table) => [
		index("ix_semantic_annotations_session_id").using(
			"btree",
			table.sessionId.asc().nullsLast(),
		),
		unique("uq_column_semantic_annotation").on(table.columnId, table.runId),
	],
);

export const sessionTables = metadataSchema.table(
	"session_tables",
	{
		sessionId: varchar("session_id")
			.notNull()
			.references(() => investigationSessions.sessionId, {
				onDelete: "cascade",
			}),
		tableId: varchar("table_id")
			.notNull()
			.references(() => tables.tableId, { onDelete: "cascade" }),
	},
	(table) => [
		primaryKey({
			columns: [table.sessionId, table.tableId],
			name: "pk_session_tables",
		}),
		index("idx_session_tables_table").using(
			"btree",
			table.tableId.asc().nullsLast(),
		),
	],
);

export const sliceDefinitions = metadataSchema.table(
	"slice_definitions",
	{
		sliceId: varchar("slice_id").primaryKey(),
		sessionId: varchar("session_id")
			.notNull()
			.references(() => investigationSessions.sessionId),
		runId: varchar("run_id"),
		tableId: varchar("table_id")
			.notNull()
			.references(() => tables.tableId, { onDelete: "cascade" }),
		columnId: varchar("column_id")
			.notNull()
			.references(() => columns.columnId, { onDelete: "cascade" }),
		columnName: varchar("column_name"),
		slicePriority: integer("slice_priority").notNull(),
		sliceType: varchar("slice_type").notNull(),
		distinctValues: json("distinct_values"),
		valueCount: integer("value_count"),
		reasoning: text(),
		businessContext: text("business_context"),
		confidence: doublePrecision(),
		sqlTemplate: text("sql_template"),
		detectionSource: varchar("detection_source").notNull(),
		createdAt: timestamp("created_at").notNull(),
	},
	(table) => [
		index("idx_slice_definitions_column").using(
			"btree",
			table.columnId.asc().nullsLast(),
		),
		index("idx_slice_definitions_table").using(
			"btree",
			table.tableId.asc().nullsLast(),
		),
		index("ix_slice_definitions_session_id").using(
			"btree",
			table.sessionId.asc().nullsLast(),
		),
	],
);

export const slicingViews = metadataSchema.table(
	"slicing_views",
	{
		viewId: varchar("view_id").primaryKey(),
		sessionId: varchar("session_id")
			.notNull()
			.references(() => investigationSessions.sessionId),
		factTableId: varchar("fact_table_id")
			.notNull()
			.references(() => tables.tableId, { onDelete: "cascade" }),
		viewName: varchar("view_name").notNull(),
		runId: varchar("run_id"),
		sliceDefinitionIds: json("slice_definition_ids"),
		sliceColumns: json("slice_columns"),
		isGrainVerified: boolean("is_grain_verified").notNull(),
		createdAt: timestamp("created_at").notNull(),
	},
	(table) => [
		index("ix_slicing_views_run_id").using(
			"btree",
			table.runId.asc().nullsLast(),
		),
		index("ix_slicing_views_session_id").using(
			"btree",
			table.sessionId.asc().nullsLast(),
		),
		unique("uq_slicing_view_fact_table").on(table.factTableId),
	],
);

export const snippetUsage = metadataSchema.table(
	"snippet_usage",
	{
		usageId: varchar("usage_id").primaryKey(),
		sessionId: varchar("session_id")
			.notNull()
			.references(() => investigationSessions.sessionId),
		executionId: varchar("execution_id").notNull(),
		executionType: varchar("execution_type").notNull(),
		snippetId: varchar("snippet_id").references(() => sqlSnippets.snippetId, {
			onDelete: "cascade",
		}),
		usageType: varchar("usage_type").notNull(),
		matchConfidence: doublePrecision("match_confidence").notNull(),
		sqlMatchRatio: doublePrecision("sql_match_ratio").notNull(),
		stepId: varchar("step_id"),
		createdAt: timestamp("created_at").notNull(),
	},
	(table) => [
		index("ix_snippet_usage_execution_id").using(
			"btree",
			table.executionId.asc().nullsLast(),
		),
		index("ix_snippet_usage_session_id").using(
			"btree",
			table.sessionId.asc().nullsLast(),
		),
		index("ix_snippet_usage_snippet_id").using(
			"btree",
			table.snippetId.asc().nullsLast(),
		),
	],
);

export const sources = metadataSchema.table(
	"sources",
	{
		sourceId: varchar("source_id").primaryKey(),
		name: varchar().notNull(),
		sourceType: varchar("source_type").notNull(),
		connectionConfig: json("connection_config"),
		createdAt: timestamp("created_at").notNull(),
		updatedAt: timestamp("updated_at").notNull(),
		status: varchar(),
		stage: varchar(),
		backend: varchar(),
		discoveredSchema: json("discovered_schema"),
		archivedAt: timestamp("archived_at"),
	},
	(table) => [unique("uq_sources_name").on(table.name)],
);

export const sqlSnippets = metadataSchema.table(
	"sql_snippets",
	{
		snippetId: varchar("snippet_id").primaryKey(),
		sessionId: varchar("session_id")
			.notNull()
			.references(() => investigationSessions.sessionId),
		snippetType: varchar("snippet_type").notNull(),
		standardField: varchar("standard_field"),
		statement: varchar(),
		aggregation: varchar(),
		schemaMappingId: varchar("schema_mapping_id").notNull(),
		parameterValue: varchar("parameter_value"),
		normalizedExpression: varchar("normalized_expression"),
		inputFields: json("input_fields"),
		sql: text().notNull(),
		description: text().notNull(),
		columnMappings: json("column_mappings").notNull(),
		source: varchar().notNull(),
		llmModel: varchar("llm_model"),
		provenance: json(),
		executionCount: integer("execution_count").notNull(),
		failureCount: integer("failure_count").notNull(),
		lastUsedAt: timestamp("last_used_at"),
		columnHash: varchar("column_hash"),
		createdAt: timestamp("created_at").notNull(),
		updatedAt: timestamp("updated_at").notNull(),
	},
	(table) => [
		index("ix_sql_snippets_normalized_expression").using(
			"btree",
			table.normalizedExpression.asc().nullsLast(),
		),
		index("ix_sql_snippets_schema_mapping_id").using(
			"btree",
			table.schemaMappingId.asc().nullsLast(),
		),
		index("ix_sql_snippets_session_id").using(
			"btree",
			table.sessionId.asc().nullsLast(),
		),
		index("ix_sql_snippets_snippet_type").using(
			"btree",
			table.snippetType.asc().nullsLast(),
		),
		index("ix_sql_snippets_standard_field").using(
			"btree",
			table.standardField.asc().nullsLast(),
		),
		unique("uq_snippet_semantic_key").on(
			table.snippetType,
			table.standardField,
			table.statement,
			table.aggregation,
			table.schemaMappingId,
			table.parameterValue,
		),
	],
);

export const statisticalProfiles = metadataSchema.table(
	"statistical_profiles",
	{
		profileId: varchar("profile_id").primaryKey(),
		sessionId: varchar("session_id")
			.notNull()
			.references(() => investigationSessions.sessionId),
		columnId: varchar("column_id")
			.notNull()
			.references(() => columns.columnId, { onDelete: "cascade" }),
		runId: varchar("run_id"),
		profiledAt: timestamp("profiled_at").notNull(),
		layer: varchar().notNull(),
		totalCount: integer("total_count").notNull(),
		nullCount: integer("null_count").notNull(),
		distinctCount: integer("distinct_count"),
		nullRatio: doublePrecision("null_ratio"),
		cardinalityRatio: doublePrecision("cardinality_ratio"),
		isUnique: integer("is_unique"),
		isNumeric: integer("is_numeric"),
		profileData: json("profile_data").notNull(),
	},
	(table) => [
		index("idx_statistical_profiles_column").using(
			"btree",
			table.columnId.asc().nullsLast(),
			table.profiledAt.desc().nullsFirst(),
		),
		index("ix_statistical_profiles_session_id").using(
			"btree",
			table.sessionId.asc().nullsLast(),
		),
		unique("uq_statistical_profiles_column_run").on(
			table.columnId,
			table.runId,
		),
	],
);

export const statisticalQualityMetrics = metadataSchema.table(
	"statistical_quality_metrics",
	{
		metricId: varchar("metric_id").primaryKey(),
		sessionId: varchar("session_id")
			.notNull()
			.references(() => investigationSessions.sessionId),
		columnId: varchar("column_id")
			.notNull()
			.references(() => columns.columnId, { onDelete: "cascade" }),
		runId: varchar("run_id"),
		computedAt: timestamp("computed_at").notNull(),
		benfordCompliant: integer("benford_compliant"),
		hasOutliers: integer("has_outliers"),
		iqrOutlierRatio: doublePrecision("iqr_outlier_ratio"),
		zscoreOutlierRatio: doublePrecision("zscore_outlier_ratio"),
		qualityData: json("quality_data").notNull(),
	},
	(table) => [
		index("idx_statistical_quality_column").using(
			"btree",
			table.columnId.asc().nullsLast(),
			table.computedAt.desc().nullsFirst(),
		),
		index("ix_statistical_quality_metrics_session_id").using(
			"btree",
			table.sessionId.asc().nullsLast(),
		),
		unique("uq_statistical_quality_metrics_column_run").on(
			table.columnId,
			table.runId,
		),
	],
);

export const tableEntities = metadataSchema.table(
	"table_entities",
	{
		entityId: varchar("entity_id").primaryKey(),
		sessionId: varchar("session_id")
			.notNull()
			.references(() => investigationSessions.sessionId),
		tableId: varchar("table_id")
			.notNull()
			.references(() => tables.tableId, { onDelete: "cascade" }),
		runId: varchar("run_id"),
		detectedEntityType: varchar("detected_entity_type").notNull(),
		description: text(),
		confidence: doublePrecision(),
		evidence: json(),
		grainColumns: json("grain_columns"),
		isFactTable: boolean("is_fact_table"),
		isDimensionTable: boolean("is_dimension_table"),
		timeColumn: varchar("time_column"),
		detectionSource: varchar("detection_source"),
		detectedAt: timestamp("detected_at").notNull(),
	},
	(table) => [
		index("ix_table_entities_session_id").using(
			"btree",
			table.sessionId.asc().nullsLast(),
		),
		unique("uq_table_entity_table_run").on(table.tableId, table.runId),
	],
);

export const tables = metadataSchema.table(
	"tables",
	{
		tableId: varchar("table_id").primaryKey(),
		sourceId: varchar("source_id")
			.notNull()
			.references(() => sources.sourceId),
		tableName: varchar("table_name").notNull(),
		layer: varchar().notNull(),
		duckdbPath: varchar("duckdb_path"),
		rowCount: integer("row_count"),
		createdAt: timestamp("created_at").notNull(),
		lastProfiledAt: timestamp("last_profiled_at"),
	},
	(table) => [
		index("idx_tables_source").using("btree", table.sourceId.asc().nullsLast()),
		unique("uq_source_table_layer").on(
			table.sourceId,
			table.tableName,
			table.layer,
		),
	],
);

export const temporalColumnProfiles = metadataSchema.table(
	"temporal_column_profiles",
	{
		profileId: varchar("profile_id").primaryKey(),
		sessionId: varchar("session_id")
			.notNull()
			.references(() => investigationSessions.sessionId),
		columnId: varchar("column_id")
			.notNull()
			.references(() => columns.columnId, { onDelete: "cascade" }),
		runId: varchar("run_id"),
		profiledAt: timestamp("profiled_at").notNull(),
		minTimestamp: timestamp("min_timestamp").notNull(),
		maxTimestamp: timestamp("max_timestamp").notNull(),
		detectedGranularity: varchar("detected_granularity").notNull(),
		completenessRatio: doublePrecision("completeness_ratio"),
		hasSeasonality: boolean("has_seasonality"),
		hasTrend: boolean("has_trend"),
		isStale: boolean("is_stale"),
		profileData: json("profile_data").notNull(),
	},
	(table) => [
		index("idx_temporal_profiles_column").using(
			"btree",
			table.columnId.asc().nullsLast(),
		),
		index("ix_temporal_column_profiles_session_id").using(
			"btree",
			table.sessionId.asc().nullsLast(),
		),
		unique("uq_temporal_column_profiles_column_run").on(
			table.columnId,
			table.runId,
		),
	],
);

export const temporalSliceAnalyses = metadataSchema.table(
	"temporal_slice_analyses",
	{
		id: varchar().primaryKey(),
		sessionId: varchar("session_id")
			.notNull()
			.references(() => investigationSessions.sessionId),
		runId: varchar("run_id"),
		sliceTableName: varchar("slice_table_name", { length: 255 }).notNull(),
		timeColumn: varchar("time_column", { length: 255 }).notNull(),
		periodLabel: varchar("period_label", { length: 50 }).notNull(),
		periodStart: date("period_start").notNull(),
		periodEnd: date("period_end").notNull(),
		rowCount: integer("row_count"),
		expectedDays: integer("expected_days"),
		observedDays: integer("observed_days"),
		coverageRatio: doublePrecision("coverage_ratio"),
		isComplete: integer("is_complete"),
		hasEarlyCutoff: integer("has_early_cutoff"),
		daysMissingAtEnd: integer("days_missing_at_end"),
		lastDayRatio: doublePrecision("last_day_ratio"),
		zScore: doublePrecision("z_score"),
		rollingAvg: doublePrecision("rolling_avg"),
		rollingStd: doublePrecision("rolling_std"),
		isVolumeAnomaly: integer("is_volume_anomaly"),
		anomalyType: varchar("anomaly_type", { length: 20 }),
		periodOverPeriodChange: doublePrecision("period_over_period_change"),
		issuesJson: json("issues_json"),
		createdAt: timestamp("created_at").notNull(),
	},
	(table) => [
		index("ix_temporal_slice_analyses_period_label").using(
			"btree",
			table.periodLabel.asc().nullsLast(),
		),
		index("ix_temporal_slice_analyses_session_id").using(
			"btree",
			table.sessionId.asc().nullsLast(),
		),
		index("ix_temporal_slice_analyses_slice_table_name").using(
			"btree",
			table.sliceTableName.asc().nullsLast(),
		),
	],
);

export const typeCandidates = metadataSchema.table(
	"type_candidates",
	{
		candidateId: varchar("candidate_id").primaryKey(),
		sessionId: varchar("session_id")
			.notNull()
			.references(() => investigationSessions.sessionId),
		columnId: varchar("column_id")
			.notNull()
			.references(() => columns.columnId, { onDelete: "cascade" }),
		runId: varchar("run_id"),
		detectedAt: timestamp("detected_at").notNull(),
		dataType: varchar("data_type").notNull(),
		confidence: doublePrecision().notNull(),
		parseSuccessRate: doublePrecision("parse_success_rate"),
		failedExamples: json("failed_examples"),
		detectedPattern: varchar("detected_pattern"),
		patternMatchRate: doublePrecision("pattern_match_rate"),
		detectedUnit: varchar("detected_unit"),
		unitConfidence: doublePrecision("unit_confidence"),
		quarantineCount: integer("quarantine_count"),
		quarantineRate: doublePrecision("quarantine_rate"),
	},
	(table) => [
		index("idx_type_candidates_column").using(
			"btree",
			table.columnId.asc().nullsLast(),
		),
		index("ix_type_candidates_session_id").using(
			"btree",
			table.sessionId.asc().nullsLast(),
		),
	],
);

export const typeDecisions = metadataSchema.table(
	"type_decisions",
	{
		decisionId: varchar("decision_id").primaryKey(),
		sessionId: varchar("session_id")
			.notNull()
			.references(() => investigationSessions.sessionId),
		columnId: varchar("column_id")
			.notNull()
			.references(() => columns.columnId, { onDelete: "cascade" }),
		runId: varchar("run_id"),
		decidedType: varchar("decided_type").notNull(),
		decisionSource: varchar("decision_source").notNull(),
		decidedAt: timestamp("decided_at").notNull(),
		decidedBy: varchar("decided_by"),
		previousType: varchar("previous_type"),
		decisionReason: varchar("decision_reason"),
	},
	(table) => [
		index("idx_type_decisions_column").using(
			"btree",
			table.columnId.asc().nullsLast(),
		),
		index("ix_type_decisions_session_id").using(
			"btree",
			table.sessionId.asc().nullsLast(),
		),
		unique("uq_column_type_decision").on(table.columnId, table.runId),
	],
);

export const validationResults = metadataSchema.table(
	"validation_results",
	{
		resultId: varchar("result_id").primaryKey(),
		sessionId: varchar("session_id")
			.notNull()
			.references(() => investigationSessions.sessionId),
		validationId: varchar("validation_id").notNull(),
		tableIds: json("table_ids").notNull(),
		status: varchar().notNull(),
		severity: varchar().notNull(),
		passed: boolean().notNull(),
		message: text(),
		executedAt: timestamp("executed_at").notNull(),
		sqlUsed: text("sql_used"),
		details: json(),
	},
	(table) => [
		index("ix_validation_results_session_id").using(
			"btree",
			table.sessionId.asc().nullsLast(),
		),
		index("ix_validation_results_validation_id").using(
			"btree",
			table.validationId.asc().nullsLast(),
		),
	],
);

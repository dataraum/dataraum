import { defineRelations } from "drizzle-orm";
import * as schema from "./schema";

export const relations = defineRelations(schema, (r) => ({
	columnDriftSummaries: {
		investigationSessions: r.one.investigationSessions({
			from: r.columnDriftSummaries.sessionId,
			to: r.investigationSessions.sessionId,
		}),
	},
	investigationSessions: {
		columnDriftSummariess: r.many.columnDriftSummaries(),
		columnEligibilitys: r.many.columnEligibility(),
		columnSliceProfiless: r.many.columnSliceProfiles(),
		derivedColumnss: r.many.derivedColumns(),
		sourcessViaDetectedBusinessCycles: r.many.sources({
			from: r.investigationSessions.sessionId.through(
				r.detectedBusinessCycles.sessionId,
			),
			to: r.sources.sourceId.through(r.detectedBusinessCycles.sourceId),
			alias:
				"investigationSessions_sessionId_sources_sourceId_via_detectedBusinessCycles",
		}),
		enrichedViewss: r.many.enrichedViews(),
		entropyObjectss: r.many.entropyObjects(),
		entropyReadinesss: r.many.entropyReadiness(),
		fixLedgers: r.many.fixLedger(),
		sources: r.one.sources({
			from: r.investigationSessions.sourceId,
			to: r.sources.sourceId,
			alias: "investigationSessions_sourceId_sources_sourceId",
		}),
		investigationStepss: r.many.investigationSteps(),
		sourcessViaQueryExecutions: r.many.sources({
			from: r.investigationSessions.sessionId.through(
				r.queryExecutions.sessionId,
			),
			to: r.sources.sourceId.through(r.queryExecutions.sourceId),
			alias:
				"investigationSessions_sessionId_sources_sourceId_via_queryExecutions",
		}),
		relationshipss: r.many.relationships(),
		columnssViaSemanticAnnotations: r.many.columns({
			alias:
				"columns_columnId_investigationSessions_sessionId_via_semanticAnnotations",
		}),
		sliceDefinitionss: r.many.sliceDefinitions(),
		tablessViaSlicingViews: r.many.tables({
			alias: "tables_tableId_investigationSessions_sessionId_via_slicingViews",
		}),
		sqlSnippetssViaSnippetUsage: r.many.sqlSnippets({
			from: r.investigationSessions.sessionId.through(r.snippetUsage.sessionId),
			to: r.sqlSnippets.snippetId.through(r.snippetUsage.snippetId),
			alias:
				"investigationSessions_sessionId_sqlSnippets_snippetId_via_snippetUsage",
		}),
		sqlSnippetssSessionId: r.many.sqlSnippets({
			alias: "sqlSnippets_sessionId_investigationSessions_sessionId",
		}),
		columnssViaStatisticalProfiles: r.many.columns({
			alias:
				"columns_columnId_investigationSessions_sessionId_via_statisticalProfiles",
		}),
		columnssViaStatisticalQualityMetrics: r.many.columns({
			alias:
				"columns_columnId_investigationSessions_sessionId_via_statisticalQualityMetrics",
		}),
		tablessViaTableEntities: r.many.tables({
			from: r.investigationSessions.sessionId.through(
				r.tableEntities.sessionId,
			),
			to: r.tables.tableId.through(r.tableEntities.tableId),
			alias: "investigationSessions_sessionId_tables_tableId_via_tableEntities",
		}),
		columnssViaTemporalColumnProfiles: r.many.columns({
			alias:
				"columns_columnId_investigationSessions_sessionId_via_temporalColumnProfiles",
		}),
		temporalSliceAnalysess: r.many.temporalSliceAnalyses(),
		columnssViaTypeCandidates: r.many.columns({
			alias:
				"columns_columnId_investigationSessions_sessionId_via_typeCandidates",
		}),
		columnssViaTypeDecisions: r.many.columns({
			alias:
				"columns_columnId_investigationSessions_sessionId_via_typeDecisions",
		}),
		validationResultss: r.many.validationResults(),
	},
	columnEligibility: {
		investigationSessions: r.one.investigationSessions({
			from: r.columnEligibility.sessionId,
			to: r.investigationSessions.sessionId,
		}),
		sources: r.one.sources({
			from: r.columnEligibility.sourceId,
			to: r.sources.sourceId,
		}),
		tables: r.one.tables({
			from: r.columnEligibility.tableId,
			to: r.tables.tableId,
		}),
	},
	sources: {
		columnEligibilitys: r.many.columnEligibility(),
		investigationSessionssViaDetectedBusinessCycles:
			r.many.investigationSessions({
				alias:
					"investigationSessions_sessionId_sources_sourceId_via_detectedBusinessCycles",
			}),
		entropyObjectss: r.many.entropyObjects(),
		entropyReadinesss: r.many.entropyReadiness(),
		fixLedgers: r.many.fixLedger(),
		investigationSessionssSourceId: r.many.investigationSessions({
			alias: "investigationSessions_sourceId_sources_sourceId",
		}),
		investigationSessionssViaQueryExecutions: r.many.investigationSessions({
			alias:
				"investigationSessions_sessionId_sources_sourceId_via_queryExecutions",
		}),
		tabless: r.many.tables(),
	},
	tables: {
		columnEligibilitys: r.many.columnEligibility(),
		columnss: r.many.columns(),
		derivedColumnss: r.many.derivedColumns(),
		enrichedViewssFactTableId: r.many.enrichedViews({
			alias: "enrichedViews_factTableId_tables_tableId",
		}),
		enrichedViewssViewTableId: r.many.enrichedViews({
			alias: "enrichedViews_viewTableId_tables_tableId",
		}),
		entropyObjectss: r.many.entropyObjects(),
		entropyReadinesss: r.many.entropyReadiness(),
		relationshipssFromTableId: r.many.relationships({
			alias: "relationships_fromTableId_tables_tableId",
		}),
		relationshipssToTableId: r.many.relationships({
			alias: "relationships_toTableId_tables_tableId",
		}),
		sliceDefinitionss: r.many.sliceDefinitions(),
		investigationSessionssViaSlicingViews: r.many.investigationSessions({
			from: r.tables.tableId.through(r.slicingViews.factTableId),
			to: r.investigationSessions.sessionId.through(r.slicingViews.sessionId),
			alias: "tables_tableId_investigationSessions_sessionId_via_slicingViews",
		}),
		investigationSessionssViaTableEntities: r.many.investigationSessions({
			alias: "investigationSessions_sessionId_tables_tableId_via_tableEntities",
		}),
		sources: r.one.sources({
			from: r.tables.sourceId,
			to: r.sources.sourceId,
		}),
	},
	columnSliceProfiles: {
		investigationSessions: r.one.investigationSessions({
			from: r.columnSliceProfiles.sessionId,
			to: r.investigationSessions.sessionId,
		}),
		columnsSliceColumnId: r.one.columns({
			from: r.columnSliceProfiles.sliceColumnId,
			to: r.columns.columnId,
			alias: "columnSliceProfiles_sliceColumnId_columns_columnId",
		}),
		columnsSourceColumnId: r.one.columns({
			from: r.columnSliceProfiles.sourceColumnId,
			to: r.columns.columnId,
			alias: "columnSliceProfiles_sourceColumnId_columns_columnId",
		}),
	},
	columns: {
		columnSliceProfilessSliceColumnId: r.many.columnSliceProfiles({
			alias: "columnSliceProfiles_sliceColumnId_columns_columnId",
		}),
		columnSliceProfilessSourceColumnId: r.many.columnSliceProfiles({
			alias: "columnSliceProfiles_sourceColumnId_columns_columnId",
		}),
		tables: r.one.tables({
			from: r.columns.tableId,
			to: r.tables.tableId,
		}),
		derivedColumnss: r.many.derivedColumns(),
		entropyObjectss: r.many.entropyObjects(),
		entropyReadinesss: r.many.entropyReadiness(),
		relationshipssFromColumnId: r.many.relationships({
			alias: "relationships_fromColumnId_columns_columnId",
		}),
		relationshipssToColumnId: r.many.relationships({
			alias: "relationships_toColumnId_columns_columnId",
		}),
		investigationSessionssViaSemanticAnnotations: r.many.investigationSessions({
			from: r.columns.columnId.through(r.semanticAnnotations.columnId),
			to: r.investigationSessions.sessionId.through(
				r.semanticAnnotations.sessionId,
			),
			alias:
				"columns_columnId_investigationSessions_sessionId_via_semanticAnnotations",
		}),
		sliceDefinitionss: r.many.sliceDefinitions(),
		investigationSessionssViaStatisticalProfiles: r.many.investigationSessions({
			from: r.columns.columnId.through(r.statisticalProfiles.columnId),
			to: r.investigationSessions.sessionId.through(
				r.statisticalProfiles.sessionId,
			),
			alias:
				"columns_columnId_investigationSessions_sessionId_via_statisticalProfiles",
		}),
		investigationSessionssViaStatisticalQualityMetrics:
			r.many.investigationSessions({
				from: r.columns.columnId.through(r.statisticalQualityMetrics.columnId),
				to: r.investigationSessions.sessionId.through(
					r.statisticalQualityMetrics.sessionId,
				),
				alias:
					"columns_columnId_investigationSessions_sessionId_via_statisticalQualityMetrics",
			}),
		investigationSessionssViaTemporalColumnProfiles:
			r.many.investigationSessions({
				from: r.columns.columnId.through(r.temporalColumnProfiles.columnId),
				to: r.investigationSessions.sessionId.through(
					r.temporalColumnProfiles.sessionId,
				),
				alias:
					"columns_columnId_investigationSessions_sessionId_via_temporalColumnProfiles",
			}),
		investigationSessionssViaTypeCandidates: r.many.investigationSessions({
			from: r.columns.columnId.through(r.typeCandidates.columnId),
			to: r.investigationSessions.sessionId.through(r.typeCandidates.sessionId),
			alias:
				"columns_columnId_investigationSessions_sessionId_via_typeCandidates",
		}),
		investigationSessionssViaTypeDecisions: r.many.investigationSessions({
			from: r.columns.columnId.through(r.typeDecisions.columnId),
			to: r.investigationSessions.sessionId.through(r.typeDecisions.sessionId),
			alias:
				"columns_columnId_investigationSessions_sessionId_via_typeDecisions",
		}),
	},
	derivedColumns: {
		columns: r.one.columns({
			from: r.derivedColumns.derivedColumnId,
			to: r.columns.columnId,
		}),
		investigationSessions: r.one.investigationSessions({
			from: r.derivedColumns.sessionId,
			to: r.investigationSessions.sessionId,
		}),
		tables: r.one.tables({
			from: r.derivedColumns.tableId,
			to: r.tables.tableId,
		}),
	},
	enrichedViews: {
		tablesFactTableId: r.one.tables({
			from: r.enrichedViews.factTableId,
			to: r.tables.tableId,
			alias: "enrichedViews_factTableId_tables_tableId",
		}),
		investigationSessions: r.one.investigationSessions({
			from: r.enrichedViews.sessionId,
			to: r.investigationSessions.sessionId,
		}),
		tablesViewTableId: r.one.tables({
			from: r.enrichedViews.viewTableId,
			to: r.tables.tableId,
			alias: "enrichedViews_viewTableId_tables_tableId",
		}),
	},
	entropyObjects: {
		columns: r.one.columns({
			from: r.entropyObjects.columnId,
			to: r.columns.columnId,
		}),
		investigationSessions: r.one.investigationSessions({
			from: r.entropyObjects.sessionId,
			to: r.investigationSessions.sessionId,
		}),
		sources: r.one.sources({
			from: r.entropyObjects.sourceId,
			to: r.sources.sourceId,
		}),
		tables: r.one.tables({
			from: r.entropyObjects.tableId,
			to: r.tables.tableId,
		}),
	},
	entropyReadiness: {
		columns: r.one.columns({
			from: r.entropyReadiness.columnId,
			to: r.columns.columnId,
		}),
		investigationSessions: r.one.investigationSessions({
			from: r.entropyReadiness.sessionId,
			to: r.investigationSessions.sessionId,
		}),
		sources: r.one.sources({
			from: r.entropyReadiness.sourceId,
			to: r.sources.sourceId,
		}),
		tables: r.one.tables({
			from: r.entropyReadiness.tableId,
			to: r.tables.tableId,
		}),
	},
	fixLedger: {
		investigationSessions: r.one.investigationSessions({
			from: r.fixLedger.sessionId,
			to: r.investigationSessions.sessionId,
		}),
		sources: r.one.sources({
			from: r.fixLedger.sourceId,
			to: r.sources.sourceId,
		}),
		fixLedger: r.one.fixLedger({
			from: r.fixLedger.supersededBy,
			to: r.fixLedger.fixId,
			alias: "fixLedger_supersededBy_fixLedger_fixId",
		}),
		fixLedgers: r.many.fixLedger({
			alias: "fixLedger_supersededBy_fixLedger_fixId",
		}),
	},
	investigationSteps: {
		investigationSessions: r.one.investigationSessions({
			from: r.investigationSteps.sessionId,
			to: r.investigationSessions.sessionId,
		}),
	},
	relationships: {
		columnsFromColumnId: r.one.columns({
			from: r.relationships.fromColumnId,
			to: r.columns.columnId,
			alias: "relationships_fromColumnId_columns_columnId",
		}),
		tablesFromTableId: r.one.tables({
			from: r.relationships.fromTableId,
			to: r.tables.tableId,
			alias: "relationships_fromTableId_tables_tableId",
		}),
		investigationSessions: r.one.investigationSessions({
			from: r.relationships.sessionId,
			to: r.investigationSessions.sessionId,
		}),
		columnsToColumnId: r.one.columns({
			from: r.relationships.toColumnId,
			to: r.columns.columnId,
			alias: "relationships_toColumnId_columns_columnId",
		}),
		tablesToTableId: r.one.tables({
			from: r.relationships.toTableId,
			to: r.tables.tableId,
			alias: "relationships_toTableId_tables_tableId",
		}),
	},
	sliceDefinitions: {
		columns: r.one.columns({
			from: r.sliceDefinitions.columnId,
			to: r.columns.columnId,
		}),
		investigationSessions: r.one.investigationSessions({
			from: r.sliceDefinitions.sessionId,
			to: r.investigationSessions.sessionId,
		}),
		tables: r.one.tables({
			from: r.sliceDefinitions.tableId,
			to: r.tables.tableId,
		}),
	},
	sqlSnippets: {
		investigationSessionss: r.many.investigationSessions({
			alias:
				"investigationSessions_sessionId_sqlSnippets_snippetId_via_snippetUsage",
		}),
		investigationSessions: r.one.investigationSessions({
			from: r.sqlSnippets.sessionId,
			to: r.investigationSessions.sessionId,
			alias: "sqlSnippets_sessionId_investigationSessions_sessionId",
		}),
	},
	temporalSliceAnalyses: {
		investigationSessions: r.one.investigationSessions({
			from: r.temporalSliceAnalyses.sessionId,
			to: r.investigationSessions.sessionId,
		}),
	},
	validationResults: {
		investigationSessions: r.one.investigationSessions({
			from: r.validationResults.sessionId,
			to: r.investigationSessions.sessionId,
		}),
	},
}));

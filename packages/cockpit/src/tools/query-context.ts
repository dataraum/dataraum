// Schema context for the query sub-agent (DAT-485).
//
// The nested `answer` sub-agent has only [snippet_search, run_steps] — it can't
// call list_tables — so it needs the workspace schema injected into its prompt to
// write valid SQL. This builds the engine's `schema_info` equivalent: each TYPED
// lake table, addressed as `lake.typed.<physical_name>`, with its columns' types
// and (the field_mappings replacement) the per-column `business_concept` from the
// promoted semantic run — so the model maps a question's terms to concrete columns
// inline, no separate field-mapping artifact (DAT-485: field_mappings NOT built).
//
// Prefer-enriched (DAT-486): mirrors the engine's shared schema_info builder
// (graphs/agent.py `_build_schema_info`) — when begin_session has materialized
// enriched views (pre-joined fact+dimension supersets, layer `enriched`, which
// resolve to the `typed` DuckDB schema via schema_for_layer), surface ONLY those;
// otherwise the `typed` tables. The producer GraphAgent mints snippets against the
// same enriched views, so matching its table context is what lets the consumer's
// reuse classify as exact_reuse rather than adapted. raw / quarantine stay
// ingestion-internal. The pure `formatSchema` + `preferEnriched` are unit-tested;
// the thin Drizzle reads are smoke/integration-covered.

import { and, asc, eq, inArray, isNull } from "drizzle-orm";

import { metadataDb } from "../db/metadata/client";
import {
	columns,
	currentSemanticAnnotations,
	sources,
	tables,
} from "../db/metadata/schema";
import { LAKE_ALIAS } from "../duckdb/lake";

/** The clean, analysis-ready layer a question is answered over. */
const TYPED_LAYER = "typed";
/** Pre-joined fact+dimension supersets begin_session materializes (DAT-486). */
const ENRICHED_LAYER = "enriched";

/**
 * The DuckDB schema that physically holds a layer's artifacts. Mirrors the
 * engine's core/duckdb_naming.schema_for_layer: enriched views are derived
 * artifacts of typed tables, so they live in the `typed` schema (addressed as
 * `lake.typed.<view_name>`), not a sibling `enriched` schema.
 */
function schemaForLayer(layer: string): string {
	return layer === ENRICHED_LAYER ? TYPED_LAYER : layer;
}

/**
 * Mirror the engine's prefer-enriched rule (graphs/agent.py `_build_schema_info`):
 * when ANY enriched view exists, surface ONLY the enriched views (pre-joined
 * supersets of the typed facts they're built from); otherwise the typed tables.
 * All-or-nothing, matching the producer — so the consumer addresses the same
 * tables the snippets were minted against. Pure; unit-tested.
 */
export function preferEnriched<T extends { layer: string }>(rows: T[]): T[] {
	const enriched = rows.filter((r) => r.layer === ENRICHED_LAYER);
	return enriched.length > 0 ? enriched : rows;
}

/** One typed table — addressed in SQL as `lake.typed.<physicalName>`. */
export interface SchemaTableRow {
	tableId: string;
	/** Raw DuckDB table name (the `lake.<layer>.<name>` address); may embed a
	 * content-keyed `src_<digest>__` prefix for uploads — fine inside SQL. */
	physicalName: string;
	layer: string;
}

/** One column of a typed table. */
export interface SchemaColumnRow {
	tableId: string;
	columnId: string;
	name: string;
	resolvedType: string | null;
}

/** The promoted semantic concept for a column (the field-mapping replacement). */
export interface SchemaConceptRow {
	columnId: string;
	businessConcept: string | null;
}

/**
 * Format the typed schema as the sub-agent's `<schema>` prompt block (pure).
 * Tables sorted by physical name, columns by name — deterministic. Each column
 * shows its resolved type and, when the semantic run mapped one, its
 * `[concept: …]`. Empty workspace → a one-line note.
 */
export function formatSchema(
	tableRows: SchemaTableRow[],
	columnRows: SchemaColumnRow[],
	conceptRows: SchemaConceptRow[],
): string {
	if (tableRows.length === 0) {
		return "<schema>\n(No queryable tables in the workspace yet — nothing to query.)\n</schema>";
	}

	const conceptByColumn = new Map<string, string>();
	for (const c of conceptRows) {
		if (c.businessConcept) conceptByColumn.set(c.columnId, c.businessConcept);
	}

	const columnsByTable = new Map<string, SchemaColumnRow[]>();
	for (const col of columnRows) {
		const list = columnsByTable.get(col.tableId);
		if (list) list.push(col);
		else columnsByTable.set(col.tableId, [col]);
	}

	const sortedTables = [...tableRows].sort((a, b) =>
		a.physicalName.localeCompare(b.physicalName),
	);

	const tableBlocks = sortedTables.map((t) => {
		const cols = [...(columnsByTable.get(t.tableId) ?? [])].sort((a, b) =>
			a.name.localeCompare(b.name),
		);
		const address = `${LAKE_ALIAS}.${schemaForLayer(t.layer)}.${t.physicalName}`;
		const colLines = cols.map((c) => {
			const type = c.resolvedType ?? "unknown";
			const concept = conceptByColumn.get(c.columnId);
			const conceptTag = concept ? `  [concept: ${concept}]` : "";
			return `  - "${c.name}" :: ${type}${conceptTag}`;
		});
		return `Table ${address}:\n${colLines.join("\n")}`;
	});

	return (
		"<schema>\n" +
		`Address each table in SQL as ${LAKE_ALIAS}.<layer>.<name> exactly as shown ` +
		"(quote column names with double quotes). Use a column's [concept: …] tag to " +
		"map a question's business terms to the concrete column.\n\n" +
		`${tableBlocks.join("\n\n")}\n` +
		"</schema>"
	);
}

/**
 * Read the typed-layer schema for the active workspace and format it as the
 * sub-agent's `<schema>` block. Three lean reads (tables, columns, semantic
 * concepts) over non-archived sources, joined in the pure `formatSchema`.
 */
export async function buildSchemaBlock(): Promise<string> {
	const tableRows = await metadataDb
		.select({
			tableId: tables.tableId,
			physicalName: tables.tableName,
			layer: tables.layer,
		})
		.from(tables)
		.innerJoin(sources, eq(sources.sourceId, tables.sourceId))
		.where(
			and(
				isNull(sources.archivedAt),
				inArray(tables.layer, [TYPED_LAYER, ENRICHED_LAYER]),
			),
		)
		.orderBy(asc(tables.tableName));

	const columnRows = await metadataDb
		.select({
			tableId: columns.tableId,
			columnId: columns.columnId,
			name: columns.columnName,
			resolvedType: columns.resolvedType,
		})
		.from(columns)
		.innerJoin(tables, eq(tables.tableId, columns.tableId))
		.innerJoin(sources, eq(sources.sourceId, tables.sourceId))
		.where(
			and(
				isNull(sources.archivedAt),
				inArray(tables.layer, [TYPED_LAYER, ENRICHED_LAYER]),
			),
		);

	const conceptRows = await metadataDb
		.select({
			columnId: currentSemanticAnnotations.columnId,
			businessConcept: currentSemanticAnnotations.businessConcept,
		})
		.from(currentSemanticAnnotations);

	// Prefer-enriched (mirror graphs/agent.py): when enriched views exist, surface
	// ONLY those; else the typed tables. formatSchema renders only the shown
	// tables' columns, so passing the full typed+enriched column set is safe.
	const shownTables = preferEnriched(
		tableRows.map((t) => ({
			tableId: t.tableId ?? "",
			physicalName: t.physicalName ?? "",
			layer: t.layer ?? TYPED_LAYER,
		})),
	);

	return formatSchema(
		shownTables,
		columnRows.map((c) => ({
			tableId: c.tableId ?? "",
			columnId: c.columnId ?? "",
			name: c.name ?? "",
			resolvedType: c.resolvedType ?? null,
		})),
		conceptRows.map((c) => ({
			columnId: c.columnId ?? "",
			businessConcept: c.businessConcept ?? null,
		})),
	);
}

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
// ingestion-internal.
//
// An enriched view's columns come from a LIVE `DESCRIBE` on the READ_ONLY lake
// reader (mirroring the engine's `_describe_table`), NOT the column metadata: the
// view is `SELECT f.*, <dim cols>` but the engine registers Column metadata for
// the dim columns ONLY, so a metadata read would hide the fact's measures. DESCRIBE
// returns the full set (names + types, no `[concept:]` tags — the engine drops them
// too); a lake-read failure falls back to the typed tables. The pure `formatSchema`
// + `preferEnriched` are unit-tested; the Drizzle reads + the DESCRIBE are
// smoke/integration-covered.

import { and, asc, eq, inArray, isNull } from "drizzle-orm";

import { metadataDb } from "../db/metadata/client";
import {
	columns,
	currentSemanticAnnotations,
	sources,
	tables,
} from "../db/metadata/schema";
import { getLakeConnection, LAKE_ALIAS } from "../duckdb/lake";
import { readerToResult } from "../duckdb/query-result";

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

/**
 * The live columns of each enriched view, via `DESCRIBE` on the READ_ONLY lake
 * reader — the same source the engine's GraphAgent uses (`_describe_table`). The
 * enriched VIEW is `SELECT f.*, <dim cols>`, but the engine registers Column
 * METADATA for the dim columns ONLY, so a metadata read would hide the fact's
 * measures; DESCRIBE returns the full set. Returns null when the lake read fails
 * (views not yet checkpointed / lake unreachable) so the caller can fall back to
 * the typed tables — a degraded but non-empty schema block beats a hard failure.
 * Carries names + types only (no `[concept:]` tags), as the engine's enriched
 * schema_info does.
 */
async function describeEnrichedViews(
	views: SchemaTableRow[],
): Promise<SchemaColumnRow[] | null> {
	try {
		const conn = await getLakeConnection();
		const out: SchemaColumnRow[] = [];
		for (const v of views) {
			const address = `${LAKE_ALIAS}.${schemaForLayer(v.layer)}."${v.physicalName}"`;
			const reader = await conn.runAndReadAll(`DESCRIBE ${address}`);
			for (const r of readerToResult(reader).rows) {
				const name = r.column_name;
				if (typeof name !== "string") continue;
				out.push({
					tableId: v.tableId,
					// Synthetic id — enriched columns carry no semantic annotation, so it
					// is never used for a concept lookup (concepts are [] for this path).
					columnId: `${v.tableId}:${name}`,
					name,
					resolvedType:
						typeof r.column_type === "string" ? r.column_type : null,
				});
			}
		}
		return out;
	} catch (err) {
		console.warn(
			`[cockpit] enriched DESCRIBE failed — falling back to typed tables: ${err}`,
		);
		return null;
	}
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

/** The promoted semantic concept for a column (the field-mapping replacement),
 * plus the resolved stock/flow adjudication (DAT-509): `temporalBehavior` is
 * the pooled-resolved value the resolve layer wrote onto the annotation
 * (`additive` = flow, `point_in_time` = stock — never SUM a stock across
 * periods), and `temporalBehaviorContested` marks an open witness conflict the
 * agent must caveat instead of silently aggregating over. */
export interface SchemaConceptRow {
	columnId: string;
	businessConcept: string | null;
	temporalBehavior: string | null;
	temporalBehaviorContested: boolean | null;
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

	const conceptByColumn = new Map<string, SchemaConceptRow>();
	for (const c of conceptRows) {
		if (c.businessConcept || c.temporalBehavior)
			conceptByColumn.set(c.columnId, c);
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
			const semantic = conceptByColumn.get(c.columnId);
			const conceptTag = semantic?.businessConcept
				? `  [concept: ${semantic.businessConcept}]`
				: "";
			// Mirror the engine's render (graphs/context.py): the resolved
			// stock/flow behaviour as a parenthesized marker; an open witness
			// conflict becomes an explicit caveat tag (DAT-509).
			const temporalTag = semantic?.temporalBehavior
				? ` (${semantic.temporalBehavior})`
				: "";
			const contestedTag = semantic?.temporalBehaviorContested
				? "  [stock/flow contested]"
				: "";
			return `  - "${c.name}" :: ${type}${conceptTag}${temporalTag}${contestedTag}`;
		});
		return `Table ${address}:\n${colLines.join("\n")}`;
	});

	return (
		"<schema>\n" +
		`Address each table in SQL as ${LAKE_ALIAS}.<layer>.<name> exactly as shown ` +
		"(quote column names with double quotes). Use a column's [concept: …] tag to " +
		"map a question's business terms to the concrete column. A column marked " +
		"(point_in_time) is a stock — never SUM it across periods (use the last or " +
		"average value); (additive) is a flow and sums safely. A column tagged " +
		"[stock/flow contested] has disagreeing evidence about which it is — state " +
		"that caveat in your answer when aggregating over it.\n\n" +
		`${tableBlocks.join("\n\n")}\n` +
		"</schema>"
	);
}

/**
 * Read the schema for the active workspace and format it as the sub-agent's
 * `<schema>` block. Prefer-enriched: when begin_session has materialized enriched
 * views, surface ONLY those (columns from a live DESCRIBE — metadata registers dim
 * columns only, so it would hide the fact's measures), falling back to the typed
 * tables on a lake-read failure. The typed path reads columns + semantic concepts
 * from metadata, so typed columns keep their `[concept:]` tags.
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

	const mapped: SchemaTableRow[] = tableRows.map((t) => ({
		tableId: t.tableId ?? "",
		physicalName: t.physicalName ?? "",
		layer: t.layer ?? TYPED_LAYER,
	}));

	// Prefer-enriched (mirror graphs/agent.py _build_schema_info): when enriched
	// views exist, surface ONLY those — columns from a live DESCRIBE (the view is
	// `SELECT f.*, dims`; metadata registers dims only). Fall through to the typed
	// tables if the lake read fails (views not yet checkpointed / unreachable).
	const enrichedViews = mapped.filter((t) => t.layer === ENRICHED_LAYER);
	if (enrichedViews.length > 0) {
		const enrichedColumns = await describeEnrichedViews(enrichedViews);
		if (enrichedColumns)
			return formatSchema(enrichedViews, enrichedColumns, []);
	}

	// Typed path: metadata columns + their semantic-concept tags.
	const typedTables = mapped.filter((t) => t.layer === TYPED_LAYER);
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
		.where(and(isNull(sources.archivedAt), eq(tables.layer, TYPED_LAYER)));

	const conceptRows = await metadataDb
		.select({
			columnId: currentSemanticAnnotations.columnId,
			businessConcept: currentSemanticAnnotations.businessConcept,
			temporalBehavior: currentSemanticAnnotations.temporalBehavior,
			temporalBehaviorContested:
				currentSemanticAnnotations.temporalBehaviorContested,
		})
		.from(currentSemanticAnnotations);

	return formatSchema(
		typedTables,
		columnRows.map((c) => ({
			tableId: c.tableId ?? "",
			columnId: c.columnId ?? "",
			name: c.name ?? "",
			resolvedType: c.resolvedType ?? null,
		})),
		conceptRows.map((c) => ({
			columnId: c.columnId ?? "",
			businessConcept: c.businessConcept ?? null,
			temporalBehavior: c.temporalBehavior ?? null,
			temporalBehaviorContested: c.temporalBehaviorContested ?? null,
		})),
	);
}

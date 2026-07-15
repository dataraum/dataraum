// Schema context for the query sub-agent (DAT-485).
//
// The nested `answer` sub-agent has only [snippet_search, run_steps] — it can't
// call list_tables — so it needs the workspace schema injected into its prompt to
// write valid SQL. This builds the engine's `schema_info` equivalent: each TYPED
// lake table, addressed as `lake.typed.<physical_name>`, with its columns' types
// and (the field_mappings replacement) the per-column `meaning` from the
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
// returns the full set (names + types, no `[meaning:]` tags — the engine drops them
// too); a lake-read failure falls back to the typed tables. The pure `formatSchema`
// + `preferEnriched` are unit-tested; the Drizzle reads + the DESCRIBE are
// smoke/integration-covered.

import { asc, desc, eq, inArray, isNull } from "drizzle-orm";

import { metadataDb } from "../db/metadata/client";
import {
	currentColumnConcepts,
	currentColumns,
	currentDimensionHierarchies,
	currentEnrichedViews,
	currentRelationships,
	currentSliceDefinitions,
	currentTableEntities,
	currentTables,
	sources,
} from "../db/metadata/schema";
import { LAKE_ALIAS, withLakeConnection } from "../duckdb/lake";
import { readerToResult } from "../duckdb/query-result";
import { type DriverRanking, lookDrivers } from "./look-drivers";
import { projectTableEntity, type TableEntity } from "./look-table";

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
 * Carries names + types only (no `[meaning:]` tags), as the engine's enriched
 * schema_info does.
 */
async function describeEnrichedViews(
	views: SchemaTableRow[],
): Promise<SchemaColumnRow[] | null> {
	try {
		return await withLakeConnection(async (conn) => {
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
		});
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
 * periods). This is the reconciled verdict served as settled fact: the resolve
 * pass already adjudicated the LLM claim vs the data-grounded structural
 * witness (DAT-786), so there is no separate doubt flag to carry here. */
export interface SchemaConceptRow {
	columnId: string;
	meaning: string | null;
	temporalBehavior: string | null;
}

/**
 * Format the typed schema as the sub-agent's `<schema>` prompt block (pure).
 * Tables sorted by physical name, columns by name — deterministic. Each column
 * shows its resolved type and, when the semantic run mapped one, its
 * `[meaning: …]`. Empty workspace → a one-line note.
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
		if (c.meaning || c.temporalBehavior) conceptByColumn.set(c.columnId, c);
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
			const conceptTag = semantic?.meaning
				? `  [meaning: ${semantic.meaning}]`
				: "";
			// Mirror the engine's render (graphs/context.py): the resolved
			// stock/flow behaviour as a parenthesized marker, served as settled
			// fact (see SchemaConceptRow).
			const temporalTag = semantic?.temporalBehavior
				? ` (${semantic.temporalBehavior})`
				: "";
			return `  - "${c.name}" :: ${type}${conceptTag}${temporalTag}`;
		});
		return `Table ${address}:\n${colLines.join("\n")}`;
	});

	return (
		"<schema>\n" +
		`Address each table in SQL as ${LAKE_ALIAS}.<layer>.<name> exactly as shown ` +
		"(quote column names with double quotes). Use a column's [meaning: …] tag — its " +
		"authored business meaning — to map a question's business terms to the concrete " +
		"column. The (additive)/" +
		"(point_in_time) marker is the stock/flow verdict RECONCILED FROM THE DATA — it is " +
		"authoritative: it OVERRIDES the meaning's wording and any domain intuition. A column " +
		"named like a balance, level, or position is NOT a stock if it is marked (additive) " +
		"— the data decided. (additive) is a flow: SUM it across ALL periods, never restrict " +
		"to a single period. (point_in_time) is a stock: never SUM it across periods (take " +
		"the latest period's value, or an average).\n\n" +
		`${tableBlocks.join("\n\n")}\n` +
		"</schema>"
	);
}

/**
 * Read the schema for the active workspace and format it as the sub-agent's
 * `<schema>` block. Prefer-enriched: when begin_session has materialized enriched
 * views, surface ONLY those (columns from a live DESCRIBE — metadata registers dim
 * columns only, so it would hide the fact's measures), falling back to the typed
 * tables on a lake-read failure. The typed path reads columns + column meanings
 * from metadata, so typed columns keep their `[meaning:]` tags.
 */
export async function buildSchemaBlock(): Promise<string> {
	// Head-scoped reads (DAT-677): enriched views from the promoted catalog head
	// (current_enriched_views), typed tables from the analyzed-representative
	// surface (current_tables) — never the raw `tables` view, whose typed row
	// exists from the START of add_source's typing phase while the generation
	// head is only promoted at the END, so a raw read can leak a not-yet-analyzed
	// table into the sub-agent's context.
	const [enrichedRows, typedRows] = await Promise.all([
		metadataDb
			.select({
				tableId: currentEnrichedViews.viewTableId,
				physicalName: currentEnrichedViews.viewName,
			})
			.from(currentEnrichedViews),
		metadataDb
			.select({
				tableId: currentTables.tableId,
				physicalName: currentTables.tableName,
			})
			.from(currentTables)
			.innerJoin(sources, eq(sources.sourceId, currentTables.sourceId))
			.where(isNull(sources.archivedAt))
			.orderBy(asc(currentTables.tableName)),
	]);

	// Prefer-enriched (mirror graphs/agent.py _build_schema_info): when enriched
	// views exist, surface ONLY those — columns from a live DESCRIBE (the view is
	// `SELECT f.*, dims`; metadata registers dims only). Fall through to the typed
	// tables if the lake read fails (views not yet checkpointed / unreachable).
	const enrichedViews: SchemaTableRow[] = enrichedRows
		.filter((v) => v.tableId && v.physicalName)
		.map((v) => ({
			tableId: v.tableId as string,
			physicalName: v.physicalName as string,
			layer: ENRICHED_LAYER,
		}));
	if (enrichedViews.length > 0) {
		const enrichedColumns = await describeEnrichedViews(enrichedViews);
		if (enrichedColumns)
			return formatSchema(enrichedViews, enrichedColumns, []);
	}

	// Typed path: metadata columns + their semantic-concept tags. current_columns
	// is already scoped to current typed tables; the join adds the archived filter.
	const typedTables: SchemaTableRow[] = typedRows.map((t) => ({
		tableId: t.tableId ?? "",
		physicalName: t.physicalName ?? "",
		layer: TYPED_LAYER,
	}));
	const columnRows = await metadataDb
		.select({
			tableId: currentColumns.tableId,
			columnId: currentColumns.columnId,
			name: currentColumns.columnName,
			resolvedType: currentColumns.resolvedType,
		})
		.from(currentColumns)
		.innerJoin(currentTables, eq(currentTables.tableId, currentColumns.tableId))
		.innerJoin(sources, eq(sources.sourceId, currentTables.sourceId))
		.where(isNull(sources.archivedAt));

	// Catalogue-grain concepts live on currentColumnConcepts (DAT-637), authored by
	// the table agent and sealed under the catalogue head — no longer on the
	// object-grain currentSemanticAnnotations.
	const conceptRows = await metadataDb
		.select({
			columnId: currentColumnConcepts.columnId,
			meaning: currentColumnConcepts.meaning,
			temporalBehavior: currentColumnConcepts.temporalBehavior,
		})
		.from(currentColumnConcepts);

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
			meaning: c.meaning ?? null,
			temporalBehavior: c.temporalBehavior ?? null,
		})),
	);
}

// --- Dimension catalog (DAT-538) -------------------------------------------------
//
// The slice catalog (DAT-536 `current_slice_definitions`) + hierarchies (DAT-537
// `current_dimension_hierarchies`) give the answer sub-agent the workspace's natural
// ANALYSIS DIMENSIONS and the structural relationships between them — context the
// `<schema>` block lacks. This is purely informational (inform-don't-block): it does
// NOT gate anything. The genuinely additive parts are the hierarchies — an ALIAS
// group ("region ≡ region_code") tells the agent not to double-count one axis as
// two, and a DRILL-DOWN chain ("city → region → country") lets it roll a "by region"
// question up from city-grain data. The flat dimension list is a soft hint at the
// columns worth grouping by. (A near-unique GROUP BY is handled separately, as a
// run-time caveat in grain-note.ts — not here, and never as a block.)

/** One catalogued slice axis (a natural analysis dimension). */
export interface CatalogAxisRow {
	tableId: string;
	// DAT-621: the slice's real column_id — rendered so the sub-agent can pass it to
	// look_values to DRILL the complete value-set on demand (it has no look_table to
	// resolve ids from). The grounding path is by id, not by name.
	columnId: string;
	columnName: string;
	// DAT-621: the dimension's distinct VALUE COUNT only — the cardinality, not the
	// values. No samples: a sample would bias the agent toward the shown subset (the
	// silently-wrong trap). The agent drills the COMPLETE set via look_values(columnId).
	distinctValues?: string[] | null;
}

/** One dimension hierarchy: an `alias` group (1:1 redundant columns) or a
 * drill-down chain. `members` is the engine's JSON array; order is carried by each
 * member's `level` (the engine's HierarchyMember contract, DAT-779), NOT by array
 * position — read ascending by level. */
export interface CatalogHierarchyRow {
	tableId: string;
	kind: string;
	members: Array<{ column_name?: string | null; level?: number | null }>;
	canonicalLabel: string | null;
}

function hierarchyLine(h: CatalogHierarchyRow): string | null {
	const names = h.members
		.map((m) => m.column_name)
		.filter((n): n is string => typeof n === "string" && n.length > 0);
	if (names.length < 2) return null;
	if (h.kind === "alias") {
		const canonical = h.canonicalLabel ?? names[0];
		const others = names.filter((n) => n !== canonical);
		if (others.length === 0) return null;
		return `  alias: "${canonical}" ≡ ${others.map((n) => `"${n}"`).join(", ")} (group by the canonical only)`;
	}
	// Drill-down / FD chain. Order is `level`, not array position (DAT-779) — sort
	// ascending by level before joining; fall back to the array index only if a
	// level is somehow absent.
	const ordered = h.members
		.map((m, i) => ({ name: m.column_name, level: m.level ?? i }))
		.sort((a, b) => a.level - b.level)
		.map((m) => m.name)
		.filter((n): n is string => typeof n === "string" && n.length > 0);
	return `  drill-down: ${ordered.map((n) => `"${n}"`).join(" → ")}`;
}

/**
 * Format the dimension catalog as the sub-agent's `<dimensions>` block (pure).
 * Grouped by table (sorted by address); per table, the natural dimensions then their
 * alias/drill-down structure. Empty catalog → a one-line note. ``tableAddressById``
 * maps a catalog ``table_id`` to its ``lake.<layer>.<name>`` address (the same
 * address the `<schema>` block uses).
 */
export function formatCatalog(
	axisRows: CatalogAxisRow[],
	hierarchyRows: CatalogHierarchyRow[],
	tableAddressById: Map<string, string>,
): string {
	if (axisRows.length === 0 && hierarchyRows.length === 0) {
		return "<dimensions>\n(No catalogued dimensions yet.)\n</dimensions>";
	}

	const byTable = new Map<
		string,
		{ dimensions: string[]; hierarchies: string[] }
	>();
	const bucket = (tableId: string) => {
		let b = byTable.get(tableId);
		if (!b) {
			b = { dimensions: [], hierarchies: [] };
			byTable.set(tableId, b);
		}
		return b;
	};
	for (const a of axisRows) {
		const values = Array.isArray(a.distinctValues)
			? a.distinctValues.filter((v): v is string => v != null).map(String)
			: [];
		// DAT-621: name + value-COUNT + the column_id — never the values themselves. NO
		// samples: a sample biases the agent toward the shown subset (the silently-wrong
		// trap). The sub-agent has look_values now, so it DRILLS the complete value-set on
		// demand via the id and grounds an IN(...) over what comes back. value_count is the
		// honest complete-set size (slice dims are low-card by construction).
		const count = values.length ? ` (${values.length} values)` : "";
		bucket(a.tableId).dimensions.push(
			`"${a.columnName}"${count} [id: ${a.columnId}]`,
		);
	}
	for (const h of hierarchyRows) {
		const line = hierarchyLine(h);
		if (line) bucket(h.tableId).hierarchies.push(line);
	}

	const addressOf = (tableId: string) =>
		tableAddressById.get(tableId) ?? tableId;
	const tableBlocks = [...byTable.entries()]
		.sort((a, b) => addressOf(a[0]).localeCompare(addressOf(b[0])))
		.map(([tableId, b]) => {
			const lines: string[] = [`Table ${addressOf(tableId)}:`];
			if (b.dimensions.length)
				lines.push(`  dimensions: ${[...b.dimensions].sort().join(", ")}`);
			lines.push(...b.hierarchies.sort());
			return lines.join("\n");
		});

	return (
		"<dimensions>\n" +
		"The workspace's natural analysis dimensions per table, and how they relate. " +
		"For an alias group, group by the canonical column (don't double-count the " +
		"same axis); to answer at a coarser level, roll a drill-down chain up along " +
		"its listed order. Each dimension shows its distinct-value COUNT and its [id: …] " +
		"— not the values themselves. To ground a filter on one, call look_values with " +
		"that id to fetch its exact values, then build an IN (...) over them. Never guess " +
		"a value or match by substring.\n\n" +
		`${tableBlocks.join("\n\n")}\n` +
		"</dimensions>"
	);
}

/**
 * Read the dimension catalog for the active workspace's promoted head and format it
 * as the sub-agent's `<dimensions>` block. Addresses mirror the `<schema>` block:
 * the catalog is keyed to the typed fact tables; the same column names surface on any
 * enriched view built from them, so the dimensions apply regardless of join path.
 */
export async function buildCatalogBlock(): Promise<string> {
	const [axisRows, hierarchyRows, tableRows] = await Promise.all([
		metadataDb
			.select({
				tableId: currentSliceDefinitions.tableId,
				columnId: currentSliceDefinitions.columnId,
				columnName: currentSliceDefinitions.columnName,
				distinctValues: currentSliceDefinitions.distinctValues,
			})
			.from(currentSliceDefinitions),
		metadataDb
			.select({
				tableId: currentDimensionHierarchies.tableId,
				kind: currentDimensionHierarchies.kind,
				members: currentDimensionHierarchies.members,
				canonicalLabel: currentDimensionHierarchies.canonicalLabel,
			})
			.from(currentDimensionHierarchies),
		// current_tables (DAT-677): the catalog axes are head-scoped already; the
		// address map resolves them against the same promoted surface.
		metadataDb
			.select({
				tableId: currentTables.tableId,
				physicalName: currentTables.tableName,
			})
			.from(currentTables)
			.innerJoin(sources, eq(sources.sourceId, currentTables.sourceId))
			.where(isNull(sources.archivedAt)),
	]);

	const tableAddressById = new Map<string, string>(
		tableRows
			.filter((t) => t.tableId)
			.map((t) => [
				t.tableId as string,
				`${LAKE_ALIAS}.${schemaForLayer(TYPED_LAYER)}.${t.physicalName}`,
			]),
	);

	return formatCatalog(
		axisRows
			.filter((a) => a.tableId && a.columnId && a.columnName)
			.map((a) => ({
				tableId: a.tableId as string,
				columnId: a.columnId as string,
				columnName: a.columnName as string,
				distinctValues: Array.isArray(a.distinctValues)
					? (a.distinctValues as string[])
					: null,
			})),
		hierarchyRows
			.filter((h) => h.tableId)
			.map((h) => ({
				tableId: h.tableId as string,
				kind: h.kind ?? "",
				members: Array.isArray(h.members)
					? (h.members as Array<{ column_name?: string | null }>)
					: [],
				canonicalLabel: h.canonicalLabel ?? null,
			})),
		tableAddressById,
	);
}

// --- Relationships (DAT-621) -----------------------------------------------------
//
// The confirmed join paths between tables — the JOIN-grounding analog of the
// <dimensions> value-grounding block. The sub-agent gets NO look_relationships tool
// (relationships are a small set needed by most multi-table queries → serve in
// context, don't gate behind a per-query tool round-trip); high-card VALUES are the
// opposite (large + per-query → look_values). Mirrors what the engine GraphAgent
// already gets (graphs/context.py "## Relationships"): the directional column pair,
// cardinality, and the fan-out caution. Only the DEFINED catalog is served
// (detection_method != 'candidate') — a bare structural candidate the run never
// confirmed is not a join path. Without this the sub-agent invents a join key
// (the `t.account = coa.account_name` guess).

/** One confirmed join edge, addressed for the prompt (the from/to lake addresses +
 * the column on each side). `introducesDuplicates` is the engine's fan-trap signal
 * (evidence.introduces_duplicates): joining here multiplies rows. */
export interface RelationshipBlockRow {
	fromAddress: string;
	fromColumn: string;
	toAddress: string;
	toColumn: string;
	cardinality: string | null;
	relationshipType: string | null;
	introducesDuplicates: boolean | null;
}

/**
 * Format the confirmed relationships as the sub-agent's `<relationships>` block (pure).
 * Each line is a directly usable JOIN predicate (`<from>."col" = <to>."col"`) plus the
 * cardinality/type and, when the edge fans out, the SUM-double-counts caution. Empty →
 * a one-line note.
 */
export function formatRelationships(rows: RelationshipBlockRow[]): string {
	if (rows.length === 0) {
		return "<relationships>\n(No confirmed relationships between tables.)\n</relationships>";
	}
	const lines = rows
		.map((r) => {
			const facts = [r.cardinality, r.relationshipType]
				.filter((f): f is string => !!f)
				.join("; ");
			const factTag = facts ? ` (${facts})` : "";
			// Fan-out caution reads the engine's introduces_duplicates flag (the fan-trap
			// check is the engine's job, not the consumer's — see DAT-628: the LLM
			// synthesis path doesn't yet populate it). Null flag → no caution.
			const fanOut =
				r.introducesDuplicates === true
					? " ⚠ fan-out: SUM across this join double-counts — pre-aggregate or COUNT DISTINCT"
					: "";
			return `- ${r.fromAddress}."${r.fromColumn}" = ${r.toAddress}."${r.toColumn}"${factTag}${fanOut}`;
		})
		.sort();
	return (
		"<relationships>\n" +
		"The confirmed join paths between tables — JOIN ON the listed column pair, never " +
		"a guessed key. A dimension table may be hidden from <schema> when enriched views " +
		"are shown; these paths still reach it (join lake.typed.<dim>). Ground EVERY join " +
		"on a pair listed here; if the join you need isn't listed, do not invent one — " +
		"abstain or state the limitation.\n\n" +
		`${lines.join("\n")}\n` +
		"</relationships>"
	);
}

/**
 * Read the confirmed relationship catalog for the active workspace's promoted head and
 * format it as the `<relationships>` block. Resolves each endpoint's lake address (the
 * SAME `lake.<layer>.<name>` form the <schema> block uses) + column name. Only
 * `detection_method != 'candidate'` (the defined catalog) is served.
 */
export async function buildRelationshipsBlock(): Promise<string> {
	const rels = await metadataDb
		.select({
			fromTableId: currentRelationships.fromTableId,
			fromColumnId: currentRelationships.fromColumnId,
			toTableId: currentRelationships.toTableId,
			toColumnId: currentRelationships.toColumnId,
			relationshipType: currentRelationships.relationshipType,
			cardinality: currentRelationships.cardinality,
			detectionMethod: currentRelationships.detectionMethod,
			evidence: currentRelationships.evidence,
		})
		.from(currentRelationships);

	const defined = rels.filter(
		(r) =>
			r.detectionMethod !== "candidate" &&
			r.fromTableId &&
			r.fromColumnId &&
			r.toTableId &&
			r.toColumnId,
	);
	if (defined.length === 0) return formatRelationships([]);

	// Resolve endpoint table addresses + column names in one pass each (no N+1).
	const tableIds = new Set<string>();
	const columnIds = new Set<string>();
	for (const r of defined) {
		tableIds.add(r.fromTableId as string);
		tableIds.add(r.toTableId as string);
		columnIds.add(r.fromColumnId as string);
		columnIds.add(r.toColumnId as string);
	}

	// current_tables/current_columns (DAT-677): a relationship endpoint whose
	// table is no longer under a promoted generation head resolves to nothing and
	// the half-resolved-skip below drops the line — same behavior as a dropped id.
	const [tableRows, columnRows] = await Promise.all([
		metadataDb
			.select({
				tableId: currentTables.tableId,
				physicalName: currentTables.tableName,
			})
			.from(currentTables)
			.where(inArray(currentTables.tableId, [...tableIds])),
		metadataDb
			.select({
				columnId: currentColumns.columnId,
				columnName: currentColumns.columnName,
			})
			.from(currentColumns)
			.where(inArray(currentColumns.columnId, [...columnIds])),
	]);

	const addressById = new Map<string, string>(
		tableRows
			.filter((t) => t.tableId)
			.map((t) => [
				t.tableId as string,
				`${LAKE_ALIAS}.${schemaForLayer(TYPED_LAYER)}.${t.physicalName}`,
			]),
	);
	const colNameById = new Map<string, string>(
		columnRows
			.filter((c) => c.columnId && c.columnName)
			.map((c) => [c.columnId as string, c.columnName as string]),
	);

	const blockRows: RelationshipBlockRow[] = [];
	for (const r of defined) {
		const fromAddress = addressById.get(r.fromTableId as string);
		const toAddress = addressById.get(r.toTableId as string);
		const fromColumn = colNameById.get(r.fromColumnId as string);
		const toColumn = colNameById.get(r.toColumnId as string);
		// A dropped endpoint (stale id) can't form a usable JOIN predicate — skip it
		// rather than render a half-resolved, un-runnable line.
		if (!fromAddress || !toAddress || !fromColumn || !toColumn) continue;
		blockRows.push({
			fromAddress,
			fromColumn,
			toAddress,
			toColumn,
			cardinality: r.cardinality ?? null,
			relationshipType: r.relationshipType ?? null,
			introducesDuplicates:
				typeof r.evidence === "object" && r.evidence !== null
					? (((r.evidence as Record<string, unknown>).introduces_duplicates as
							| boolean
							| null) ?? null)
					: null,
		});
	}
	return formatRelationships(blockRows);
}

// --- Table entities (DAT-607) ----------------------------------------------------
//
// Per-table entity grounding (DAT-565/566): what each table represents, its grain
// (one row per …), event-time axes, and recurring identities (would-be FKs). The
// prefer-enriched `<schema>` block is column-grain and, when enriched views exist,
// surfaces ONLY those views — hiding the typed facts AND every dimension table. This
// block is table-grain over the typed facts/dims (where `TableEntity` lives), giving
// the agent the natural grouping keys ("per <entity>" → an identity column) and a
// reminder that the hidden dimension tables exist. Same projection the look_table
// tool uses (`projectTableEntity`: `src_<digest>` strip, degrade-to-empty).
// Informational (inform-don't-block). Empty (no promoted catalog run) → a one-line note.

/** One typed table's entity header, addressed for the prompt. */
export interface EntityBlockRow {
	address: string;
	entity: TableEntity;
}

/** Clamp an LLM-authored identity note — keeps one stanza to a readable line. */
const MAX_NOTE_CHARS = 140;

function clampNote(note: string): string {
	const n = note.trim();
	return n.length > MAX_NOTE_CHARS
		? `${n.slice(0, MAX_NOTE_CHARS - 1).trimEnd()}…`
		: n;
}

/**
 * Format the table entities as the sub-agent's `<entities>` block (pure). One stanza
 * per table that carries an entity signal (grain / time / identity), sorted by
 * address; a table with no signal is dropped (a bare entity_type is noise for SQL
 * grounding). Identity notes are clamped and the per-table identity list capped.
 * Empty input → a one-line note.
 */
export function formatEntities(rows: EntityBlockRow[]): string {
	const stanzas: string[] = [];
	for (const { address, entity } of [...rows].sort((a, b) =>
		a.address.localeCompare(b.address),
	)) {
		const lines: string[] = [];
		if (entity.grain.length) lines.push(`  grain: ${entity.grain.join(", ")}`);
		// EVENT axes only (DAT-780): the answer agent trends/groups by these, so an
		// attribute date (role='attribute' — due_date, valid_until) must never be
		// offered as a time axis. Tolerant of un-roled rows (shown), strict against
		// explicit attributes (hidden). The is_anchor axis is marked as the primary
		// lens. Mirrors the engine's graphs/context.py treatment (two SQL agents).
		const eventAxes = entity.time_columns.filter((t) => t.role !== "attribute");
		if (eventAxes.length)
			lines.push(
				`  time: ${eventAxes
					.map((t) => {
						const label = t.aspect ? `${t.column} (${t.aspect})` : t.column;
						return t.is_anchor ? `${label} [anchor]` : label;
					})
					.join(", ")}`,
			);
		if (entity.identity_columns.length)
			lines.push(
				`  identities: ${entity.identity_columns
					.map((i) =>
						i.note ? `${i.column} — ${clampNote(i.note)}` : i.column,
					)
					.join("; ")}`,
			);
		// A table with no grain/time/identity is noise for SQL grounding — drop it.
		if (lines.length === 0) continue;
		// The engine's full role, verbatim (DAT-728): "fact" | "periodic snapshot"
		// | "dimension". A periodic snapshot is a period-end level the agent must
		// NOT sum across periods (unlike an event fact) — surfaced, not flattened.
		const kind = entity.table_role
			? entity.table_role.replace(/_/g, " ")
			: null;
		const head = entity.entity_type
			? `Table ${address} — ${entity.entity_type}${kind ? ` (${kind})` : ""}:`
			: `Table ${address}${kind ? ` (${kind})` : ""}:`;
		stanzas.push(`${head}\n${lines.join("\n")}`);
	}
	if (stanzas.length === 0)
		return "<entities>\n(No table entities detected yet.)\n</entities>";
	// DAT-621: no cap — every table (already address-sorted) is served; the workspace's
	// table set is bounded and a truncation here is a silent grounding gap.
	return (
		"<entities>\n" +
		"What each table represents and its natural keys. Grain is the table's unit " +
		"(one row per these columns). Identities are recurring real-world keys (would-be " +
		'foreign keys) — to answer "per <entity>", group by the matching identity ' +
		"column. Time columns are the event-time axes. When the <schema> block shows an " +
		"enriched view instead of the typed table below, these columns apply to that view " +
		"too — an enriched view includes every column of the typed table it's built from.\n\n" +
		`${stanzas.join("\n\n")}\n` +
		"</entities>"
	);
}

/**
 * Read the table entities for the active workspace's promoted catalog head and format
 * them as the sub-agent's `<entities>` block. Joined to the typed tables for their
 * `lake.typed.<name>` address. The view is head-resolved to one row per table_id; the
 * `detected_at desc` order + first-seen-wins dedup is the deterministic tiebreak the
 * session-grain contract (DAT-474) requires for a multi-row read.
 */
export async function buildEntitiesBlock(): Promise<string> {
	// current_tables (DAT-677) replaces the raw join + manual layer filter: the
	// address join is now the same promoted typed surface the entity rows are
	// sealed against.
	const rows = await metadataDb
		.select({
			tableId: currentTableEntities.tableId,
			physicalName: currentTables.tableName,
			detectedEntityType: currentTableEntities.detectedEntityType,
			tableRole: currentTableEntities.tableRole,
			grainColumns: currentTableEntities.grainColumns,
			timeColumns: currentTableEntities.timeColumns,
			identityColumns: currentTableEntities.identityColumns,
			description: currentTableEntities.description,
		})
		.from(currentTableEntities)
		.innerJoin(
			currentTables,
			eq(currentTables.tableId, currentTableEntities.tableId),
		)
		.innerJoin(sources, eq(sources.sourceId, currentTables.sourceId))
		.where(isNull(sources.archivedAt))
		.orderBy(desc(currentTableEntities.detectedAt));

	const seen = new Set<string>();
	const blockRows: EntityBlockRow[] = [];
	for (const r of rows) {
		const tableId = r.tableId ?? "";
		const physicalName = r.physicalName ?? "";
		if (!tableId || !physicalName || seen.has(tableId)) continue;
		seen.add(tableId);
		const address = `${LAKE_ALIAS}.${schemaForLayer(TYPED_LAYER)}.${physicalName}`;
		blockRows.push({
			address,
			entity: projectTableEntity({
				detectedEntityType: r.detectedEntityType ?? null,
				tableRole: r.tableRole ?? null,
				grainColumns: r.grainColumns,
				timeColumns: r.timeColumns,
				identityColumns: r.identityColumns,
				description: r.description ?? null,
			}),
		});
	}
	return formatEntities(blockRows);
}

// --- Driver rankings (DAT-548) ---------------------------------------------------
//
// The pre-computed driver rankings (DAT-545) tell the answer sub-agent which
// dimensions most explain each measure's variation — context that turns a "why did X
// change" / "X by ?" question from a guessed GROUP BY into a grounded one. Same read +
// projection as the look_drivers tool (DAT-546), so the injected context and an
// explicit look_drivers call never drift. Informational (inform-don't-block): the
// agent still authors the SQL. Empty (no promoted begin_session run, or no measures
// with a significant driver) → a one-line note.

// No cap on interesting_slices (DAT-616): the driver engine ALREADY bounds them —
// FDR-gated, per-node top-5 by |effect|, effect-sorted — so the persisted set is a small
// curated list, not a dump. A second display cap (was 3) was a SILENT recall gate: on a
// larger table it dropped most of the curated signal where neither the user nor the agent
// could see the loss. Serve the full set (matching the engine GraphAgent's `## Drivers`).

/** Render a ranking's grain for the prompt: "row-level", or "within <identity>". */
function grainLabel(grain: string, entity: string | null): string {
	if (grain === "row") return "row-level";
	return entity ? `within ${entity}` : grain;
}

/**
 * Format the driver rankings as the sub-agent's `<drivers>` block (pure). One stanza
 * per measure that has a significant driver (input order preserved — the view yields
 * one row per measure column): the ranked dimensions (gain, strongest first), the
 * surviving drill paths, a few sharp slices, and any other-grain (secondary) drivers
 * kept labeled with their own grain. Measures with no driver are dropped; an entirely
 * empty set → a one-line note.
 */
export function formatDrivers(rankings: DriverRanking[]): string {
	const withDrivers = rankings.filter(
		(r) => r.ranked_dimensions.length > 0 || r.driver_paths.length > 0,
	);
	if (withDrivers.length === 0) {
		return "<drivers>\n(No driver rankings yet.)\n</drivers>";
	}

	const g = (n: number) => n.toFixed(2);
	const stanzas = withDrivers.map((r) => {
		const lines: string[] = [
			`Measure "${r.measure}" (${r.target_type}, ${grainLabel(r.grain, r.entity)}, n=${r.n_rows}):`,
		];
		if (r.ranked_dimensions.length)
			lines.push(
				`  top drivers: ${r.ranked_dimensions
					.map((d) => `"${d.dimension}" (${g(d.gain)})`)
					.join(", ")}`,
			);
		const paths = r.driver_paths.filter((p) => p.length > 0);
		if (paths.length)
			lines.push(
				`  drill paths: ${paths
					.map((p) => p.map((n) => `"${n}"`).join(" → "))
					.join("; ")}`,
			);
		const slices = r.interesting_slices;
		if (slices.length)
			lines.push(
				`  notable slices: ${slices
					.map(
						(s) =>
							`"${s.dimension}"=${s.value} (effect ${g(s.effect)}, support ${s.support})`,
					)
					.join(", ")}`,
			);
		if (r.secondary_dimensions.length)
			lines.push(
				// DAT-621: no cap — secondary families are naturally few; serve all (no silent cut).
				`  other-grain drivers: ${r.secondary_dimensions
					.map(
						(s) =>
							`"${s.dimension}" (${grainLabel(s.grain, s.entity)}, ${g(s.gain)})`,
					)
					.join(", ")}`,
			);
		return lines.join("\n");
	});

	return (
		"<drivers>\n" +
		"Pre-computed driver rankings — which dimensions most explain each measure's " +
		'variation (from the begin_session analysis). For a "why did X change / what ' +
		'drives X" question, lean on these; for an open "X by ?" breakdown, the ' +
		"top-ranked dimension is the sensible default. A drill path is an ordered way to " +
		"decompose a measure (group by its columns coarse→fine). Other-grain drivers " +
		"hold at a different unit (e.g. within an identity) — don't compare their gains " +
		"to the primary list. Informational — you still author the SQL.\n\n" +
		`${stanzas.join("\n\n")}\n` +
		"</drivers>"
	);
}

/**
 * Read the workspace's promoted driver rankings and format them as the sub-agent's
 * `<drivers>` block. Reuses look_drivers' read + projection (DAT-546) — the view
 * already resolves to the promoted begin_session catalog head — so the injected
 * context and an explicit look_drivers call never drift.
 */
export async function buildDriversBlock(): Promise<string> {
	// Soft-fail: driver grounding is degradable context (the agent still writes valid
	// SQL without it), so a metadata read failure must not fail the whole answer —
	// honor inform-don't-block. Mirrors describeEnrichedViews' fallback above.
	try {
		const { rankings } = await lookDrivers({});
		return formatDrivers(rankings);
	} catch (err) {
		console.warn(
			`[cockpit] buildDriversBlock failed — omitting drivers context: ${err}`,
		);
		return "<drivers>\n(No driver rankings yet.)\n</drivers>";
	}
}

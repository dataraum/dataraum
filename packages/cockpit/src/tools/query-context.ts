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

import { and, asc, desc, eq, inArray, isNull } from "drizzle-orm";

import { metadataDb } from "../db/metadata/client";
import {
	columns,
	currentDimensionHierarchies,
	currentSemanticAnnotations,
	currentSliceDefinitions,
	currentTableEntities,
	sources,
	tables,
} from "../db/metadata/schema";
import { getLakeConnection, LAKE_ALIAS } from "../duckdb/lake";
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
		if (c.businessConcept || c.temporalBehavior || c.temporalBehaviorContested)
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
	columnName: string;
	// DAT-616: the dimension's actual values — the answer agent's grounding value-set.
	// The slice catalog is low-card by construction, so this is naturally bounded
	// (no fire-hose); a metric filter grounds in these literals, not a guessed ILIKE.
	distinctValues?: string[] | null;
}

// Cap rendered values per dimension — the catalog is already low-card, this just
// guards a pathological row from bloating the prompt.
const MAX_DIMENSION_VALUES = 30;

/** One dimension hierarchy: an `alias` group (1:1 redundant columns) or a
 * drill-down chain. `members` is the engine's JSON array of `{column_name}`. */
export interface CatalogHierarchyRow {
	tableId: string;
	kind: string;
	members: Array<{ column_name?: string | null }>;
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
	// Drill-down / FD chain — members are ordered coarse→fine by the engine.
	return `  drill-down: ${names.map((n) => `"${n}"`).join(" → ")}`;
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
		let line = `"${a.columnName}"`;
		if (values.length) {
			const shown = values.slice(0, MAX_DIMENSION_VALUES).join(", ");
			const more =
				values.length > MAX_DIMENSION_VALUES
					? `, +${values.length - MAX_DIMENSION_VALUES} more`
					: "";
			line += ` [${shown}${more}]`;
		}
		bucket(a.tableId).dimensions.push(line);
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
		"its listed order.\n\n" +
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
		metadataDb
			.select({
				tableId: tables.tableId,
				physicalName: tables.tableName,
				layer: tables.layer,
			})
			.from(tables)
			.innerJoin(sources, eq(sources.sourceId, tables.sourceId))
			.where(and(isNull(sources.archivedAt), eq(tables.layer, TYPED_LAYER))),
	]);

	const tableAddressById = new Map<string, string>(
		tableRows
			.filter((t) => t.tableId)
			.map((t) => [
				t.tableId as string,
				`${LAKE_ALIAS}.${schemaForLayer(t.layer ?? TYPED_LAYER)}.${t.physicalName}`,
			]),
	);

	return formatCatalog(
		axisRows
			.filter((a) => a.tableId && a.columnName)
			.map((a) => ({
				tableId: a.tableId as string,
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

/** Cap a single table's identity list — bounds the block on a wide entity. */
const MAX_IDENTITIES_PER_TABLE = 8;
/** Clamp an LLM-authored identity note — keeps one stanza to a readable line. */
const MAX_NOTE_CHARS = 140;
/** Cap the number of table stanzas — bounds the block on a wide workspace; the
 * overflow is summarized in a tail note rather than silently dropped. */
const MAX_ENTITY_TABLES = 25;

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
		if (entity.time_columns.length)
			lines.push(
				`  time: ${entity.time_columns
					.map((t) => (t.aspect ? `${t.column} (${t.aspect})` : t.column))
					.join(", ")}`,
			);
		if (entity.identity_columns.length)
			lines.push(
				`  identities: ${entity.identity_columns
					.slice(0, MAX_IDENTITIES_PER_TABLE)
					.map((i) =>
						i.note ? `${i.column} — ${clampNote(i.note)}` : i.column,
					)
					.join("; ")}`,
			);
		// A table with no grain/time/identity is noise for SQL grounding — drop it.
		if (lines.length === 0) continue;
		const kind = entity.is_fact_table
			? "fact"
			: entity.is_dimension_table
				? "dimension"
				: null;
		const head = entity.entity_type
			? `Table ${address} — ${entity.entity_type}${kind ? ` (${kind})` : ""}:`
			: `Table ${address}${kind ? ` (${kind})` : ""}:`;
		stanzas.push(`${head}\n${lines.join("\n")}`);
	}
	if (stanzas.length === 0)
		return "<entities>\n(No table entities detected yet.)\n</entities>";
	// Bound the block on a wide workspace — keep the first N (already address-sorted)
	// and summarize the rest rather than bloat the prompt or silently truncate.
	const overflow = stanzas.length - MAX_ENTITY_TABLES;
	const kept = overflow > 0 ? stanzas.slice(0, MAX_ENTITY_TABLES) : stanzas;
	const tail = overflow > 0 ? `\n\n(… ${overflow} more tables omitted.)` : "";
	return (
		"<entities>\n" +
		"What each table represents and its natural keys. Grain is the table's unit " +
		"(one row per these columns). Identities are recurring real-world keys (would-be " +
		'foreign keys) — to answer "per <entity>", group by the matching identity ' +
		"column. Time columns are the event-time axes. When the <schema> block shows an " +
		"enriched view instead of the typed table below, these columns apply to that view " +
		"too — an enriched view includes every column of the typed table it's built from.\n\n" +
		`${kept.join("\n\n")}${tail}\n` +
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
	const rows = await metadataDb
		.select({
			tableId: currentTableEntities.tableId,
			physicalName: tables.tableName,
			layer: tables.layer,
			detectedEntityType: currentTableEntities.detectedEntityType,
			isFactTable: currentTableEntities.isFactTable,
			isDimensionTable: currentTableEntities.isDimensionTable,
			grainColumns: currentTableEntities.grainColumns,
			timeColumns: currentTableEntities.timeColumns,
			identityColumns: currentTableEntities.identityColumns,
			description: currentTableEntities.description,
		})
		.from(currentTableEntities)
		.innerJoin(tables, eq(tables.tableId, currentTableEntities.tableId))
		.innerJoin(sources, eq(sources.sourceId, tables.sourceId))
		.where(and(isNull(sources.archivedAt), eq(tables.layer, TYPED_LAYER)))
		.orderBy(desc(currentTableEntities.detectedAt));

	const seen = new Set<string>();
	const blockRows: EntityBlockRow[] = [];
	for (const r of rows) {
		const tableId = r.tableId ?? "";
		const physicalName = r.physicalName ?? "";
		if (!tableId || !physicalName || seen.has(tableId)) continue;
		seen.add(tableId);
		const address = `${LAKE_ALIAS}.${schemaForLayer(r.layer ?? TYPED_LAYER)}.${physicalName}`;
		blockRows.push({
			address,
			entity: projectTableEntity({
				detectedEntityType: r.detectedEntityType ?? null,
				isFactTable: r.isFactTable ?? null,
				isDimensionTable: r.isDimensionTable ?? null,
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

/** Cap the per-measure slice list so the block stays a hint, not a data dump. */
const MAX_SLICES_PER_MEASURE = 3;
/** Cap other-grain drivers too — bounds the block on a workspace with many
 * identity columns (the engine doesn't cap secondary families before persisting). */
const MAX_SECONDARY_PER_MEASURE = 5;

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
		const slices = r.interesting_slices.slice(0, MAX_SLICES_PER_MEASURE);
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
				`  other-grain drivers: ${r.secondary_dimensions
					.slice(0, MAX_SECONDARY_PER_MEASURE)
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

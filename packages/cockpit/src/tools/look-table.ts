// look_table tool (DAT-350) — a table's per-column readiness overview.
//
// Pure read via the Drizzle metadata client: LEFT JOINs ws_<id>.columns to the
// persisted `entropy_readiness` rows the engine's terminal `detect` step writes
// (DAT-394/399). Surfaces, per column, the calibrated band (ready/investigate/
// blocked) across the three intents (query/aggregation/reporting) plus the top
// quality drivers — reading the PERSISTED, calibrated band, never re-deriving it
// in TS (the engine owns the noisy-OR rollup; the cockpit reads it).
//
// The drivers are self-describing (DAT-399 B): each carries its own `label` +
// `dimension_path`, so this tool needs no engine network vocabulary. `why_column`
// (DAT-351) drills into the full per-intent drivers + evidence; `look_table` is
// the at-a-glance grid. Read-only → no approval.
//
// The DB join is covered by the browser smoke (a live ws_<id> with readiness
// rows); the pure row→shape projection is unit-tested directly here.

import { toolDefinition } from "@tanstack/ai";
import { and, asc, eq, sql } from "drizzle-orm";
import { z } from "zod";

import { metadataDb } from "../db/metadata/client";
import { getPendingOverlays } from "../db/metadata/pending-overlays";
import {
	PersistedIntent,
	ReadinessDriver,
} from "../db/metadata/readiness-schemas";
import {
	sessionHeadTarget,
	tableTargetKey,
} from "../db/metadata/relationship-target";
import {
	columns,
	entropyReadiness,
	metadataSnapshotHead,
	tables,
} from "../db/metadata/schema";

// The persisted JSONB grammar (intents / top_drivers) lives in
// `readiness-schemas.ts`, shared with why_column. Parsed leniently below: a
// malformed/absent blob degrades to empty, never throws.

// --- The tool's output: per-column bands + a few top driver labels per column.

const IntentBand = z.object({
	intent: z.string(),
	band: z.string(),
	risk: z.number(),
});

const TopDriver = z.object({
	label: z.string(),
	state: z.string(),
	impact_delta: z.number(),
});

const ColumnReadiness = z.object({
	column_id: z.string(),
	column_name: z.string(),
	resolved_type: z.string().nullable(),
	// null band = this column has no readiness row yet (not analyzed).
	band: z.string().nullable(),
	worst_intent_risk: z.number().nullable(),
	intents: z.array(IntentBand),
	top_drivers: z.array(TopDriver),
});
export type ColumnReadiness = z.infer<typeof ColumnReadiness>;

// The table-grain readiness band (DAT-415) — begin_session's `dimension_coverage`
// rolled up for the whole table, sealed at the session head. Same overview shape
// as a column (band + per-intent bands + top drivers), but for the table itself;
// `why_table` drills into the full per-intent drivers + evidence.
const TableReadiness = z.object({
	band: z.string().nullable(),
	worst_intent_risk: z.number().nullable(),
	intents: z.array(IntentBand),
	top_drivers: z.array(TopDriver),
});
export type TableReadiness = z.infer<typeof TableReadiness>;

const LookTableResult = z.object({
	table_id: z.string(),
	table_name: z.string(),
	// False when no column carries a readiness row — the table hasn't been
	// analyzed (no `detect` run yet), so the grid should say so rather than imply
	// everything is clean.
	analyzed: z.boolean(),
	pending_teaches: z.number(),
	columns: z.array(ColumnReadiness),
	// The table-grain band for the begin_session `session_id` passed in (DAT-415);
	// null when no session_id was given or the session has no table-grain readiness
	// for this table (never begun / table not in the session). The per-column grid
	// above is add_source-grain; this is the begin_session whole-table rollup.
	table_readiness: TableReadiness.nullable(),
});
export type LookTableResult = z.infer<typeof LookTableResult>;

// How many of a column's top drivers to surface in the overview (why_column
// shows the full ranked list).
const TOP_DRIVERS_SHOWN = 3;

/** One joined (columns ⟕ entropy_readiness) row, as Drizzle returns it. */
export interface ReadinessRow {
	columnId: string;
	columnName: string;
	resolvedType: string | null;
	band: string | null;
	worstIntentRisk: number | null;
	intents: unknown;
	topDrivers: unknown;
}

/**
 * Project one joined row to the tool's per-column shape. Pure (no DB) so the
 * JSONB-parsing + null-handling logic is unit-testable without a live schema.
 * A column with no readiness row (left-join miss) keeps `band: null` and empty
 * intents/drivers; a malformed JSONB blob degrades to empty rather than throwing.
 */
export function projectColumnReadiness(row: ReadinessRow): ColumnReadiness {
	const intents = PersistedIntent.array().safeParse(row.intents);
	const drivers = ReadinessDriver.array().safeParse(row.topDrivers);
	return {
		column_id: row.columnId,
		column_name: row.columnName,
		resolved_type: row.resolvedType,
		band: row.band ?? null,
		worst_intent_risk: row.worstIntentRisk ?? null,
		intents: intents.success
			? intents.data.map((i) => ({
					intent: i.intent,
					band: i.band,
					risk: i.risk,
				}))
			: [],
		top_drivers: drivers.success
			? drivers.data.slice(0, TOP_DRIVERS_SHOWN).map((d) => ({
					label: d.label,
					state: d.state,
					impact_delta: d.impact_delta,
				}))
			: [],
	};
}

/** One table-grain `entropy_readiness` row, as Drizzle returns it. */
export interface TableBandRow {
	band: string | null;
	worstIntentRisk: number | null;
	intents: unknown;
	topDrivers: unknown;
}

/**
 * Project the table-grain readiness row to the overview shape. Pure (no DB), the
 * table analog of {@link projectColumnReadiness} minus the column identity: the
 * per-intent overview keeps band + risk (drivers are why_table's drill-down), and
 * the top drivers are capped + self-describing. A malformed JSONB blob degrades to
 * empty rather than throwing.
 */
export function projectTableBand(row: TableBandRow): TableReadiness {
	const intents = PersistedIntent.array().safeParse(row.intents);
	const drivers = ReadinessDriver.array().safeParse(row.topDrivers);
	return {
		band: row.band ?? null,
		worst_intent_risk: row.worstIntentRisk ?? null,
		intents: intents.success
			? intents.data.map((i) => ({
					intent: i.intent,
					band: i.band,
					risk: i.risk,
				}))
			: [],
		top_drivers: drivers.success
			? drivers.data.slice(0, TOP_DRIVERS_SHOWN).map((d) => ({
					label: d.label,
					state: d.state,
					impact_delta: d.impact_delta,
				}))
			: [],
	};
}

export interface LookTableInput {
	table_id: string;
	// Optional: when this table is being inspected inside a begin_session, the
	// session whose table-grain readiness to also surface (DAT-415). Omitted for a
	// plain add_source column overview.
	session_id?: string;
}

/** Per-column readiness for one table, plus a pending-teach hint. */
export async function lookTable(
	input: LookTableInput,
): Promise<LookTableResult> {
	const [table] = await metadataDb
		.select({ tableId: tables.tableId, tableName: tables.tableName })
		.from(tables)
		.where(eq(tables.tableId, input.table_id))
		.limit(1);

	if (!table) {
		// Unknown table id — return an empty shell, not an error, so the agent can
		// say "no such table" cleanly rather than surfacing a tool failure.
		return {
			table_id: input.table_id,
			table_name: "",
			analyzed: false,
			pending_teaches: 0,
			columns: [],
			table_readiness: null,
		};
	}

	// Readiness is versioned by run_id now (DAT-413): a column has one readiness
	// row PER run, so resolve the PROMOTED detect snapshot for this table via the
	// head pointer and join only that run's rows. No promoted run (never detected)
	// → the join matches nothing and every column reads back unanalyzed.
	const [head] = await metadataDb
		.select({ runId: metadataSnapshotHead.runId })
		.from(metadataSnapshotHead)
		.where(
			and(
				// Head key generalized to a target string (DAT-408): column readiness
				// is table-grain, so the key is `table:{id}`.
				eq(metadataSnapshotHead.target, `table:${input.table_id}`),
				eq(metadataSnapshotHead.stage, "detect"),
			),
		)
		.limit(1);
	const headRunId = head?.runId ?? null;

	const rows = await metadataDb
		.select({
			columnId: columns.columnId,
			columnName: columns.columnName,
			resolvedType: columns.resolvedType,
			band: entropyReadiness.band,
			worstIntentRisk: entropyReadiness.worstIntentRisk,
			intents: entropyReadiness.intents,
			topDrivers: entropyReadiness.topDrivers,
		})
		.from(columns)
		.leftJoin(
			entropyReadiness,
			headRunId
				? and(
						eq(entropyReadiness.columnId, columns.columnId),
						eq(entropyReadiness.runId, headRunId),
					)
				: sql`false`,
		)
		.where(eq(columns.tableId, input.table_id))
		.orderBy(asc(columns.columnPosition));

	const cols = rows.map(projectColumnReadiness);
	// Table-grain band (DAT-415): only when inspecting inside a begin_session. It
	// is sealed at the SESSION head (`session:{id}`), a different head than the
	// per-column rows above (those are the table's add_source detect run), so it's
	// a separate resolve — null when no session_id or no table-grain row.
	const tableReadiness = input.session_id
		? await loadTableBand(input.session_id, table.tableName)
		: null;
	// Workspace-wide count, NOT table-scoped: getPendingOverlays returns every
	// un-superseded teach in the workspace (the helper leaves relevance to the
	// caller). Surfaced as a coarse "a replay may be due" nudge — the description
	// says as much so the agent doesn't over-claim it's specific to this table.
	const pending = await getPendingOverlays();

	return {
		table_id: table.tableId,
		table_name: table.tableName,
		analyzed: cols.some((c) => c.band !== null),
		pending_teaches: pending.length,
		columns: cols,
		table_readiness: tableReadiness,
	};
}

/** Resolve a session's table-grain readiness band for one table (DAT-415).
 * begin_session seals table readiness at the `session:{id}` detect head; read that
 * promoted run's `table:{name}` row. Null when the session never sealed or this
 * table carries no table-grain row in it. */
async function loadTableBand(
	sessionId: string,
	tableName: string,
): Promise<TableReadiness | null> {
	const [head] = await metadataDb
		.select({ runId: metadataSnapshotHead.runId })
		.from(metadataSnapshotHead)
		.where(
			and(
				eq(metadataSnapshotHead.target, sessionHeadTarget(sessionId)),
				eq(metadataSnapshotHead.stage, "detect"),
			),
		)
		.limit(1);
	const headRunId = head?.runId ?? null;
	if (!headRunId) return null;

	const [row] = await metadataDb
		.select({
			band: entropyReadiness.band,
			worstIntentRisk: entropyReadiness.worstIntentRisk,
			intents: entropyReadiness.intents,
			topDrivers: entropyReadiness.topDrivers,
		})
		.from(entropyReadiness)
		.where(
			and(
				eq(entropyReadiness.sessionId, sessionId),
				eq(entropyReadiness.runId, headRunId),
				eq(entropyReadiness.target, tableTargetKey(tableName)),
			),
		)
		.limit(1);
	return row ? projectTableBand(row) : null;
}

export const lookTableTool = toolDefinition({
	name: "look_table",
	description:
		"Show a table's per-column readiness — ready/investigate/blocked across the " +
		"query, aggregation, and reporting intents — with the top quality drivers " +
		"per column. Read-only; reflects the latest analysis (the calibrated, " +
		"persisted band). Pass a begin_session session_id to also get the table's " +
		"whole-table readiness band (table_readiness) from that session; use " +
		"`why_table` to explain it. pending_teaches counts un-applied teaches across " +
		"the workspace (not scoped to this table); if > 0, suggest a `replay` before " +
		"trusting the bands. Use `why_column` to explain a specific column's band.",
	inputSchema: z.object({
		table_id: z
			.string()
			.describe("The table to inspect (a table_id from list_tables)."),
		session_id: z
			.string()
			.optional()
			.describe(
				"Optional begin_session session_id — when set, also returns the " +
					"table-grain readiness band sealed in that session.",
			),
	}),
	outputSchema: LookTableResult,
}).server((input) => lookTable(input));

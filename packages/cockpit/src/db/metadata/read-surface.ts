// Hand-declared READ surface on the reader role's promoted-read schema (DAT-881,
// DAT-882) — the read-side counterpart of ./write-surface.ts.
//
// The generated mirror (./schema.ts, `bun run db:pull:metadata`) introspects
// whatever the CURRENT engine schema looks like at regen time. Regenerating it is
// an OWNER-ONLY integration step (a live Postgres + coordinating across every
// lane's concurrent schema changes, never a single lane's call) — so a lane that
// adds an engine table/column cannot depend on the generated mirror knowing about
// it yet. This file hand-declares the columns two lanes need NOW, exactly the
// technique ./write-surface.ts already established for tables the generated
// mirror can't see (there: because the reader role can't SELECT raw control
// tables; here: because the regen hasn't run). Only the columns the cockpit
// actually reads — the engine's SQLAlchemy models stay the source of truth for
// the full shapes.
//
// DELETE an entry here once `db:pull:metadata` regen supersedes it (the generated
// `./schema.ts` will carry the same columns, view-scoped identically) — this file
// is scaffolding for the gap between an engine schema change landing and the
// mirror catching up, not a permanent second source of truth. Repoint these two
// call sites to the generated `./schema.ts` export at that point:
//   - `cycleTypesRead` → teach-cycle.ts:53 (`readWorkspaceCycleTypes`'s import)
//   - `metricDagRead`  → teach-metric.ts:53 (`readWorkspaceMetricDag`'s import)
// `metricDagRead` OVERLAPS the generated `./schema.ts` `metrics` export today
// (both carry graphId/name/category) — at regen, drop `metricDagRead` entirely
// and read `description`/`output`/`dependencies` as NEW fields added onto that
// SAME generated `metrics` export, rather than keeping two overlapping
// declarations of the same table alive.
//
// Both views ride `metadataDb` (the READER role, ./client.ts) — its search_path is
// pinned to the promoted-read schema, already scoped to the workspace's bound
// active_vertical (see storage/read_views.py's `_vertical_scoped_view_sql`), so
// callers only need `.where(isNull(x.supersededAt))` (the `prompts/conventions.ts`
// precedent) — no vertical filter belongs at the call site.

import { json, pgTable, text, timestamp, varchar } from "drizzle-orm/pg-core";

/** Raw `cycle_types` (DAT-881) — the shipped cycle-type vocabulary. SELECT only;
 * 'seed' is the table's sole writer (engine-side), no cockpit write path exists.
 * Backs `readWorkspaceCycleTypes` (teach-cycle.ts) — the WORKSPACE reader ONLY
 * (see that module's header for the LIBRARY/WORKSPACE split); the LIBRARY
 * reader (`readShippedCycles`) reads the vertical's cycles.yaml off disk and
 * never touches this table. */
export const cycleTypesRead = pgTable("cycle_types", {
	name: varchar("name"),
	description: text("description"),
	businessValue: varchar("business_value"),
	aliases: json("aliases").$type<string[]>(),
	typicalStages:
		json("typical_stages").$type<
			{ name: string; order: number; indicators: string[] }[]
		>(),
	completionIndicators: json("completion_indicators").$type<string[]>(),
	feedsInto: json("feeds_into").$type<string[]>(),
	supersededAt: timestamp("superseded_at", { mode: "date" }),
});

/** Raw `metrics` (DAT-882) — ONLY the description/DAG-body columns
 * `readWorkspaceMetricDag` (teach-metric.ts) needs (the generated `./schema.ts`
 * `metrics` view already carries graphId/name/category etc. — this is a distinct,
 * narrower hand-declaration, not a replacement of that export). Backs the
 * WORKSPACE reader ONLY (see teach-metric.ts's module header for the
 * LIBRARY/WORKSPACE split) — consumed by `readWorkspaceMetricDag`'s two callers
 * (`teachMetric`'s shadow detection + `/api/shipped-metric-dag`); the LIBRARY
 * reader (`readShippedMetrics`) rglobs the vertical's metrics/**​/*.yaml off disk
 * and never touches this table. `output` / `dependencies` stay `unknown` at
 * this boundary (rule 11) — opaque pass-through to the induction few-shot /
 * override-shadow canvas, never inspected here. */
export const metricDagRead = pgTable("metrics", {
	graphId: varchar("graph_id"),
	name: varchar("name"),
	description: text("description"),
	category: varchar("category"),
	output: json("output").$type<unknown>(),
	dependencies: json("dependencies").$type<unknown>(),
	supersededAt: timestamp("superseded_at", { mode: "date" }),
});

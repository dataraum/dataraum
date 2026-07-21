// Workspace Briefing — shared shapes (DAT-632, epic DAT-574).
//
// One deterministic "state of the union" read-model over already-persisted state.
// The module is split by concern: types (here) · assemble (pure derivation) ·
// next-actions (pure ladder) · project (pure per-chat split) · build (IO). The
// pure pieces are unit-tested over fixtures; `build` only fetches + delegates.
//
// Source-aware by construction: the unit is (source, table). Table names are
// stored source-qualified (`{source}__{table}`), so a workspace with several
// sources has several distinct tables that share a bare name — the briefing keeps
// the source and never collapses them.

import type { ConversationKind } from "#/db/cockpit/conversations";

/** The engine's readiness bands (`entropy_readiness.band`, DAT-442). */
export type ReadinessBand = "ready" | "investigate" | "blocked";

/**
 * A chat-stage's standing status, derived from snapshot heads + runs + attention.
 * `nothing_declared` is the analyse stage's own terminal state (DAT-845): the
 * operating_model run COMPLETED but the framed vertical declared no
 * validations/cycles/metrics, so no operating model exists — distinct from `empty`
 * (never run) and `ready` (promoted). Only analyse ever takes it (connect/stage
 * share this union but never derive it, like `needs_attention`).
 */
export type StageStatus =
	| "empty"
	| "in_progress"
	| "ready"
	| "needs_attention"
	| "nothing_declared";

export interface BriefingBandCounts {
	ready: number;
	investigate: number;
	blocked: number;
	/** Current tables with no resolved band yet (not analyzed). */
	unknown: number;
}

/** One current typed table, qualified by its source (names repeat across sources). */
export interface BriefingTable {
	tableId: string;
	/** Source display name. */
	source: string;
	/** De-prefixed table name (unique WITHIN a source). */
	name: string;
	/** Worst column band in the table; null until readiness has run. */
	band: ReadinessBand | null;
}

/**
 * The data inventory: a band summary plus the FULL source-qualified table list
 * (no cap — the UI paginates). Grouping/sorting is by source then worst band.
 */
export interface BriefingInventory {
	sourceCount: number;
	tableCount: number;
	bandCounts: BriefingBandCounts;
	tables: BriefingTable[];
}

export interface BriefingProgress {
	connect: StageStatus;
	stage: StageStatus;
	analyse: StageStatus;
}

export interface ReadinessBlocker {
	/** Raw engine target (`column:…` / `relationship:…` / `table:…`) — drives the
	 * drill seed (resolved to the matching `why_*` tool). */
	target: string;
	/** Source display name ("" for grains with no table, e.g. relationships). */
	source: string;
	/** Clean display label: for a column, the de-prefixed `table.column`. */
	label: string;
	band: ReadinessBand;
	/** Top driver label, or null when the row carries no parseable drivers. */
	topDriver: string | null;
}

/**
 * Operating-model artifacts (validations / cycles / metrics) that stalled short
 * of `executed` — COUNTS only, by type. The full lifecycle detail lives in the
 * Model route, so Governance summarizes + links rather than duplicating it.
 */
export interface StuckArtifactSummary {
	total: number;
	byType: { type: string; count: number }[];
}

export interface BriefingAttention {
	columnsBlocked: number;
	columnsInvestigate: number;
	/** ALL blocked targets, worst-risk first — no cap; the UI paginates. */
	readinessBlockers: ReadinessBlocker[];
	stuckArtifacts: StuckArtifactSummary;
	pendingTeaches: { count: number; needsReplay: boolean };
	awaitingInput: { workflowId: string; stage: string; note: string | null }[];
}

/** What the next action asks the user to do — maps to an existing cockpit tool. */
export type BriefingActionKind =
	| "review_blocker"
	| "replay"
	| "teach"
	| "begin_session"
	| "operating_model"
	// The framed vertical declares no validations/cycles/metrics (DAT-845) — the
	// honest nudge is to ADD declarations (frame), not to re-run the operating model.
	| "declare"
	| "answer";

export interface BriefingAction {
	kind: BriefingActionKind;
	label: string;
	/** Which chat this action belongs in — drives cross-chat routing. */
	targetChat: ConversationKind;
	/** Deterministic rank; lower = more urgent (sort ascending). */
	priority: number;
}

export interface WorkspaceBriefing {
	workspace: { id: string; vertical: string | null };
	inventory: BriefingInventory;
	progress: BriefingProgress;
	attention: BriefingAttention;
	nextActions: BriefingAction[];
}

// ── Per-chat projection ─────────────────────────────────────────────────────

/** A one-line pointer to attention waiting in another chat. */
export interface BriefingBackgroundPointer {
	chat: ConversationKind;
	label: string;
}

/** A briefing seen from one chat's seat: the actions it can act on plus a
 * one-liner per OTHER chat that has actions waiting. */
export interface ProjectedBriefing {
	kind: ConversationKind;
	foreground: BriefingAction[];
	background: BriefingBackgroundPointer[];
}

// ── Pure-assembly inputs (what `build` fetches and hands to `assemble`) ──────

/** One head-resolved readiness row, as the briefing reads it. */
export interface BriefingReadinessRow {
	target: string | null;
	tableId: string | null;
	columnId: string | null;
	band: string | null;
	worstIntentRisk: number | null;
	topDrivers: unknown;
}

/** One head-resolved lifecycle-artifact row. */
export interface BriefingArtifactRow {
	artifactType: string | null;
	artifactKey: string | null;
	state: string | null;
	stateReason: string | null;
}

/** Per-tableId source + display name, resolved once in the IO. */
export interface BriefingTableMeta {
	source: string;
	name: string;
}

/** Stage-promotion + in-flight flags, derived from snapshot heads + runs. */
export interface BriefingStageFlags {
	hasImportedTables: boolean;
	catalogPromoted: boolean;
	operatingModelPromoted: boolean;
	addSourceRunning: boolean;
	beginSessionRunning: boolean;
	operatingModelRunning: boolean;
	/**
	 * The workspace's LATEST operating_model run terminated `nothing_declared`
	 * (DAT-845): it COMPLETED without flipping the head because the framed vertical
	 * declared no validations/cycles/metrics. Distinct from `operatingModelPromoted`
	 * (no head flip) — the analyse stage's honest terminal state. Read from cockpit_db
	 * `runs` (the persisted outcome), NOT the metadata heads: a `nothing_declared` run
	 * writes nothing to `ws_<id>`.
	 */
	operatingModelNothingDeclared: boolean;
}

export interface BriefingAwaitingItem {
	workflowId: string;
	stage: string;
	awaitingNote: string | null;
}

export interface BriefingInputs {
	workspace: { id: string; vertical: string | null };
	readiness: BriefingReadinessRow[];
	artifacts: BriefingArtifactRow[];
	/** tableId → { source, display name } for every run's tableId. */
	tableMetaById: Record<string, BriefingTableMeta>;
	pendingTeachCount: number;
	awaitingInput: BriefingAwaitingItem[];
	flags: BriefingStageFlags;
}

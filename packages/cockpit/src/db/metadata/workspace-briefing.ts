// Workspace Briefing — the cockpit's single "state of the union" read-model
// (DAT-632, epic DAT-574). One deterministic aggregation over already-persisted
// state — the head-resolved `current_*` views (`ws_<id>`) plus `runs` (cockpit_db)
// — surfaced in three places: the chat-open canvas + agent digest (DAT-634), the
// Governance section (DAT-633), and the post-run completion note (DAT-635).
//
// No engine code path, no LLM: the engine already persists readiness, lifecycle
// artifacts and run status; this just reads, ranks and phrases. The evaluation
// lives in the cockpit by design (the agent-tier boundary, ADR-0004).
//
// Single active workspace: `buildWorkspaceBriefing()` takes no `wsId` — the
// metadata client is pinned to `ws_<config.dataraumWorkspaceId>_read`, so the
// briefing reads the active workspace like every other tool (the `$wsId` route
// param stays decorative until per-request scoping lands with DAT-357).
//
// Shape: the IO (`buildWorkspaceBriefing`) only fetches rows and hands them to the
// PURE `assembleBriefing` / `computeNextActions` / `projectBriefing`, which carry
// all the logic and are unit-tested over fixtures with no DB mocks.

import { count, eq } from "drizzle-orm";
import type { ConversationKind } from "#/db/cockpit/conversations";
import { resolveActiveWorkspaceRow } from "#/db/cockpit/registry";
import {
	type AwaitingInputItem,
	hasRunningRun,
	listAwaitingInput,
} from "#/db/cockpit/runs";
import { displayTableName } from "#/lib/display-names";
import { metadataDb } from "./client";
import { getPendingOverlays } from "./pending-overlays";
import { ReadinessDriver } from "./readiness-schemas";
import { CATALOG_HEAD_TARGET } from "./relationship-target";
import {
	columns,
	currentEntropyReadiness,
	currentLifecycleArtifacts,
	metadataSnapshotHead,
	sources,
	tables,
} from "./schema";

// ── Vocabulary ──────────────────────────────────────────────────────────────

/** The engine's readiness bands (`entropy_readiness.band`, DAT-442). */
export type ReadinessBand = "ready" | "investigate" | "blocked";

/** A chat-stage's standing status, derived from snapshot heads + runs + attention. */
export type StageStatus = "empty" | "in_progress" | "ready" | "needs_attention";

const BAND_RANK: Record<ReadinessBand, number> = {
	ready: 0,
	investigate: 1,
	blocked: 2,
};

/** Narrow the view's nullable `varchar` band to a known band, else null. */
function asBand(raw: string | null): ReadinessBand | null {
	if (raw === "ready" || raw === "investigate" || raw === "blocked") return raw;
	return null;
}

/** The worse (higher-rank) of two bands; null is "no signal yet" and loses. */
function worseBand(
	a: ReadinessBand | null,
	b: ReadinessBand | null,
): ReadinessBand | null {
	if (a === null) return b;
	if (b === null) return a;
	return BAND_RANK[a] >= BAND_RANK[b] ? a : b;
}

/** Caps — the read-model bounds its own ranked lists; the UI bounds further. */
const READINESS_BLOCKER_CAP = 5;
const STUCK_ARTIFACT_CAP = 10;
const AWAITING_INPUT_CAP = 10;

// ── The briefing shape ──────────────────────────────────────────────────────

export interface BriefingTable {
	/** De-prefixed display name (`displayTableName`). */
	name: string;
	columnCount: number;
	/** Worst column band in the table; null until readiness has run. */
	band: ReadinessBand | null;
}

export interface BriefingInventory {
	tableCount: number;
	tables: BriefingTable[];
}

export interface BriefingProgress {
	connect: StageStatus;
	stage: StageStatus;
	analyse: StageStatus;
}

export interface ReadinessBlocker {
	/** The engine readiness target (`column:…` / `relationship:…` / `table:…`). */
	target: string;
	band: ReadinessBand;
	/** Top driver label, or null when the row carries no parseable drivers. */
	topDriver: string | null;
}

export interface StuckArtifact {
	/** `artifact_type` — validation / business_cycle / metric. */
	type: string;
	/** `artifact_key` — e.g. the validation_id. */
	key: string;
	/** Lifecycle state it stalled at (`declared` / `grounded`). */
	state: string;
	/** Why it stopped short (`state_reason`). */
	reason: string;
}

export interface BriefingAttention {
	columnsBlocked: number;
	columnsInvestigate: number;
	readinessBlockers: ReadinessBlocker[];
	stuckArtifacts: StuckArtifact[];
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

/**
 * A briefing seen from one chat's seat: the actions this chat can act on
 * (`foreground`), and a one-liner per OTHER chat that has actions waiting
 * (`background`). State is singular (the full briefing); this only reorders
 * emphasis, so a Connect chat foregrounds typing/import and points at "Stage:
 * 3 columns blocked →" rather than dumping it inline.
 */
export interface ProjectedBriefing {
	kind: ConversationKind;
	foreground: BriefingAction[];
	background: BriefingBackgroundPointer[];
}

const ALL_KINDS: readonly ConversationKind[] = ["connect", "stage", "analyse"];

/**
 * Project the (already-ranked) briefing onto one chat kind. Pure — no recompute;
 * it partitions `nextActions` by `targetChat`. `nextActions` is assumed sorted by
 * priority (as `assembleBriefing` returns it), so the first action per kind is its
 * most urgent and becomes that kind's background pointer.
 */
export function projectBriefing(
	briefing: WorkspaceBriefing,
	kind: ConversationKind,
): ProjectedBriefing {
	const foreground = briefing.nextActions.filter((a) => a.targetChat === kind);
	const background: BriefingBackgroundPointer[] = [];
	for (const other of ALL_KINDS) {
		if (other === kind) continue;
		const top = briefing.nextActions.find((a) => a.targetChat === other);
		if (top !== undefined) background.push({ chat: other, label: top.label });
	}
	return { kind, foreground, background };
}

// ── Next-action rules ───────────────────────────────────────────────────────

/** Which chat owns a run-stage's follow-up (the stage's tools live there). */
function stageToChat(stage: string): ConversationKind {
	// add_source → Connect; begin_session + operating_model → Stage.
	return stage === "add_source" ? "connect" : "stage";
}

function plural(n: number, one: string, many: string): string {
	return n === 1 ? one : many;
}

/**
 * The deterministic call-to-action ladder over `progress` + `attention`, returned
 * sorted by priority (ascending). `awaiting_input` outranks everything — a run is
 * parked ON the user; then pending replays, then unblock-by-teach, then the
 * forward-motion staging actions, then "ready to answer".
 */
export function computeNextActions(
	progress: BriefingProgress,
	attention: BriefingAttention,
): BriefingAction[] {
	const actions: BriefingAction[] = [];

	// P0 — a run is parked waiting for a human teach (DAT-553). One per item so
	// the chat routing stays accurate; the note IS the human-facing reason.
	for (const item of attention.awaitingInput) {
		actions.push({
			kind: "review_blocker",
			label: item.note ?? `A ${item.stage} run needs your input`,
			targetChat: stageToChat(item.stage),
			priority: 0,
		});
	}

	// P1 — teaches written but not yet applied; a replay re-grounds them.
	if (attention.pendingTeaches.needsReplay) {
		const n = attention.pendingTeaches.count;
		actions.push({
			kind: "replay",
			label: `${n} ${plural(n, "teach", "teaches")} pending — replay to apply`,
			targetChat: "stage",
			priority: 1,
		});
	}

	// P2 — blocked columns need a teach to unblock.
	if (attention.columnsBlocked > 0) {
		const n = attention.columnsBlocked;
		actions.push({
			kind: "teach",
			label: `${n} ${plural(n, "column", "columns")} blocked — teach to unblock`,
			targetChat: "stage",
			priority: 2,
		});
	}

	// P2 — operating-model artifacts that couldn't ground; a teach fixes the bind.
	if (attention.stuckArtifacts.length > 0) {
		const n = attention.stuckArtifacts.length;
		actions.push({
			kind: "teach",
			label: `${n} operating-model ${plural(n, "item", "items")} need grounding — teach to fix`,
			targetChat: "stage",
			priority: 2,
		});
	}

	// P3 — forward motion: imported but not staged → begin_session.
	if (
		progress.stage === "empty" &&
		(progress.connect === "ready" || progress.connect === "needs_attention")
	) {
		actions.push({
			kind: "begin_session",
			label: "Tables imported — start a Stage chat to build the model",
			targetChat: "stage",
			priority: 3,
		});
	}

	// P3 — staged but no operating model yet → run it.
	if (
		progress.analyse === "empty" &&
		(progress.stage === "ready" || progress.stage === "needs_attention")
	) {
		actions.push({
			kind: "operating_model",
			label: "Model staged — run the operating model",
			targetChat: "stage",
			priority: 3,
		});
	}

	// P4 — everything's ready and nothing blocks answers.
	if (progress.analyse === "ready" && attention.columnsBlocked === 0) {
		actions.push({
			kind: "answer",
			label: "Ready to answer questions",
			targetChat: "analyse",
			priority: 4,
		});
	}

	// Stable sort by priority (Array.prototype.sort is stable) — preserves the
	// insertion order within a tier (e.g. blocked-columns before stuck-artifacts).
	return actions.sort((a, b) => a.priority - b.priority);
}

// ── Pure assembly ───────────────────────────────────────────────────────────

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

/** One table row joined to its source name, plus its column count. */
export interface BriefingTableRow {
	tableId: string;
	tableName: string;
	sourceName: string | null;
	columnCount: number;
}

/** Stage-promotion + in-flight flags, derived from snapshot heads + runs. */
export interface BriefingStageFlags {
	hasImportedTables: boolean;
	catalogPromoted: boolean;
	operatingModelPromoted: boolean;
	addSourceRunning: boolean;
	beginSessionRunning: boolean;
	operatingModelRunning: boolean;
}

export interface BriefingInputs {
	workspace: { id: string; vertical: string | null };
	tables: BriefingTableRow[];
	readiness: BriefingReadinessRow[];
	artifacts: BriefingArtifactRow[];
	pendingTeachCount: number;
	awaitingInput: AwaitingInputItem[];
	flags: BriefingStageFlags;
}

/** Top driver label from a readiness row's `top_drivers` JSONB (lenient). */
function topDriverLabel(topDrivers: unknown): string | null {
	const parsed = ReadinessDriver.array().safeParse(topDrivers);
	if (!parsed.success || parsed.data.length === 0) return null;
	return parsed.data[0].label;
}

function buildInventory(
	tableRows: BriefingTableRow[],
	worstByTable: Map<string, ReadinessBand | null>,
): BriefingInventory {
	const tablesOut: BriefingTable[] = tableRows.map((t) => ({
		name: displayTableName(t.tableName, t.sourceName ?? undefined),
		columnCount: t.columnCount,
		band: worstByTable.get(t.tableId) ?? null,
	}));
	tablesOut.sort((a, b) => a.name.localeCompare(b.name));
	return { tableCount: tablesOut.length, tables: tablesOut };
}

function deriveProgress(
	flags: BriefingStageFlags,
	columnsBlocked: number,
	stuckCount: number,
): BriefingProgress {
	const stageNeedsAttention = columnsBlocked > 0 || stuckCount > 0;

	const connect: StageStatus = flags.addSourceRunning
		? "in_progress"
		: flags.hasImportedTables
			? "ready"
			: "empty";

	const stage: StageStatus = flags.beginSessionRunning
		? "in_progress"
		: !flags.catalogPromoted
			? "empty"
			: stageNeedsAttention
				? "needs_attention"
				: "ready";

	const analyse: StageStatus = flags.operatingModelRunning
		? "in_progress"
		: !flags.operatingModelPromoted
			? "empty"
			: columnsBlocked > 0
				? "needs_attention"
				: "ready";

	return { connect, stage, analyse };
}

/**
 * Build the briefing from already-fetched rows. Pure — all the ranking/derivation
 * logic, no IO — so it's unit-tested directly over fixtures.
 */
export function assembleBriefing(input: BriefingInputs): WorkspaceBriefing {
	// Per-table worst column band + global blocked/investigate counts, in one pass
	// over the column-grain readiness rows (the view already resolves one row per
	// target, DAT-506 — no per-target pick needed here).
	const worstByTable = new Map<string, ReadinessBand | null>();
	let columnsBlocked = 0;
	let columnsInvestigate = 0;
	for (const r of input.readiness) {
		if (r.columnId === null) continue; // table/relationship grain — not a column
		const band = asBand(r.band);
		if (band === "blocked") columnsBlocked++;
		else if (band === "investigate") columnsInvestigate++;
		if (r.tableId !== null) {
			worstByTable.set(
				r.tableId,
				worseBand(worstByTable.get(r.tableId) ?? null, band),
			);
		}
	}

	// Ranked blockers across ALL grains (column + relationship + table): the worst
	// risk first, capped. Risk null sorts last.
	const readinessBlockers: ReadinessBlocker[] = input.readiness
		.filter((r) => asBand(r.band) === "blocked" && r.target !== null)
		.sort((a, b) => (b.worstIntentRisk ?? -1) - (a.worstIntentRisk ?? -1))
		.slice(0, READINESS_BLOCKER_CAP)
		.map((r) => ({
			target: r.target as string,
			band: "blocked",
			topDriver: topDriverLabel(r.topDrivers),
		}));

	// Stuck = stopped short of `executed` WITH a reason (a declared/grounded spec
	// that explicitly couldn't ground is actionable; one with no reason is just
	// not-yet-processed, so the reason gate is what keeps this from crying wolf).
	const stuckArtifacts: StuckArtifact[] = input.artifacts
		.filter(
			(a) =>
				a.state !== "executed" &&
				a.stateReason !== null &&
				a.stateReason !== "" &&
				a.artifactKey !== null,
		)
		// Deterministic order before the cap (the view's row order is unstable, so
		// slicing it raw would drop arbitrary offenders): by type, then key.
		.sort(
			(a, b) =>
				(a.artifactType ?? "").localeCompare(b.artifactType ?? "") ||
				(a.artifactKey ?? "").localeCompare(b.artifactKey ?? ""),
		)
		.slice(0, STUCK_ARTIFACT_CAP)
		.map((a) => ({
			type: a.artifactType ?? "",
			key: a.artifactKey as string,
			state: a.state ?? "",
			reason: a.stateReason as string,
		}));

	const attention: BriefingAttention = {
		columnsBlocked,
		columnsInvestigate,
		readinessBlockers,
		stuckArtifacts,
		pendingTeaches: {
			count: input.pendingTeachCount,
			needsReplay: input.pendingTeachCount > 0,
		},
		awaitingInput: input.awaitingInput.map((i) => ({
			workflowId: i.workflowId,
			stage: i.stage,
			note: i.awaitingNote,
		})),
	};

	const progress = deriveProgress(
		input.flags,
		columnsBlocked,
		stuckArtifacts.length,
	);
	const inventory = buildInventory(input.tables, worstByTable);
	const nextActions = computeNextActions(progress, attention);

	return {
		workspace: input.workspace,
		inventory,
		progress,
		attention,
		nextActions,
	};
}

// ── IO: fetch + assemble ────────────────────────────────────────────────────

/**
 * Read the active workspace's state of the union. Spans the two clients — the
 * `metadataDb` views (`ws_<id>`: tables, columns, readiness, lifecycle artifacts)
 * and cockpit_db `runs` (in-flight + awaiting-input) — then hands the rows to the
 * pure `assembleBriefing`. No `wsId` param: the active workspace, like every tool.
 */
export async function buildWorkspaceBriefing(): Promise<WorkspaceBriefing> {
	const workspace = await resolveActiveWorkspaceRow();

	const [
		tableRows,
		columnCounts,
		readinessRows,
		artifactRows,
		headRows,
		pendingTeaches,
		awaitingInput,
		addSourceRunning,
		beginSessionRunning,
		operatingModelRunning,
	] = await Promise.all([
		metadataDb
			.select({
				tableId: tables.tableId,
				tableName: tables.tableName,
				sourceName: sources.name,
			})
			.from(tables)
			.leftJoin(sources, eq(sources.sourceId, tables.sourceId)),
		metadataDb
			.select({ tableId: columns.tableId, n: count() })
			.from(columns)
			.groupBy(columns.tableId),
		metadataDb
			.select({
				target: currentEntropyReadiness.target,
				tableId: currentEntropyReadiness.tableId,
				columnId: currentEntropyReadiness.columnId,
				band: currentEntropyReadiness.band,
				worstIntentRisk: currentEntropyReadiness.worstIntentRisk,
				topDrivers: currentEntropyReadiness.topDrivers,
			})
			.from(currentEntropyReadiness),
		metadataDb
			.select({
				artifactType: currentLifecycleArtifacts.artifactType,
				artifactKey: currentLifecycleArtifacts.artifactKey,
				state: currentLifecycleArtifacts.state,
				stateReason: currentLifecycleArtifacts.stateReason,
			})
			.from(currentLifecycleArtifacts),
		metadataDb
			.select({
				target: metadataSnapshotHead.target,
				stage: metadataSnapshotHead.stage,
			})
			.from(metadataSnapshotHead),
		getPendingOverlays(),
		listAwaitingInput(workspace.id, AWAITING_INPUT_CAP),
		hasRunningRun(workspace.id, "add_source"),
		hasRunningRun(workspace.id, "begin_session"),
		hasRunningRun(workspace.id, "operating_model"),
	]);

	// Column count per table — a grouped aggregate, so a wide workspace doesn't
	// stream every column row over the wire just to be counted.
	const columnCountByTable = new Map<string, number>();
	for (const c of columnCounts) {
		if (c.tableId === null) continue;
		columnCountByTable.set(c.tableId, c.n);
	}
	const briefingTables: BriefingTableRow[] = tableRows
		.filter((t): t is typeof t & { tableId: string } => t.tableId !== null)
		.map((t) => ({
			tableId: t.tableId,
			tableName: t.tableName ?? "",
			sourceName: t.sourceName,
			columnCount: columnCountByTable.get(t.tableId) ?? 0,
		}));

	const flags: BriefingStageFlags = {
		hasImportedTables: briefingTables.length > 0,
		catalogPromoted: headRows.some(
			(h) => h.target === CATALOG_HEAD_TARGET && h.stage === "catalog",
		),
		operatingModelPromoted: headRows.some(
			(h) => h.target === CATALOG_HEAD_TARGET && h.stage === "operating_model",
		),
		addSourceRunning,
		beginSessionRunning,
		operatingModelRunning,
	};

	return assembleBriefing({
		workspace: { id: workspace.id, vertical: workspace.vertical },
		tables: briefingTables,
		readiness: readinessRows,
		artifacts: artifactRows,
		pendingTeachCount: pendingTeaches.length,
		awaitingInput,
		flags,
	});
}

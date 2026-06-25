// Pure assembly (DAT-632) — turns already-fetched rows into the WorkspaceBriefing.
// All the derivation/ranking lives here, no IO, so it's unit-tested over fixtures.

import { ReadinessDriver } from "../readiness-schemas";
import { computeNextActions } from "./next-actions";
import type {
	BriefingAttention,
	BriefingBandCounts,
	BriefingInputs,
	BriefingInventory,
	BriefingProgress,
	BriefingStageFlags,
	BriefingTable,
	BriefingTableMeta,
	ReadinessBand,
	ReadinessBlocker,
	StageStatus,
	StuckArtifactSummary,
	WorkspaceBriefing,
} from "./types";

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

/** Top driver label from a readiness row's `top_drivers` JSONB (lenient). */
function topDriverLabel(topDrivers: unknown): string | null {
	const parsed = ReadinessDriver.array().safeParse(topDrivers);
	if (!parsed.success || parsed.data.length === 0) return null;
	return parsed.data[0].label;
}

/** Clean display label for a blocker target. For a column, the de-prefixed
 * `table.column` (table name from `meta`); otherwise the parsed target. */
function blockerLabel(
	target: string,
	meta: BriefingTableMeta | undefined,
): string {
	if (target.startsWith("column:")) {
		const rest = target.slice("column:".length);
		const dot = rest.indexOf(".");
		const col = dot >= 0 ? rest.slice(dot + 1) : rest;
		const tbl = meta?.name ?? (dot >= 0 ? rest.slice(0, dot) : rest);
		return `${tbl}.${col}`;
	}
	if (target.startsWith("relationship:")) return "relationship";
	if (target.startsWith("table:")) return target.slice("table:".length);
	return target;
}

/**
 * Inventory over the CURRENT table set — `worstByTable`'s keys, which are the
 * head-resolved `current_entropy_readiness` tableIds (each logical table once
 * PER SOURCE). Source-qualified via `metaById`; sorted by source, then worst
 * band, then name. No cap — the UI paginates.
 */
function buildInventory(
	worstByTable: Map<string, ReadinessBand | null>,
	metaById: Record<string, BriefingTableMeta>,
): BriefingInventory {
	const bandCounts: BriefingBandCounts = {
		ready: 0,
		investigate: 0,
		blocked: 0,
		unknown: 0,
	};
	const sources = new Set<string>();
	const tables: BriefingTable[] = [];
	for (const [tableId, band] of worstByTable) {
		const meta = metaById[tableId];
		const source = meta?.source ?? "";
		sources.add(source);
		if (band === null) bandCounts.unknown++;
		else bandCounts[band]++;
		tables.push({ tableId, source, name: meta?.name ?? tableId, band });
	}
	tables.sort(
		(a, b) =>
			a.source.localeCompare(b.source) ||
			BAND_RANK[(b.band ?? "ready") as ReadinessBand] -
				BAND_RANK[(a.band ?? "ready") as ReadinessBand] ||
			a.name.localeCompare(b.name),
	);
	return {
		sourceCount: sources.size,
		tableCount: tables.length,
		bandCounts,
		tables,
	};
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

/** Build the briefing from already-fetched rows. Pure. */
export function assembleBriefing(input: BriefingInputs): WorkspaceBriefing {
	// Per-table worst column band + global blocked/investigate counts, in one pass
	// over the column-grain readiness rows (the view resolves one row per target,
	// DAT-506). The tableIds seen here ARE the current table set (head-resolved),
	// so they drive the inventory.
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

	// ALL blocked targets, worst risk first (no cap — the UI paginates). Label is
	// de-prefixed for display; the raw target still drives the drill. Source kept
	// so a bare `table.column` is never ambiguous across sources.
	const readinessBlockers: ReadinessBlocker[] = input.readiness
		.filter((r) => asBand(r.band) === "blocked" && r.target !== null)
		.sort((a, b) => (b.worstIntentRisk ?? -1) - (a.worstIntentRisk ?? -1))
		.map((r) => {
			const meta =
				r.tableId !== null ? input.tableMetaById[r.tableId] : undefined;
			return {
				target: r.target as string,
				source: meta?.source ?? "",
				label: blockerLabel(r.target as string, meta),
				band: "blocked" as const,
				topDriver: topDriverLabel(r.topDrivers),
			};
		});

	// Stuck = stopped short of `executed` WITH a reason — counts by type only; the
	// full per-item detail lives in the Model route.
	const stuckByType = new Map<string, number>();
	let stuckTotal = 0;
	for (const a of input.artifacts) {
		if (a.state === "executed" || !a.stateReason || a.artifactKey === null) {
			continue;
		}
		stuckTotal++;
		const t = a.artifactType ?? "";
		stuckByType.set(t, (stuckByType.get(t) ?? 0) + 1);
	}
	const stuckArtifacts: StuckArtifactSummary = {
		total: stuckTotal,
		byType: [...stuckByType.entries()]
			.map(([type, count]) => ({ type, count }))
			.sort((a, b) => a.type.localeCompare(b.type)),
	};

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

	const progress = deriveProgress(input.flags, columnsBlocked, stuckTotal);
	const inventory = buildInventory(worstByTable, input.tableMetaById);
	const nextActions = computeNextActions(progress, attention);

	return {
		workspace: input.workspace,
		inventory,
		progress,
		attention,
		nextActions,
	};
}

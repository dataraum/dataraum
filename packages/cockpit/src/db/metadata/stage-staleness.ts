// Derived stage staleness (DAT-531) — "which stage's result is behind?" computed
// from the generation-head LOG, never a stored flag (the WAL analogy: staleness is
// just reading whether your head trails an upstream write). Two derived sources,
// both reads:
//
//   teach-pending  — an un-superseded config_overlay of type T was written AFTER
//                    stage(T)'s head was promoted ⇒ that stage hasn't applied the
//                    teach yet (uses the teach→stage map).
//   upstream-newer — a stage's head is older than an upstream stage's head ⇒ its
//                    result predates fresher upstream data (the cascade).
//
// Self-healing: a re-run advances the stage's head, so the staleness clears with no
// reconcile. Conservative-correct: a newer upstream head always flags stale even if
// the output wouldn't change (worst case: an offered no-op re-run, never a miss).
//
// The DB read lives below `deriveStaleness`/`collapseHeads`, which are PURE (the
// real SQL is smoke-verified per the metadata-read convention; the logic is
// unit-tested in isolation).

import { isNull } from "drizzle-orm";
import type { RunStage } from "#/db/cockpit/runs";
import { metadataDb } from "#/db/metadata/client";
import {
	CATALOG_HEAD_TARGET,
	GENERATION_STAGE,
} from "#/db/metadata/relationship-target";
import { metadataSnapshotHead } from "#/db/metadata/schema";
import { configOverlayWrite } from "#/db/metadata/write-surface";
import { affectedStage } from "#/tools/teach-routing";

/** Upstream → downstream. Staleness only ever looks LEFT of a stage. */
const STAGE_ORDER: readonly RunStage[] = [
	"add_source",
	"begin_session",
	"operating_model",
];

/** The `metadata_snapshot_head.stage` value each pipeline stage promotes under
 * (`storage/snapshot_head.py`): add_source promotes per-table `generation` heads;
 * begin_session the workspace `catalog` head; operating_model its own. */
const HEAD_STAGE_FOR: Record<RunStage, string> = {
	add_source: GENERATION_STAGE,
	begin_session: "catalog",
	operating_model: "operating_model",
};

/** One raw head row (a slice of `metadata_snapshot_head`). */
export interface RawHead {
	target: string;
	stage: string;
	promotedAt: Date;
}

/** A pipeline stage's effective head time — the latest moment it promoted output.
 * `null` = the stage has never run (not stale, just unrun). */
export interface StageHead {
	stage: RunStage;
	promotedAt: Date | null;
}

/** An un-superseded teach overlay — only `type` + `createdAt` matter to staleness. */
export interface OverlayRow {
	type: string;
	createdAt: Date;
}

export type StaleReason = "teach-pending" | "upstream-newer";

export interface StageStaleness {
	stage: RunStage;
	stale: boolean;
	/** Why it's stale, or null when fresh / unrun. */
	reason: StaleReason | null;
}

/**
 * Collapse the raw per-(target,stage) heads into one effective head time per
 * pipeline stage. add_source spans many per-table `generation` heads → the MAX
 * promoted_at (the most recent re-ground); begin_session/operating_model are the
 * single `catalog`-target heads. A stage with no head row is `null` (unrun).
 */
export function collapseHeads(heads: readonly RawHead[]): StageHead[] {
	return STAGE_ORDER.map((stage) => {
		const headStage = HEAD_STAGE_FOR[stage];
		const times = heads
			.filter((h) => {
				if (h.stage !== headStage) return false;
				// begin_session / operating_model are catalog-target; add_source's
				// generation heads are per-table (target `table:<id>`), so don't filter
				// those by target — any generation head counts toward the data's freshness.
				if (stage === "add_source") return true;
				return h.target === CATALOG_HEAD_TARGET;
			})
			.map((h) => h.promotedAt.getTime());
		return {
			stage,
			promotedAt: times.length ? new Date(Math.max(...times)) : null,
		};
	});
}

/**
 * Derive per-stage staleness from the collapsed heads + the un-superseded teach
 * overlays. Pure — no I/O. An unrun stage (`promotedAt === null`) is never "stale"
 * (it's simply not run yet; readiness/the monitor cover that). teach-pending wins
 * over upstream-newer when both hold (the more actionable reason).
 */
export function deriveStaleness(
	heads: readonly StageHead[],
	overlays: readonly OverlayRow[],
): StageStaleness[] {
	const headBy = new Map(heads.map((h) => [h.stage, h.promotedAt]));
	return STAGE_ORDER.map((stage, i) => {
		const myHead = headBy.get(stage) ?? null;
		if (myHead === null) return { stage, stale: false, reason: null };

		const teachPending = overlays.some(
			(o) => affectedStage(o.type) === stage && o.createdAt > myHead,
		);
		if (teachPending) return { stage, stale: true, reason: "teach-pending" };

		const upstreamNewer = STAGE_ORDER.slice(0, i).some((up) => {
			const upHead = headBy.get(up) ?? null;
			return upHead !== null && upHead > myHead;
		});
		return {
			stage,
			stale: upstreamNewer,
			reason: upstreamNewer ? "upstream-newer" : null,
		};
	});
}

/**
 * Read the workspace's per-stage staleness (DAT-531) — the thin I/O shell over the
 * pure logic above: fetch the generation-head log + the un-superseded teach
 * overlays, collapse, derive. Workspace-scoped via the `ws_<id>` metadata schema
 * the read client binds (DAT-505). Both reads are over engine metadata (read-only);
 * the SQL is smoke-verified per convention (the logic is unit-tested). Degrades to
 * "nothing stale" on a read blip — a missing staleness hint is a soft advisory, it
 * must never break the monitor.
 */
export async function readStageStaleness(): Promise<StageStaleness[]> {
	try {
		const [rawHeads, overlays] = await Promise.all([
			metadataDb
				.select({
					target: metadataSnapshotHead.target,
					stage: metadataSnapshotHead.stage,
					promotedAt: metadataSnapshotHead.promotedAt,
				})
				.from(metadataSnapshotHead),
			metadataDb
				.select({
					type: configOverlayWrite.type,
					createdAt: configOverlayWrite.createdAt,
				})
				.from(configOverlayWrite)
				.where(isNull(configOverlayWrite.supersededAt)),
		]);
		// The head view's columns are nullable; keep only complete rows.
		const heads: RawHead[] = rawHeads.flatMap((h) =>
			h.target && h.stage && h.promotedAt
				? [{ target: h.target, stage: h.stage, promotedAt: h.promotedAt }]
				: [],
		);
		return deriveStaleness(collapseHeads(heads), overlays);
	} catch (err) {
		console.warn(`[stage-staleness] read failed, assuming fresh: ${err}`);
		return [];
	}
}

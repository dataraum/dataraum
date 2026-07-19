// The DB read that feeds the pure stage-staleness logic (DAT-531). Split from
// `stage-staleness.ts` so the pure module stays client-free (a unit test importing
// the logic must not drag in the postgres client — the at-import hang trap). Both
// reads are SELECTs over engine metadata; the SQL is smoke-verified per the
// metadata-read convention (`grounding-readiness.ts` precedent).

import { isNull } from "drizzle-orm";
import { metadataDb, metadataWriteDb } from "#/db/metadata/client";
import { metadataSnapshotHead } from "#/db/metadata/schema";
import {
	collapseHeads,
	deriveStaleness,
	type RawHead,
	type StageStaleness,
} from "#/db/metadata/stage-staleness";
import { configOverlayWrite } from "#/db/metadata/write-surface";

/**
 * Read the workspace's per-stage staleness (DAT-531): fetch the generation-head log
 * + the un-superseded teach overlays, collapse, derive. Workspace-scoped via the
 * metadata roles' search_paths (DAT-816). Degrades to "nothing stale" on a read
 * blip — a missing staleness hint is a soft advisory, it must never break the
 * monitor.
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
			// Raw config_overlay rides the WRITER role (its search_path is the
			// raw schema); the typed write-surface columns keep the non-null
			// shape deriveStaleness expects.
			metadataWriteDb
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

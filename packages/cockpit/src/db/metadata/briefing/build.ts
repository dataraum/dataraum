// IO (DAT-632) — fetch the active workspace's state across the two clients
// (`metadataDb` views + cockpit_db `runs`) and hand the rows to the pure
// `assembleBriefing`. No workspace param: the cockpit boots with a single
// workspace identity and the metadata client's DB role resolves the schema.

import { eq } from "drizzle-orm";
import { resolveActiveWorkspaceRow } from "#/db/cockpit/registry";
import { hasRunningRun, listAwaitingInput } from "#/db/cockpit/runs";
import { displayTableName } from "#/lib/display-names";
import { metadataDb } from "../client";
import { getPendingOverlays } from "../pending-overlays";
import { CATALOG_HEAD_TARGET } from "../relationship-target";
import {
	currentEntropyReadiness,
	currentLifecycleArtifacts,
	metadataSnapshotHead,
	sources,
	tables,
} from "../schema";
import { assembleBriefing } from "./assemble";
import type {
	BriefingStageFlags,
	BriefingTableMeta,
	WorkspaceBriefing,
} from "./types";

// "Needs you" is a self-clearing worklist, not a log — naturally small, but read
// generously (no UI cap; the page paginates everything else).
const AWAITING_INPUT_LIMIT = 200;

export async function buildWorkspaceBriefing(): Promise<WorkspaceBriefing> {
	const workspace = await resolveActiveWorkspaceRow();

	const [
		tableRows,
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
		listAwaitingInput(workspace.id, AWAITING_INPUT_LIMIT),
		hasRunningRun(workspace.id, "add_source"),
		hasRunningRun(workspace.id, "begin_session"),
		hasRunningRun(workspace.id, "operating_model"),
	]);

	// tableId → { source, de-prefixed name }, over every run's tableId. The
	// inventory looks up only the CURRENT ones (head-resolved readiness tableIds),
	// but a blocker's table may be any of them, so resolve them all.
	const tableMetaById: Record<string, BriefingTableMeta> = {};
	for (const t of tableRows) {
		if (t.tableId !== null) {
			tableMetaById[t.tableId] = {
				source: t.sourceName ?? "",
				name: displayTableName(t.tableName ?? "", t.sourceName ?? undefined),
			};
		}
	}

	const flags: BriefingStageFlags = {
		hasImportedTables: tableRows.length > 0,
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
		readiness: readinessRows,
		artifacts: artifactRows,
		tableMetaById,
		pendingTeachCount: pendingTeaches.length,
		awaitingInput,
		flags,
	});
}

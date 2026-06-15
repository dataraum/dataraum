// add_source integration smoke (DAT-344; per-table fan-out DAT-370; teach+replay
// DAT-343): prove the Client → Python workflow path end-to-end against a running
// compose stack (temporal + engine-worker + postgres). A dev/test harness — NOT
// app code (lives in scripts/, not src/); run manually against a live stack.
//
// In production the addSourceWorkflow's caller seeds the Source +
// InvestigationSession. Here this script does it directly, then drives:
//
//   1. initial addSourceWorkflow run — asserts import discovered raw tables
//      and every table was processed to a typed table.
//   2. two teaches via `teach(...)` (batchable; no replay between them).
//   3. one replay via `replay(...)` — a full add_source re-run (DAT-413: no
//      scope, no from_phase; the engine mints a fresh run_id internally).
//      Asserts the re-run completes as a NEW Temporal execution (a fresh
//      Temporal run id — the engine's internal snapshot run_id is opaque to the
//      Client), the table count is stable, and the active teach overlay count
//      did NOT drop (replay reads the overlays, it doesn't consume them).
//
// Run against the published compose ports, e.g.:
//   COCKPIT_DATABASE_URL=postgresql://dataraum:dataraum@localhost:5432/cockpit \
//   METADATA_DATABASE_URL=postgresql://dataraum:dataraum@localhost:5432/dataraum \
//   DATARAUM_WORKSPACE_ID=00000000-0000-0000-0000-000000000001 \
//   DATARAUM_LAKE_PATH=/var/lib/dataraum/lake \
//   ANTHROPIC_API_KEY=$ANTHROPIC_API_KEY \
//   TEMPORAL_HOST=localhost:7233 TEMPORAL_NAMESPACE=default \
//   TEMPORAL_TASK_QUEUE=dataraum-pipeline \
//   S3_BUCKET=dataraum-lake \
//   SOURCE_PATH=s3://dataraum-lake/invoices.csv,s3://dataraum-lake/payments.csv \
//   bun run scripts/smoke-add-source.ts

import { randomUUID } from "node:crypto";
import { Client, Connection } from "@temporalio/client";
import { count, isNull } from "drizzle-orm";
import { z } from "zod";
import { recordRun } from "#/db/cockpit/runs";
import { metadataDb } from "#/db/metadata/client";
import { configOverlay } from "#/db/metadata/schema";
import { sourcesWrite } from "#/db/metadata/write-surface";
import { replay } from "#/tools/replay";
import { teach } from "#/tools/teach";
import type { AddSourceInput, AddSourceResult } from "#/temporal/types";
import { addSourceWorkflowId } from "#/temporal/workflow-id";

const env = z
	.object({
		TEMPORAL_HOST: z.string().min(1),
		TEMPORAL_NAMESPACE: z.string().min(1),
		TEMPORAL_TASK_QUEUE: z.string().min(1),
		DATARAUM_WORKSPACE_ID: z.string().min(1),
		// Object-store bucket holding both the lake and uploaded source files.
		S3_BUCKET: z.string().min(1).default("dataraum-lake"),
		// One or more source URIs the engine-worker reads over httpfs (DAT-389) —
		// opaque s3:// URIs, no sources mount. COMMA-SEPARATED for a multi-table
		// source (the add_source fan-out types each file into its own table).
		// REQUIRED: the driver names its data explicitly — no hidden fixture default.
		SOURCE_PATH: z.string().min(1),
	})
	.parse(process.env);

const fileUris = env.SOURCE_PATH.split(",")
	.map((u) => u.trim())
	.filter(Boolean);

async function seed(sourceId: string, sessionId: string): Promise<void> {
	// Seed the `sources` row through the Drizzle metadata write seam (the one-gate
	// `select` writes it the same way). No engine session seed (DAT-506): sessions
	// live in cockpit_db. This driver seeds DIRECTLY (not through select), then
	// records the run in cockpit_db so replay can resolve the session's sources
	// (replay reads cockpit_db sessions/session_runs → engine run_tables).
	// Source.name is UNIQUE — keep it unique per run so the driver is repeatable.
	const name = `source_${sourceId.slice(0, 8)}`;
	const now = new Date();
	await metadataDb
		.insert(sourcesWrite)
		.values({
			sourceId,
			name,
			sourceType: "csv",
			connectionConfig: { file_uris: fileUris },
			status: "configured",
			createdAt: now,
			updatedAt: now,
		})
		.onConflictDoNothing({ target: sourcesWrite.sourceId });
	// Record the cockpit session + run (DAT-506) keyed by this session id, so the
	// replay step below resolves the session's sources from cockpit_db.
	await recordRun({
		workspaceId: env.DATARAUM_WORKSPACE_ID,
		engineSessionId: sessionId,
		kind: "onboarding",
		stage: "add_source",
		workflowId: addSourceWorkflowId(env.DATARAUM_WORKSPACE_ID, sessionId),
	});
}

async function countOverlays(): Promise<number> {
	// Count the active (non-superseded) overlay rows in the workspace's schema —
	// proves the teach rows landed. Drizzle issues real SQL to Postgres (no echo
	// to be fooled by), and the teaches we're counting were written through this
	// same seam. Workspace scope is implicit in the ws_<id> schema (DAT-343
	// dropped the workspace_id column).
	const [row] = await metadataDb
		.select({ n: count() })
		.from(configOverlay)
		.where(isNull(configOverlay.supersededAt));
	return row?.n ?? 0;
}

async function runInitial(
	client: Client,
	sourceId: string,
	sessionId: string,
): Promise<{ result: AddSourceResult; runId: string }> {
	const input: AddSourceInput = {
		// FLAT, source-free input (DAT-506): no identity, no session/source id on the
		// wire. The run's source SET (DAT-422) — one source here, so a 1-element set.
		workspace_id: env.DATARAUM_WORKSPACE_ID,
		sources: [sourceId],
		// `_adhoc` is the empty / start-here vertical (DAT-371), on the workflow INPUT
		// now (DAT-506): cold-start induction generates concepts from the data and
		// stores them as `concept` overlay rows. This smoke is the real DAT-371
		// acceptance test — a clean run proves induction works against the mounted config.
		verticals: ["_adhoc"],
	};
	// `start` (not `execute`) so we can capture the run id — the replay
	// assertion compares its fresh run_id against the initial one.
	const handle = await client.workflow.start<
		(input: AddSourceInput) => Promise<AddSourceResult>
	>("addSourceWorkflow", {
		taskQueue: env.TEMPORAL_TASK_QUEUE,
		workflowId: addSourceWorkflowId(env.DATARAUM_WORKSPACE_ID, sessionId),
		args: [input],
	});
	const result = (await handle.result()) as AddSourceResult;

	if (result.raw_table_ids.length === 0) {
		throw new Error("initial run: import discovered no raw tables");
	}
	if (result.tables.length !== result.raw_table_ids.length) {
		throw new Error(
			`initial run: fan-out incomplete: ${result.tables.length} processed vs ` +
				`${result.raw_table_ids.length} raw tables`,
		);
	}
	for (const table of result.tables) {
		if (!table.typed_table_id) {
			throw new Error(
				`initial run: table ${table.raw_table_id} produced no typed table`,
			);
		}
	}
	return { result, runId: handle.firstExecutionRunId };
}

async function awaitReplay(
	client: Client,
	workflowId: string,
	runId: string,
): Promise<AddSourceResult> {
	// The replay reuses the run's session, so its workflow id is the session-keyed
	// one the replay returned (DAT-422) — get the handle by that exact id + run.
	const handle = client.workflow.getHandle(workflowId, runId);
	return (await handle.result()) as AddSourceResult;
}

async function main(): Promise<void> {
	const sourceId = randomUUID();
	const sessionId = randomUUID();
	await seed(sourceId, sessionId);

	const connection = await Connection.connect({ address: env.TEMPORAL_HOST });
	try {
		const client = new Client({
			connection,
			namespace: env.TEMPORAL_NAMESPACE,
		});

		// ---- Initial run -------------------------------------------------
		const { result: initial, runId: initialRunId } = await runInitial(
			client,
			sourceId,
			sessionId,
		);
		console.log(
			`✓ initial run: ${initial.tables.length} table(s) fanned out + typed via Temporal`,
		);

		// ---- Two teaches (batched — no replay between them) --------------
		const teach1 = await teach({
			type: "type_pattern",
			payload: {
				name: `drive_iso_date_${sourceId.slice(0, 8)}`,
				pattern: "^\\d{4}-\\d{2}-\\d{2}$",
				inferred_type: "DATE",
			},
		});
		const teach2 = await teach({
			type: "null_value",
			payload: {
				category: "placeholder_nulls",
				value: `drive_placeholder_${sourceId.slice(0, 8)}`,
			},
		});
		console.log(`✓ wrote teaches: ${teach1.overlay_id}, ${teach2.overlay_id}`);

		const overlayCount = await countOverlays();
		if (overlayCount < 2) {
			throw new Error(
				`expected at least 2 active overlay rows post-teach, got ${overlayCount}`,
			);
		}

		// ---- Replay: re-run the workspace's sources (DAT-422, DAT-506) ----
		// Replay takes the session we just built (the named unit), resolves the
		// workspace's imported sources (the generation heads), and re-runs add_source
		// over them as a NEW session — the engine mints a fresh run_id internally. No
		// scope/from_phase; a full, non-destructive re-run. The vertical is the
		// workspace property (sourced from the registry, not passed here).
		const replayResult = await replay({ session_id: sessionId });
		const replayed = await awaitReplay(
			client,
			replayResult.workflow_id,
			replayResult.run_id,
		);

		// A replay is a fresh execution, so it carries a NEW run_id (≠ the
		// initial run). The engine mints the version run_id internally; here we
		// only assert the replay started its own Temporal run.
		if (replayResult.run_id === initialRunId) {
			throw new Error(
				`replay: expected a fresh run_id, got the initial run ${initialRunId}`,
			);
		}

		// The re-run is non-destructive (versioned, append-only snapshots — no
		// in-place delete/re-type surgery): it re-processes the same source, so
		// the table count is stable across the re-run.
		if (replayed.tables.length !== initial.tables.length) {
			throw new Error(
				`replay: table count changed ${initial.tables.length} -> ${replayed.tables.length}`,
			);
		}
		console.log(
			`✓ replay (full re-run): ${replayed.tables.length} table(s) re-processed ` +
				`under fresh run ${replayResult.run_id}`,
		);

		// Sanity: the overlay rows are still active after replay (replay
		// reads them; it doesn't supersede them — that's `undoTeach`'s job).
		const postReplayCount = await countOverlays();
		if (postReplayCount < overlayCount) {
			throw new Error(
				`replay: active overlay count dropped from ${overlayCount} to ${postReplayCount} ` +
					"(should not — replay reads but doesn't consume)",
			);
		}

		console.log("✓ smoke complete — teach+replay end-to-end via cockpit tools");
	} finally {
		await connection.close();
	}
}

main().catch((err) => {
	console.error("✗ drive failed:", err);
	process.exit(1);
});

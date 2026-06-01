// Integration driver (DAT-344; per-table fan-out DAT-370; teach+replay DAT-343):
// prove the Client → Python workflow path end-to-end against a running compose
// stack (temporal + engine-worker + postgres).
//
// In production the addSourceWorkflow's caller seeds the Source +
// InvestigationSession. Here this script does it directly, then drives:
//
//   1. initial addSourceWorkflow run — asserts import discovered raw tables
//      and every table was processed to a typed table.
//   2. two teaches via `teach(...)` (batchable; no replay between them).
//   3. one replay via `replay(...)` with from_phase="typing" — asserts the
//      replay completed, the typed_table_ids are STABLE across the in-place
//      re-type (DAT-373: typing reconciles Columns by name, no re-mint), and
//      both teach overlay rows landed in config_overlay.
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
//   SOURCE_PATH=s3://dataraum-lake/orders.csv \
//   bun run src/temporal/drive-add-source.ts

import { randomUUID } from "node:crypto";
import { Client, Connection } from "@temporalio/client";
import postgres from "postgres";
import { z } from "zod";
import { replay } from "../tools/replay";
import { teach } from "../tools/teach";
import type { AddSourceInput, AddSourceResult } from "./types";
import { addSourceWorkflowId } from "./workflow-id";

const env = z
	.object({
		TEMPORAL_HOST: z.string().min(1),
		TEMPORAL_NAMESPACE: z.string().min(1),
		TEMPORAL_TASK_QUEUE: z.string().min(1),
		METADATA_DATABASE_URL: z.string().min(1),
		DATARAUM_WORKSPACE_ID: z.string().min(1),
		// Object-store bucket holding both the lake and uploaded source files.
		S3_BUCKET: z.string().min(1).default("dataraum-lake"),
		// Source URI the engine-worker reads over httpfs (DAT-389). An opaque
		// s3:// URI — no sources mount. Defaults to the fixture the lane smoke
		// seeds into the bucket root; uploads land under s3://<bucket>/uploads/.
		SOURCE_PATH: z.string().optional(),
	})
	.parse(process.env);

const sourcePath = env.SOURCE_PATH ?? `s3://${env.S3_BUCKET}/orders.csv`;

const schema = `ws_${env.DATARAUM_WORKSPACE_ID.replaceAll("-", "_")}`;

async function seed(sourceId: string, sessionId: string): Promise<void> {
	// Source.name is UNIQUE — keep it unique per run so the driver is repeatable.
	const name = `orders_${sourceId.slice(0, 8)}`;
	const sql = postgres(env.METADATA_DATABASE_URL, { onnotice: () => {} });
	try {
		await sql.begin(async (tx) => {
			await tx.unsafe(`SET LOCAL search_path TO "${schema}", public`);
			await tx`
				INSERT INTO sources (source_id, name, source_type, connection_config, status, created_at, updated_at)
				VALUES (${sourceId}, ${name}, 'csv', ${sql.json({ file_uris: [sourcePath] })}, 'configured', now(), now())
				ON CONFLICT (source_id) DO NOTHING`;
			await tx`
				INSERT INTO investigation_sessions (session_id, source_id, intent, status, started_at, step_count)
				VALUES (${sessionId}, ${sourceId}, 'e4a drive', 'active', now(), 0)
				ON CONFLICT (session_id) DO NOTHING`;
		});
	} finally {
		await sql.end();
	}
}

async function countOverlays(): Promise<number> {
	// Counted via raw SQL (not Drizzle) so the assertion stays independent
	// of the metadata client — proves the rows actually landed in the
	// engine's schema, not just that Drizzle returned what it inserted.
	// Workspace scope is implicit in the ws_<id> schema (DAT-343 dropped
	// the workspace_id column).
	const sql = postgres(env.METADATA_DATABASE_URL, { onnotice: () => {} });
	try {
		const rows = await sql<{ count: number }[]>`
			SELECT count(*)::int AS count
			FROM ${sql(schema)}.config_overlay
			WHERE superseded_at IS NULL`;
		return rows[0]?.count ?? 0;
	} finally {
		await sql.end();
	}
}

async function runInitial(
	client: Client,
	sourceId: string,
	sessionId: string,
): Promise<AddSourceResult> {
	const input: AddSourceInput = {
		identity: {
			workspace_id: env.DATARAUM_WORKSPACE_ID,
			source_id: sourceId,
			session_id: sessionId,
			// `_adhoc` is the empty / start-here vertical (DAT-371): cold-start
			// induction generates concepts from the data and stores them as
			// `concept` overlay rows, not as YAML writes. This smoke is the
			// real DAT-371 acceptance test — a clean run proves induction
			// works against the read-only mounted config.
			vertical: "_adhoc",
		},
	};
	const result = await client.workflow.execute<
		(input: AddSourceInput) => Promise<AddSourceResult>
	>("addSourceWorkflow", {
		taskQueue: env.TEMPORAL_TASK_QUEUE,
		workflowId: addSourceWorkflowId(env.DATARAUM_WORKSPACE_ID, sourceId),
		args: [input],
	});

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
	return result;
}

async function awaitReplay(
	client: Client,
	sourceId: string,
	runId: string,
): Promise<AddSourceResult> {
	const handle = client.workflow.getHandle(
		addSourceWorkflowId(env.DATARAUM_WORKSPACE_ID, sourceId),
		runId,
	);
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
		const initial = await runInitial(client, sourceId, sessionId);
		const initialTypedIds = new Set(
			initial.tables.map((t) => t.typed_table_id),
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

		// ---- Replay from typing for the affected raw tables --------------
		// Reuse the seeded InvestigationSession — per-session rows the replay
		// re-creates (TypeCandidate, etc.) FK to investigation_sessions, and
		// the replay tool would otherwise mint a random session_id with no
		// matching row. Slice 1 has no session lifecycle; one session per
		// driver run is fine.
		const replayResult = await replay({
			source_id: sourceId,
			session_id: sessionId,
			vertical: "_adhoc",
			scope: {
				from_phase: "typing",
				raw_table_ids: initial.raw_table_ids,
			},
		});
		const replayed = await awaitReplay(client, sourceId, replayResult.run_id);

		if (replayed.tables.length !== initial.tables.length) {
			throw new Error(
				`replay: table count changed ${initial.tables.length} -> ${replayed.tables.length}`,
			);
		}
		// DAT-373: typing.replay_cleanup no longer drops the typed Table —
		// re-typing reconciles Columns/Table by (table_id, column_name), so the
		// typed_table_id is STABLE for every affected table on a
		// from_phase="typing" replay. That stability is what lets a second
		// stage's per-Column data survive an add_source teach (cross-stage
		// survival itself is proven by the engine integration test
		// test_replay_cross_stage). Here we assert the ids did NOT change.
		for (const table of replayed.tables) {
			if (!initialTypedIds.has(table.typed_table_id)) {
				throw new Error(
					`replay: typed_table_id for raw ${table.raw_table_id} changed to ` +
						`${table.typed_table_id} — DAT-373 expects stable ids on re-type`,
				);
			}
		}
		console.log(
			`✓ replay (from_phase=typing): ${replayed.tables.length} table(s) re-typed in place; ` +
				`typed_table_ids stable for all of them (DAT-373)`,
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

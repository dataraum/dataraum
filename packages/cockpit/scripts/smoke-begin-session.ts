// begin_session integration smoke (DAT-409): prove the full relationship loop
// end-to-end against a running compose stack (temporal + engine-worker + postgres
// + seaweedfs). A dev/test harness — NOT app code (scripts/, not src/); run
// manually against a live stack.
//
// Drives:
//   1. add_source over the SOURCE_PATH file(s) → typed tables (real LLM).
//   2. begin_session over those typed tables via the begin_session tool → the
//      relationships → semantic_per_table → materialize → detect → keepers →
//      promote chain (real LLM in semantic_per_table).
//   3. look_relationships — asserts the session sealed and reads back the
//      per-relationship readiness bands at the workspace catalog head.
//   4. why_relationship on the first relationship — the grounded drill-down.
//
// Run against the published compose ports, e.g.:
//   COCKPIT_DATABASE_URL=postgresql://dataraum:dataraum@localhost:5432/cockpit \
//   METADATA_DATABASE_URL=postgresql://ws_00000000_0000_0000_0000_000000000001_reader:cockpit-reader-dev@localhost:5432/dataraum \
//   METADATA_WRITER_DATABASE_URL=postgresql://ws_00000000_0000_0000_0000_000000000001_writer:cockpit-writer-dev@localhost:5432/dataraum \
//   DATARAUM_WORKSPACE_ID=00000000-0000-0000-0000-000000000001 \
//   ANTHROPIC_API_KEY=$ANTHROPIC_API_KEY \
//   TEMPORAL_HOST=localhost:7233 TEMPORAL_NAMESPACE=default S3_BUCKET=dataraum-lake \
//   SOURCE_PATH=s3://dataraum-lake/invoices.csv,s3://dataraum-lake/payments.csv \
//   bun run scripts/smoke-begin-session.ts

import { randomUUID } from "node:crypto";
import { Client, Connection } from "@temporalio/client";
import { z } from "zod";

import { cockpitDb } from "#/db/cockpit/client";
import { engineTaskQueueFor } from "#/db/cockpit/registry";
import { recordRun } from "#/db/cockpit/runs";
import { workspaces } from "#/db/cockpit/schema";
import { metadataWriteDb } from "#/db/metadata/client";
import { sourcesWrite } from "#/db/metadata/write-surface";
import { lookRelationships } from "#/tools/look-relationships";
import { lookTable } from "#/tools/look-table";
import { whyRelationship } from "#/tools/why-relationship";
import { whyTable } from "#/tools/why-table";
import type { AddSourceInput, AddSourceResult } from "#/temporal/types";
import {
	addSourceWorkflowId,
	beginSessionWorkflowId,
} from "#/temporal/workflow-id";

const env = z
	.object({
		TEMPORAL_HOST: z.string().min(1),
		TEMPORAL_NAMESPACE: z.string().min(1),
		DATARAUM_WORKSPACE_ID: z.string().min(1),
		SOURCE_PATH: z.string().min(1),
	})
	.parse(process.env);

const fileUris = env.SOURCE_PATH.split(",")
	.map((u) => u.trim())
	.filter(Boolean);

// The builtin vertical the smoke grounds against (its ontology ships concepts).
const VERTICAL = "finance";

/** Seed the workspace registry (vertical=finance, so the begin_session driver
 * sources it), the `sources` row, and record the add_source run in cockpit_db,
 * then run addSourceWorkflow to type the files. Returns the typed table ids. */
async function ingest(client: Client): Promise<string[]> {
	const sourceId = randomUUID();
	const now = new Date();

	// The workspace registry carries the vertical (DAT-506): seed it = finance so
	// the begin_session driver (and any other driver) sources the right ontology.
	// (No user row: better-auth owns identity (DAT-819) and nothing in this
	// flow FKs users.)
	await cockpitDb
		.insert(workspaces)
		.values({
			id: env.DATARAUM_WORKSPACE_ID,
			name: `Workspace ${env.DATARAUM_WORKSPACE_ID}`,
			vertical: VERTICAL,
		})
		// onConflictDoUPDATE, not DoNothing: the cockpit boot-seeds this workspace
		// row (vertical `_adhoc`) before any smoke runs, so DoNothing left the stale
		// vertical in place and the journey grounded the wrong ontology — begin_session
		// then fails loud ("Vertical '_adhoc' has no concepts"). Bit the 2026-07-15 smoke.
		.onConflictDoUpdate({ target: workspaces.id, set: { vertical: VERTICAL } });

	await metadataWriteDb
		.insert(sourcesWrite)
		.values({
			sourceId,
			name: `smoke_${sourceId.slice(0, 8)}`,
			sourceType: "csv",
			connectionConfig: { file_uris: fileUris },
			createdAt: now,
			updatedAt: now,
		})
		.onConflictDoNothing({ target: sourcesWrite.sourceId });

	const input: AddSourceInput = {
		// FLAT, source-free input (DAT-506): no identity, no session/source id on the
		// wire. The run's source SET (DAT-422) — one source here, so a 1-element set;
		// `verticals` is a one-element array of the workspace ontology.
		workspace_id: env.DATARAUM_WORKSPACE_ID,
		sources: [sourceId],
		verticals: [VERTICAL],
	};
	const handle = await client.workflow.start<
		(p: AddSourceInput) => Promise<AddSourceResult>
	>("addSourceWorkflow", {
		// The workspace's own queue (`engine-<id>`, DAT-505) — the same scheme the
		// production driver resolves from the registry (trigger-add-source.ts), NOT
		// the bare TEMPORAL_TASK_QUEUE env (which predated per-workspace queues and
		// left the workflow stranded on a queue no worker polls).
		taskQueue: engineTaskQueueFor(env.DATARAUM_WORKSPACE_ID),
		workflowId: addSourceWorkflowId(env.DATARAUM_WORKSPACE_ID),
		args: [input],
	});
	// Record the add_source run in cockpit_db AFTER start with the real execution id
	// (DAT-562/DAT-595 — runs group by workspace; the row carries the Temporal exec id).
	await recordRun({
		workspaceId: env.DATARAUM_WORKSPACE_ID,
		kind: "onboarding",
		stage: "add_source",
		workflowId: addSourceWorkflowId(env.DATARAUM_WORKSPACE_ID),
		runId: handle.firstExecutionRunId,
	});
	const result = (await handle.result()) as AddSourceResult;
	const typed = result.tables.map((t) => t.typed_table_id).filter(Boolean);
	if (typed.length < 2) {
		throw new Error(
			`add_source produced ${typed.length} typed table(s); need ≥2 for relationships ` +
				`(SOURCE_PATH must list ≥2 related files).`,
		);
	}
	console.log(`✓ add_source: ${typed.length} typed tables from ${fileUris.length} file(s)`);
	return typed;
}

async function main(): Promise<void> {
	const connection = await Connection.connect({ address: env.TEMPORAL_HOST });
	try {
		const client = new Client({ connection, namespace: env.TEMPORAL_NAMESPACE });

		const tableIds = await ingest(client);

		// ---- begin_session: drive the WORKFLOW directly ---------------------------
		// NOT via the beginSession tool: that tool is a JOURNEY SIGNAL (DAT-530) whose
		// returned run_id is a placeholder (== workflow_id), so awaiting
		// getHandle(wf, run).result() throws "Invalid RunId" — and the journey also
		// auto-cascades operating_model, which a smoke does not want implicitly.
		// A smoke drives beginSessionWorkflow directly on the engine queue with the
		// vertical explicit (the smoke runbook pattern). Bit the 2026-07-15 smoke.
		const bsHandle = await client.workflow.start("beginSessionWorkflow", {
			taskQueue: engineTaskQueueFor(env.DATARAUM_WORKSPACE_ID),
			workflowId: beginSessionWorkflowId(env.DATARAUM_WORKSPACE_ID),
			args: [
				{
					workspace_id: env.DATARAUM_WORKSPACE_ID,
					tables: tableIds,
					verticals: [VERTICAL],
				},
			],
		});
		await recordRun({
			workspaceId: env.DATARAUM_WORKSPACE_ID,
			kind: "onboarding",
			stage: "begin_session",
			workflowId: beginSessionWorkflowId(env.DATARAUM_WORKSPACE_ID),
			runId: bsHandle.firstExecutionRunId,
		});
		console.log(`✓ begin_session started: workflow=${bsHandle.workflowId}`);
		// Await the workflow to completion (it seals + promotes the relationship heads).
		await bsHandle.result();
		console.log("✓ begin_session workflow completed (sealed + promoted)");

		// ---- look_relationships --------------------------------------------------
		const look = await lookRelationships();
		console.log(
			`\nlook_relationships → analyzed=${look.analyzed} pending_teaches=${look.pending_teaches} ` +
				`relationships=${look.relationships.length}`,
		);
		for (const r of look.relationships) {
			console.log(
				`  ${r.from_table_name}.${r.from_column_name} → ${r.to_table_name}.${r.to_column_name}` +
					`  band=${r.band} risk=${r.worst_intent_risk?.toFixed(3) ?? "—"}` +
					`  drivers=[${r.top_drivers.map((d) => d.label).join(", ")}]`,
			);
		}
		if (look.relationships.length === 0) {
			throw new Error(
				"look_relationships returned 0 relationships — the detect pass found none " +
					"(check the selected files actually share a key).",
			);
		}

		// ---- why_relationship on the first relationship --------------------------
		const first = look.relationships[0];
		const why = await whyRelationship({
			from_column_id: first.from_column_id,
			to_column_id: first.to_column_id,
		});
		console.log(
			`\nwhy_relationship(${first.from_table_name}.${first.from_column_name} → ` +
				`${first.to_table_name}.${first.to_column_name}):` +
				`\n  found=${why.found} analyzed=${why.analyzed} band=${why.band} ` +
				`signals=${why.signal_count}` +
				`\n  analysis: ${why.analysis.slice(0, 400)}${why.analysis.length > 400 ? "…" : ""}`,
		);

		// ---- look_table (table-grain band) + why_table (DAT-415) -----------------
		// Each typed table gets a begin_session whole-table readiness band (the
		// dimension_coverage rollup), sealed at the workspace catalog head (DAT-506);
		// look_table surfaces it as table_readiness, why_table explains it.
		console.log("\nlook_table(table_readiness) per typed table:");
		let analyzedTableId: string | null = null;
		for (const tableId of tableIds) {
			const lt = await lookTable({ table_id: tableId });
			const band = lt.table_readiness?.band ?? "—";
			console.log(`  ${lt.table_name}: table_readiness.band=${band}`);
			if (lt.table_readiness && analyzedTableId === null) analyzedTableId = tableId;
		}
		if (analyzedTableId === null) {
			throw new Error(
				"look_table returned no table_readiness for any session table — the " +
					"table-grain rollup didn't seal at the session head (DAT-415).",
			);
		}

		const wt = await whyTable({
			table_id: analyzedTableId,
		});
		console.log(
			`\nwhy_table(${wt.table_name}):` +
				`\n  found=${wt.found} analyzed=${wt.analyzed} band=${wt.band} ` +
				`signals=${wt.signal_count}` +
				`\n  analysis: ${wt.analysis.slice(0, 400)}${wt.analysis.length > 400 ? "…" : ""}`,
		);
		if (!wt.found) {
			throw new Error("why_table did not find the table-grain readiness row.");
		}

		console.log("\n✅ begin_session smoke passed");
	} finally {
		await connection.close();
	}
}

main().then(
	() => process.exit(0),
	(err) => {
		console.error("❌ begin_session smoke failed:", err);
		process.exit(1);
	},
);

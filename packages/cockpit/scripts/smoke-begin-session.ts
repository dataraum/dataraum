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
//   3. look_relationships(session_id) — asserts the session sealed and reads back
//      the per-relationship readiness bands.
//   4. why_relationship on the first relationship — the grounded drill-down.
//
// Run against the published compose ports, e.g.:
//   COCKPIT_DATABASE_URL=postgresql://dataraum:dataraum@localhost:5432/cockpit \
//   METADATA_DATABASE_URL=postgresql://dataraum:dataraum@localhost:5432/dataraum \
//   DATARAUM_WORKSPACE_ID=00000000-0000-0000-0000-000000000001 \
//   ANTHROPIC_API_KEY=$ANTHROPIC_API_KEY \
//   TEMPORAL_HOST=localhost:7233 TEMPORAL_NAMESPACE=default \
//   TEMPORAL_TASK_QUEUE=dataraum-pipeline S3_BUCKET=dataraum-lake \
//   SOURCE_PATH=s3://dataraum-lake/invoices.csv,s3://dataraum-lake/payments.csv \
//   bun run scripts/smoke-begin-session.ts

import { randomUUID } from "node:crypto";
import { Client, Connection } from "@temporalio/client";
import { z } from "zod";

import { metadataDb } from "#/db/metadata/client";
import { investigationSessions, sources } from "#/db/metadata/schema";
import { beginSession } from "#/tools/begin-session";
import { lookRelationships } from "#/tools/look-relationships";
import { lookTable } from "#/tools/look-table";
import { whyRelationship } from "#/tools/why-relationship";
import { whyTable } from "#/tools/why-table";
import type { AddSourceInput, AddSourceResult } from "#/temporal/types";
import { addSourceWorkflowId } from "#/temporal/workflow-id";

const env = z
	.object({
		TEMPORAL_HOST: z.string().min(1),
		TEMPORAL_NAMESPACE: z.string().min(1),
		TEMPORAL_TASK_QUEUE: z.string().min(1),
		DATARAUM_WORKSPACE_ID: z.string().min(1),
		SOURCE_PATH: z.string().min(1),
	})
	.parse(process.env);

const fileUris = env.SOURCE_PATH.split(",")
	.map((u) => u.trim())
	.filter(Boolean);

// The builtin vertical the smoke grounds against (its ontology ships concepts).
const VERTICAL = "finance";

/** Seed the `sources` row + the add_source session, then run addSourceWorkflow to
 * type the files. Returns the typed table ids (the begin_session selection). */
async function ingest(client: Client): Promise<string[]> {
	const sourceId = randomUUID();
	const sessionId = randomUUID();
	const now = new Date();
	await metadataDb
		.insert(sources)
		.values({
			sourceId,
			name: `smoke_${sourceId.slice(0, 8)}`,
			sourceType: "csv",
			connectionConfig: { file_uris: fileUris },
			status: "configured",
			createdAt: now,
			updatedAt: now,
		})
		.onConflictDoNothing({ target: sources.sourceId });
	await metadataDb
		.insert(investigationSessions)
		.values({
			sessionId,
			intent: "smoke add_source",
			status: "active",
			startedAt: now,
			stepCount: 0,
			// Builtin `finance` ontology (22 concepts) — _adhoc induction (DAT-371)
			// isn't landed, so grounding needs a vertical that already ships concepts.
			vertical: VERTICAL,
		})
		.onConflictDoNothing({ target: investigationSessions.sessionId });

	const input: AddSourceInput = {
		identity: {
			workspace_id: env.DATARAUM_WORKSPACE_ID,
			source_id: sourceId,
			session_id: sessionId,
			vertical: VERTICAL,
		},
	};
	const handle = await client.workflow.start<
		(p: AddSourceInput) => Promise<AddSourceResult>
	>("addSourceWorkflow", {
		taskQueue: env.TEMPORAL_TASK_QUEUE,
		workflowId: addSourceWorkflowId(env.DATARAUM_WORKSPACE_ID, sourceId),
		args: [input],
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

		// ---- begin_session via the agent tool -----------------------------------
		const begun = await beginSession({ table_ids: tableIds, vertical: VERTICAL });
		console.log(
			`✓ begin_session started: workflow=${begun.workflow_id} session=${begun.session_id}`,
		);
		// Await the workflow to completion (it seals + promotes the relationship heads).
		await client.workflow.getHandle(begun.workflow_id, begun.run_id).result();
		console.log("✓ begin_session workflow completed (sealed + promoted)");

		// ---- look_relationships --------------------------------------------------
		const look = await lookRelationships({ session_id: begun.session_id });
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
			session_id: begun.session_id,
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

		// ---- look_table (session-head table-grain band) + why_table (DAT-415) -----
		// Each typed table gets a begin_session whole-table readiness band (the
		// dimension_coverage rollup), sealed at the session head; look_table surfaces
		// it when passed the session_id, why_table explains it.
		console.log("\nlook_table(table_readiness) per typed table:");
		let analyzedTableId: string | null = null;
		for (const tableId of tableIds) {
			const lt = await lookTable({ table_id: tableId, session_id: begun.session_id });
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
			session_id: begun.session_id,
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

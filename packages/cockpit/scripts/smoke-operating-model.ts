// operating_model integration smoke (DAT-438): prove the lifecycle stage
// end-to-end against a running compose stack (temporal + engine-worker +
// postgres + seaweedfs). A dev/test harness — NOT app code (scripts/, not
// src/); run manually against a live stack.
//
// Drives:
//   1. add_source over the SOURCE_PATH file(s) → typed tables (real LLM).
//   2. begin_session over those typed tables → the workspace (real LLM).
//   3. operatingModelWorkflow over the SAME session — resolve (pins) →
//      validation (declare → bind → execute, real LLM SQL generation per
//      declared spec) → promote (session:{id}, "operating_model").
//   4. Verification THROUGH THE PROMOTED-READ SURFACE (ADR-0008): reads
//      current_lifecycle_artifacts / current_validation_results via the
//      cockpit_reader role — so a pass proves workflow → promote → views →
//      grants in one chain, exactly what DAT-440 will consume.
//
// Run against the published compose ports, e.g.:
//   COCKPIT_DATABASE_URL=postgresql://dataraum:dataraum@localhost:5432/cockpit \
//   METADATA_DATABASE_URL=postgresql://cockpit_reader:cockpit-reader-dev@localhost:5432/dataraum \
//   DATARAUM_WORKSPACE_ID=00000000-0000-0000-0000-000000000001 \
//   ANTHROPIC_API_KEY=$ANTHROPIC_API_KEY \
//   TEMPORAL_HOST=localhost:7233 TEMPORAL_NAMESPACE=default \
//   TEMPORAL_TASK_QUEUE=dataraum-pipeline S3_BUCKET=dataraum-lake \
//   SOURCE_PATH=s3://dataraum-lake/invoices.csv,s3://dataraum-lake/payments.csv \
//   bun run scripts/smoke-operating-model.ts

import { randomUUID } from "node:crypto";
import { Client, Connection } from "@temporalio/client";
import { z } from "zod";

import { cockpitDb } from "#/db/cockpit/client";
import { recordRun } from "#/db/cockpit/runs";
import { actors, workspaces } from "#/db/cockpit/schema";
import { metadataDb } from "#/db/metadata/client";
import {
	currentLifecycleArtifacts,
	currentValidationResults,
} from "#/db/metadata/schema";
import { sourcesWrite } from "#/db/metadata/write-surface";
import { beginSession } from "#/tools/begin-session";
import type {
	AddSourceInput,
	AddSourceResult,
	OperatingModelInput,
	OperatingModelResult,
} from "#/temporal/types";
import {
	addSourceWorkflowId,
	operatingModelWorkflowId,
} from "#/temporal/workflow-id";

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

// The builtin vertical the smoke grounds against — finance ships 9 declared
// validations, which IS the operating_model slice-1 input.
const VERTICAL = "finance";

/** Seed the `sources` row + the add_source session, then run addSourceWorkflow to
 * type the files. Returns the typed table ids (the begin_session selection). */
async function ingest(client: Client): Promise<string[]> {
	const sourceId = randomUUID();
	const now = new Date();

	// The workspace registry carries the vertical (DAT-506): seed it = finance so
	// the begin_session / operating_model drivers source the right ontology.
	await cockpitDb
		.insert(actors)
		.values({ id: "default", displayName: "Default user" })
		.onConflictDoNothing();
	await cockpitDb
		.insert(workspaces)
		.values({
			id: env.DATARAUM_WORKSPACE_ID,
			name: `Workspace ${env.DATARAUM_WORKSPACE_ID}`,
			engineSchema: `ws_${env.DATARAUM_WORKSPACE_ID.replaceAll("-", "_")}`,
			vertical: VERTICAL,
		})
		.onConflictDoNothing();

	await metadataDb
		.insert(sourcesWrite)
		.values({
			sourceId,
			name: `smoke_${sourceId.slice(0, 8)}`,
			sourceType: "csv",
			connectionConfig: { file_uris: fileUris },
			status: "configured",
			createdAt: now,
			updatedAt: now,
		})
		.onConflictDoNothing({ target: sourcesWrite.sourceId });

	// Record the add_source run in cockpit_db (DAT-562 — runs group by workspace,
	// no session row; keyed by the workspace's deterministic add_source workflow id).
	await recordRun({
		workspaceId: env.DATARAUM_WORKSPACE_ID,
		kind: "onboarding",
		stage: "add_source",
		workflowId: addSourceWorkflowId(env.DATARAUM_WORKSPACE_ID),
	});

	const input: AddSourceInput = {
		// FLAT, source-free input (DAT-506): no identity, no session/source id.
		workspace_id: env.DATARAUM_WORKSPACE_ID,
		sources: [sourceId],
		verticals: [VERTICAL],
	};
	const handle = await client.workflow.start<
		(p: AddSourceInput) => Promise<AddSourceResult>
	>("addSourceWorkflow", {
		taskQueue: env.TEMPORAL_TASK_QUEUE,
		workflowId: addSourceWorkflowId(env.DATARAUM_WORKSPACE_ID),
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
	console.log(
		`✓ add_source: ${typed.length} typed tables from ${fileUris.length} file(s)`,
	);
	return typed;
}

async function main(): Promise<void> {
	const connection = await Connection.connect({ address: env.TEMPORAL_HOST });
	try {
		const client = new Client({ connection, namespace: env.TEMPORAL_NAMESPACE });

		const tableIds = await ingest(client);

		// ---- begin_session: compose the workspace ---------------------------------
		// Vertical is the workspace property (seeded = finance); the driver sources it.
		const begun = await beginSession({ table_ids: tableIds });
		// beginSession now returns a born-loud {error} when the workspace has no
		// typed tables (DAT-534); the smoke ingested above, so it must have started.
		if ("error" in begun) throw new Error(`begin_session refused: ${begun.error}`);
		await client.workflow.getHandle(begun.workflow_id, begun.run_id).result();
		console.log(`✓ begin_session completed: workflow=${begun.workflow_id}`);

		// ---- operatingModelWorkflow: flat input (DAT-438, DAT-506) ----------------
		// The stage re-reads the session's table set from the catalog head's
		// run_tables and pins the base-run map in its resolve activity — only the
		// workspace id + verticals travel in (no identity, no session id on the wire).
		const input: OperatingModelInput = {
			workspace_id: env.DATARAUM_WORKSPACE_ID,
			verticals: [VERTICAL],
		};
		const handle = await client.workflow.start<
			(p: OperatingModelInput) => Promise<OperatingModelResult>
		>("operatingModelWorkflow", {
			taskQueue: env.TEMPORAL_TASK_QUEUE,
			workflowId: operatingModelWorkflowId(env.DATARAUM_WORKSPACE_ID),
			args: [input],
		});
		const result = (await handle.result()) as OperatingModelResult;
		console.log(
			`✓ operating_model completed` +
				`\n  validation_summary: ${result.validation_summary}`,
		);

		// ---- verify THROUGH the promoted-read surface (ADR-0008) ------------------
		// cockpit_reader can only see current_* views; rows appearing here proves
		// the run promoted (catalog, "operating_model") AND the head join works.
		// The views resolve at the workspace catalog head (DAT-506) — no session filter.
		const artifacts = await metadataDb.select().from(currentLifecycleArtifacts);
		console.log(`\ncurrent_lifecycle_artifacts (${artifacts.length}):`);
		for (const a of artifacts) {
			console.log(
				`  ${a.artifactKey}: state=${a.state}` +
					(a.stateReason ? `  reason=${a.stateReason.slice(0, 90)}` : ""),
			);
		}
		if (artifacts.length === 0) {
			throw new Error(
				"current_lifecycle_artifacts returned 0 rows — either the declare step " +
					"wrote nothing or the operating_model head didn't promote.",
			);
		}

		const executed = artifacts.filter((a) => a.state === "executed");
		const undeclaredReasons = artifacts.filter(
			(a) => a.state !== "executed" && !a.stateReason,
		);
		if (executed.length === 0) {
			throw new Error(
				"no validation reached `executed` — binding failed across the board " +
					"(check the engine worker logs for the bind failures).",
			);
		}
		if (undeclaredReasons.length > 0) {
			throw new Error(
				`visibly-impossible contract violated: ${undeclaredReasons.length} ` +
					`non-executed artifact(s) carry NO state_reason: ` +
					undeclaredReasons.map((a) => a.artifactKey).join(", "),
			);
		}

		const results = await metadataDb.select().from(currentValidationResults);
		console.log(`\ncurrent_validation_results (${results.length}):`);
		for (const r of results) {
			console.log(
				`  ${r.validationId}: status=${r.status} passed=${r.passed}` +
					(r.message ? `  ${r.message.slice(0, 90)}` : ""),
			);
		}
		// current_lifecycle_artifacts now holds all three families (validation +
		// cycle + metric); the result-parity contract is validation-only — cycles
		// and metrics don't emit validation_results — so compare against the
		// validation-typed artifacts, not the whole set.
		const validationArtifacts = artifacts.filter(
			(a) => a.artifactType === "validation",
		);
		if (results.length !== validationArtifacts.length) {
			throw new Error(
				`result/artifact count mismatch: ${results.length} validation results vs ` +
					`${validationArtifacts.length} validation lifecycle artifacts — every ` +
					`declared validation must leave BOTH a lifecycle row and a result row.`,
			);
		}

		console.log(
			`\n✅ operating_model smoke passed — ${executed.length}/${artifacts.length} executed, ` +
				`${artifacts.length - executed.length} visibly impossible (with reasons), ` +
				`all read through the promoted-read surface`,
		);
	} finally {
		await connection.close();
	}
}

main().then(
	() => process.exit(0),
	(err) => {
		console.error("❌ operating_model smoke failed:", err);
		process.exit(1);
	},
);

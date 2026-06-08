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
import { eq } from "drizzle-orm";
import { z } from "zod";

import { metadataDb } from "#/db/metadata/client";
import {
	currentLifecycleArtifacts,
	currentValidationResults,
} from "#/db/metadata/schema";
import {
	investigationSessionsWrite,
	sourcesWrite,
} from "#/db/metadata/write-surface";
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
	const sessionId = randomUUID();
	const now = new Date();
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
	await metadataDb
		.insert(investigationSessionsWrite)
		.values({
			sessionId,
			intent: "smoke operating_model",
			status: "active",
			startedAt: now,
			stepCount: 0,
			vertical: VERTICAL,
		})
		.onConflictDoNothing({ target: investigationSessionsWrite.sessionId });

	const input: AddSourceInput = {
		identity: {
			workspace_id: env.DATARAUM_WORKSPACE_ID,
			session_id: sessionId,
			vertical: VERTICAL,
		},
		source_ids: [sourceId],
	};
	const handle = await client.workflow.start<
		(p: AddSourceInput) => Promise<AddSourceResult>
	>("addSourceWorkflow", {
		taskQueue: env.TEMPORAL_TASK_QUEUE,
		workflowId: addSourceWorkflowId(env.DATARAUM_WORKSPACE_ID, sessionId),
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
		const begun = await beginSession({ table_ids: tableIds, vertical: VERTICAL });
		await client.workflow.getHandle(begun.workflow_id, begun.run_id).result();
		console.log(`✓ begin_session completed: session=${begun.session_id}`);

		// ---- operatingModelWorkflow: identity ONLY (DAT-438) ----------------------
		// The stage re-reads the session's table set from session_tables and pins
		// the base-run map in its resolve activity — nothing else travels in.
		const input: OperatingModelInput = {
			identity: {
				workspace_id: env.DATARAUM_WORKSPACE_ID,
				session_id: begun.session_id,
			},
		};
		const handle = await client.workflow.start<
			(p: OperatingModelInput) => Promise<OperatingModelResult>
		>("operatingModelWorkflow", {
			taskQueue: env.TEMPORAL_TASK_QUEUE,
			workflowId: operatingModelWorkflowId(
				env.DATARAUM_WORKSPACE_ID,
				begun.session_id,
			),
			args: [input],
		});
		const result = (await handle.result()) as OperatingModelResult;
		console.log(
			`✓ operating_model completed over ${result.table_ids.length} table(s)` +
				`\n  validation_summary: ${result.validation_summary}`,
		);

		// ---- verify THROUGH the promoted-read surface (ADR-0008) ------------------
		// cockpit_reader can only see current_* views; rows appearing here proves
		// the run promoted (session:{id}, "operating_model") AND the head join works.
		const artifacts = await metadataDb
			.select()
			.from(currentLifecycleArtifacts)
			.where(eq(currentLifecycleArtifacts.sessionId, begun.session_id));
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

		const results = await metadataDb
			.select()
			.from(currentValidationResults)
			.where(eq(currentValidationResults.sessionId, begun.session_id));
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

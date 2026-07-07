// measure-grounding — KPI measure for `clean_executed_correct`
// (epics/relational-grounding.md, ADR-0019): how many oracle metrics are
// executed with values matching ground truth.
//
// ADR-0019 measure contract: the LAST stdout line is the verdict JSON
//   {"value": <executed_and_correct>, "executed": N, "total": M,
//    "mismatches": [...names], "unverified": [...], "not_executed": [...],
//    "missing": [...]}
// — the runner reads `value`; ALL diagnostics go to stderr.
//
// Default mode is measure-only: read the workspace's CURRENT promoted surface
// (current_lifecycle_artifacts, artifact_type='metric' — ADR-0008) and compare
// against the ground-truth YAML. No pipeline run, no LLM call.
//
// `--run` drives the full pipeline first (add_source → begin_session →
// operating_model, exactly like smoke-operating-model.ts) — REAL LLM SPEND.
//
// KNOWN GAP (the measure's loudest finding, 2026-07): the promoted surface
// exposes NO executed metric values — the engine discards
// GraphExecution.output_value (graphs/models.py: "ephemeral"; durable knowledge
// is the SQL). Until the engine-side value exposure lands, every executed
// metric classifies as `unverified` and `value` is 0 — the honest fail-on-main
// state ADR-0019's fail-to-pass discipline requires. See
// `extractMetricValue` (src/lib/measure/compare-values.ts) for the seam.
//
// Env: the smoke's set (config.ts is parsed at import — bun auto-loads
// packages/cockpit/.env), plus:
//   GROUND_TRUTH_PATH  ground-truth YAML (e.g.
//                      ../../../dataraum-testdata/output/clean/ground_truth.yaml)
//   TOLERANCE_PCT      relative tolerance percent (default 0.5)
//   SOURCE_PATH        --run only: ≥2 related files, like the smoke
//   VERTICAL           --run only: frame ontology (default finance)
//
//   bun run --cwd packages/cockpit scripts/measure-grounding.ts [--run]

import { randomUUID } from "node:crypto";
import { readFileSync } from "node:fs";
import { resolve } from "node:path";
import { Client, Connection } from "@temporalio/client";
import { eq } from "drizzle-orm";
import { z } from "zod";

import { cockpitDb } from "#/db/cockpit/client";
import { recordRun } from "#/db/cockpit/runs";
import { actors, workspaces } from "#/db/cockpit/schema";
import { metadataDb } from "#/db/metadata/client";
import { readOperatingModelHead } from "#/db/metadata/lifecycle-artifacts";
import { currentLifecycleArtifacts } from "#/db/metadata/schema";
import { sourcesWrite } from "#/db/metadata/write-surface";
import {
	compareMetricValues,
	extractMetricValue,
	type MeasuredMetric,
} from "#/lib/measure/compare-values";
import { parseGroundTruth } from "#/lib/measure/ground-truth";
import type {
	AddSourceInput,
	AddSourceResult,
	BeginSessionInput,
	BeginSessionResult,
	OperatingModelInput,
	OperatingModelResult,
} from "#/temporal/types";
import {
	addSourceWorkflowId,
	beginSessionWorkflowId,
	operatingModelWorkflowId,
} from "#/temporal/workflow-id";

// stderr-only logging — stdout is reserved for the final verdict line.
const log = (...args: unknown[]) => console.error(...args);

const runMode = process.argv.includes("--run");

const env = z
	.object({
		GROUND_TRUTH_PATH: z.string().min(1),
		TOLERANCE_PCT: z.coerce.number().positive().default(0.5),
	})
	.parse(process.env);

/** Drive add_source → begin_session → operating_model, mirroring
 * smoke-operating-model.ts (same env contract, same seeding). */
async function runPipeline(): Promise<void> {
	const runEnv = z
		.object({
			TEMPORAL_HOST: z.string().min(1),
			TEMPORAL_NAMESPACE: z.string().min(1),
			TEMPORAL_TASK_QUEUE: z.string().min(1),
			DATARAUM_WORKSPACE_ID: z.string().min(1),
			SOURCE_PATH: z.string().min(1),
			VERTICAL: z.string().min(1).default("finance"),
		})
		.parse(process.env);

	log("=".repeat(72));
	log("measure-grounding --run: REAL-LLM SPEND");
	log("Driving add_source → begin_session → operating_model on the live");
	log("stack (ANTHROPIC_API_KEY billed by the engine worker) before measuring.");
	log("=".repeat(72));

	const fileUris = runEnv.SOURCE_PATH.split(",")
		.map((u) => u.trim())
		.filter(Boolean);
	const workspaceId = runEnv.DATARAUM_WORKSPACE_ID;
	const vertical = runEnv.VERTICAL;

	const connection = await Connection.connect({ address: runEnv.TEMPORAL_HOST });
	try {
		const client = new Client({
			connection,
			namespace: runEnv.TEMPORAL_NAMESPACE,
		});

		// Seed the workspace registry (vertical forced to this run's — a stale
		// vertical from a prior run would ground the wrong ontology) + the source
		// row, like the smoke's ingest.
		await cockpitDb
			.insert(actors)
			.values({ id: "default", displayName: "Default user" })
			.onConflictDoNothing();
		await cockpitDb
			.insert(workspaces)
			.values({
				id: workspaceId,
				name: `Workspace ${workspaceId}`,
				engineSchema: `ws_${workspaceId.replaceAll("-", "_")}`,
				vertical,
			})
			.onConflictDoUpdate({ target: workspaces.id, set: { vertical } });

		const sourceId = randomUUID();
		const now = new Date();
		await metadataDb
			.insert(sourcesWrite)
			.values({
				sourceId,
				name: `measure_${sourceId.slice(0, 8)}`,
				sourceType: "csv",
				connectionConfig: { file_uris: fileUris },
				status: "configured",
				createdAt: now,
				updatedAt: now,
			})
			.onConflictDoNothing({ target: sourcesWrite.sourceId });

		const addInput: AddSourceInput = {
			workspace_id: workspaceId,
			sources: [sourceId],
			verticals: [vertical],
		};
		const addHandle = await client.workflow.start<
			(p: AddSourceInput) => Promise<AddSourceResult>
		>("addSourceWorkflow", {
			taskQueue: runEnv.TEMPORAL_TASK_QUEUE,
			workflowId: addSourceWorkflowId(workspaceId),
			args: [addInput],
		});
		await recordRun({
			workspaceId,
			kind: "onboarding",
			stage: "add_source",
			workflowId: addSourceWorkflowId(workspaceId),
			runId: addHandle.firstExecutionRunId,
		});
		const addResult = (await addHandle.result()) as AddSourceResult;
		const tableIds = addResult.tables
			.map((t) => t.typed_table_id)
			.filter(Boolean);
		if (tableIds.length < 2) {
			throw new Error(
				`add_source produced ${tableIds.length} typed table(s); need ≥2 for ` +
					`relationships (SOURCE_PATH must list ≥2 related files).`,
			);
		}
		log(`✓ add_source: ${tableIds.length} typed tables`);

		const beginInput: BeginSessionInput = {
			workspace_id: workspaceId,
			tables: tableIds,
			verticals: [vertical],
		};
		const beginHandle = await client.workflow.start<
			(p: BeginSessionInput) => Promise<BeginSessionResult>
		>("beginSessionWorkflow", {
			taskQueue: runEnv.TEMPORAL_TASK_QUEUE,
			workflowId: beginSessionWorkflowId(workspaceId),
			args: [beginInput],
		});
		const beginResult = (await beginHandle.result()) as BeginSessionResult;
		log(`✓ begin_session: run_id=${beginResult.run_id}`);

		const omInput: OperatingModelInput = {
			workspace_id: workspaceId,
			verticals: [vertical],
		};
		const omHandle = await client.workflow.start<
			(p: OperatingModelInput) => Promise<OperatingModelResult>
		>("operatingModelWorkflow", {
			taskQueue: runEnv.TEMPORAL_TASK_QUEUE,
			workflowId: operatingModelWorkflowId(workspaceId),
			args: [omInput],
		});
		const omResult = (await omHandle.result()) as OperatingModelResult;
		log(`✓ operating_model: ${omResult.validation_summary}`);
	} finally {
		await connection.close();
	}
}

/** Read the promoted metric family (values via `extractMetricValue` — null on
 * today's surface, see the KNOWN GAP note) and compare against ground truth. */
async function measure(): Promise<void> {
	const head = await readOperatingModelHead();
	if (head === null) {
		throw new Error(
			"no promoted operating_model run — nothing to measure. Run the " +
				"pipeline first (--run, or scripts/smoke-operating-model.ts).",
		);
	}

	const rows = await metadataDb
		.select({
			artifactKey: currentLifecycleArtifacts.artifactKey,
			state: currentLifecycleArtifacts.state,
			graphDefinition: currentLifecycleArtifacts.graphDefinition,
		})
		.from(currentLifecycleArtifacts)
		.where(eq(currentLifecycleArtifacts.artifactType, "metric"));
	const measured: MeasuredMetric[] = rows.map((r) => ({
		name: r.artifactKey ?? "",
		state: r.state,
		value: extractMetricValue(r.graphDefinition),
	}));

	const groundTruth = parseGroundTruth(
		Bun.YAML.parse(readFileSync(resolve(env.GROUND_TRUTH_PATH), "utf8")),
	);
	if (groundTruth.length === 0) {
		throw new Error(
			`ground truth at ${env.GROUND_TRUTH_PATH} contains no metric entries`,
		);
	}

	const cmp = compareMetricValues(groundTruth, measured, env.TOLERANCE_PCT);

	log(
		`\nmeasured against promoted run ${head} — oracle ${cmp.total} metric(s), ` +
			`±${env.TOLERANCE_PCT}% default tolerance:`,
	);
	for (const name of cmp.correct) log(`  ✓ ${name}: executed, value correct`);
	for (const m of cmp.mismatches)
		log(
			`  ✗ ${m.name}: expected ${m.expected}, got ${m.actual} (±${m.tolerancePct}%)`,
		);
	for (const name of cmp.unverified) log(`  ? ${name}: executed, NO value exposed`);
	for (const name of cmp.notExecuted) log(`  – ${name}: declared, not executed`);
	for (const name of cmp.missing) log(`  ∅ ${name}: no metric artifact`);

	if (cmp.unverified.length > 0) {
		log("\n" + "!".repeat(72));
		log(
			`! ${cmp.unverified.length} executed metric(s) have NO value on the promoted`,
		);
		log("! surface — the engine does not persist executed metric values yet");
		log("! (GraphExecution.output_value is ephemeral). They count as NOT correct.");
		log("! The epic's engine work must expose values to flip this measure.");
		log("!".repeat(72));
	}

	// The measure contract: the LAST stdout line, `value` first.
	console.log(
		JSON.stringify({
			value: cmp.correct.length,
			executed: cmp.executed.length,
			total: cmp.total,
			mismatches: cmp.mismatches.map((m) => m.name),
			unverified: cmp.unverified,
			not_executed: cmp.notExecuted,
			missing: cmp.missing,
		}),
	);
}

async function main(): Promise<void> {
	if (runMode) await runPipeline();
	await measure();
}

main().then(
	() => process.exit(0),
	(err) => {
		console.error("measure-grounding failed:", err);
		process.exit(1);
	},
);

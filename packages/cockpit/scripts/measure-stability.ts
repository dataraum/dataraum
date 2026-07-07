// measure-stability — KPI measure for `rerun_stability_flips`
// (epics/relational-grounding.md, docs/architecture/development-process.md): drive begin_session →
// operating_model TWICE on unchanged data and count grounding-verdict flips
// between the two promoted runs.
//
// docs/architecture/development-process.md measure contract: the LAST stdout line is the verdict JSON
//   {"value": <flip_count>, "flips": [{kind, key, before, after}, ...]}
// — the runner reads `value`; ALL diagnostics go to stderr.
//
// What counts as a flip (src/lib/measure/verdict-diff.ts):
//   - surrogate-intent status (confirmed/declined) or membership, keyed by
//     `intent_digest` — the content identity that is stable across runs
//     (NEVER intent_id/relationship_id, which are per-run uuids);
//   - metric artifact state (executed ↔ grounded/declared), state_reason on an
//     unchanged state (the low-confidence caveat), lineage (a digest of the
//     effective graph_definition), or membership — keyed by artifact_key.
// Each snapshot is read through the promoted current_* views after its run
// promotes, so the diff compares exactly what a practitioner sees run-over-run.
//
// `--run` is REQUIRED: this measure inherently executes the pipeline twice —
// REAL LLM SPEND. There is no measure-only mode (a single promoted surface
// holds one run; flips need two).
//
// The workspace must already be onboarded (add_source done — e.g. via
// smoke-operating-model.ts or measure-grounding --run): the session's table
// set is re-read from the promoted surface, so both passes run on the SAME
// unchanged tables and no new source rows are minted.
//
// Env: the smoke's set (config.ts is parsed at import — bun auto-loads
// packages/cockpit/.env); VERTICAL defaults to finance.
//
//   bun run --cwd packages/cockpit scripts/measure-stability.ts --run

import { Client, Connection } from "@temporalio/client";
import { and, eq } from "drizzle-orm";
import { z } from "zod";

import { metadataDb } from "#/db/metadata/client";
import { catalogHeadTarget } from "#/db/metadata/relationship-target";
import {
	currentLifecycleArtifacts,
	currentSurrogateKeyIntents,
	currentTables,
	metadataSnapshotHead,
	runTables,
} from "#/db/metadata/schema";
import {
	canonicalDigest,
	diffVerdicts,
	type VerdictSnapshot,
} from "#/lib/measure/verdict-diff";
import type {
	BeginSessionInput,
	BeginSessionResult,
	OperatingModelInput,
	OperatingModelResult,
} from "#/temporal/types";
import {
	beginSessionWorkflowId,
	operatingModelWorkflowId,
} from "#/temporal/workflow-id";

// stderr-only logging — stdout is reserved for the final verdict line.
const log = (...args: unknown[]) => console.error(...args);

if (!process.argv.includes("--run")) {
	log(
		"measure-stability requires --run: the flip count only exists across two " +
			"fresh begin_session → operating_model passes (REAL LLM SPEND). " +
			"There is no measure-only mode.",
	);
	process.exit(2);
}

const env = z
	.object({
		TEMPORAL_HOST: z.string().min(1),
		TEMPORAL_NAMESPACE: z.string().min(1),
		TEMPORAL_TASK_QUEUE: z.string().min(1),
		DATARAUM_WORKSPACE_ID: z.string().min(1),
		VERTICAL: z.string().min(1).default("finance"),
	})
	.parse(process.env);

/** The unchanged table set both passes run on: the promoted catalog head's
 * run_tables (the existing session's selection), falling back to all promoted
 * typed tables when the workspace has typed tables but no session yet. Sorted
 * so the begin_session input is byte-identical across passes. */
async function sessionTables(): Promise<string[]> {
	const [head] = await metadataDb
		.select({ runId: metadataSnapshotHead.runId })
		.from(metadataSnapshotHead)
		.where(
			and(
				eq(metadataSnapshotHead.target, catalogHeadTarget()),
				eq(metadataSnapshotHead.stage, "catalog"),
			),
		)
		.limit(1);
	if (head?.runId) {
		const rows = await metadataDb
			.select({ tableId: runTables.tableId })
			.from(runTables)
			.where(eq(runTables.runId, head.runId));
		const ids = rows.map((r) => r.tableId).filter((id): id is string => !!id);
		if (ids.length > 0) return ids.sort();
	}
	const typed = await metadataDb
		.select({ tableId: currentTables.tableId })
		.from(currentTables);
	const ids = typed.map((r) => r.tableId).filter((id): id is string => !!id);
	if (ids.length === 0) {
		throw new Error(
			"workspace has no typed tables — onboard it first (add_source via " +
				"smoke-operating-model.ts or measure-grounding --run).",
		);
	}
	return ids.sort();
}

/** Read the run's grounding verdicts through the promoted surface. */
async function snapshotVerdicts(): Promise<VerdictSnapshot> {
	const intents = await metadataDb
		.select({
			digest: currentSurrogateKeyIntents.intentDigest,
			status: currentSurrogateKeyIntents.status,
		})
		.from(currentSurrogateKeyIntents);
	const metrics = await metadataDb
		.select({
			key: currentLifecycleArtifacts.artifactKey,
			state: currentLifecycleArtifacts.state,
			stateReason: currentLifecycleArtifacts.stateReason,
			graphDefinition: currentLifecycleArtifacts.graphDefinition,
		})
		.from(currentLifecycleArtifacts)
		.where(eq(currentLifecycleArtifacts.artifactType, "metric"));
	return {
		intents: intents.map((i) => ({
			digest: i.digest ?? "",
			status: i.status ?? "",
		})),
		metrics: metrics.map((m) => ({
			key: m.key ?? "",
			state: m.state,
			stateReason: m.stateReason,
			lineageDigest:
				m.graphDefinition == null ? null : canonicalDigest(m.graphDefinition),
		})),
	};
}

/** One full pass: begin_session over the fixed table set, then
 * operating_model — the same direct engine starts as the smoke. */
async function runPass(client: Client, tables: string[]): Promise<void> {
	const beginInput: BeginSessionInput = {
		workspace_id: env.DATARAUM_WORKSPACE_ID,
		tables,
		verticals: [env.VERTICAL],
	};
	const beginHandle = await client.workflow.start<
		(p: BeginSessionInput) => Promise<BeginSessionResult>
	>("beginSessionWorkflow", {
		taskQueue: env.TEMPORAL_TASK_QUEUE,
		workflowId: beginSessionWorkflowId(env.DATARAUM_WORKSPACE_ID),
		args: [beginInput],
	});
	const beginResult = (await beginHandle.result()) as BeginSessionResult;
	log(`  ✓ begin_session: run_id=${beginResult.run_id}`);

	const omInput: OperatingModelInput = {
		workspace_id: env.DATARAUM_WORKSPACE_ID,
		verticals: [env.VERTICAL],
	};
	const omHandle = await client.workflow.start<
		(p: OperatingModelInput) => Promise<OperatingModelResult>
	>("operatingModelWorkflow", {
		taskQueue: env.TEMPORAL_TASK_QUEUE,
		workflowId: operatingModelWorkflowId(env.DATARAUM_WORKSPACE_ID),
		args: [omInput],
	});
	const omResult = (await omHandle.result()) as OperatingModelResult;
	log(`  ✓ operating_model: ${omResult.validation_summary}`);
}

async function main(): Promise<void> {
	log("=".repeat(72));
	log("measure-stability --run: REAL-LLM SPEND — TWO full begin_session →");
	log("operating_model passes on the live stack (ANTHROPIC_API_KEY billed");
	log("by the engine worker), unchanged data.");
	log("=".repeat(72));

	const connection = await Connection.connect({ address: env.TEMPORAL_HOST });
	try {
		const client = new Client({
			connection,
			namespace: env.TEMPORAL_NAMESPACE,
		});

		const tables = await sessionTables();
		log(`table set (${tables.length}, fixed across both passes)`);

		log("pass 1/2:");
		await runPass(client, tables);
		const first = await snapshotVerdicts();
		log(
			`  snapshot: ${first.intents.length} intent(s), ${first.metrics.length} metric(s)`,
		);

		log("pass 2/2:");
		await runPass(client, tables);
		const second = await snapshotVerdicts();
		log(
			`  snapshot: ${second.intents.length} intent(s), ${second.metrics.length} metric(s)`,
		);

		const flips = diffVerdicts(first, second);
		for (const f of flips) {
			log(`  FLIP [${f.kind}] ${f.key}: ${f.before ?? "∅"} → ${f.after ?? "∅"}`);
		}
		if (flips.length === 0) log("no verdict flips — stable run-over-run");

		// The measure contract: the LAST stdout line, `value` first.
		console.log(JSON.stringify({ value: flips.length, flips }));
	} finally {
		await connection.close();
	}
}

main().then(
	() => process.exit(0),
	(err) => {
		console.error("measure-stability failed:", err);
		process.exit(1);
	},
);

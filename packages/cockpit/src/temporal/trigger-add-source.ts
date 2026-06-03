// add_source TRIGGER (DAT-352) — the explicit "Add source" action that starts
// the engine's addSourceWorkflow for a source `select` already persisted.
//
// `select` (DAT-398) writes the `sources` row and advances its stage cursor to
// `add_source`, but does NOT start the import — that is this trigger. It is the
// cockpit-side caller the engine import phase assumes seeded the workspace state.
//
// HARD PRECONDITION (DAT-407 FK): the addSourceWorkflow's typing phase writes a
// `session_tables` row with a NOT-NULL FK to `investigation_sessions.session_id`
// (typing_phase.link_session_tables). `select` persists only the `sources` row —
// it never creates an investigation_sessions row. So a random session_id with no
// parent row passes `workflow.start` (non-blocking) but kills the run deep in the
// per-table fan-out at that FK, surfacing only as a stuck/failed progress poll.
// This trigger therefore INSERTs the investigation_sessions SEED (status='active',
// step_count=0, intent, started_at, vertical) through the SAME metadata-client
// write seam select/teach/frame use, BEFORE starting the workflow.
//
// The start is NON-blocking (`workflow.start`, not `.execute`): it returns the
// workflow + run id immediately so the cockpit polls progress via the
// `get_progress` query (see `progress.ts`). The workflow id is reused per source
// (addsource-<workspace_id>-<source_id>) under ALLOW_DUPLICATE so replays group
// under one id — so callers MUST target the precise `run_id` when querying.

import { randomUUID } from "node:crypto";
import { Client, Connection } from "@temporalio/client";
import { WorkflowIdReusePolicy } from "@temporalio/common";
import { z } from "zod";

import { config } from "../config";
import { metadataDb } from "../db/metadata/client";
import { investigationSessions } from "../db/metadata/schema";
import { verticalConceptCount } from "../tools/list-verticals";
import type { AddSourceInput, AddSourceResult, SourceIdentity } from "./types";
import { addSourceWorkflowId } from "./workflow-id";

// The intent label seeded onto the investigation_sessions row. Mirrors the
// onboarding context — a cold-start "add source" pass, not a query session.
const SEED_INTENT = "onboarding";
// Session status the engine treats as live (mirrors drive-add-source.ts seed).
const SEED_STATUS = "active";

/**
 * The chosen vertical resolves to ZERO concepts (no builtin ontology.yaml
 * concepts AND no overlay rows) — so add_source would fail loud deep in the
 * engine's grounding phase (semantic_per_column). A user-fixable PRECONDITION,
 * not a server fault: the API route surfaces it as a 400 with this message, and
 * the trigger never starts the doomed workflow.
 */
export class NoConceptsError extends Error {
	constructor(public readonly vertical: string) {
		super(
			vertical === "_adhoc"
				? "No concepts declared yet — run frame to declare concepts before adding this source."
				: `The "${vertical}" vertical has no concepts — frame it (or pick a builtin with list_verticals) before adding this source.`,
		);
		this.name = "NoConceptsError";
	}
}

export interface TriggerAddSourceInput {
	source_id: string;
	// Vertical the engine resolves phase config + ontology against. Cold-start
	// workspaces use "_adhoc" (induction generates concepts from the data); pass
	// the source's framed vertical to keep the run on the same ontology. Defaults
	// to "_adhoc" when unset, matching the engine default.
	vertical?: string;
}

export interface TriggerAddSourceResult {
	workflow_id: string;
	run_id: string;
	source_id: string;
	session_id: string;
}

/** The Temporal-unconfigured guard, identical to replay.ts: Temporal config is
 * OPTIONAL in config.ts, so the trigger fails loud (not silently) when it isn't
 * wired. Narrows the three optionals to non-undefined for the start call. */
function requireTemporalConfig(): {
	host: string;
	namespace: string;
	taskQueue: string;
} {
	if (
		!config.temporalHost ||
		!config.temporalNamespace ||
		!config.temporalTaskQueue
	) {
		throw new Error(
			"Temporal client is not configured. Set TEMPORAL_HOST, " +
				"TEMPORAL_NAMESPACE, TEMPORAL_TASK_QUEUE in the cockpit env.",
		);
	}
	return {
		host: config.temporalHost,
		namespace: config.temporalNamespace,
		taskQueue: config.temporalTaskQueue,
	};
}

/**
 * Seed the investigation_sessions parent row the per-table fan-out FKs against,
 * then start addSourceWorkflow NON-blocking. Returns the workflow + run id (and
 * the seeded session id) immediately — the caller polls `get_progress`.
 *
 * The seed write goes through the metadata client's documented onboarding write
 * seam (otherwise read-only; the engine owns the schema). Workspace scope is
 * implicit in the ws_<id> schema the client targets (no workspace_id column
 * post-DAT-343).
 */
export async function triggerAddSource(
	input: TriggerAddSourceInput,
): Promise<TriggerAddSourceResult> {
	const { host, namespace, taskQueue } = requireTemporalConfig();

	const sessionId = randomUUID();
	const vertical = input.vertical ?? "_adhoc";

	// PRE-FLIGHT (Theme B obs 4, generalized in Theme A): refuse early when the
	// chosen vertical resolves to zero concepts — builtin ontology.yaml concepts
	// PLUS active overlay rows. The engine fails loud on this deep in
	// semantic_per_column (semantic_per_column_phase.py), surfacing only as a
	// dead Temporal run; catching it here gives the user a readable message and
	// never starts the doomed workflow (no orphan session row either — this runs
	// before the seed). An adopted builtin (finance) ships concepts → passes; an
	// empty _adhoc or an un-framed vertical → refused.
	if ((await verticalConceptCount(vertical)) === 0) {
		throw new NoConceptsError(vertical);
	}

	// CRITICAL: seed the session BEFORE starting the workflow. typing_phase's
	// link_session_tables writes a session_tables row with a NOT-NULL FK to this
	// session_id; without the parent row the run dies mid-fan-out at that FK. No
	// source_id on the session (DAT-407): a session's source is derived from its
	// linked tables.
	await metadataDb.insert(investigationSessions).values({
		sessionId,
		intent: SEED_INTENT,
		status: SEED_STATUS,
		startedAt: new Date(),
		stepCount: 0,
		vertical,
	});

	const identity: SourceIdentity = {
		workspace_id: config.dataraumWorkspaceId,
		source_id: input.source_id,
		session_id: sessionId,
		vertical,
	};
	const payload: AddSourceInput = { identity };

	const workflowId = addSourceWorkflowId(
		config.dataraumWorkspaceId,
		input.source_id,
	);

	const connection = await Connection.connect({ address: host });
	try {
		const client = new Client({ connection, namespace });
		const handle = await client.workflow.start<
			(p: AddSourceInput) => Promise<AddSourceResult>
		>("addSourceWorkflow", {
			taskQueue,
			workflowId,
			args: [payload],
			// Reused per source across replays so Temporal UI groups iterations
			// under one id — same policy the replay tool uses.
			workflowIdReusePolicy: WorkflowIdReusePolicy.ALLOW_DUPLICATE,
		});

		return {
			workflow_id: workflowId,
			run_id: handle.firstExecutionRunId,
			source_id: input.source_id,
			session_id: sessionId,
		};
	} finally {
		await connection.close();
	}
}

/** Request-body schema for `POST /api/add-source` — the API route validates the
 * trigger input against this before firing the workflow. The API is the trust
 * boundary: a direct call bypasses `select`/`frame` validation, and the vertical
 * flows into `OntologyLoader.load(vertical)` → `verticals/<v>/ontology.yaml` path
 * construction (engine) + a config-tree fs read (`verticalConceptCount`), so it
 * MUST be a safe segment here — a path-traversal `../…` is rejected. Allows the
 * `_adhoc` default (leading underscore) plus the engine-valid name shape. */
export const TriggerAddSourceInputSchema = z.object({
	source_id: z.string().min(1),
	vertical: z
		.string()
		.refine((v) => v === "_adhoc" || /^[a-z][a-z0-9_]{1,48}$/.test(v), {
			message:
				"Invalid vertical (lowercase, starts with a letter, 2–49 chars of [a-z0-9_]) or '_adhoc'.",
		})
		.optional(),
});

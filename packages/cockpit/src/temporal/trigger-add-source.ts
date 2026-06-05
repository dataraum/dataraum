// add_source TRIGGER (DAT-352, folded into the select approval gate by DAT-436)
// — seeds the run's investigation_sessions row and starts the engine's
// addSourceWorkflow for the source set `select` just persisted.
//
// Since DAT-436 the ONLY caller is `select.server` (tools/select.ts): approving
// the `select` tool is the single gate that registers the source(s) AND starts
// the import — there is no separate "Add source" button or `/api/add-source`
// route. select runs the vertical pre-flight (NoConceptsError) BEFORE any write,
// so by the time this trigger runs the vertical is known to resolve to ≥1
// concept; this function does not re-check it.
//
// HARD PRECONDITION (DAT-407 FK): the addSourceWorkflow's typing phase writes a
// `session_tables` row with a NOT-NULL FK to `investigation_sessions.session_id`
// (typing_phase.link_session_tables). A random session_id with no parent row
// passes `workflow.start` (non-blocking) but kills the run deep in the per-table
// fan-out at that FK, surfacing only as a stuck/failed progress poll. This
// trigger therefore INSERTs the investigation_sessions SEED (status='active',
// step_count=0, intent, started_at, vertical) through the SAME metadata-client
// write seam select/teach/frame use, BEFORE starting the workflow.
//
// The start is NON-blocking (`workflow.start`, not `.execute`): it returns the
// workflow + run id immediately so the cockpit polls progress via the
// `get_progress` query (see `progress.ts`). A run ingests a SET of objects from
// 1–N sources (DAT-422), so the workflow id is keyed by the run's `session_id`
// (addsource-<workspace_id>-<session_id>), mirroring begin_session, and reused
// under ALLOW_DUPLICATE so replays group under one id — callers MUST target the
// precise `run_id` when querying.

import { randomUUID } from "node:crypto";
import { Client, Connection } from "@temporalio/client";
import { WorkflowIdReusePolicy } from "@temporalio/common";

import { config } from "../config";
import { metadataDb } from "../db/metadata/client";
import { investigationSessions } from "../db/metadata/schema";
import type { AddSourceInput, AddSourceResult, SourceIdentity } from "./types";
import { addSourceWorkflowId } from "./workflow-id";

// The intent label seeded onto the investigation_sessions row. Mirrors the
// onboarding context — a cold-start "add source" pass, not a query session.
const SEED_INTENT = "onboarding";
// Session status the engine treats as live (mirrors scripts/smoke-add-source.ts seed).
const SEED_STATUS = "active";

/**
 * The chosen vertical resolves to ZERO concepts (no builtin ontology.yaml
 * concepts AND no overlay rows) — so add_source would fail loud deep in the
 * engine's grounding phase (semantic_per_column). A user-fixable PRECONDITION,
 * not a server fault: `select` raises it BEFORE any source write, so a refused
 * vertical leaves no half-state and never starts the doomed workflow.
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
	// The sources this run imports (DAT-422): a run is over a SET of objects from
	// 1–N sources. One file-upload `select` mints one content-keyed source per file;
	// a database `select` mints one. Must be non-empty.
	source_ids: string[];
	// Vertical the engine resolves phase config + ontology against. Cold-start
	// workspaces use "_adhoc" (induction generates concepts from the data); pass
	// the run's framed vertical to keep it on the same ontology. Defaults to
	// "_adhoc" when unset, matching the engine default. The caller (select) has
	// already pre-flighted it against the effective concept count.
	vertical?: string;
}

export interface TriggerAddSourceResult {
	workflow_id: string;
	run_id: string;
	source_ids: string[];
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

	// CRITICAL: seed the session BEFORE starting the workflow. typing_phase's
	// link_session_tables writes a session_tables row with a NOT-NULL FK to this
	// session_id; without the parent row the run dies mid-fan-out at that FK. No
	// source_id on the session (DAT-407): a session's source is derived from its
	// linked tables.
	//
	// Failure seam: if `workflow.start` below throws AFTER this insert (Temporal
	// down / misconfigured), the row stays behind as an orphan — a session no
	// workflow ever runs against. That is accepted: it is FK-satisfied and
	// harmless (nothing joins to it until a run links tables), and the next
	// approval seeds a FRESH session_id rather than reusing it. No cleanup
	// machinery — the select call surfaces the error and a re-approval recovers.
	await metadataDb.insert(investigationSessions).values({
		sessionId,
		intent: SEED_INTENT,
		status: SEED_STATUS,
		startedAt: new Date(),
		stepCount: 0,
		vertical,
	});

	// Correlation breadcrumb for the orphan seam above: logged BEFORE the start
	// (not in a catch) so ANY failure mode — a thrown start, a crash, a hang —
	// leaves the seeded session_id in the log next to its approval. An
	// investigation_sessions row with no run is then traceable to this line.
	console.warn(
		`[trigger-add-source] seeded investigation_session ${sessionId} ` +
			`(sources: ${input.source_ids.join(", ")}) — starting addSourceWorkflow; ` +
			"if no run follows, this row is an orphan from a failed approval",
	);

	// Source-free identity (DAT-422): the per-source ids ride in `source_ids`; the
	// engine scopes each `import` to one of them and the run-level reduce/detect are
	// session-scoped. The run is keyed by `session_id`, not a source.
	const identity: SourceIdentity = {
		workspace_id: config.dataraumWorkspaceId,
		session_id: sessionId,
		vertical,
	};
	const payload: AddSourceInput = { identity, source_ids: input.source_ids };

	const workflowId = addSourceWorkflowId(config.dataraumWorkspaceId, sessionId);

	const connection = await Connection.connect({ address: host });
	try {
		const client = new Client({ connection, namespace });
		const handle = await client.workflow.start<
			(p: AddSourceInput) => Promise<AddSourceResult>
		>("addSourceWorkflow", {
			taskQueue,
			workflowId,
			args: [payload],
			// Reused per run (keyed by session) across replays so Temporal UI groups
			// iterations under one id — same policy the replay tool uses.
			//
			// DUPLICATE RUNS ARE BY DESIGN (decision pinned in the PR #231 review):
			// every select approval mints a fresh session_id, so a re-called select
			// starts an INDEPENDENT full run. Under the versioned-snapshot model
			// (DAT-412) runs coexist — each writes its own run_id-stamped metadata,
			// none clobbers another — so there is nothing to guard. The human gate
			// IS the approval card: a re-called select requires a visible second
			// approval the user can deny. Deliberately NO idempotency key and NO
			// in-flight check here.
			workflowIdReusePolicy: WorkflowIdReusePolicy.ALLOW_DUPLICATE,
		});

		return {
			workflow_id: workflowId,
			run_id: handle.firstExecutionRunId,
			source_ids: input.source_ids,
			session_id: sessionId,
		};
	} finally {
		await connection.close();
	}
}

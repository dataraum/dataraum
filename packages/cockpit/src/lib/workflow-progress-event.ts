// The live workflow-progress push contract (Phase 2A.3) — shared by the server
// watcher (emits) and the cockpit provider (consumes), so the widget no longer
// POLLS. The watcher publishes a CUSTOM StreamChunk per progress tick onto the
// conversation's bus; the provider's `onChunk` writes each one into the TanStack
// Query cache under `progressQueryKey`, and the existing progress `useQuery`
// re-renders live (its `refetchInterval` is dropped to a one-shot seed).
//
// CUSTOM (not STATE): the installed @tanstack/ai client has no STATE_SNAPSHOT/
// STATE_DELTA handling, and a CUSTOM event delivered via `onChunk` is the
// sanctioned live channel (the DAT-435 emitCustomEvent→onCustomEvent precedent).
//
// Client-safe: type-only import of WorkflowProgress (erased), so importing this
// from the browser provider never pulls the Temporal/cockpit_db server modules.

import type { WorkflowProgress } from "#/temporal/progress";

/** The CUSTOM event `name` the watcher emits and the provider filters on. */
export const WORKFLOW_PROGRESS_EVENT = "workflow-progress";

/** The CUSTOM event's `value` payload — which run, and its current snapshot. */
export interface WorkflowProgressEventValue {
	workflow_id: string;
	run_id: string;
	progress: WorkflowProgress;
}

/** The TanStack Query key the widget reads and the provider writes — MUST match
 * on both sides, so it lives here. Keyed by `workflowId` ALONE (DAT-530): the
 * workflow id is the stable run identity and progress always reflects the LATEST
 * execution, so the volatile Temporal `runId` is not part of the key. This also
 * lets a journey-started run (whose runId isn't known at tool-return time) and
 * the watcher's push agree on the key. Distinct workflows keep distinct keys, so
 * concurrent runs are tracked independently; only same-id re-runs collapse to
 * latest (intended). */
export function progressQueryKey(workflowId: string): [string, string] {
	return ["workflow-progress", workflowId];
}

/** A loose StreamChunk shape — the provider narrows raw chunks off `onChunk`
 * without importing the SDK's chunk union. */
interface MaybeCustomChunk {
	type?: unknown;
	name?: unknown;
	value?: unknown;
}

/** Narrow a raw stream chunk to a workflow-progress CUSTOM event, returning its
 * typed value or null. Defensive: the payload crosses the wire as `unknown`. */
export function asWorkflowProgressEvent(
	chunk: unknown,
): WorkflowProgressEventValue | null {
	const c = chunk as MaybeCustomChunk;
	if (c?.type !== "CUSTOM" || c.name !== WORKFLOW_PROGRESS_EVENT) return null;
	const v = c.value as Partial<WorkflowProgressEventValue> | undefined;
	if (
		!v ||
		typeof v.workflow_id !== "string" ||
		typeof v.run_id !== "string" ||
		v.progress == null
	) {
		return null;
	}
	return {
		workflow_id: v.workflow_id,
		run_id: v.run_id,
		progress: v.progress,
	};
}

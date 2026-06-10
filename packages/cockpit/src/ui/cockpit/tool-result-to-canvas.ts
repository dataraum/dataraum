// Tool-result → canvas bridge (DAT-347 seam, extended for DAT-353).
//
// Two pure functions, no React:
//   toolResultToCanvas(name, result)  — maps ONE tool's result to a CanvasState,
//     or null to leave the canvas unchanged (write/compute tools like teach/
//     replay render in the chat rail, not the canvas). A new canvas tool adds
//     one case here + its widget + one register() line — never edits the rail.
//   canvasFromMessages(messages)      — adapts the useChat message list to the
//     bridge: finds the latest completed tool result and maps it. Returns null
//     when there's nothing new to show.
//
// Row types are type-only imports (erased — no server code in the client bundle).

import type { UIMessage } from "@tanstack/ai-react";
import type { ConnectSchema } from "#/duckdb/connect";
import { isAgentError } from "#/tools/agent-error";
import type { FrameResult } from "#/tools/frame";
import type { AvailableSource } from "#/tools/list-sources";
import type { InventoryTable } from "#/tools/list-tables";
import type { LookCycleResult } from "#/tools/look-cycle";
import type { LookMetricResult } from "#/tools/look-metric";
import type { LookProfileResult } from "#/tools/look-profile";
import type { LookRelationshipsResult } from "#/tools/look-relationships";
import type { LookTableResult } from "#/tools/look-table";
import type { LookValidationResult } from "#/tools/look-validation";
import type { WhyColumnResult } from "#/tools/why-column";
import type { WhyCycleResult } from "#/tools/why-cycle";
import type { WhyMetricResult } from "#/tools/why-metric";
import type { WhyRelationshipResult } from "#/tools/why-relationship";
import type { WhyTableResult } from "#/tools/why-table";
import type { WhyValidationResult } from "#/tools/why-validation";
import type { CanvasState } from "#/ui/cockpit/canvas-state";

/** Projects one tool's result (+ call input) to a CanvasState, or null to leave
 * the canvas unchanged. */
type CanvasProjector = (result: unknown, input: unknown) => CanvasState | null;

/** A complete why_* result carries the boolean `found` discriminant — the SDK's
 * errored-call output (`{ error: string }`) does not, and must not project. */
function isWhyResult(result: unknown): boolean {
	return typeof (result as { found?: unknown } | null)?.found === "boolean";
}

/**
 * The canonical tool → canvas projector map — the SINGLE source of truth for
 * which tools produce a canvas member. A new canvas tool adds ONE entry here
 * (plus its widget + one register() line); `CANVAS_TOOLS` / `isCanvasTool`
 * derive from these keys, so the chat rail's chip clickability never needs a
 * second, hand-maintained list (DAT-354 de-dup). Tools absent from the map
 * (teach / probe) project nothing → their chips are display-only.
 */
const PROJECTORS: Record<string, CanvasProjector> = {
	// Project only a real array — a partial/streaming or errored output can be a
	// truthy NON-array, and the SourceList/Inventory widgets call .filter/.reduce/
	// .length on it ("e.filter is not a function"). Non-array → leave unchanged.
	list_sources: (result) =>
		Array.isArray(result)
			? { kind: "source-list", sources: result as AvailableSource[] }
			: null,
	list_tables: (result) =>
		Array.isArray(result)
			? { kind: "workspace-inventory", tables: result as InventoryTable[] }
			: null,
	// The per-table readiness grid; a missing result (e.g. an errored read)
	// leaves the canvas unchanged.
	look_table: (result) =>
		result
			? { kind: "table-readiness", readiness: result as LookTableResult }
			: null,
	// The per-column explanation. Guard on the `found` discriminant, not mere
	// truthiness (DAT-434 review): an ERRORED tool call's output is the SDK's
	// truthy `{ error }` object, which would project and render as "No such
	// column" — a failure masquerading as not-found. Error shape → leave the
	// canvas unchanged; the failure surfaces in the chat rail (connect behavior).
	why_column: (result) =>
		isWhyResult(result)
			? { kind: "column-why", why: result as WhyColumnResult }
			: null,
	// The per-column descriptive profile (DAT-475) — same `found` discriminant
	// guard as the why_* tools: an ERRORED call's `{ error }` output has no
	// boolean `found`, so it won't project (would otherwise render as "No such
	// column"); the failure surfaces in the chat rail instead.
	look_profile: (result) =>
		isWhyResult(result)
			? { kind: "column-profile", profile: result as LookProfileResult }
			: null,
	// The per-table explanation (DAT-434) — same `found` guard.
	why_table: (result) =>
		isWhyResult(result)
			? { kind: "table-why", why: result as WhyTableResult }
			: null,
	// The per-relationship explanation (DAT-434) — same `found` guard.
	why_relationship: (result) =>
		isWhyResult(result)
			? { kind: "relationship-why", why: result as WhyRelationshipResult }
			: null,
	// The begin_session relationship-readiness list (DAT-434). Project only a
	// complete result (a `relationships` array) — a partial/streaming or errored
	// output would crash the list widget's .map (same guard as connect/frame).
	look_relationships: (result) =>
		Array.isArray((result as { relationships?: unknown } | null)?.relationships)
			? { kind: "relationship-list", look: result as LookRelationshipsResult }
			: null,
	// The operating_model validation list (DAT-440). Project only a complete
	// result (a `validations` array) — same partial/errored-output guard as
	// look_relationships.
	look_validation: (result) =>
		Array.isArray((result as { validations?: unknown } | null)?.validations)
			? { kind: "validation-list", look: result as LookValidationResult }
			: null,
	// The per-validation drill-down (DAT-440) — same `found` guard as the other
	// why_* tools. NB the operating_model DRIVER tool deliberately has no entry:
	// like begin_session it returns run ids, not a renderable surface — the
	// outcome arrives via look_validation; its chip stays display-only.
	why_validation: (result) =>
		isWhyResult(result)
			? { kind: "validation-why", why: result as WhyValidationResult }
			: null,
	// The operating_model business-cycle list (DAT-465). Project only a complete
	// result (a `cycles` array) — same partial/errored-output guard as
	// look_validation.
	look_cycle: (result) =>
		Array.isArray((result as { cycles?: unknown } | null)?.cycles)
			? { kind: "cycle-list", look: result as LookCycleResult }
			: null,
	// The per-cycle drill-down (DAT-465) — same `found` guard as the other why_*
	// tools.
	why_cycle: (result) =>
		isWhyResult(result)
			? { kind: "cycle-why", why: result as WhyCycleResult }
			: null,
	// The operating_model metric list (DAT-466). Project only a complete result
	// (a `metrics` array) — same partial/errored-output guard as look_validation.
	look_metric: (result) =>
		Array.isArray((result as { metrics?: unknown } | null)?.metrics)
			? { kind: "metric-list", look: result as LookMetricResult }
			: null,
	// The per-metric drill-down (DAT-466) — same `found` guard as the other why_*
	// tools.
	why_metric: (result) =>
		isWhyResult(result)
			? { kind: "metric-why", why: result as WhyMetricResult }
			: null,
	// DAT-482: a teach_metric OVERRIDE surfaces the shipped DAG it replaces. Unlike
	// the other teach tools (rail-only), an override carries a renderable surface —
	// but only an override: a fresh declaration (override:false) or an errored
	// result has nothing to replace, so it projects null (chip stays display-only).
	// Carries ONLY the (vertical, graph_id) KEY — the widget re-fetches the DAG, so
	// the lean teach result never carries it into the model's context.
	teach_metric: (result) => {
		const r = result as {
			override?: unknown;
			vertical?: unknown;
			graph_id?: unknown;
		} | null;
		return r?.override === true &&
			typeof r.vertical === "string" &&
			typeof r.graph_id === "string"
			? { kind: "metric-shadow", vertical: r.vertical, graphId: r.graph_id }
			: null;
	},
	// Only project once the result is a COMPLETE schema (a `tables` array): a
	// partial/streaming or errored connect output is truthy-but-tables-less, and
	// projecting it crashes SchemaPreview on `schema.tables.length` (the multi-file
	// drag-drop crash). Not-yet-complete → leave the canvas unchanged (the last
	// good preview stays; a failed connect surfaces its error in the chat rail).
	connect: (result) =>
		Array.isArray((result as { tables?: unknown } | null)?.tables)
			? { kind: "schema-preview", schema: result as ConnectSchema }
			: null,
	// The framed model (concepts + validations + metric DAGs) renders as the
	// ModelFrame widget; only once the result carries a `concepts` array (the
	// model's foundation — same partial-output guard as connect; validations and
	// metrics may each be an empty array).
	frame: (result) =>
		Array.isArray((result as { concepts?: unknown } | null)?.concepts)
			? { kind: "model-frame", frame: result as FrameResult }
			: null,
	// Calling select STARTS the import (DAT-436): the result carries the
	// started run's workflow_id + run_id, so project the live add-source-progress
	// widget directly — the same member replay projects. A refused/failed select
	// (no ids — e.g. NoConceptsError) leaves the canvas unchanged.
	select: (result) => {
		const r = result as { workflow_id?: string; run_id?: string } | null;
		return r?.workflow_id && r?.run_id
			? {
					kind: "add-source-progress",
					workflowId: r.workflow_id,
					runId: r.run_id,
				}
			: null;
	},
	// A replay is an addSourceWorkflow run (reused workflow id, fresh run_id), so
	// project the SAME live-progress widget — sourced from the tool result. A
	// rejected/failed replay (no ids) leaves the canvas unchanged.
	replay: (result) => {
		const r = result as { workflow_id?: string; run_id?: string } | null;
		return r?.workflow_id && r?.run_id
			? {
					kind: "add-source-progress",
					workflowId: r.workflow_id,
					runId: r.run_id,
				}
			: null;
	},
	// begin_session STARTS the session workflow and returns immediately
	// (DAT-435): the result carries the started run's ids, so project the live
	// session-progress widget — the session analogue of select/replay above. A
	// failed start (no ids) leaves the canvas unchanged.
	begin_session: (result) => {
		const r = result as { workflow_id?: string; run_id?: string } | null;
		return r?.workflow_id && r?.run_id
			? {
					kind: "session-progress",
					workflowId: r.workflow_id,
					runId: r.run_id,
				}
			: null;
	},
	// operating_model STARTS the validation run and returns immediately — the
	// canvas flips to the live progress widget (DAT-440, DAT-435 follow-on).
	operating_model: (result) => {
		const r = result as { workflow_id?: string; run_id?: string } | null;
		return r?.workflow_id && r?.run_id
			? {
					kind: "operating-model-progress",
					workflowId: r.workflow_id,
					runId: r.run_id,
				}
			: null;
	},
	// The agent's run_sql returns a small sample for the LLM; the human grid
	// re-issues the query against the stateless streaming endpoint, so it maps
	// from the CALL INPUT (sql + bind params), not the result. No sql → unchanged.
	run_sql: (result, input) => {
		// A query the agent couldn't run came back as `{ error }` (pass 2) — don't
		// drive the human grid with SQL we already know fails.
		if (isAgentError(result)) return null;
		const args = (input ?? {}) as { sql?: unknown; params?: unknown };
		if (typeof args.sql !== "string" || args.sql.length === 0) return null;
		// Carry params ONLY when there are bind values: an empty array and
		// "absent" are the same query, so collapse them — else the grid's queryKey
		// flips between `[]` and `undefined` as the streamed args settle, re-issuing
		// the whole stream for no reason.
		const params =
			Array.isArray(args.params) && args.params.length > 0
				? (args.params as (string | number | boolean | null)[])
				: undefined;
		return params
			? { kind: "result-grid", sql: args.sql, params }
			: { kind: "result-grid", sql: args.sql };
	},
	// The `upload` UI tool carries no data — it just opens the upload area so the
	// user can drop local files.
	upload: () => ({ kind: "upload-area" }),
};

/**
 * The tool names whose result rehydrates the focus canvas — DERIVED from the
 * projector map, so there is no second hand-maintained list (DAT-354 de-dup).
 */
export const CANVAS_TOOLS: ReadonlySet<string> = new Set(
	Object.keys(PROJECTORS),
);

/** A tool whose result maps to a canvas member → its chip is clickable. */
export function isCanvasTool(toolName: string): boolean {
	return toolName in PROJECTORS;
}

/**
 * Map a single tool result to the canvas. `null` = leave the canvas as-is
 * (the result still shows in the chat rail's tool card).
 */
export function toolResultToCanvas(
	toolName: string,
	result: unknown,
	input?: unknown,
): CanvasState | null {
	return PROJECTORS[toolName]?.(result, input) ?? null;
}

/**
 * Adapt the useChat message list to the canvas. Walks every message part and
 * tracks the latest completed tool result that MAPS to a canvas, tolerating both
 * shapes the SDK can emit: an `output` on the `tool-call` part, or a correlated
 * `tool-result` part (content is JSON; fall back to the raw string if it isn't).
 *
 * "Maps to a canvas" is the key word: a non-canvas tool (list_verticals, teach,
 * probe) completing LAST must NOT shadow the last real canvas result — otherwise
 * `canvasFromMessages` returns null and the caller, which optimistically set the
 * canvas to "loading" on submit, has nothing to reconcile to (the stuck-spinner
 * bug after a refused/failed select, whose turn ends on a non-canvas list_verticals).
 * Returns the latest mappable CanvasState, or null when no part maps at all.
 */
export function canvasFromMessages(
	messages: ReadonlyArray<UIMessage>,
): CanvasState | null {
	let latestCanvas: CanvasState | null = null;
	const callById = new Map<string, { name: string; input: unknown }>();

	for (const message of messages) {
		for (const part of message.parts) {
			if (part.type === "tool-call") {
				const input = parseToolArguments(part);
				callById.set(part.id, { name: part.name, input });
				if (part.output !== undefined) {
					const canvas = toolResultToCanvas(part.name, part.output, input);
					if (canvas) latestCanvas = canvas;
				}
			} else if (part.type === "tool-result") {
				const call = callById.get(part.toolCallId);
				if (call) {
					let output: unknown = part.content;
					// `part.content` is `string | ContentPart[]`: only a JSON string is
					// parsed; structured content (or a non-JSON string) passes through.
					if (typeof part.content === "string") {
						try {
							output = JSON.parse(part.content);
						} catch {
							// content wasn't JSON — keep the raw string.
						}
					}
					const canvas = toolResultToCanvas(call.name, output, call.input);
					if (canvas) latestCanvas = canvas;
				}
			}
		}
	}

	return latestCanvas;
}

/**
 * Resolve ONE specific tool-call by its id and map it to the canvas (DAT-354
 * rehydration). Walks the message list for the call's name + input and its
 * completed output (tolerating both the `output`-on-the-call shape and a
 * correlated `tool-result` part, mirroring `canvasFromMessages`), then reuses
 * the same `toolResultToCanvas` mapper. Returns null when the call id isn't
 * found, hasn't completed, or maps to no canvas member (teach/replay/probe) —
 * the caller then leaves the canvas unchanged (those chips are display-only).
 */
export function canvasFromCallId(
	messages: ReadonlyArray<UIMessage>,
	callId: string,
): CanvasState | null {
	let name: string | undefined;
	let input: unknown;
	let output: unknown;
	let hasOutput = false;

	for (const message of messages) {
		for (const part of message.parts) {
			if (part.type === "tool-call" && part.id === callId) {
				name = part.name;
				input = parseToolArguments(part);
				if (part.output !== undefined) {
					output = part.output;
					hasOutput = true;
				}
			} else if (part.type === "tool-result" && part.toolCallId === callId) {
				let parsed: unknown = part.content;
				// `part.content` is `string | ContentPart[]`: only a JSON string is
				// parsed; structured content (or a non-JSON string) passes through.
				if (typeof part.content === "string") {
					try {
						parsed = JSON.parse(part.content);
					} catch {
						// content wasn't JSON — keep the raw string.
					}
				}
				output = parsed;
				hasOutput = true;
			}
		}
	}

	if (name === undefined || !hasOutput) return null;
	return toolResultToCanvas(name, output, input);
}

/**
 * Lift a tool-call's input off the part's JSON `arguments` string (the SDK
 * carries the call input there). Tolerates a missing or non-JSON value →
 * undefined. The `run_sql` → result-grid mapping needs this: the grid re-issues
 * the agent's query, so it reads the SQL from the call input, not the result.
 */
function parseToolArguments(part: { arguments?: unknown }): unknown {
	const raw = part.arguments;
	if (typeof raw !== "string") return raw ?? undefined;
	try {
		return JSON.parse(raw);
	} catch {
		return undefined;
	}
}

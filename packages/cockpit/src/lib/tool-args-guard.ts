// Tool-boundary args guard — coercion + rejection counters for DAT-661.
//
// The cockpit mirror of the engine's provider-boundary coercion telemetry
// (`stringified_tool_payload_coerced` in llm/providers/anthropic.py): when a model
// emits malformed tool arguments, make it COUNTABLE from logs — and rescue the one
// shape that is mechanically recoverable — so the DAT-661 strict-tools decision
// can measure the baseline malformation rate. Two structured console.info lines:
//   tool_args_coerced  { label, tool, argument } — a JSON-STRING payload where the
//     schema expects an array/object was parse-rescued; the call then proceeded.
//   tool_args_rejected { label, tool, error }    — args failed schema validation
//     with no rescue; the SDK fed the failure back to the model as a tool error.
//
// WHERE the SDK lets us intercept (verified against the installed @tanstack/ai
// dist, activities/chat/tools/tool-calls.js `executeToolCalls`): the SDK
// hard-validates a zod `inputSchema` (via Standard Schema) BEFORE the `.server()`
// handler runs — handlers can never see malformed input. The order per tool call:
//   1. JSON.parse the accumulated args string; a top-level NON-OBJECT parse result
//      (e.g. a double-encoded whole payload) is silently flattened to `{}` — the
//      raw string is discarded before any extension point sees it. An UNPARSEABLE
//      args string throws and kills the whole run (surfaces via onError).
//   2. `inputSchema` validation; a failure becomes an output-error tool result
//      (`{ error: "Input validation failed for tool <name>: …" }`) fed back to the
//      model — the handler never runs, and onBeforeToolCall does NOT fire for it.
//   3. onBeforeToolCall → handler.
// Consequences for this module:
//   - The ONLY rescue point is the schema itself: `onConfig` (a documented
//     transform hook) wraps each tool's zod inputSchema in a `z.preprocess` that
//     parse-rescues string-where-container fields before validation. The wrapper
//     is invisible to the provider — zod pipes report the INNER schema as their
//     input-side JSON Schema, which is exactly what the SDK advertises
//     (verified: identical bytes with and without the wrapper).
//   - Rejections are only visible to middleware in `onToolPhaseComplete.results`,
//     as the SDK's distinctive "Input validation failed for tool " error results.
//   - A top-level stringified payload (case 1) cannot be rescued at all: it
//     reaches the schema as `{}` and is counted as a rejection, not a coercion.
//
// The coercion logic mirrors the engine's `_coerce_stringified_args` exactly
// (observed malformation: Sonnet 5 serializing a whole array/object argument into
// a JSON string — `{"steps": "[{…}]"}` — plus the whole-payload-in-one-field
// variant). Only top-level fields whose declared JSON type is exactly
// `array`/`object` are candidates; everything else passes through untouched, and
// zod still validates the parsed value, so the rescue cannot admit bad data.
//
// Observe-only beyond that one rescue. Attach next to llmOtel() with the same
// label at every chat() that passes `tools`. chat({ outputSchema }) has no tool
// boundary (native structured output) — nothing to guard there.

import type { ChatMiddleware, Tool } from "@tanstack/ai";
import { z } from "zod";

type ContainerKind = "array" | "object";

/** The top-level view of a tool's input schema the coercion needs: every
 * property name, plus which properties are declared as containers. */
export interface TopLevelProperties {
	allKeys: ReadonlySet<string>;
	containers: ReadonlyMap<string, ContainerKind>;
}

/**
 * Read a zod input schema's top-level properties off its input-side JSON Schema —
 * the same view the SDK advertises to the provider. Returns null when the schema
 * can't be converted or has no object shape (leave such tools unwrapped).
 */
export function readTopLevelProperties(
	schema: z.ZodType,
): TopLevelProperties | null {
	let json: unknown;
	try {
		json = z.toJSONSchema(schema, { io: "input", target: "draft-7" });
	} catch {
		return null;
	}
	if (json === null || typeof json !== "object") return null;
	const properties = (json as Record<string, unknown>).properties;
	if (properties === null || typeof properties !== "object") return null;
	const allKeys = new Set<string>();
	const containers = new Map<string, ContainerKind>();
	for (const [key, prop] of Object.entries(properties)) {
		allKeys.add(key);
		if (prop === null || typeof prop !== "object") continue;
		const type = (prop as Record<string, unknown>).type;
		// Exact-`type` match, mirroring the engine: nullable/union containers
		// surface as `anyOf` (no top-level type) and are deliberately skipped.
		if (type === "array" || type === "object") containers.set(key, type);
	}
	return { allKeys, containers };
}

/**
 * Parse tool arguments the model JSON-stringified against the schema's declared
 * containers (the engine's `_coerce_stringified_args`, ported): a string value
 * where the schema expects an array/object is JSON.parsed; everything else passes
 * through untouched. Handles the whole-payload variant too — the model serializing
 * the ENTIRE input object into one array-typed field (`{"steps": '{"steps": […]}'}`);
 * when the parsed object's keys are the tool's own top-level properties, it IS the
 * input and is adopted wholesale. Each rescue reports the argument name via
 * `onCoerced`. Never throws; unparseable strings are left for zod to reject.
 */
export function coerceStringifiedToolArgs(
	raw: unknown,
	props: TopLevelProperties,
	onCoerced: (argument: string) => void,
): unknown {
	if (raw === null || typeof raw !== "object" || Array.isArray(raw)) return raw;
	const input = raw as Record<string, unknown>;
	const out = { ...input };
	let changed = false;
	for (const [key, kind] of props.containers) {
		const value = input[key];
		if (typeof value !== "string") continue;
		let parsed: unknown;
		try {
			parsed = JSON.parse(value);
		} catch {
			continue;
		}
		if (
			kind !== "object" &&
			parsed !== null &&
			typeof parsed === "object" &&
			!Array.isArray(parsed) &&
			Object.keys(parsed).every((k) => props.allKeys.has(k))
		) {
			// Whole-payload variant: adopt the parsed object as the input.
			onCoerced(key);
			return parsed;
		}
		if (parsed !== null && typeof parsed === "object") {
			out[key] = parsed;
			changed = true;
			onCoerced(key);
		}
	}
	return changed ? out : raw;
}

/** The SDK's input-validation failure results carry this exact prefix (the only
 * producer is `executeToolCalls`); handler-authored `{ error }` envelopes (the
 * agent-error contract) never match it. */
const VALIDATION_ERROR_PREFIX = "Input validation failed for tool ";

/** Narrow an `onToolPhaseComplete` result to the SDK's input-validation failure
 * message, or null for everything else (successes, handler errors, envelopes). */
function readValidationError(result: unknown): string | null {
	if (result === null || typeof result !== "object") return null;
	const error = (result as Record<string, unknown>).error;
	if (typeof error !== "string" || !error.startsWith(VALIDATION_ERROR_PREFIX))
		return null;
	return error;
}

/** Wrap one tool's zod inputSchema in the coercion preprocess. Non-zod schemas,
 * schemas without top-level container fields, and unconvertible schemas are left
 * untouched (rejection counting still covers them). */
function guardTool(label: string, tool: Tool): Tool {
	const schema = tool.inputSchema;
	if (!(schema instanceof z.ZodType)) return tool;
	const props = readTopLevelProperties(schema);
	if (props === null || props.containers.size === 0) return tool;
	const guarded = z.preprocess(
		(raw) =>
			coerceStringifiedToolArgs(raw, props, (argument) => {
				console.info("tool_args_coerced", { label, tool: tool.name, argument });
			}),
		schema,
	);
	return { ...tool, inputSchema: guarded };
}

/**
 * Build the tool-args guard middleware for one chat() run. `label` tags the call
 * site, matching the site's llmOtel label. A fresh instance per chat()
 * invocation, like the telemetry middleware.
 */
export function toolArgsGuardMiddleware(label: string): ChatMiddleware {
	// onConfig re-fires per agent iteration and the transformed tools PERSIST
	// across iterations (applyMiddlewareConfig keeps the returned array), so the
	// wrap must be idempotent: tools this instance already processed are skipped.
	const seen = new WeakSet<object>();
	return {
		name: "tool-args-guard",
		onConfig(_ctx, config) {
			let changed = false;
			const tools = config.tools.map((tool) => {
				if (seen.has(tool)) return tool;
				const guarded = guardTool(label, tool);
				seen.add(guarded);
				if (guarded !== tool) changed = true;
				return guarded;
			});
			return changed ? { tools } : undefined;
		},
		onToolPhaseComplete(_ctx, info) {
			for (const r of info.results) {
				const error = readValidationError(r.result);
				if (error !== null) {
					console.info("tool_args_rejected", {
						label,
						tool: r.toolName,
						error,
					});
				}
			}
		},
		onError(_ctx, info) {
			// An unparseable args string aborts the whole run before validation (the
			// SDK throws out of executeToolCalls) — count it here so it isn't lost.
			const message =
				info.error instanceof Error ? info.error.message : undefined;
			if (message?.startsWith("Failed to parse tool arguments as JSON")) {
				console.info("tool_args_rejected", {
					label,
					tool: null,
					error: message,
				});
			}
		},
	};
}

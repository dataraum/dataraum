// Tool-call chip summaries (DAT-354).
//
// A pure, no-React mapper from a tool call's {name, input, output} to a compact,
// HUMAN-READABLE one-liner — never raw JSON. The chat rail renders each tool
// card as a chip showing this summary instead of a `<Collapse>` JSON dump.
//
// Chip clickability (canvas-producing vs display-only) is decided by
// `isCanvasTool` / `CANVAS_TOOLS`, which now live in — and DERIVE from — the
// toolResultToCanvas projector map (single source of truth); they're re-exported
// here so the chat rail keeps importing them from one place. The non-canvas
// tools (probe / teach) project nothing → display-only chips (click is a no-op);
// replay now projects the live add-source-progress widget, so its chip is
// clickable.
//
// Input is lifted off the SDK part's `arguments` string (present in EVERY state,
// including approval-requested) so a teach chip is readable at approval time —
// `{type, payload}` before it runs, `{overlay_id, type}` once complete. Output
// is the part's `output` (undefined until the call completes).

import type { ConnectSchema } from "#/duckdb/connect";
import { humanizeIdentifier } from "#/lib/display-names";
import { fileName } from "#/lib/file-uri";
import { isAgentError } from "#/tools/agent-error";
import type { FrameResult } from "#/tools/frame";
import type { AvailableSource } from "#/tools/list-sources";
import type { InventoryTable } from "#/tools/list-tables";
import type { Vertical } from "#/tools/list-verticals";
import type { LookRelationshipsResult } from "#/tools/look-relationships";
import type { LookTableResult } from "#/tools/look-table";
import type { LookValidationResult } from "#/tools/look-validation";
import type { SelectResult } from "#/tools/select";
import type { TeachResult } from "#/tools/teach";
import type { TeachValidationResult } from "#/tools/teach-validation";
import type { WhyColumnResult } from "#/tools/why-column";
import type { WhyRelationshipResult } from "#/tools/why-relationship";
import type { WhyTableResult } from "#/tools/why-table";
import type { WhyValidationResult } from "#/tools/why-validation";
import { groupLogicalTables } from "#/ui/cockpit/widgets/inventory-grouping";

// Re-exported from the canvas bridge: defined ONCE there (derived from the
// projector map), surfaced here so the chat rail's existing import is unchanged.
export {
	CANVAS_TOOLS,
	isCanvasTool,
} from "#/ui/cockpit/tool-result-to-canvas";

function plural(n: number, noun: string): string {
	return `${n} ${noun}${n === 1 ? "" : "s"}`;
}

function truncate(s: string, max = 60): string {
	const flat = s.replace(/\s+/g, " ").trim();
	return flat.length > max ? `${flat.slice(0, max - 1)}…` : flat;
}

// Human-facing chip TITLE per tool — what the user reads, never the raw verb
// (`list_tables`, `run_sql`, `why_column`). The summary line under it carries the
// specifics; this is just the plain-language "what is happening".
const TOOL_LABELS: Record<string, string> = {
	list_sources: "Available data",
	list_verticals: "Domains",
	list_tables: "Workspace tables",
	look_table: "Table readiness",
	why_column: "Column detail",
	why_table: "Table detail",
	why_relationship: "Relationship detail",
	look_relationships: "Relationships",
	look_validation: "Validations",
	why_validation: "Validation detail",
	operating_model: "Starting validation run",
	connect: "Reading source",
	frame: "Framing the model",
	select: "Registering source",
	begin_session: "Starting session",
	run_sql: "Query",
	probe: "Data check",
	teach: "Teaching",
	teach_validation: "Declaring validation",
	replay: "Re-running",
	upload: "File upload",
};

// Past-tense / settled titles for the few tools whose default label reads as an
// in-progress verb. Once the call completes the chip flips to these so a finished
// card never says "Registering source" — the rest of TOOL_LABELS are already
// nouns ("Workspace tables", "Query") that read fine in both states.
const TOOL_LABELS_DONE: Record<string, string> = {
	connect: "Source schema",
	select: "Registered source",
	begin_session: "Session started",
	teach: "Taught",
	teach_validation: "Validation declared",
	replay: "Re-ran",
	// "Started", not "done" — the driver returns as soon as the durable run
	// kicks off (non-blocking, the begin_session pattern).
	operating_model: "Validation run started",
};

/**
 * The plain-language title for a tool call. When `done`, a progressive verb flips
 * to its settled form (TOOL_LABELS_DONE). Falls back to a humanized form of the
 * tool name (underscores → spaces, sentence case) so an unmapped future tool
 * still never shows a raw snake_case verb.
 */
export function toolLabel(toolName: string, done = false): string {
	if (done && TOOL_LABELS_DONE[toolName]) return TOOL_LABELS_DONE[toolName];
	const mapped = TOOL_LABELS[toolName];
	if (mapped) return mapped;
	const spaced = toolName.replace(/_/g, " ").trim();
	return spaced ? spaced.charAt(0).toUpperCase() + spaced.slice(1) : "Working";
}

/**
 * A compact, no-JSON summary of one tool call. `input` is the parsed call
 * arguments (may be undefined before they stream in); `output` is the result
 * (undefined until the call completes). Falls back to a neutral "running…" /
 * "done" string when the relevant payload isn't present yet, so a streaming or
 * approval-gated call still renders something readable.
 */
export function toolChipSummary(
	toolName: string,
	input: unknown,
	output: unknown,
): string {
	const done = output !== undefined;
	// A tool that returned the agent-actionable `{ error }` envelope (consistency
	// pass 2): show the message, not the success-shape fields — `select`'s
	// `${name} (${source_type})` on an `{ error }` object reads as
	// "undefined (undefined)". The chip's failed STATE is set separately
	// (tool-chip-state); this is just the readable subtitle.
	if (done && isAgentError(output)) return truncate(output.error);
	switch (toolName) {
		case "list_sources": {
			if (!done) return "listing available inputs…";
			// `Array.isArray` not `?? []`: a partial/streaming or errored output can be
			// a truthy NON-array, which `?? []` wouldn't catch → `.filter` then throws
			// "e.filter is not a function" and crashes the rail. Degrade to empty.
			const sources = Array.isArray(output)
				? (output as AvailableSource[])
				: [];
			if (sources.length === 0) return "no available inputs";
			const dbs = sources.filter((s) => s.kind === "database").length;
			const files = sources.filter((s) => s.kind === "file").length;
			const parts: string[] = [];
			if (dbs > 0) parts.push(plural(dbs, "database"));
			if (files > 0) parts.push(plural(files, "file"));
			return parts.join(", ");
		}
		case "list_verticals": {
			if (!done) return "listing verticals…";
			const verticals = Array.isArray(output) ? (output as Vertical[]) : [];
			if (verticals.length === 0) return "no verticals";
			const builtin = verticals.filter((v) => v.kind === "builtin").length;
			const framed = verticals.filter((v) => v.kind === "framed").length;
			const parts: string[] = [];
			if (builtin > 0) parts.push(`${builtin} builtin`);
			if (framed > 0) parts.push(`${framed} framed`);
			return `${plural(verticals.length, "vertical")} (${parts.join(", ")})`;
		}
		case "list_tables": {
			// The input `source_id` is only a SIGNAL that the call was filtered — for
			// uploads it's the content-keyed `src_<40hex>` digest, which must never
			// reach the chip text (running state included).
			const filtered = Boolean(
				(input as { source_id?: string } | undefined)?.source_id,
			);
			if (!done) return "listing tables…";
			const tables = Array.isArray(output) ? (output as InventoryTable[]) : [];
			// Count LOGICAL tables (DAT-437): the engine emits one row per physical
			// layer (raw / typed / quarantine), so the raw length triples what the
			// user thinks of as "their tables" — collapse layers the same way the
			// inventory widget does.
			const logical = groupLogicalTables(tables).length;
			// Name the filter by the rows' HUMAN source label (`source_name` — post-
			// DAT-433 the filename for uploads, the connection name for db sources).
			// An empty filtered result has no label to show — drop the suffix.
			const label =
				filtered && tables.length > 0 ? tables[0]?.source_name : undefined;
			return label
				? `${plural(logical, "table")} in ${label}`
				: plural(logical, "table");
		}
		case "look_table": {
			const r = output as LookTableResult | undefined;
			if (!r || !Array.isArray(r.columns)) return "reading table readiness…";
			const cols = plural(r.columns.length, "column");
			// `table_name` arrives in display form (projected in the tool, DAT-433).
			return r.analyzed
				? `${r.table_name} — ${cols}`
				: `${r.table_name} — ${cols}, not yet analyzed`;
		}
		case "why_column": {
			const r = output as WhyColumnResult | undefined;
			if (!r) return "explaining column…";
			if (!r.found) return "column not found";
			const band = r.band ?? "not analyzed";
			// `table_name` arrives in display form (projected in the tool, DAT-431).
			return `${r.column_name} (${r.table_name}) — ${band}`;
		}
		case "why_table": {
			const r = output as WhyTableResult | undefined;
			if (!r) return "explaining table…";
			if (!r.found) return "table not found";
			const band = r.band ?? "not analyzed";
			// `table_name` arrives in display form (DAT-431); null → no id fallback.
			return `${r.table_name ?? "table"} — ${band}`;
		}
		case "why_relationship": {
			const r = output as WhyRelationshipResult | undefined;
			if (!r) return "explaining relationship…";
			if (!r.found) return "relationship not found";
			const band = r.band ?? "not analyzed";
			// Endpoint names arrive in display form (DAT-431); nulls degrade to a
			// placeholder word (matching why_table) — never a column id.
			const from = r.from_table_name ?? "table";
			const to = r.to_table_name ?? "table";
			return `${from} → ${to} — ${band}`;
		}
		case "look_relationships": {
			const r = output as LookRelationshipsResult | undefined;
			if (!r || !Array.isArray(r.relationships))
				return "reading relationships…";
			return r.analyzed
				? plural(r.relationships.length, "relationship")
				: "not yet analyzed";
		}
		case "look_validation": {
			const r = output as LookValidationResult | undefined;
			if (!r || !Array.isArray(r.validations)) return "reading validations…";
			if (!r.analyzed) return "not yet run";
			if (r.validations.length === 0) return "no validations declared";
			const executed = r.validations.filter(
				(v) => v.state === "executed",
			).length;
			return `${plural(r.validations.length, "validation")} (${executed} executed)`;
		}
		case "why_validation": {
			const r = output as WhyValidationResult | undefined;
			if (!r) return "explaining validation…";
			if (!r.found) return "validation not found";
			// The validation key is a snake_case identifier — humanize it (the
			// naming rule: never surface code-shaped tokens in chip text).
			const label = humanizeIdentifier(r.validation_id) || "validation";
			// Loose `== null` on purpose: a partial/streaming output can lack
			// `passed` entirely (undefined) — that must read as the lifecycle
			// state, never as a "failed" verdict.
			const verdict =
				r.passed == null
					? (r.state ?? "not run")
					: r.passed
						? "passed"
						: "failed";
			return `${label} — ${verdict}`;
		}
		case "operating_model": {
			// Non-blocking driver: done = the durable run STARTED (ids in the
			// output), not finished — completion arrives via workflow_status /
			// look_validation.
			return done
				? "validation run started — outcomes via the validations view"
				: "starting the validation run…";
		}
		case "connect": {
			const s = output as ConnectSchema | undefined;
			// `output` can be a truthy-but-PARTIAL object while the result streams in
			// (tables not populated yet) — treat a missing tables array as still
			// connecting rather than crashing on `.length` (the multi-file drag-drop
			// crash). The complete result always carries the array.
			if (!s || !Array.isArray(s.tables)) return "connecting…";
			// A file source's `source` is the full `s3://…/<id>/<name>` URI — show the
			// filename, not the bucket/prefix plumbing (a database source is a name).
			const src = s.sourceKind === "file" ? fileName(s.source) : s.source;
			return `${src} — ${plural(s.tables.length, "table")}`;
		}
		case "frame": {
			const f = output as FrameResult | undefined;
			if (!f || !Array.isArray(f.concepts)) return "framing the model…";
			const parts = [plural(f.concepts.length, "concept")];
			if (Array.isArray(f.validations) && f.validations.length > 0) {
				parts.push(plural(f.validations.length, "validation"));
			}
			return `${f.vertical} — ${parts.join(", ")}`;
		}
		case "select": {
			const s = output as SelectResult | undefined;
			if (!s) return "registering source…";
			return `${s.name} (${s.source_type})`;
		}
		case "begin_session": {
			// The tool returns as soon as the workflow STARTS — the run keeps going
			// (the session-progress widget tracks it), so the settled summary still
			// reads as in-flight analysis. Count from the INPUT selection (present
			// from approval time); ids (workflow/run/session uuids) never reach the
			// chip.
			const args = input as { table_ids?: unknown } | undefined;
			const count = Array.isArray(args?.table_ids)
				? args.table_ids.length
				: null;
			if (!done) return "starting the session…";
			return count !== null
				? `analyzing ${plural(count, "table")}`
				: "analysis running";
		}
		case "run_sql": {
			const sql = (input as { sql?: string } | undefined)?.sql;
			if (typeof sql === "string" && sql.length > 0) return truncate(sql);
			return done ? "query run" : "running query…";
		}
		case "probe": {
			const args = input as { source_name?: string; sql?: string } | undefined;
			const where = args?.source_name ? ` on ${args.source_name}` : "";
			const out = output as { rowCount?: number } | undefined;
			if (out && typeof out.rowCount === "number") {
				return `probe${where} — ${plural(out.rowCount, "row")}`;
			}
			return `probe${where}…`;
		}
		case "teach":
			return teachChipSummary(input, output);
		case "teach_validation":
			return teachValidationChipSummary(input, output);
		case "replay": {
			const args = input as { source_id?: string } | undefined;
			const out = output as { run_id?: string } | undefined;
			if (out?.run_id) return `replay — run ${out.run_id}`;
			return args?.source_id ? `replay ${args.source_id}` : "replay…";
		}
		case "upload":
			return "drop files to import";
		default:
			// Never surface a raw snake_case verb as the summary — humanize unmapped
			// tools the same way the title does (look_relationships → "Look relationships").
			// Relationship tools (look_relationships / why_relationship) already emit
			// `from_table_name`/`to_table_name` in display form (stripped in the tool
			// projections, DAT-431) — no extra `displayTableName` needed for new cases.
			return toolLabel(toolName);
	}
}

/**
 * The teach chip is readable at every state (DAT-354): at approval time it
 * shows the proposed `{type, payload}` lifted off `arguments`; once complete it
 * shows `{overlay_id, type}`. Display-only — teach maps to no canvas member.
 */
export function teachChipSummary(input: unknown, output: unknown): string {
	const result = output as TeachResult | { error?: string } | undefined;
	if (result && "overlay_id" in result && result.overlay_id) {
		return `taught ${result.type} → ${result.overlay_id}`;
	}
	if (result && "error" in result && result.error) {
		return `teach rejected: ${truncate(result.error)}`;
	}
	const args = input as
		| { type?: string; payload?: Record<string, unknown> }
		| undefined;
	if (args?.type) {
		const keys = args.payload ? Object.keys(args.payload) : [];
		const fields = keys.length > 0 ? ` {${keys.join(", ")}}` : "";
		return `teach ${args.type}${fields}`;
	}
	return "teach…";
}

/**
 * The teach_validation chip (DAT-441). At approval time it shows the proposed
 * check ("declare <id> (<check_type>)" off `arguments`); once complete it flips
 * to "declared <id>" or — when the id shadows a shipped spec — "overrode <id>
 * (was <shadowed name>)", making the upsert-replace VISIBLE in the rail, never
 * silent. Display-only — like teach it maps to no canvas member (the outcome
 * lands in look_validation after a re-run).
 */
export function teachValidationChipSummary(
	input: unknown,
	output: unknown,
): string {
	// No `{error}` branch: unlike the generic `teach` (which validates per-type
	// inside its handler and returns a structured error), this tool's closed enums
	// + required fields are enforced by zod at the SDK boundary and a DB write
	// failure propagates — so the output is always the success shape.
	const result = output as TeachValidationResult | undefined;
	if (result && "validation_id" in result && result.validation_id) {
		const label =
			humanizeIdentifier(result.validation_id) || result.validation_id;
		if (result.override) {
			const shadowed =
				result.shadowed_spec?.name ?? result.shadowed_spec?.validation_id;
			return shadowed
				? `overrode ${label} (was ${truncate(shadowed, 32)})`
				: `overrode ${label}`;
		}
		return `declared ${label}`;
	}
	const args = input as
		| { validation_id?: string; check_type?: string }
		| undefined;
	if (args?.validation_id) {
		const label = humanizeIdentifier(args.validation_id) || args.validation_id;
		return args.check_type
			? `declare ${label} (${args.check_type})`
			: `declare ${label}`;
	}
	return "declaring validation…";
}

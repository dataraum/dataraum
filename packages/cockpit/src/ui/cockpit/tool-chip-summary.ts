// Tool-call chip summaries (DAT-354).
//
// A pure, no-React mapper from a tool call's {name, input, output} to a compact,
// HUMAN-READABLE one-liner — never raw JSON. The chat rail renders each tool
// card as a chip showing this summary instead of a `<Collapse>` JSON dump.
//
// `clickable` marks the canvas-producing tools (the 8 whose result rehydrates
// the focus canvas via toolResultToCanvas). The non-canvas tools (probe / teach
// / replay) are display-only — they read in the rail but never project, so a
// click on them is a no-op. This set is the inverse of toolResultToCanvas's
// `default: return null`, kept here as data so the rail stays declarative.
//
// Input is lifted off the SDK part's `arguments` string (present in EVERY state,
// including approval-requested) so a teach chip is readable at approval time —
// `{type, payload}` before it runs, `{overlay_id, type}` once complete. Output
// is the part's `output` (undefined until the call completes).

import type { ConnectSchema } from "#/duckdb/connect";
import type { FrameResult } from "#/tools/frame";
import type { SourceSummary } from "#/tools/list-sources";
import type { TableSummary } from "#/tools/list-tables";
import type { LookTableResult } from "#/tools/look-table";
import type { SelectResult } from "#/tools/select";
import type { TeachResult } from "#/tools/teach";
import type { WhyColumnResult } from "#/tools/why-column";

/** The tool names whose result rehydrates the focus canvas (clickable chips). */
export const CANVAS_TOOLS: ReadonlySet<string> = new Set([
	"list_sources",
	"list_tables",
	"look_table",
	"why_column",
	"connect",
	"frame",
	"select",
	"run_sql",
]);

/** A tool whose result maps to a canvas member → its chip is clickable. */
export function isCanvasTool(toolName: string): boolean {
	return CANVAS_TOOLS.has(toolName);
}

function plural(n: number, noun: string): string {
	return `${n} ${noun}${n === 1 ? "" : "s"}`;
}

function truncate(s: string, max = 60): string {
	const flat = s.replace(/\s+/g, " ").trim();
	return flat.length > max ? `${flat.slice(0, max - 1)}…` : flat;
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
	switch (toolName) {
		case "list_sources": {
			if (!done) return "listing sources…";
			const sources = (output as SourceSummary[]) ?? [];
			return plural(sources.length, "source");
		}
		case "list_tables": {
			const src = (input as { source_id?: string } | undefined)?.source_id;
			if (!done) return src ? `listing tables for ${src}…` : "listing tables…";
			const tables = (output as TableSummary[]) ?? [];
			return src
				? `${plural(tables.length, "table")} in ${src}`
				: plural(tables.length, "table");
		}
		case "look_table": {
			const r = output as LookTableResult | undefined;
			if (!r) return "reading table readiness…";
			const cols = plural(r.columns.length, "column");
			return r.analyzed
				? `${r.table_name} — ${cols}`
				: `${r.table_name} — ${cols}, not yet analyzed`;
		}
		case "why_column": {
			const r = output as WhyColumnResult | undefined;
			if (!r) return "explaining column…";
			if (!r.found) return "column not found";
			const band = r.band ?? "not analyzed";
			return `${r.column_name} (${r.table_name}) — ${band}`;
		}
		case "connect": {
			const s = output as ConnectSchema | undefined;
			if (!s) return "connecting…";
			return `${s.source} — ${plural(s.tables.length, "table")}`;
		}
		case "frame": {
			const f = output as FrameResult | undefined;
			if (!f) return "framing concepts…";
			return `${f.vertical} — ${plural(f.concepts.length, "concept")}`;
		}
		case "select": {
			const s = output as SelectResult | undefined;
			if (!s) return "registering source…";
			return `${s.name} (${s.source_type})`;
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
		case "replay": {
			const args = input as { source_id?: string; scope?: string } | undefined;
			const scope = args?.scope ? ` (${args.scope})` : "";
			const out = output as { run_id?: string } | undefined;
			if (out?.run_id) return `replay${scope} — run ${out.run_id}`;
			return args?.source_id ? `replay ${args.source_id}${scope}` : "replay…";
		}
		default:
			return toolName;
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

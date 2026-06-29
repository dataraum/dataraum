// Vertical conventions for the cockpit Q&A agent (DAT-645).
//
// The TS mirror of the engine's `OntologyLoader.format_conventions_for_prompt`
// (`packages/engine/.../analysis/semantic/ontology.py`). The engine pipes a
// vertical's conventions verbatim into its two SQL-authoring agents (extraction +
// validation); the cockpit Q&A agent is the third SQL author (`targets: [..., qa]`)
// and gets the same rule here. The cockpit NEVER interprets the content — it reads
// the bind-mounted `verticals/<v>/ontology.yaml`, routes by the generic `target`
// label, and emits the `statement` + `concept_groups` as-is for the LLM. The
// finance-specific sign/credit/debit vocabulary lives only in the YAML.
//
// Q&A only ever uses the BROAD `qa` target (no per-spec qualifier), so the match is
// a plain membership test — unlike validation, which routes per spec.

import { readFile } from "node:fs/promises";
import { join } from "node:path";

import { config } from "../config";

// Untrusted shape off disk (rule 11) — narrowed below, never trusted.
type ConventionDoc = {
	targets?: unknown;
	statement?: unknown;
	concept_groups?: unknown;
};

function asStringArray(value: unknown): string[] {
	return Array.isArray(value)
		? value.filter((v): v is string => typeof v === "string")
		: [];
}

/**
 * Pure render of a parsed ontology's `conventions` list into a
 * `<domain_conventions>` block for consumers whose label is `target`. Untrusted
 * input (rule 11) — narrows every field. Returns "" when nothing applies (caller
 * omits the section). Q&A uses the broad `qa` target, so this is a plain membership
 * test (no per-spec qualifier, unlike the engine's validation routing).
 */
export function formatConventionsBlock(
	conventions: unknown,
	target = "qa",
): string {
	if (!Array.isArray(conventions)) return "";
	const blocks: string[] = [];
	for (const raw of conventions as ConventionDoc[]) {
		if (typeof raw?.statement !== "string") continue;
		if (!asStringArray(raw.targets).includes(target)) continue;
		const lines = [raw.statement.trim()];
		const groups = raw.concept_groups;
		if (groups && typeof groups === "object") {
			for (const [label, members] of Object.entries(
				groups as Record<string, unknown>,
			)) {
				const names = asStringArray(members);
				if (names.length > 0) lines.push(`${label}: ${names.join(", ")}`);
			}
		}
		blocks.push(lines.join("\n"));
	}
	if (blocks.length === 0) return "";
	return `<domain_conventions>\n${blocks.join("\n\n")}\n</domain_conventions>`;
}

/**
 * Read the vertical's conventions and render those targeting `target` (default
 * `qa`) for the Q&A agent's user turn. "" when the vertical is null, the ontology
 * is missing/unparseable, or none target this consumer — best-effort, never throws
 * (a config-read blip must not fail an answer). The cockpit never interprets the
 * content; it routes by label and emits the YAML's statement + groups as-is.
 */
export async function buildConventionsBlock(
	vertical: string | null,
	target = "qa",
): Promise<string> {
	if (!vertical) return "";
	try {
		const text = await readFile(
			join(config.dataraumConfigPath, "verticals", vertical, "ontology.yaml"),
			"utf8",
		);
		// Bun's YAML, imported lazily (mirrors list-verticals.ts): a static import
		// of "bun" would make merely importing this module pull "bun", which the
		// node-run vitest workers can't resolve.
		const { YAML } = await import("bun");
		const doc = (YAML.parse(text) ?? null) as { conventions?: unknown } | null;
		return formatConventionsBlock(doc?.conventions, target);
	} catch {
		return "";
	}
}

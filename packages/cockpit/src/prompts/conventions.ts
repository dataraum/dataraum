// Vertical conventions for the cockpit Q&A agent (DAT-645 → DAT-789).
//
// The TS mirror of the engine's `OntologyLoader.format_conventions_for_prompt`
// (`packages/engine/.../analysis/semantic/ontology.py`). The engine pipes a
// vertical's conventions verbatim into its two SQL-authoring agents (extraction +
// validation); the cockpit Q&A agent is the third SQL author (`targets: [..., qa]`)
// and gets the same rule here. The cockpit NEVER interprets the content — it routes by
// the generic `target` label and emits the `statement` + `concept_groups` as-is.
//
// Conventions moved config→DB (DAT-789): the source is no longer the bind-mounted
// `verticals/<v>/ontology.yaml` — it is the mirrored `conventions` read view (the
// reader role's promoted-read surface, already scoped to the workspace's bound active
// vertical), so a `frame`-authored convention reaches Q&A exactly as it reaches
// extraction + validation. The finance-specific sign/credit/debit vocabulary lives in
// the vertical seed, never in this code.
//
// Q&A only ever uses the BROAD `qa` target (no per-spec qualifier), so the match is
// a plain membership test — unlike validation, which routes per spec.

import { isNull } from "drizzle-orm";

import { conventions as conventionsView } from "#/db/metadata/schema";

// Untrusted shape (rule 11) — narrowed below, never trusted. The DB read is typed, but
// `formatConventionsBlock` stays defensive so it also serves raw callers + tests.
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
 * Read the workspace's active-vertical conventions from the mirrored `conventions`
 * view and render those targeting `target` (default `qa`) for the Q&A agent's user
 * turn. "" when none target this consumer — best-effort, never throws (a metadata-read
 * blip must not fail an answer). The reader-role view is ALREADY scoped to the
 * workspace's bound active vertical (DAT-848), so no vertical argument is needed; this
 * function only filters out superseded rows and routes by label. The cockpit never
 * interprets the content; it emits the DB's statement + groups as-is.
 */
export async function buildConventionsBlock(target = "qa"): Promise<string> {
	try {
		// The metadata client is imported lazily (it constructs the reader-role SQL
		// client at module scope): a static import would pull it into every consumer of
		// this module + the node-run vitest workers. Mirrors the old lazy `bun` import.
		const { metadataDb } = await import("#/db/metadata/client");
		const rows = await metadataDb
			.select({
				targets: conventionsView.targets,
				statement: conventionsView.statement,
				concept_groups: conventionsView.conceptGroups,
			})
			.from(conventionsView)
			.where(isNull(conventionsView.supersededAt));
		return formatConventionsBlock(rows, target);
	} catch {
		return "";
	}
}

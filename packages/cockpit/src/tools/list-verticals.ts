// list_verticals tool (DAT-411) — the analytical verticals available to `frame`
// a workspace with, unified across the two places a vertical can live.
//
// A "vertical" is a domain ontology the engine resolves phase config against
// (its concepts, and for the curated ones the richer cycles/validations/metrics).
// Two kinds:
//   - builtin: ships in `dataraum-config/verticals/<name>/` (e.g. finance), read
//     here off the bind-mounted config tree (`config.dataraumConfigPath`). The
//     vertical's KEY is the directory name — that is what `frame`/the workflow
//     resolve config by, NOT the ontology's `name:` field.
//   - framed: declared in this workspace via `frame`, living only as `concept`
//     rows in `config_overlay` (no on-disk directory). `_adhoc` is the seed of
//     these — a builtin directory whose concepts come from the overlay, not its
//     (empty) ontology.yaml.
//
// Pure read (an fs scan of the mounted config + a grouped overlay count), no
// approval. The agent calls this before `frame` to pick an existing vertical
// that matches the data instead of inducing concepts from scratch.

import type { Dirent } from "node:fs";
import { readdir, readFile, stat } from "node:fs/promises";
import { join } from "node:path";
import { toolDefinition } from "@tanstack/ai";
import { and, count, eq, isNull, sql } from "drizzle-orm";
import { z } from "zod";

import { config } from "../config";
import { metadataDb } from "../db/metadata/client";
import { configOverlay } from "../db/metadata/schema";

const Vertical = z.object({
	// The key to pass to `frame` (and the workflow `vertical`) — the directory
	// name for builtins, the declared name for framed verticals.
	name: z.string(),
	// "builtin" = ships with DataRaum; "framed" = declared here via `frame`.
	kind: z.enum(["builtin", "framed"]),
	// One-line domain description from the builtin's ontology.yaml; null for a
	// framed vertical (it has no curated description).
	description: z.string().nullable(),
	// Concepts available for the vertical: a builtin's curated ontology.yaml
	// concepts plus any active concept overlay rows naming it (so `_adhoc` and
	// framed verticals report their induced/taught concepts).
	concept_count: z.number(),
	// Whether the builtin ships the richer operating-model objects. Always false
	// for framed verticals (concepts only).
	has_cycles: z.boolean(),
	has_validations: z.boolean(),
	has_metrics: z.boolean(),
});
export type Vertical = z.infer<typeof Vertical>;

/** Active `concept` overlay rows grouped by their `payload.vertical`. */
async function conceptCountsByVertical(): Promise<Map<string, number>> {
	const rows = await metadataDb
		.select({
			vertical: sql<string>`${configOverlay.payload}->>'vertical'`,
			n: count(),
		})
		.from(configOverlay)
		.where(
			and(
				isNull(configOverlay.supersededAt),
				eq(configOverlay.type, "concept"),
			),
		)
		.groupBy(sql`${configOverlay.payload}->>'vertical'`);
	return new Map(rows.map((r) => [r.vertical, Number(r.n)]));
}

async function pathExists(path: string): Promise<boolean> {
	try {
		await stat(path);
		return true;
	} catch {
		return false;
	}
}

/** Parse a vertical's ontology.yaml for its description + concept list; a
 * missing/unparseable file yields nulls (the directory still counts as a
 * vertical — `_adhoc`'s ontology is intentionally empty). */
async function readOntology(
	path: string,
): Promise<{ description?: string; concepts?: unknown[] } | null> {
	try {
		const text = await readFile(path, "utf8");
		// Bun's YAML, imported lazily: a static `import … from "bun"` would make
		// merely importing this tool (e.g. via the registry) pull "bun", which the
		// node-run test workers can't resolve unless they mock it.
		const { YAML } = await import("bun");
		return (YAML.parse(text) ?? null) as {
			description?: string;
			concepts?: unknown[];
		} | null;
	} catch {
		return null;
	}
}

/** The builtin verticals — every directory under `<config>/verticals/`. */
async function builtinVerticals(
	overlayCounts: Map<string, number>,
): Promise<Vertical[]> {
	const root = join(config.dataraumConfigPath, "verticals");
	let entries: Dirent<string>[];
	try {
		entries = await readdir(root, { withFileTypes: true, encoding: "utf8" });
	} catch {
		// Config tree not mounted/readable → no builtins (framed still resolve).
		return [];
	}
	const out: Vertical[] = [];
	for (const entry of entries) {
		if (!entry.isDirectory()) continue;
		const dir = join(root, entry.name);
		const onto = await readOntology(join(dir, "ontology.yaml"));
		out.push({
			name: entry.name,
			kind: "builtin",
			description: onto?.description?.trim() ?? null,
			concept_count:
				(onto?.concepts?.length ?? 0) + (overlayCounts.get(entry.name) ?? 0),
			has_cycles: await pathExists(join(dir, "cycles.yaml")),
			has_validations: await pathExists(join(dir, "validations")),
			has_metrics: await pathExists(join(dir, "metrics")),
		});
	}
	return out;
}

/** All verticals available to frame with: builtin directories ∪ framed names
 * (overlay verticals without a directory). Sorted builtins-first, then by name. */
export async function listVerticals(): Promise<Vertical[]> {
	const overlayCounts = await conceptCountsByVertical();
	const builtins = await builtinVerticals(overlayCounts);
	const builtinNames = new Set(builtins.map((v) => v.name));

	const framed: Vertical[] = [...overlayCounts.entries()]
		.filter(([vertical]) => vertical && !builtinNames.has(vertical))
		.map(([vertical, n]) => ({
			name: vertical,
			kind: "framed" as const,
			description: null,
			concept_count: n,
			has_cycles: false,
			has_validations: false,
			has_metrics: false,
		}));

	const byName = (a: Vertical, b: Vertical) => a.name.localeCompare(b.name);
	return [...builtins.sort(byName), ...framed.sort(byName)];
}

export const listVerticalsTool = toolDefinition({
	name: "list_verticals",
	description:
		"List the analytical verticals (domain ontologies) available to `frame` a " +
		"workspace with. Each entry has a `name` (pass it to `frame`), a `kind` " +
		"(builtin = ships with DataRaum, e.g. finance; framed = declared here via " +
		"`frame`), a `description`, the `concept_count`, and whether it ships the " +
		"richer operating-model objects (cycles / validations / metrics). Call it " +
		"BEFORE `frame` to pick an existing vertical that matches the data (e.g. " +
		"finance for invoices / ledgers / statements) instead of inducing concepts " +
		"from scratch.",
	inputSchema: z.object({}),
	outputSchema: z.array(Vertical),
}).server(() => listVerticals());

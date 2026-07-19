// Builtin-vertical scan for the portal's create-workspace flow (DAT-821).
//
// PORTAL-SAFE by construction: the richer `tools/list-verticals.ts` is
// workspace-role code (it reads the throwing workspace config and counts
// typed concept rows in the metadata DB — surfaces a portal container
// deliberately lacks). Creating a workspace only needs the BUILTIN choices —
// the directories shipped in the bind-mounted config tree — so this is a pure
// fs scan off `provisionerConfig().configPath`, sharing that module's rules:
// a vertical's KEY is the directory name, and `_`-prefixed directories
// (`_adhoc`, the induction substrate) are internal seeds, never offered.
//
// Framed verticals are deliberately absent: they are per-workspace overlay
// declarations, meaningless as the frame ontology of a workspace that does
// not exist yet.

import "@tanstack/react-start/server-only";

import { readdir, readFile } from "node:fs/promises";
import { join } from "node:path";
// `#/` alias (not `./`) so the unit test's vi.mock intercepts — the vitest
// mock registry matches the importer's specifier (vitest-mock-alias gotcha).
import { provisionerConfig } from "#/portal/provisioner-config";

/** One pickable vertical: the key `createWorkspace` receives, plus the
 * one-line domain description from its ontology.yaml (null when the file is
 * missing/unparseable — the directory still counts, mirroring
 * list-verticals.ts). */
export interface BuiltinVertical {
	name: string;
	description: string | null;
}

async function readDescription(path: string): Promise<string | null> {
	try {
		const text = await readFile(path, "utf8");
		// Bun's YAML, imported lazily (list-verticals.ts convention): a static
		// `import … from "bun"` would break the node-run test workers.
		const { YAML } = await import("bun");
		const onto = YAML.parse(text) as { description?: string } | null;
		return onto?.description?.trim() || null;
	} catch {
		return null;
	}
}

/**
 * Every builtin vertical under `<configPath>/verticals/`, name-sorted. Throws
 * when the tree is not readable: unlike the workspace listing (where a missing
 * mount degrades to framed-only), a create form with zero verticals is a dead
 * end — the portal's config mount being absent is a deployment bug to surface,
 * not to swallow.
 */
export async function listBuiltinVerticals(): Promise<BuiltinVertical[]> {
	const root = join(provisionerConfig().configPath, "verticals");
	const entries = await readdir(root, {
		withFileTypes: true,
		encoding: "utf8",
	});
	const out: BuiltinVertical[] = [];
	for (const entry of entries) {
		if (!entry.isDirectory() || entry.name.startsWith("_")) {
			continue;
		}
		out.push({
			name: entry.name,
			description: await readDescription(
				join(root, entry.name, "ontology.yaml"),
			),
		});
	}
	return out.sort((a, b) => a.name.localeCompare(b.name));
}

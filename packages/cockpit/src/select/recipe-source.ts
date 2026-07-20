// Persist arbitrary probed queries as sources (DAT-592) — one single-statement
// `db_recipe` source PER query, the producer behind the probe surface's import
// set (`server/import-sources.ts`).
//
// This is the 1-query = 1-source path the epic locked: where the retired agent
// `select` tool bundled a table-pick into ONE source carrying N `{name, sql}`
// recipe entries, the import set turns each staged query into its OWN named
// source carrying a SINGLE recipe entry. Both write the same row shape
// (`source-write.ts`) and feed the SAME batched addSourceWorkflow run; only the
// grain differs. The engine import path is unchanged — it
// materializes each `RecipeTable.sql` VERBATIM (sources/db_recipe), so an
// arbitrary JOIN/filter/projection already works.
//
// NO trigger here: persistence + validation only, mirroring `persistFileSources`.
// `server/import-sources.ts` composes this with ONE `triggerAddSource` over the
// whole set so N queries import as one coherent run (one grounding pass over the
// union), after a proper frame.

import { SUPPORTED_BACKENDS } from "../duckdb/probe";
import {
	RESERVED_SOURCE_NAME_PREFIXES,
	reservedSourceNamePrefix,
	SOURCE_NAME_PATTERN,
	sanitizeRecipeName,
} from "./mappers";
import { recipeContentHash } from "./source-content-hash";
import { importedRecipeHash, upsertSource } from "./source-write";

/** One staged query the user chose to import as its own source. */
export interface RecipeSourceSpec {
	/** The user-chosen source name (lowercase, letter-led — `SOURCE_NAME_PATTERN`). */
	source_name: string;
	/** The configured CONNECTION the query reads through — the probed source name
	 * (DAT-592). Distinct from `source_name`: the engine resolves credentials from
	 * THIS (`DATARAUM_{credential_source}_URL`), so a query imported as a new name
	 * still reads the right DB. Never the secret URL — just the reference. */
	credential_source: string;
	/** The DB backend the query runs against (persisted as the `backend` column). */
	backend: string;
	/** The verbatim read-only SQL — materialized by the engine into one raw table. */
	sql: string;
}

/** One persisted source: its id + the single-statement recipe that was stored. */
export interface PersistedRecipeSource {
	source_id: string;
	source_name: string;
	backend: string;
	recipe_table: { name: string; sql: string };
}

/** Validation failure the UI surfaces verbatim (names/SQL/backend are all
 * user-provided — no credentials pass through here, so echoing is safe). */
export class ImportSetError extends Error {}

/** Reject a single spec's name + backend up front, before any write, so a bad
 * batch fails loud as a whole (the loop below validates ALL before persisting). */
function validateSpec(spec: RecipeSourceSpec): void {
	const name = spec.source_name;
	if (!name || !SOURCE_NAME_PATTERN.test(name)) {
		throw new ImportSetError(
			`Invalid source name '${name ?? ""}'. Must match ${SOURCE_NAME_PATTERN.source} ` +
				"(lowercase, start with a letter, 2–49 chars of [a-z0-9_]).",
		);
	}
	if (!spec.credential_source) {
		throw new ImportSetError(
			`Source '${name}' has no credential_source — the configured connection ` +
				"to read through (the probed source) is required.",
		);
	}
	const reserved = reservedSourceNamePrefix(name);
	if (reserved !== null) {
		throw new ImportSetError(
			`Source name '${name}' starts with the reserved prefix '${reserved}' — ` +
				`${RESERVED_SOURCE_NAME_PREFIXES.join("/")} name derived-table families. ` +
				"Pick a different name.",
		);
	}
	if (!SUPPORTED_BACKENDS.includes(spec.backend)) {
		throw new ImportSetError(
			`Unsupported backend '${spec.backend}' for source '${name}' ` +
				`(supported: ${SUPPORTED_BACKENDS.join(", ")}).`,
		);
	}
	if (spec.sql.trim().length === 0) {
		throw new ImportSetError(`Source '${name}' has empty SQL.`);
	}
}

/**
 * UPSERT one `db_recipe` source per staged query and return their ids + the
 * recipe each stored. Persistence ONLY — the caller triggers the batched import.
 *
 * Each source carries a SINGLE recipe entry (`tables: [{name, sql}]`): the recipe
 * `name` is a sanitized identifier derived from the source name (it becomes the
 * `<source>__<name>` raw table), the `sql` is the user's verbatim query.
 * `recipe_hash` is computed per source over its `{backend, [one table]}` so the
 * engine's re-import witnessing (DAT-430) holds per query; the engine-stamped
 * `imported_recipe_hash` on an existing row is carried forward.
 *
 * Fails loud (before any write) on an empty batch or a duplicate source name in
 * the batch — two queries sharing a name would UPSERT the same row, silently
 * dropping one.
 *
 * The write loop is NOT a single transaction (matching the sibling file-source
 * loop in `file-source.ts`): a transient mid-loop failure can leave the earlier
 * sources upserted while the trigger never fires. That's recoverable, not
 * corrupting — every upsert is idempotent (name UNIQUE + the `recipe_hash`
 * witness), and the caller (`server/import-sources.ts` via the widget's mutation)
 * keeps the import set on error, so re-running the same batch re-upserts the
 * already-written rows and starts the run. The orphaned rows sit registered
 * with no typed tables under them until that retry imports them.
 */
export async function persistRecipeSources(
	specs: RecipeSourceSpec[],
): Promise<PersistedRecipeSource[]> {
	if (specs.length === 0) {
		throw new ImportSetError("No queries to import — the import set is empty.");
	}
	const seen = new Set<string>();
	for (const spec of specs) {
		validateSpec(spec);
		if (seen.has(spec.source_name)) {
			throw new ImportSetError(
				`Duplicate source name '${spec.source_name}' in the import set — ` +
					"each query needs a distinct name.",
			);
		}
		seen.add(spec.source_name);
	}

	const now = new Date();
	const persisted: PersistedRecipeSource[] = [];
	for (const spec of specs) {
		const recipeTable = {
			name: sanitizeRecipeName(spec.source_name),
			sql: spec.sql,
		};
		const witness = await importedRecipeHash(spec.source_name);
		const connectionConfig: Record<string, unknown> = {
			// DISTINCT key from the file `file_uris` key — never folded together.
			tables: [recipeTable],
			// The connection to read through (DAT-592) — a NAME reference the engine
			// resolves credentials from, never the secret URL.
			credential_source: spec.credential_source,
			recipe_hash: recipeContentHash(
				spec.backend,
				[recipeTable],
				spec.credential_source,
			),
			...(witness === null ? {} : { imported_recipe_hash: witness }),
		};
		const sourceId = await upsertSource({
			name: spec.source_name,
			sourceType: "db_recipe",
			backend: spec.backend,
			connectionConfig,
			now,
		});
		persisted.push({
			source_id: sourceId,
			source_name: spec.source_name,
			backend: spec.backend,
			recipe_table: recipeTable,
		});
	}
	return persisted;
}

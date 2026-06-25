// Content-hashing for source IDENTITY (DAT-422 / DAT-430) — split out of
// select/mappers so the pure shape helpers there stay client-safe.
//
// SERVER-ONLY: these two functions use `node:crypto` (createHash). mappers.ts is
// (transitively) reachable from a CLIENT graph — the select/import flow is driven
// from the connect canvas — so a `node:crypto` import there crashes the browser
// bundle (prod: shimmed empty; dev: Vite's throwing browser-external stub on the
// import line). Hashing has exactly two server callers (file-source / recipe-
// source), so it lives here behind the server-only marker; the crypto-free naming
// helpers stay in mappers. Mirrors the earlier `sourceTypeForUri → upload/policy`
// move (mappers.ts) and the run-context cut. See
// [[feedback_cockpit_isomorphic_import_side_effects]].

import "@tanstack/react-start/server-only";

import { createHash } from "node:crypto";

import { UPLOAD_PREFIX } from "../upload/policy";
import { type RecipeTable, SOURCE_NAME_PATTERN } from "./mappers";

// Each uploaded FILE is its own source, keyed by its content digest (the model:
// one file = one content-keyed source, DD/30900226 v2). A staged upload's object
// key is the locked `<ws>/uploads/<digest>/<filename>` shape (upload/policy.ts
// buildUploadKey, DAT-505) whose digest segment IS the file's content digest, so
// the source name is `src_<digest>`: identical bytes (same digest → same key →
// same name) UPSERT one row (re-upload dedup), and two distinct files never
// collide on a source — even when their basenames match — because the digests
// differ. The name is engine-valid by construction (a 40-char sha-1 hex digest →
// `src_` + 40 = 44 chars, lowercase, letter-led).
const CONTENT_SOURCE_PREFIX = "src_";

/**
 * The content-keyed source NAME (`src_<digest>`) for a staged upload URI.
 *
 * Parses the content digest from the locked `s3://<bucket>/<ws>/uploads/<digest>/
 * <filename>` upload shape (upload/policy.ts, DAT-505). A URI that is NOT
 * upload-shaped — a bare bucket key, or a prefix-enumerated object that was never
 * content-addressed — cannot be content-keyed and is a loud failure here: content
 * identity requires the upload digest, and the bucket/prefix connector is a
 * separate future concern (DAT-390), not a silently path-keyed source. The
 * returned name is asserted against `SOURCE_NAME_PATTERN` so a malformed digest
 * segment fails loud rather than persisting an unusable row.
 */
export function contentKeyedSourceName(uri: string): string {
	const path = uri.replace(/^s3:\/\/[^/]+\//, "");
	const segments = path.split("/").filter(Boolean);
	// Exactly `<ws>/uploads/<digest>/<filename>` — the workspace prefix, then the
	// locked upload triple. Locate the `uploads` marker rather than hard-coding an
	// index so the workspace segment (which can itself be a dashed UUID) is robust.
	const uploadsAt = segments.indexOf(UPLOAD_PREFIX);
	const digest = uploadsAt >= 0 ? segments[uploadsAt + 1] : undefined;
	const filename = uploadsAt >= 0 ? segments[uploadsAt + 2] : undefined;
	if (uploadsAt !== 1 || segments.length !== 4 || !digest || !filename) {
		throw new Error(
			`Cannot content-key '${uri}' — a file source must be a staged upload ` +
				`(s3://<bucket>/<ws>/${UPLOAD_PREFIX}/<digest>/<filename>). A bucket/prefix ` +
				"source is not content-addressed (a future connector).",
		);
	}
	// SHA-1 hex (what digestBytes produces) is already lowercase; toLowerCase() is
	// defensive so a non-canonical digest segment still yields an engine-valid name.
	const name = `${CONTENT_SOURCE_PREFIX}${digest.toLowerCase()}`;
	if (!SOURCE_NAME_PATTERN.test(name)) {
		throw new Error(
			`Content key '${name}' for '${uri}' is not a valid source name ` +
				"(lowercase, letter-led, 2–49 chars of [a-z0-9_]) — the upload digest " +
				"segment is malformed.",
		);
	}
	return name;
}

/**
 * The recipe content hash (`connection_config.recipe_hash`) — sha256 over the
 * canonical `{backend, tables}` JSON (DAT-430).
 *
 * Canonical = `JSON.stringify` of a `{backend, tables}` object: key order is
 * fixed by construction (this literal + the `{name, sql}` entries) and array
 * order follows the connected schema's table order, so a re-select of the SAME
 * pick serializes — and hashes — identically. The backend is PART of the
 * identity: the recipe SQL is interpreted against the connected backend, so the
 * same table names against a DIFFERENT backend are a different recipe — without
 * it, re-selecting a source name against another DBMS with identical table
 * names would match the import witness and silently skip over raw tables
 * extracted from the old backend. The engine treats the value as an OPAQUE
 * token: it never recomputes it, only copies it to `imported_recipe_hash` at
 * import success and compares the two on a later run
 * (`ImportPhase.should_skip`) — so no cross-language canonicalization contract
 * exists beyond this one function. This is what kills the silent-staleness
 * hole for name-keyed db sources: a re-pointed recipe stops matching the
 * import witness and the run fails loud instead of presence-skipping over the
 * old raw tables.
 */
export function recipeContentHash(
	backend: string,
	tables: RecipeTable[],
	credentialSource?: string,
): string {
	// `credentialSource` (DAT-592) is part of the recipe identity when present: a
	// query-source reads through a named connection, so re-pointing it (same SQL,
	// different DB) is a DIFFERENT recipe. Omitted for the table-pick path (a source
	// that is its own credential), keeping that path's hash byte-identical.
	const canonical =
		credentialSource === undefined
			? { backend, tables }
			: { backend, credential_source: credentialSource, tables };
	return createHash("sha256").update(JSON.stringify(canonical)).digest("hex");
}

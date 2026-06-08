// Upload → agent handoff (DAT-423) — kill the prose-baked `s3://` path.
//
// When the user drops files, the staged objects must reach the agent so it can
// connect/select them — but the raw `s3://…/uploads/<hash>/…` URIs must NEVER
// appear in a chat bubble (they leaked there before, baked into an English
// sentence the model also had to re-parse). Since the DAT-462 refs flip, the
// upload turn is a CLEAN bubble (filenames only) plus a model-only `refs` body
// sent via `forwardedProps` — the server persists it as a model-only row and
// folds it into the user turn for the model. The ordered batch flows
// STRUCTURALLY; the agent never extracts paths from prose, and no path reaches
// the rail.

import { fileName } from "#/lib/file-uri";

/** The clean, human-facing bubble for an upload turn: filenames only, no paths. */
export function uploadBubbleText(uris: string[]): string {
	const names = uris.map(fileName);
	if (names.length === 1) return `Uploaded ${names[0]}.`;
	return `Uploaded ${names.length} files: ${names.join(", ")}.`;
}

/**
 * The model-only refs body for an upload turn: the ordered objects and their
 * `s3://` uris, so the agent can connect/select them without any path reaching
 * the bubble. Sent as `SendOptions.refs` (forwardedProps).
 *
 * Carries the OBJECTS only. The standing "connect by uri → vertical → select,
 * narrate by filename, never echo the uri" instruction lives once in the
 * orchestrator prompt (`prompts/orchestrator.ts`, the `upload` tool entry) — not
 * duplicated here, so the two can't drift.
 */
export function uploadRefs(uris: string[]): string {
	const lines = uris.map((u, i) => `${i + 1}. ${fileName(u)} — ${u}`);
	return `The user just uploaded these objects, in order (filename — uri):\n${lines.join("\n")}`;
}

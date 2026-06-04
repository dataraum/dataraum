// Upload → agent handoff (DAT-423) — kill the prose-baked `s3://` path.
//
// When the user drops files, the staged objects must reach the agent so it can
// connect/select them — but the raw `s3://…/uploads/<hash>/…` URIs must NEVER
// appear in a chat bubble (they leaked there before, baked into an English
// sentence the model also had to re-parse). The fix: the upload turn carries TWO
// text parts —
//   1. a CLEAN bubble (filenames only) the chat rail renders, and
//   2. a marked REFS block (the ordered objects + their uris) the model reads but
//      the rail skips (`isUploadRefsPart`).
// Self-contained on the message — no side-channel state to thread or clear. The
// ordered batch flows STRUCTURALLY; the agent never extracts paths from prose.

import { fileName } from "#/lib/file-uri";

// Sentinel prefix marking the model-only refs part. The chat rail skips any user
// text part that starts with it; nothing a human types begins this way.
export const UPLOAD_REFS_MARKER = "[[dataraum:uploaded-objects]]";

/** True when a user text part is the structured upload-refs block — the rail
 * must NOT render it (it carries the `s3://` uris, model-only). */
export function isUploadRefsPart(content: string): boolean {
	return content.startsWith(UPLOAD_REFS_MARKER);
}

/** The clean, human-facing bubble for an upload turn: filenames only, no paths. */
export function uploadBubbleText(uris: string[]): string {
	const names = uris.map(fileName);
	if (names.length === 1) return `Uploaded ${names[0]}.`;
	return `Uploaded ${names.length} files: ${names.join(", ")}.`;
}

/** The model-only refs block: the ordered objects + their `s3://` uris, so the
 * agent can connect/select them without any path reaching the bubble. The rail
 * skips this part; the model reads it as the user turn's context. */
export function uploadRefsBlock(uris: string[]): string {
	const lines = uris.map((u, i) => `${i + 1}. ${fileName(u)} — ${u}`);
	return (
		`${UPLOAD_REFS_MARKER} The user just uploaded these objects, in order. ` +
		"Onboard them: connect to each (source_kind=file) by its uri to preview the " +
		"schema, match a vertical, then register with select. Refer to them by " +
		`filename in your replies — never echo the uri.\n${lines.join("\n")}`
	);
}

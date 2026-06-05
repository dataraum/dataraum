// Upload → agent handoff (DAT-423) — kill the prose-baked `s3://` path.
//
// When the user drops files, the staged objects must reach the agent so it can
// connect/select them — but the raw `s3://…/uploads/<hash>/…` URIs must NEVER
// appear in a chat bubble (they leaked there before, baked into an English
// sentence the model also had to re-parse). The fix is the shared refs pattern
// (lib/agent-refs, generalized from here in DAT-437): the upload turn carries a
// CLEAN bubble (filenames only) plus a marked, model-only refs part the rail
// skips (`isAgentRefsPart`). Self-contained on the message — no side-channel
// state to thread or clear. The ordered batch flows STRUCTURALLY; the agent
// never extracts paths from prose.

import { type RefsTurn, turnWithRefs } from "#/lib/agent-refs";
import { fileName } from "#/lib/file-uri";

/** The clean, human-facing bubble for an upload turn: filenames only, no paths. */
export function uploadBubbleText(uris: string[]): string {
	const names = uris.map(fileName);
	if (names.length === 1) return `Uploaded ${names[0]}.`;
	return `Uploaded ${names.length} files: ${names.join(", ")}.`;
}

/**
 * The complete upload turn: the clean bubble + the model-only refs part carrying
 * the ordered objects and their `s3://` uris, so the agent can connect/select
 * them without any path reaching the bubble.
 *
 * The refs part carries the OBJECTS only. The standing "connect by uri →
 * vertical → select, narrate by filename, never echo the uri" instruction lives
 * once in the orchestrator prompt (`prompts/orchestrator.ts`, the `upload` tool
 * entry) — not duplicated here, so the two can't drift.
 */
export function uploadTurn(uris: string[]): RefsTurn {
	const lines = uris.map((u, i) => `${i + 1}. ${fileName(u)} — ${u}`);
	return turnWithRefs(
		uploadBubbleText(uris),
		`The user just uploaded these objects, in order (filename — uri):\n${lines.join("\n")}`,
	);
}

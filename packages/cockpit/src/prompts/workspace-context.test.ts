// Unit tests for the WORKSPACE CONTEXT formatter (session-awareness). The DB read
// lives in `buildWorkspaceContext` (smoke-covered); the wording + the CURRENT tag
// are pure and tested here. Mock the metadata client so importing the module
// (which imports it for the DB readers) opens no connection.

import { describe, expect, it, vi } from "vitest";

vi.mock("#/db/metadata/client", () => ({ metadataDb: {} }));

import { formatWorkspaceContext } from "#/prompts/workspace-context";

describe("formatWorkspaceContext", () => {
	it("returns null when there are no sessions (nothing to tell the agent)", () => {
		expect(formatWorkspaceContext([])).toBeNull();
	});

	it("tags the most-recent session CURRENT and names what each spans", () => {
		const text = formatWorkspaceContext([
			{
				sessionId: "s1",
				vertical: "finance",
				tableNames: ["invoices", "payments"],
			},
			{ sessionId: "s2", vertical: "_adhoc", tableNames: ["orders"] },
		]) as string;

		expect(text).not.toBeNull();
		// The first (most recent) carries the CURRENT tag with its tables + vertical.
		expect(text).toContain(
			"s1 — invoices, payments · vertical finance ← CURRENT",
		);
		// Older sessions are listed but NOT tagged current — only one ← CURRENT tag
		// (the word "CURRENT" also appears in the instruction prose, hence the tag).
		expect(text).toContain("s2 — orders · vertical _adhoc");
		expect(text.match(/← CURRENT/g)).toHaveLength(1);
		// And it tells the agent to stop asking for a session id.
		expect(text).toContain("never ask the user for a session id");
	});

	it("handles a session with no tables or vertical yet", () => {
		const text = formatWorkspaceContext([
			{ sessionId: "s1", vertical: null, tableNames: [] },
		]) as string;
		expect(text).toContain("no tables yet");
		expect(text).toContain("vertical _adhoc");
	});
});

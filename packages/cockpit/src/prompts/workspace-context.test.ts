// Unit tests for the WORKSPACE CONTEXT formatter (workspace-awareness, DAT-562).
// The DB read lives in `buildWorkspaceContext` (smoke-covered); the wording is pure
// and tested here. Mock the metadata client so importing the module (which imports
// it for the DB readers) opens no connection.

import { describe, expect, it, vi } from "vitest";

// Mock every DB seam the module imports so a bare import opens no connection
// (the DB readers themselves are smoke-covered; only the formatter is unit-tested).
vi.mock("#/db/metadata/client", () => ({ metadataDb: {} }));
// The briefing digest (DAT-634) pulls the cockpit_db client (bun:sql) via runs.ts;
// mock it so a bare import opens no connection under node-vitest.
vi.mock("#/db/cockpit/client", () => ({ cockpitDb: {} }));
vi.mock("#/db/cockpit/registry", () => ({
	resolveActiveWorkspaceRow: async () => ({
		id: "ws-test",
		taskQueue: "engine-ws-test",
		vertical: "_adhoc",
	}),
}));

import { formatWorkspaceContext } from "#/prompts/workspace-context";

describe("formatWorkspaceContext (DAT-562)", () => {
	it("returns null when there are no imported tables (nothing to tell the agent)", () => {
		expect(formatWorkspaceContext("finance", [])).toBeNull();
	});

	it("names the workspace's imported tables + vertical and tells the agent not to ask for a session id", () => {
		const text = formatWorkspaceContext("finance", [
			"invoices",
			"payments",
		]) as string;

		expect(text).not.toBeNull();
		expect(text).toContain("Imported tables: invoices, payments");
		expect(text).toContain("vertical finance");
		// There is no session id to ask for any more.
		expect(text).toContain("never ask the user for a session id");
	});

	it("falls back to _adhoc when the workspace has no vertical", () => {
		const text = formatWorkspaceContext(null, ["orders"]) as string;
		expect(text).toContain("Imported tables: orders");
		expect(text).toContain("vertical _adhoc");
	});
});

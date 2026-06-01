// Wiring test for the chat route's buildChatOptions (DAT-353). Pure: asserts the
// bug-prone glue — the system prompt is sent as a cached block and the full tool
// registry is attached — without calling the model or touching the network.
// The live agentic loop is acceptance-tested via the compose smoke (real LLM).
//
// Importing the route transitively pulls config.ts + the Postgres metadata
// client (via the tool registry). We MOCK both so the test needs no real env and
// opens no connection — and sets NO process.env, which would leak across files
// in a reused worker and un-skip the gated integration tests.

import { describe, expect, it, vi } from "vitest";

vi.mock("#/config", () => ({ config: { anthropicApiKey: "sk-ant-test" } }));
vi.mock("#/db/metadata/client", () => ({ metadataDb: {} }));

import { buildChatOptions } from "./chat";

describe("chat route wiring (DAT-353)", () => {
	it("sends the orchestrator instructions as a cached system block", () => {
		const opts = buildChatOptions([{ role: "user", content: "hi" }]);
		expect(opts.systemPrompts).toHaveLength(1);
		expect(opts.systemPrompts[0].metadata.cache_control).toEqual({
			type: "ephemeral",
		});
		expect(opts.systemPrompts[0].content.length).toBeGreaterThan(0);
	});

	it("attaches the full tool registry to the loop", () => {
		const opts = buildChatOptions([{ role: "user", content: "hi" }]);
		const names = opts.tools.map((t: { name: string }) => t.name);
		expect(new Set(names)).toEqual(
			new Set([
				"list_sources",
				"list_tables",
				"look_table",
				"why_column",
				"run_sql",
				"probe",
				"connect",
				"frame",
				"select",
				"teach",
				"replay",
			]),
		);
	});

	it("passes the conversation messages through unchanged", () => {
		const messages = [{ role: "user" as const, content: "hi" }];
		expect(buildChatOptions(messages).messages).toBe(messages);
	});
});

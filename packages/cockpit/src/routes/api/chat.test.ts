// Wiring test for the chat route's buildChatOptions (DAT-353). Pure: asserts the
// bug-prone glue — the system prompt is sent as a cached block and the full tool
// registry is attached — without calling the model or touching the network.
// The live agentic loop is acceptance-tested via the compose smoke (real LLM).
//
// Booting the route transitively imports config.ts (via the tool registry →
// metadataDb). Env-stub + dynamic import, same as registry.test.ts.

import { beforeAll, describe, expect, it } from "vitest";

const ENV_DEFAULTS: Record<string, string> = {
	COCKPIT_DATABASE_URL: "postgresql://u:p@127.0.0.1:5432/cockpit_db",
	METADATA_DATABASE_URL: "postgresql://u:p@127.0.0.1:5432/ws_test",
	DATARAUM_WORKSPACE_ID: "test",
	DATARAUM_LAKE_PATH: "/tmp/lake",
	ANTHROPIC_API_KEY: "sk-ant-test-placeholder",
};
for (const [k, v] of Object.entries(ENV_DEFAULTS)) {
	if (!process.env[k]) process.env[k] = v;
}

let buildChatOptions: (
	messages: Array<{ role: "user"; content: string }>,
	// biome-ignore lint/suspicious/noExplicitAny: the option object's adapter is provider-internal
) => any;
beforeAll(async () => {
	({ buildChatOptions } = await import("./chat"));
});

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
			new Set(["list_sources", "list_tables", "teach", "replay"]),
		);
	});

	it("passes the conversation messages through unchanged", () => {
		const messages = [{ role: "user" as const, content: "hi" }];
		expect(buildChatOptions(messages).messages).toBe(messages);
	});
});

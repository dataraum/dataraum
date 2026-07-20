// Output-MECHANISM contract for the answer sub-agent's COMBINED call (DAT-807).
//
// This is the site the eval leans on hardest, and it was the riskiest edit in the
// lane: it keeps a real multi-tool loop AND takes its final draft from native
// structured output, in ONE streaming request (`tools` + `output_config.format`),
// mirroring worker/grounding-agent.ts. Two regressions would both ship green
// against the rest of the suite — reintroducing an `emit_result` envelope tool,
// or dropping `outputSchema` so the draft silently comes back as prose. These
// assertions pin the shape at the seam; no API call is made.
//
// The config invariant is pinned too. Note what is DELIBERATELY absent: this site
// leaves `thinking` unset (adaptive default ON), unlike the frame inductions and
// chart author which disable it. Asserting that absence is the point — a stray
// `thinking: {type:"disabled"}` here would be a silent config change the eval
// would misattribute to the mechanism.

import { beforeEach, describe, expect, it, vi } from "vitest";

const h = vi.hoisted(() => ({
	chat: vi.fn(
		async (_opts: unknown): Promise<unknown> => ({
			answer: "42",
			assumptions: [],
			concepts_used: [],
			tables_touched: [],
		}),
	),
}));

vi.mock("#/config", () => ({
	get config() {
		return { anthropicApiKey: "test-key", dataraumConfigPath: "/nonexistent" };
	},
}));
vi.mock("#/config.base", () => ({ baseConfig: {} }));
vi.mock("@tanstack/ai", async (importOriginal) => {
	const actual = await importOriginal<typeof import("@tanstack/ai")>();
	return { ...actual, chat: (opts: unknown) => h.chat(opts) };
});
vi.mock("@tanstack/ai-anthropic", () => ({
	createAnthropicChat: () => ({ __adapter: true }),
}));

// The sub-agent gathers its whole workspace context before the LLM call; stub
// every read at the seam so the test exercises the call shape, not the DB.
vi.mock("#/db/cockpit/registry", () => ({
	resolveActiveWorkspaceRow: async () => ({ vertical: "_adhoc" }),
}));
vi.mock("#/db/metadata/snippet-library", () => ({
	findById: async () => null,
}));
vi.mock("#/db/metadata/snippet-writer", () => ({
	saveQuerySnippet: async () => undefined,
}));
vi.mock("./query-context", () => ({
	buildSchemaBlock: async () => "<schema/>",
	buildEntitiesBlock: async () => "<entities/>",
	buildCatalogBlock: async () => "<catalog/>",
	buildRelationshipsBlock: async () => "<relationships/>",
	buildDriversBlock: async () => "<drivers/>",
}));
vi.mock("./snippet-search", () => ({
	buildVocabularyBlock: async () => "<vocabulary/>",
	snippetSearchTool: { name: "snippet_search" },
}));
vi.mock("./look-values", () => ({ lookValuesTool: { name: "look_values" } }));
vi.mock("./grain-note", () => ({
	loadNearUniqueColumns: async () => [],
	computeGrainNote: () => undefined,
}));
vi.mock("#/prompts", () => ({
	getQueryInstructions: () => "instructions",
	buildConventionsBlock: async () => "",
}));
// The metadata client opens a Bun `SQL` handle at import; vitest runs on node,
// so stub the seam (the cockpit vitest rule — mock `#/db/metadata/client`).
vi.mock("#/db/metadata/client", () => ({
	metadataDb: {},
	metadataWriteDb: {},
}));
vi.mock("./list-tables", () => ({ listTables: async () => [] }));

import { querySubAgent } from "./query";

/** The single chat() call's options, narrowed for assertion. */
function callOptions(): Record<string, unknown> {
	expect(h.chat).toHaveBeenCalledTimes(1);
	return h.chat.mock.calls[0]?.[0] as Record<string, unknown>;
}

describe("query sub-agent — combined tools + structured output", () => {
	beforeEach(() => {
		h.chat.mockClear();
	});

	it("sends the real tools AND the schema in one request", async () => {
		await querySubAgent("what is revenue");
		const opts = callOptions();

		// The draft must be schema-guaranteed, not parsed out of tool arguments.
		expect(opts.outputSchema).toBeDefined();

		// The three genuine tools stay — this is a real loop, not a one-shot emit.
		const toolNames = (opts.tools as Array<{ name?: string }>).map(
			(t) => t?.name,
		);
		expect(toolNames).toEqual(["snippet_search", "look_values", "run_steps"]);

		// ...and the envelope tool is gone. Its return is the regression to catch.
		expect(toolNames).not.toContain("emit_result");
	});

	it("keeps the args guard — unlike the tool-less sites, this one has a real tool boundary", async () => {
		await querySubAgent("what is revenue");
		const middleware = callOptions().middleware as Array<{ name?: string }>;
		expect(middleware.some((m) => m?.name === "tool-args-guard")).toBe(true);
	});

	it("leaves thinking at its adaptive default and keeps the loop budget", async () => {
		await querySubAgent("what is revenue");
		const modelOptions = callOptions().modelOptions as Record<string, unknown>;

		// Absent, NOT disabled — the frame inductions disable thinking, this site
		// must not. A value here would be a config change, not a mechanism change.
		expect(modelOptions).not.toHaveProperty("thinking");
		expect(modelOptions.max_tokens).toBe(24576);
	});

	it("returns the model's draft when chat() resolves one", async () => {
		h.chat.mockResolvedValueOnce({
			answer: "Revenue was 1.2M.",
			assumptions: ["Treated nulls as zero."],
			concepts_used: ["revenue"],
			tables_touched: ["invoices"],
		});
		const result = await querySubAgent("what is revenue");
		// No run_steps ran (the mock never invokes the tools), so there is no
		// validated query — the honest no-result narrative wins over the draft's
		// prose, and the grid stays null. That precedence is the DAT-608 contract.
		expect(result.grid).toBeNull();
		expect(result.answer).not.toBe("Revenue was 1.2M.");
	});

	it("does NOT swallow a provider failure as a no-result answer", async () => {
		// A 401/429/network error arrives from chat() UNCODED (the adapter yields a
		// RUN_ERROR chunk and the engine throws its generic fallback), so it must
		// fail the turn rather than be reported as "I couldn't compose a query".
		h.chat.mockRejectedValueOnce(new Error("401 unauthorized"));
		await expect(querySubAgent("what is revenue")).rejects.toThrow(
			"401 unauthorized",
		);
	});

	it("salvages a missing structured result instead of failing the turn", async () => {
		const err = new Error("missing structured result");
		Object.defineProperty(err, "code", {
			value: "structured-output-missing-result",
			enumerable: true,
		});
		h.chat.mockRejectedValueOnce(err);

		// No validated query either, so this lands on the no-result narrative —
		// but it must RESOLVE, not throw: exhaustion is a recoverable outcome.
		const result = await querySubAgent("what is revenue");
		expect(result.grid).toBeNull();
		expect(result.answer.length).toBeGreaterThan(0);
	});
});

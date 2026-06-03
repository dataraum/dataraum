// Unit tests for the frame tool (DAT-382) — the agent-tier induction step.
//
// Two mocked seams (the AC): the Anthropic adapter (`@tanstack/ai-anthropic`)
// + the SDK `chat()` structured-output call stand in for the induction LLM, and
// the Drizzle metadata client stands in for the `config_overlay` write `teach`
// performs. We assert the proposed concepts are written as `concept` overlay
// rows whose payload matches the engine's OntologyConcept field set (vertical
// "_adhoc"), and that an explicit edited concept set skips induction entirely.
//
// Importing frame.ts transitively pulls config.ts + the Postgres metadata
// client (via teach.ts). We mock both — same approach as registry.test.ts.

import { beforeEach, describe, expect, it, vi } from "vitest";

import type { ConnectSchema } from "#/duckdb/connect";

// The SDK chat() call — mocked to return a fixed structured induction result.
const chatMock = vi.fn();
vi.mock("@tanstack/ai", async (importOriginal) => {
	const actual = await importOriginal<typeof import("@tanstack/ai")>();
	return { ...actual, chat: (...args: unknown[]) => chatMock(...args) };
});

// The Anthropic adapter — mocked so no model is constructed / called.
vi.mock("@tanstack/ai-anthropic", () => ({
	createAnthropicChat: vi.fn(() => ({ kind: "text-adapter-stub" })),
}));

vi.mock("#/config", () => ({
	config: { anthropicApiKey: "sk-ant-test" },
}));

// Capture the inserted overlay rows. teach() calls
// metadataDb.insert(table).values(row); the values stub records each row.
const insertedRows: Array<Record<string, unknown>> = [];
const valuesMock = vi.fn(async (row: Record<string, unknown>) => {
	insertedRows.push(row);
});
vi.mock("#/db/metadata/client", () => ({
	metadataDb: { insert: vi.fn(() => ({ values: valuesMock })) },
}));

import { frame, induceConcepts } from "./frame";

const SCHEMA: ConnectSchema = {
	sourceKind: "file",
	source: "/data/orders.csv",
	tables: [
		{
			name: "orders.csv",
			rowCountEstimate: 3,
			columns: [
				{
					name: "amount",
					position: 1,
					sourceType: "DOUBLE",
					nullable: false,
					sampleValues: [10, 20, 30],
				},
			],
		},
	],
};

beforeEach(() => {
	insertedRows.length = 0;
	chatMock.mockReset();
	valuesMock.mockClear();
});

describe("frame (DAT-382)", () => {
	it("induces concepts from a ConnectSchema and writes them as concept overlay rows", async () => {
		chatMock.mockResolvedValue({
			concepts: [
				{
					name: "revenue",
					description: "Total income",
					indicators: ["amount", "revenue"],
					typical_role: "measure",
				},
				{ name: "order_id", typical_role: "key" },
			],
		});

		const result = await frame({ schema: SCHEMA });

		// Induction call happened with the schema in the user turn.
		expect(chatMock).toHaveBeenCalledTimes(1);

		// One `concept` overlay row per induced concept, vertical-tagged "_adhoc".
		expect(insertedRows).toHaveLength(2);
		for (const row of insertedRows) {
			expect(row.type).toBe("concept");
			expect((row.payload as { vertical: string }).vertical).toBe("_adhoc");
			expect(row.sessionId).toBeNull();
		}
		const revenue = insertedRows[0].payload as Record<string, unknown>;
		expect(revenue.name).toBe("revenue");
		expect(revenue.indicators).toEqual(["amount", "revenue"]);
		expect(revenue.typical_role).toBe("measure");
		// exclude_none parity: optional fields the model omitted are not sprayed.
		expect(revenue).not.toHaveProperty("exclude_patterns");

		// The tool result carries the written concepts + their overlay ids.
		expect(result.vertical).toBe("_adhoc");
		expect(result.concepts).toHaveLength(2);
		expect(result.concepts[0].name).toBe("revenue");
		expect(result.concepts[0].overlay_id).toEqual(expect.any(String));
	});

	it("declares concepts under a named vertical and returns it", async () => {
		const result = await frame({
			schema: SCHEMA,
			vertical_name: "sales",
			concepts: [{ name: "deal_value", typical_role: "measure" }],
		});
		expect((insertedRows[0].payload as { vertical: string }).vertical).toBe(
			"sales",
		);
		expect(result.vertical).toBe("sales");
	});

	it("rejects an unsafe vertical name", async () => {
		await expect(
			frame({
				schema: SCHEMA,
				vertical_name: "../etc",
				concepts: [{ name: "x" }],
			}),
		).rejects.toThrow(/Invalid vertical name/);
	});

	it("treats an explicit '_adhoc' vertical_name as the default (no throw)", async () => {
		const result = await frame({
			schema: SCHEMA,
			vertical_name: "_adhoc",
			concepts: [{ name: "x" }],
		});
		expect(result.vertical).toBe("_adhoc");
	});

	it("declares a user-edited concept set verbatim, skipping induction", async () => {
		const result = await frame({
			schema: SCHEMA,
			concepts: [{ name: "gross_margin", typical_role: "measure" }],
		});

		// No LLM call on the edit/declare path.
		expect(chatMock).not.toHaveBeenCalled();
		expect(insertedRows).toHaveLength(1);
		expect((insertedRows[0].payload as { name: string }).name).toBe(
			"gross_margin",
		);
		expect(result.concepts[0].name).toBe("gross_margin");
	});

	it("rejects an induction that returns no concepts", async () => {
		chatMock.mockResolvedValue({ concepts: [] });
		await expect(frame({ schema: SCHEMA })).rejects.toThrow(/no concepts/i);
		expect(insertedRows).toHaveLength(0);
	});

	it("induceConcepts returns the model's proposed set without writing", async () => {
		chatMock.mockResolvedValue({
			concepts: [{ name: "customer_id", typical_role: "key" }],
		});
		const concepts = await induceConcepts(SCHEMA);
		expect(concepts).toEqual([{ name: "customer_id", typical_role: "key" }]);
		expect(insertedRows).toHaveLength(0);
	});
});

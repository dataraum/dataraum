// Unit tests for the frame tool (DAT-382, DAT-469, DAT-470) — the agent-tier
// model induction step.
//
// Two mocked seams (the AC): the Anthropic adapter (`@tanstack/ai-anthropic`)
// + the SDK `chat()` structured-output call stand in for the induction LLM, and
// the Drizzle metadata client stands in for the `config_overlay` write `teach`
// performs. We assert the proposed concepts + validations + cycles are written as
// overlay rows whose payloads match the engine's shapes, and that an explicit
// edited set skips induction entirely. `frame` now frames the WHOLE model:
// concepts AND the validations + cycles over them, in one call — so the induce
// path makes THREE chat calls (concepts, then validations, then cycles over those
// concepts).
//
// Importing frame.ts transitively pulls config.ts + the Postgres metadata
// client (via teach.ts). We mock both — same approach as registry.test.ts. The
// nearest-shipped-vertical seed read uses a nonexistent config path so it
// degrades to an empty seed (no fs, no `bun` import in the node worker).

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
	config: {
		anthropicApiKey: "sk-ant-test",
		dataraumConfigPath: "/nonexistent",
	},
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

import {
	frame,
	induceConcepts,
	induceCycles,
	induceValidations,
	type ProposedCycle,
	type ProposedValidation,
} from "./frame";

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

// A minimal well-formed validation the induction LLM "returns" (ProposedValidation
// = ValidationSpec minus `vertical`, which frame fixes on write).
const PROPOSED_VALIDATION: ProposedValidation = {
	validation_id: "non_negative_amounts",
	name: "Non-negative amounts",
	description: "Every amount must be >= 0.",
	category: "data_quality",
	severity: "error",
	check_type: "constraint",
};

// A minimal well-formed cycle the induction LLM "returns" (ProposedCycle =
// CycleSpec minus `vertical`, which frame fixes on write). Free-form name +
// closed business_value, completion_indicators drive structural scoring.
const PROPOSED_CYCLE: ProposedCycle = {
	name: "order_to_cash",
	description: "Order through to payment collection.",
	business_value: "high",
	completion_indicators: ["paid", "settled"],
};

const rowsOfType = (type: string) =>
	insertedRows.filter((r) => r.type === type);

beforeEach(() => {
	insertedRows.length = 0;
	chatMock.mockReset();
	valuesMock.mockClear();
});

describe("frame (DAT-382, DAT-469, DAT-470)", () => {
	it("induces concepts AND validations AND cycles and writes them as overlay rows", async () => {
		// Concepts induce first, then validations, then cycles over them — three
		// chat calls.
		chatMock
			.mockResolvedValueOnce({
				concepts: [
					{
						name: "revenue",
						description: "Total income",
						indicators: ["amount", "revenue"],
						typical_role: "measure",
					},
					{ name: "order_id", typical_role: "key" },
				],
			})
			.mockResolvedValueOnce({ validations: [PROPOSED_VALIDATION] })
			.mockResolvedValueOnce({ cycles: [PROPOSED_CYCLE] });

		const result = await frame({ schema: SCHEMA });

		// All three families induced: one concept, one validation, one cycle call.
		expect(chatMock).toHaveBeenCalledTimes(3);

		// One `concept` overlay row per induced concept, vertical-tagged "_adhoc".
		const conceptRows = rowsOfType("concept");
		expect(conceptRows).toHaveLength(2);
		for (const row of conceptRows) {
			expect((row.payload as { vertical: string }).vertical).toBe("_adhoc");
			expect(row.sessionId).toBeNull();
		}
		const revenue = conceptRows[0].payload as Record<string, unknown>;
		expect(revenue.name).toBe("revenue");
		expect(revenue.indicators).toEqual(["amount", "revenue"]);
		// exclude_none parity: optional fields the model omitted are not sprayed.
		expect(revenue).not.toHaveProperty("exclude_patterns");

		// One `validation` overlay row, vertical-tagged + carrying the full spec.
		const validationRows = rowsOfType("validation");
		expect(validationRows).toHaveLength(1);
		const v = validationRows[0].payload as Record<string, unknown>;
		expect(validationRows[0].type).toBe("validation");
		expect(v.vertical).toBe("_adhoc");
		expect(v.validation_id).toBe("non_negative_amounts");
		expect(v.check_type).toBe("constraint");

		// One `cycle` overlay row, vertical-tagged + carrying the full spec.
		const cycleRows = rowsOfType("cycle");
		expect(cycleRows).toHaveLength(1);
		const c = cycleRows[0].payload as Record<string, unknown>;
		expect(cycleRows[0].type).toBe("cycle");
		expect(c.vertical).toBe("_adhoc");
		expect(c.name).toBe("order_to_cash");
		expect(c.business_value).toBe("high");
		expect(c.completion_indicators).toEqual(["paid", "settled"]);

		// The tool result carries the written model (concepts + validations +
		// cycles + ids).
		expect(result.vertical).toBe("_adhoc");
		expect(result.concepts).toHaveLength(2);
		expect(result.concepts[0].overlay_id).toEqual(expect.any(String));
		expect(result.validations).toHaveLength(1);
		expect(result.validations[0].validation_id).toBe("non_negative_amounts");
		expect(result.validations[0].overlay_id).toEqual(expect.any(String));
		expect(result.cycles).toHaveLength(1);
		expect(result.cycles[0].name).toBe("order_to_cash");
		expect(result.cycles[0].overlay_id).toEqual(expect.any(String));
	});

	it("declares an edited concept + validation + cycle model verbatim, skipping all induction", async () => {
		const result = await frame({
			schema: SCHEMA,
			vertical_name: "sales",
			concepts: [{ name: "deal_value", typical_role: "measure" }],
			validations: [PROPOSED_VALIDATION],
			cycles: [PROPOSED_CYCLE],
		});

		// No LLM call on the full declare path.
		expect(chatMock).not.toHaveBeenCalled();
		expect(
			(rowsOfType("concept")[0].payload as { vertical: string }).vertical,
		).toBe("sales");
		expect(
			(rowsOfType("validation")[0].payload as { vertical: string }).vertical,
		).toBe("sales");
		const cycleRow = rowsOfType("cycle")[0].payload as Record<string, unknown>;
		expect(cycleRow.vertical).toBe("sales");
		// The edited cycle is declared VERBATIM — name + completion_indicators
		// land exactly as supplied (no LLM rewrite).
		expect(cycleRow.name).toBe("order_to_cash");
		expect(cycleRow.completion_indicators).toEqual(["paid", "settled"]);
		expect(result.vertical).toBe("sales");
		expect(result.concepts[0].name).toBe("deal_value");
		expect(result.validations[0].validation_id).toBe("non_negative_amounts");
		expect(result.cycles[0].name).toBe("order_to_cash");
	});

	it("declares edited concepts but INDUCES validations + cycles over them (mixed path)", async () => {
		chatMock
			.mockResolvedValueOnce({ validations: [PROPOSED_VALIDATION] })
			.mockResolvedValueOnce({ cycles: [PROPOSED_CYCLE] });

		const result = await frame({
			schema: SCHEMA,
			concepts: [{ name: "gross_margin", typical_role: "measure" }],
			// validations + cycles absent → induce over the declared concepts.
		});

		// Validation + cycle induction ran (concepts were declared verbatim).
		expect(chatMock).toHaveBeenCalledTimes(2);
		expect(rowsOfType("concept")).toHaveLength(1);
		expect(rowsOfType("validation")).toHaveLength(1);
		expect(rowsOfType("cycle")).toHaveLength(1);
		expect(result.concepts[0].name).toBe("gross_margin");
		expect(result.validations).toHaveLength(1);
		expect(result.cycles).toHaveLength(1);
	});

	it("declares zero validations + cycles when given empty edited sets (no induction)", async () => {
		const result = await frame({
			schema: SCHEMA,
			concepts: [{ name: "gross_margin", typical_role: "measure" }],
			validations: [],
			cycles: [],
		});
		expect(chatMock).not.toHaveBeenCalled();
		expect(rowsOfType("validation")).toHaveLength(0);
		expect(rowsOfType("cycle")).toHaveLength(0);
		expect(result.validations).toEqual([]);
		expect(result.cycles).toEqual([]);
	});

	it("rejects an unsafe vertical name", async () => {
		await expect(
			frame({
				schema: SCHEMA,
				vertical_name: "../etc",
				concepts: [{ name: "x" }],
				validations: [],
				cycles: [],
			}),
		).rejects.toThrow(/Invalid vertical name/);
	});

	it("treats an explicit '_adhoc' vertical_name as the default (no throw)", async () => {
		const result = await frame({
			schema: SCHEMA,
			vertical_name: "_adhoc",
			concepts: [{ name: "x" }],
			validations: [],
			cycles: [],
		});
		expect(result.vertical).toBe("_adhoc");
	});

	it("rejects an induction that returns no concepts (before inducing validations)", async () => {
		chatMock.mockResolvedValue({ concepts: [] });
		await expect(frame({ schema: SCHEMA })).rejects.toThrow(/no concepts/i);
		// Threw after the concept call, before any validation induction or write.
		expect(chatMock).toHaveBeenCalledTimes(1);
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

	it("induceValidations induces over the concepts + seed, returns the set, writes nothing", async () => {
		chatMock.mockResolvedValue({ validations: [PROPOSED_VALIDATION] });
		// Inject a shipped-spec reader so the seed wiring is exercised without fs.
		const readSeed = vi.fn(async (v: string) =>
			v === "finance"
				? [
						{
							validation_id: "trial_balance",
							name: "Trial Balance",
							description: null,
							check_type: "balance",
							severity: "critical",
							parameters: null,
						},
					]
				: [],
		);
		// Frame ON TOP of finance so the seed reader's own-vertical specs feed the
		// few-shot (the fallback scan over the config tree is covered in
		// frame-family.test.ts).
		const validations = await induceValidations(
			SCHEMA,
			[{ name: "amount", typical_role: "measure" }],
			"finance",
			undefined,
			readSeed,
		);

		expect(validations).toEqual([PROPOSED_VALIDATION]);
		expect(insertedRows).toHaveLength(0);

		// The induce call used the validation instructions + carried the concepts
		// and the structural few-shot from the seed reader.
		const call = chatMock.mock.calls[0]?.[0] as {
			systemPrompts: string[];
			messages: { content: string }[];
		};
		expect(call.systemPrompts[0]).toMatch(/data-quality expert/);
		expect(call.messages[0].content).toContain("amount");
		expect(call.messages[0].content).toContain("trial_balance");
		expect(call.messages[0].content).toMatch(/EXAMPLE/);
	});

	it("induceCycles induces over the concepts + seed, returns the set, writes nothing", async () => {
		chatMock.mockResolvedValue({ cycles: [PROPOSED_CYCLE] });
		// Inject a shipped-cycle reader so the seed wiring is exercised without fs.
		const readSeed = vi.fn(async (v: string) =>
			v === "finance"
				? [
						{
							name: "order_to_cash",
							description: "Revenue cycle",
							business_value: "high",
							completion_indicators: ["paid"],
						},
					]
				: [],
		);
		// Frame ON TOP of finance so the seed reader's own-vertical specs feed the
		// few-shot (the fallback scan over the config tree is covered in
		// frame-family.test.ts).
		const cycles = await induceCycles(
			SCHEMA,
			[{ name: "amount", typical_role: "measure" }],
			"finance",
			undefined,
			readSeed,
		);

		expect(cycles).toEqual([PROPOSED_CYCLE]);
		expect(insertedRows).toHaveLength(0);

		// The induce call used the cycle instructions + carried the concepts and
		// the structural few-shot from the seed reader, flagged as do-not-copy.
		const call = chatMock.mock.calls[0]?.[0] as {
			systemPrompts: string[];
			messages: { content: string }[];
		};
		expect(call.systemPrompts[0]).toMatch(/business-process expert/);
		expect(call.messages[0].content).toContain("amount");
		expect(call.messages[0].content).toContain("order_to_cash");
		// The library-as-seed framing the AC requires: examples / structural /
		// do-not-copy, tagged by the cycle family + source vertical.
		expect(call.messages[0].content).toMatch(/EXAMPLE/);
		expect(call.messages[0].content).toMatch(/STRUCTURE|structural/i);
		expect(call.messages[0].content).toContain(
			'<cycle_examples vertical="finance">',
		);
	});

	it("forwards the tool-context abort into the nested induction chat() (DAT-449)", async () => {
		// The pattern shared by all the nested-synthesis sites: the .server()
		// context's abortSignal is bridged into the abortController chat() expects,
		// so a user stop() cancels the in-flight nested Anthropic call.
		chatMock.mockResolvedValue({ concepts: [] });
		const source = new AbortController();
		await induceConcepts(SCHEMA, source.signal);

		const options = chatMock.mock.calls[0]?.[0] as {
			abortController?: AbortController;
		};
		expect(options.abortController).toBeDefined();
		expect(options.abortController?.signal.aborted).toBe(false);
		source.abort();
		expect(options.abortController?.signal.aborted).toBe(true);
	});

	it("passes NO abortController when the tool context carries no signal", async () => {
		chatMock.mockResolvedValue({ concepts: [] });
		await induceConcepts(SCHEMA);
		const options = chatMock.mock.calls[0]?.[0] as {
			abortController?: AbortController;
		};
		expect(options.abortController).toBeUndefined();
	});
});

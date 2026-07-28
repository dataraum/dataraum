// Parts-at-source for an answer (DAT-678), against a real in-memory DuckDB.
//
// The subject under test is not "does the composer emit SQL" — parts.ts is
// already pinned elsewhere — it is the PROOF: a declaration the model wrote is
// worthless unless something executes it and checks it reproduces the number
// the user was actually shown. These tests exercise the failure modes that
// matter, on real data: a dropped predicate, a hallucinated relation, a
// swapped aggregate, a non-scalar answer, and the NULL≡NULL degenerate case
// that a naive comparison would wave through.

import { type DuckDBConnection, DuckDBInstance } from "@duckdb/node-api";
import { afterAll, beforeAll, describe, expect, it } from "vitest";

import {
	type AnswerDrillSource,
	answerNodeSteps,
	answerSourceProofSql,
	composeAnswerSource,
	narrowDeclaredSource,
	runAnswerSourceProof,
} from "./answer-source";

let instance: DuckDBInstance;
let conn: DuckDBConnection;

beforeAll(async () => {
	instance = await DuckDBInstance.create(":memory:");
	conn = await instance.connect();
	await conn.run(
		"CREATE TABLE orders (region VARCHAR, year BIGINT, amount DOUBLE, cost DOUBLE)",
	);
	await conn.run(
		"INSERT INTO orders VALUES " +
			"('EU',2024,100,40),('EU',2024,50,10),('US',2024,25,5),('EU',2023,999,999)",
	);
});
afterAll(() => {
	conn?.closeSync();
	instance?.closeSync();
});

/** The answer the model validated: 2024 revenue = 175. */
const ANSWER_2024 = "SELECT SUM(amount) AS value FROM orders WHERE year = 2024";

const source = (
	sources: AnswerDrillSource["sources"],
	expression: string,
): AnswerDrillSource => ({ sources, expression });

const revenue2024 = (where: string[] = ["year = 2024"]) => [
	{
		name: "revenue",
		parts: { selectExpr: "SUM(amount)", relation: "orders", where },
	},
];

/** Compose a candidate's scalar and run the proof against `answerSql`. */
async function prove(
	candidate: AnswerDrillSource,
	answerSql: string,
): Promise<boolean> {
	const composed = composeAnswerSource(candidate);
	if ("refusal" in composed) return false;
	return runAnswerSourceProof(conn, composed.sql, answerSql);
}

describe("narrowDeclaredSource", () => {
	it("narrows a full declaration and drops blank filters", () => {
		expect(
			narrowDeclaredSource({
				relation: " orders ",
				valueExpr: " SUM(amount) ",
				filters: ["year = 2024", "  ", ""],
			}),
		).toEqual({
			selectExpr: "SUM(amount)",
			relation: "orders",
			where: ["year = 2024"],
		});
	});

	// The declared ABSTENTION — a step that is a join, a window, or a multi-column
	// projection has no single-value shape to state, and saying so is correct.
	it("reads an empty relation or value expression as an abstention", () => {
		expect(
			narrowDeclaredSource({ relation: "", valueExpr: "SUM(x)", filters: [] }),
		).toBeNull();
		expect(
			narrowDeclaredSource({ relation: "orders", valueExpr: "", filters: [] }),
		).toBeNull();
	});
});

describe("answerNodeSteps", () => {
	it("makes a bare-ref expression the extract itself (the engine-parity shape)", () => {
		const steps = answerNodeSteps(source(revenue2024(), "revenue"));
		expect(steps).toHaveLength(1);
		expect(steps?.[0]).toMatchObject({
			stepId: "revenue",
			kind: "extract",
			outputStep: true,
		});
	});

	it("adds a combining formula step for real arithmetic, depending on the declared sources only", () => {
		const steps = answerNodeSteps(
			source(
				[
					...revenue2024(),
					{
						name: "cost",
						parts: {
							selectExpr: "SUM(cost)",
							relation: "orders",
							where: ["year = 2024"],
						},
					},
				],
				"revenue - cost",
			),
		);
		expect(steps).toHaveLength(3);
		const output = steps?.find((s) => s.outputStep);
		expect(output).toMatchObject({
			kind: "formula",
			expression: "revenue - cost",
			dependsOn: ["revenue", "cost"],
		});
	});

	it("refuses an expression that is off-grammar or names an undeclared source", () => {
		expect(answerNodeSteps(source(revenue2024(), "SUM(x)"))).toBeNull();
		expect(answerNodeSteps(source(revenue2024(), "margin"))).toBeNull();
		expect(answerNodeSteps(source([], "revenue"))).toBeNull();
	});
});

describe("the value proof", () => {
	it("accepts a declaration that reproduces the answer's number", async () => {
		expect(await prove(source(revenue2024(), "revenue"), ANSWER_2024)).toBe(
			true,
		);
	});

	// The failure this whole mechanism exists for: the declaration looks right,
	// reads the right table, computes the right aggregate — and quietly omits the
	// filter, so a drill from it would break down a DIFFERENT population than the
	// number the user was shown.
	it("rejects a declaration that drops the answer's filter", async () => {
		expect(await prove(source(revenue2024([]), "revenue"), ANSWER_2024)).toBe(
			false,
		);
	});

	it("rejects a swapped aggregate and a hallucinated relation", async () => {
		expect(
			await prove(
				source(
					[
						{
							name: "revenue",
							parts: {
								selectExpr: "SUM(cost)",
								relation: "orders",
								where: ["year = 2024"],
							},
						},
					],
					"revenue",
				),
				ANSWER_2024,
			),
		).toBe(false);
		expect(
			await prove(
				source(
					[
						{
							name: "revenue",
							parts: {
								selectExpr: "SUM(amount)",
								relation: "invoices",
								where: [],
							},
						},
					],
					"revenue",
				),
				ANSWER_2024,
			),
		).toBe(false);
	});

	// A multi-row answer is not a scalar, so there is nothing for a single-value
	// declaration to equal. DuckDB reports it as a subquery error, and the drill
	// falls back to tier A — which is the RIGHT path there anyway: a breakdown
	// already carries its dimensions as columns.
	it("rejects a non-scalar answer instead of comparing against one of its rows", async () => {
		expect(
			await prove(
				source(revenue2024(), "revenue"),
				"SELECT region, SUM(amount) AS value FROM orders WHERE year = 2024 GROUP BY region",
			),
		).toBe(false);
	});

	// The degenerate agreement: a declaration whose predicate matches nothing
	// aggregates to NULL, and an answer over an empty population is NULL too.
	// `IS NOT DISTINCT FROM` alone would call that a match — the NOT NULL floor
	// is what stops a drill being built on two absences agreeing.
	it("rejects two NULLs agreeing with each other", async () => {
		expect(
			await prove(
				source(revenue2024(["year = 1999"]), "revenue"),
				"SELECT SUM(amount) AS value FROM orders WHERE year = 1999",
			),
		).toBe(false);
	});

	it("proves a two-source formula answer", async () => {
		const candidate = source(
			[
				...revenue2024(),
				{
					name: "cost",
					parts: {
						selectExpr: "SUM(cost)",
						relation: "orders",
						where: ["year = 2024"],
					},
				},
			],
			"revenue - cost",
		);
		expect(
			await prove(
				candidate,
				"SELECT SUM(amount) - SUM(cost) AS value FROM orders WHERE year = 2024",
			),
		).toBe(true);
		// …and rejects it when the arithmetic is stated backwards.
		expect(
			await prove(
				{ ...candidate, expression: "cost - revenue" },
				"SELECT SUM(amount) - SUM(cost) AS value FROM orders WHERE year = 2024",
			),
		).toBe(false);
	});
});

describe("composing a proven source under a drill", () => {
	it("slices at SOURCE by a dimension the answer never projected", async () => {
		const composed = composeAnswerSource(source(revenue2024(), "revenue"), {
			slices: [{ column: "region" }],
			pins: [],
		});
		if ("refusal" in composed) throw new Error(composed.refusal);
		const rows = (
			await conn.runAndReadAll(composed.sql)
		).getRowObjectsJson() as Record<string, unknown>[];
		expect(
			[...rows].sort((a, b) =>
				String(a.region).localeCompare(String(b.region)),
			),
		).toEqual([
			{ region: "EU", value: 150 },
			{ region: "US", value: 25 },
		]);
	});

	it("pins a sliced group back to the row it came from", async () => {
		const composed = composeAnswerSource(source(revenue2024(), "revenue"), {
			slices: [{ column: "region" }],
			pins: [{ column: "region", value: "EU" }],
		});
		if ("refusal" in composed) throw new Error(composed.refusal);
		const rows = (
			await conn.runAndReadAll(composed.sql, composed.params)
		).getRowObjectsJson();
		expect(rows).toEqual([{ region: "EU", value: 150 }]);
	});

	it("refuses a source it cannot compose rather than emitting something", () => {
		const composed = composeAnswerSource(source(revenue2024(), "not_declared"));
		expect(composed).toHaveProperty("refusal");
	});
});

describe("answerSourceProofSql", () => {
	it("compares both sides as scalar subqueries and floors on NOT NULL", () => {
		const sql = answerSourceProofSql("SELECT 1", "SELECT 2");
		expect(sql).toContain("(SELECT 1) IS NOT DISTINCT FROM");
		expect(sql).toContain('"answer"."value" IS NOT NULL');
		expect(sql).toContain("(SELECT 2)");
	});
});

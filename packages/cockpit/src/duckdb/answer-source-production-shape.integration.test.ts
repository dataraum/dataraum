// The answer-source proof/parts path on PRODUCTION-SHAPED SQL.
//
// answer-source.test.ts already covers the proof's logic on idealized input:
// a bare table `orders`, a bare aggregate `SUM(amount)`. Every shape that
// actually broke this wave was invisible to it, because production SQL does
// not look like that. The three that bit us, all present below:
//
//   1. QUALIFIED RELATIONS — the model declares `lake.typed.<view>`, because
//      that is what the prompt shows it. Reduction to the bare name happens in
//      exactly ONE place (narrowDeclaredSource); anything reaching the
//      composer without passing through it emits FROM "lake.typed.x" as a
//      single quoted identifier and dies at bind time.
//   2. ALIASED PROJECTIONS — the model writes `SUM(amount) AS revenue`
//      despite the prompt saying not to. Nothing strips it; it becomes
//      `SUM(amount) AS revenue AS "value"`.
//   3. CASE-GUARDED SCALARS — the house empty-aggregation rule wraps every
//      scalar in `CASE WHEN COUNT(*) = 0 THEN NULL ELSE agg END`. This is the
//      NORMAL shape of a real answer, not an edge case.
//
// The fixture is therefore shaped like the engine's output, not like a demo:
// an enriched-view relation name, catalog column spellings (`account_id__name`),
// and the CASE-guarded aggregate the rule produces. See src/test/README.md —
// boundary tests use production-shape fixtures, never idealized bare-name SQL.
//
// Real DuckDB, no lake: runAnswerSourceProof takes an explicit connection, so
// the proof executes for real against an in-memory instance. Only the ducklake
// ATTACH (proveAnswerSource → withLakeConnection) is out of reach here.

import { type DuckDBConnection, DuckDBInstance } from "@duckdb/node-api";
import { afterAll, beforeAll, describe, expect, it } from "vitest";

import {
	type AnswerDrillSource,
	answerSourceScalarSql,
	bareRelationName,
	narrowDeclaredSource,
	runAnswerSourceProof,
} from "./answer-source";

let instance: DuckDBInstance;
let conn: DuckDBConnection;

// Shaped like an engine enriched view: the name the catalog carries, and the
// `<fk>__<attr>` column spellings the enrichment produces.
const RELATION = "current_orders_enriched";
const QUALIFIED = `lake.typed.${RELATION}`;

beforeAll(async () => {
	instance = await DuckDBInstance.create(":memory:");
	conn = await instance.connect();
	await conn.run(
		`CREATE TABLE ${RELATION} (
			account_id VARCHAR,
			account_id__name VARCHAR,
			region_id__name VARCHAR,
			fiscal_year BIGINT,
			amount DOUBLE,
			cost DOUBLE
		)`,
	);
	await conn.run(
		`INSERT INTO ${RELATION} VALUES
			('a1','Acme','EU',2024,100,40),
			('a1','Acme','EU',2024,50,10),
			('a2','Globex','US',2024,25,5),
			('a3','Initech','EU',2023,999,999)`,
	);
});

afterAll(() => {
	conn?.closeSync();
	instance?.closeSync();
});

/** The house empty-aggregation rule's shape — how a real scalar arrives. */
const caseGuarded = (agg: string) =>
	`CASE WHEN COUNT(*) = 0 THEN NULL ELSE ${agg} END`;

/** The answer the user was actually shown: 2024 revenue = 175. */
const ANSWER_2024 =
	`SELECT ${caseGuarded("SUM(amount)")} AS value ` +
	`FROM ${RELATION} WHERE fiscal_year = 2024`;

const declared = (relation: string, valueExpr: string, filters: string[]) => ({
	relation,
	valueExpr,
	filters,
});

const single = (
	parts: NonNullable<ReturnType<typeof narrowDeclaredSource>>,
): AnswerDrillSource => ({
	sources: [{ name: "revenue", parts }],
	expression: "revenue",
});

/** Narrow a declaration that must have succeeded, loudly if it didn't. */
function narrowed(
	d: Parameters<typeof narrowDeclaredSource>[0],
): NonNullable<ReturnType<typeof narrowDeclaredSource>> {
	const parts = narrowDeclaredSource(d);
	if (parts === null)
		throw new Error(`declaration failed to narrow: ${JSON.stringify(d)}`);
	return parts;
}

async function proves(candidate: AnswerDrillSource): Promise<boolean> {
	const scalar = answerSourceScalarSql(candidate);
	if (scalar === null) return false;
	return runAnswerSourceProof(conn, scalar, ANSWER_2024);
}

describe("answer-source on production-shaped declarations (DAT-671)", () => {
	describe("qualified relations", () => {
		it("reduces `lake.typed.<view>` to the bare name the engine scope resolves", () => {
			expect(bareRelationName(QUALIFIED)).toBe(RELATION);
		});

		it("proves a declaration whose relation arrived fully qualified", async () => {
			// The whole declaration path in one assertion: the model's qualified
			// spelling survives narrowing, composes, binds against a real relation,
			// and reproduces the number the user saw.
			const parts = narrowed(
				declared(QUALIFIED, caseGuarded("SUM(amount)"), ["fiscal_year = 2024"]),
			);
			expect(parts.relation).toBe(RELATION);
			await expect(proves(single(parts))).resolves.toBe(true);
		});

		it("a qualified relation that SKIPS narrowing cannot bind", async () => {
			// RECORDED DEFECT (not fixed here — W2-e is test-only): the reduction
			// lives ONLY in narrowDeclaredSource. POST /api/drill/parts builds
			// SnippetParts straight from the wire body without re-reducing
			// (routes/api/drill/parts.ts), so a client sending the qualified
			// spelling emits FROM "lake.typed.current_orders_enriched" as ONE
			// quoted identifier — a Catalog Error, surfaced as an unexplained
			// refusal. Production is safe only because the client happens to send
			// an already-proven handle. Pinned so a fix flips this to true.
			const unreduced = {
				selectExpr: caseGuarded("SUM(amount)"),
				relation: QUALIFIED,
				where: ["fiscal_year = 2024"],
			};
			await expect(proves(single(unreduced))).resolves.toBe(false);
		});
	});

	describe("CASE-guarded scalars (the house empty-aggregation rule)", () => {
		it("composes and proves — the normal shape of a real answer", async () => {
			const parts = narrowed(
				declared(RELATION, caseGuarded("SUM(amount)"), ["fiscal_year = 2024"]),
			);
			await expect(proves(single(parts))).resolves.toBe(true);
		});

		it("still detects a WRONG declaration under the guard", async () => {
			// The guard must not become a blanket pass: a dropped filter is still
			// a disagreement (it would total 1174, not 175).
			const parts = narrowed(
				declared(RELATION, caseGuarded("SUM(amount)"), []),
			);
			await expect(proves(single(parts))).resolves.toBe(false);
		});

		it("refuses the degenerate empty case rather than proving NULL ≡ NULL", async () => {
			// A filter matching nothing makes the guard emit NULL. The answer would
			// also be NULL, and a naive IS NOT DISTINCT FROM would call that
			// "proven". The proof's NOT NULL arm is what stops it.
			const emptyAnswer =
				`SELECT ${caseGuarded("SUM(amount)")} AS value ` +
				`FROM ${RELATION} WHERE fiscal_year = 1900`;
			const parts = narrowed(
				declared(RELATION, caseGuarded("SUM(amount)"), ["fiscal_year = 1900"]),
			);
			const scalar = answerSourceScalarSql(single(parts));
			expect(scalar).not.toBeNull();
			await expect(
				runAnswerSourceProof(conn, scalar as string, emptyAnswer),
			).resolves.toBe(false);
		});
	});

	describe("aliased projections", () => {
		it("an `AS <alias>` in the declared value expression cannot bind", async () => {
			// RECORDED DEFECT (not fixed here): the prompt tells the model to write
			// the expression WITHOUT the `AS value` alias, and nothing enforces it —
			// narrowDeclaredSource only trims. `SUM(amount) AS revenue` becomes
			// `SUM(amount) AS revenue AS "value"`, a parse error the proof swallows
			// into a silent tier-A downgrade. The proof is the ONLY thing standing
			// between a model typo and a wrong number, which is why it must stay
			// executed rather than assumed.
			const parts = narrowed(
				declared(RELATION, "SUM(amount) AS revenue", ["fiscal_year = 2024"]),
			);
			await expect(proves(single(parts))).resolves.toBe(false);
		});
	});

	describe("multi-source formulas on catalog-spelled columns", () => {
		it("proves margin = revenue - cost across two declared extracts", async () => {
			const revenue = narrowed(
				declared(QUALIFIED, caseGuarded("SUM(amount)"), ["fiscal_year = 2024"]),
			);
			const cost = narrowed(
				declared(QUALIFIED, caseGuarded("SUM(cost)"), ["fiscal_year = 2024"]),
			);
			const candidate: AnswerDrillSource = {
				sources: [
					{ name: "revenue", parts: revenue },
					{ name: "cost", parts: cost },
				],
				expression: "revenue - cost",
			};
			const marginAnswer =
				`SELECT ${caseGuarded("SUM(amount) - SUM(cost)")} AS value ` +
				`FROM ${RELATION} WHERE fiscal_year = 2024`;
			const scalar = answerSourceScalarSql(candidate);
			expect(scalar).not.toBeNull();
			await expect(
				runAnswerSourceProof(conn, scalar as string, marginAnswer),
			).resolves.toBe(true);
		});
	});
});

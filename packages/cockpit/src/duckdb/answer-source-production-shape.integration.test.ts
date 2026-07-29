// The answer-source proof/parts path on PRODUCTION-SHAPED SQL.
//
// answer-source.test.ts already covers the proof's logic on idealized input:
// a bare table `orders`, a bare aggregate `SUM(amount)`. Every shape that
// actually broke this wave was invisible to it, because production SQL does
// not look like that. The three that bit us, all present below:
//
//   1. QUALIFIED RELATIONS — the model declares `lake.typed.<view>`, because
//      that is what the prompt shows it. Reduction to the bare name happens in
//      exactly ONE place (`bareRelationName`); anything reaching the composer
//      without passing through it emits FROM "lake.typed.x" as a single quoted
//      identifier and dies at bind time. Both doors — the model's declaration
//      (`narrowDeclaredSource`) and the drill wire (`acceptWireSources`) — now
//      open onto it; the wire one did not, and that was DAT-671's defect.
//   2. ALIASED PROJECTIONS — the model writes `SUM(amount) AS revenue`
//      despite the prompt saying not to. Nothing strips it; it becomes
//      `SUM(amount) AS revenue AS "value"`, which does not parse. Since
//      DAT-671 that is REFUSED at declaration acceptance and reported back to
//      the model, instead of dying inside the proof as a silent tier-A
//      downgrade.
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
	acceptWireSources,
	answerSourceScalarSql,
	bareRelationName,
	narrowDeclaredSource,
	runAnswerSourceProof,
} from "./answer-source";
import { declaredValueExprRefusal } from "./sql-ast";

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
	sources: [{ name: "revenue", snippetId: null, parts }],
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

		it("reduces a qualified relation arriving on the DRILL WIRE (DAT-671)", async () => {
			// WAS A DEFECT, now fixed: the reduction lived ONLY in
			// narrowDeclaredSource, and POST /api/drill/parts built SnippetParts
			// straight from the wire body without re-reducing — so a client sending
			// the qualified spelling emitted FROM "lake.typed.current_orders_enriched"
			// as ONE quoted identifier, a Catalog Error surfaced as an unexplained
			// refusal. Production was safe only because the client happens to echo
			// back an already-proven handle. The route now accepts through
			// `acceptWireSources`, which is the one door to `bareRelationName`.
			const accepted = acceptWireSources(
				[
					{
						name: "revenue",
						snippetId: null,
						parts: {
							selectExpr: caseGuarded("SUM(amount)"),
							relation: QUALIFIED,
							where: ["fiscal_year = 2024"],
						},
					},
				],
				"revenue",
			);
			if ("refusal" in accepted) throw new Error(accepted.refusal);
			expect(accepted.sources[0]?.parts.relation).toBe(RELATION);
			await expect(proves(accepted)).resolves.toBe(true);
		});

		it("refuses a wire source BY NAME when it names no usable relation", async () => {
			// Dropping it instead would compose a different calculation than the one
			// asked for, and show the user a number nobody requested.
			const accepted = acceptWireSources(
				[
					{
						name: "revenue",
						snippetId: null,
						parts: {
							selectExpr: "SUM(amount)",
							relation: '"quoted.thing"',
							where: [],
						},
					},
				],
				"revenue",
			);
			expect(accepted).toEqual({
				refusal: expect.stringContaining("'revenue'"),
			});
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
		it("an `AS <alias>` in the declared value expression still cannot bind", async () => {
			// The underlying arithmetic, unchanged and still true: nothing about the
			// composer tolerates a second alias. `SUM(amount) AS revenue` composes to
			// `SUM(amount) AS revenue AS "value"` and dies at parse time. This is WHY
			// the acceptance gate below exists — and why the executed proof stays the
			// arbiter rather than an assumption.
			const parts = narrowed(
				declared(RELATION, "SUM(amount) AS revenue", ["fiscal_year = 2024"]),
			);
			await expect(proves(single(parts))).resolves.toBe(false);
		});

		it("is refused at ACCEPTANCE, naming the alias (DAT-671)", async () => {
			// WAS A SILENT DEFECT, now disclosed: the failure above used to reach the
			// proof, be recorded as "did not bind", and downgrade the answer to tier A
			// with no trace of a model typo. The declaration is now checked
			// structurally — DuckDB's own parser, never a regex over model SQL — and
			// the reason is handed back to the model through run_steps.
			const refusal = await declaredValueExprRefusal("SUM(amount) AS revenue");
			expect(refusal).toContain("AS revenue");
		});

		it("passes the CASE-guarded scalar the same gate refuses aliases with", async () => {
			// The gate must not become a blanket refusal of complicated expressions:
			// the house empty-aggregation shape is THREE aggregate calls and is the
			// normal form of a correct answer.
			await expect(
				declaredValueExprRefusal(caseGuarded("SUM(amount)")),
			).resolves.toBeNull();
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
					{ name: "revenue", snippetId: null, parts: revenue },
					{ name: "cost", snippetId: null, parts: cost },
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

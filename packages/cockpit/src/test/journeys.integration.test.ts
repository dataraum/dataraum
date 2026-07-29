// J1-J8: the practitioner journeys (DAT-671 lane R1) — the acceptance net for
// the graph recovery.
//
// WHAT MAKES THIS DIFFERENT FROM THE 2,281 UNIT TESTS
// The lead's verdict opening this slice: the unit suite pins branch logic
// against itself while the standard domain operations fail — "we patched our
// way through but never defined tests that cover reality." So this suite is
// built to be unable to do that:
//
//   · REAL ROUTES. Every step calls the route a practitioner's browser calls
//     (`Route.options.server.handlers.POST`), with a real `Request` and a real
//     `Response`. Nothing is mocked — not fetch, not the lake, not the catalog.
//   · REAL DATA. A real DuckLake carrying the real `dataraum-testdata` ledger,
//     read through the cockpit's own `lake.ts` bootstrap.
//   · AN EXTERNAL ANSWER KEY. Every figure is derived from the corpus and
//     cross-checked against its `ground_truth.yaml` (see journey-answer-key.ts,
//     which carries the SQL for each). A journey reproduces the ledger or it is
//     wrong — there is no assertion here that can be quietly adjusted to match
//     whatever the code happens to return.
//
// THE RED PINS ARE ALL FLIPPED (R1 wrote six as `it.fails` TARGET assertions
// paired with a green pin on the behaviour of the day; R2 landed the
// resolution and merged each pair into the single green assertion of the target
// that stands here now). Every journey below asserts what the product SHOULD do
// and does. There is no `.fails` left, and a new one should be rare: it means a
// journey cannot be expressed without a product change, which IS the finding.
//
// Two of the six did not flip by moving code, and both are recorded where they
// happened rather than here:
//   · J4's request moved to the parts path — a bare `resultSql` carries no
//     identity and tier A composes by WRAPPING, so the capability it lacks
//     traces to missing DATA, not to a lesser path (ADR-0024 decision 2). The
//     tier-A behaviour is retained as its own green assertion beside it.
//   · J1/J6 now send the `snippetId` their production caller sends — the
//     identity the whole verdict resolution keys on. J1 keeps a sibling
//     assertion that REMOVING it withholds the grain again, so "consults the
//     verdict" cannot decay into "offers a grain to anything with a date".

import { afterAll, beforeAll, describe, expect, it } from "vitest";

import {
	GROSS_MARGIN_BY_MONTH,
	GROSS_MARGIN_PCT,
	JOURNEY_RELATION,
	REVENUE_BY_ACCOUNT,
	REVENUE_BY_MONTH,
	TOTAL_REVENUE,
	toCents,
} from "./journey-answer-key";
import { attachJourneyWorkspace } from "./journey-fixture";
import {
	ACCOUNT_NAME_COLUMN,
	COGS_FIELD,
	COGS_PREDICATE,
	COGS_SELECT_EXPR,
	COGS_SNIPPET_ID,
	COST_CENTER_COLUMN,
	ENTRY_DATE_COLUMN,
	GROSS_MARGIN_METRIC,
	REVENUE_FIELD,
	REVENUE_PREDICATE,
	REVENUE_SELECT_EXPR,
	REVENUE_SNIPPET_ID,
} from "./seed-journey";

const jx = attachJourneyWorkspace();

/** The qualified spelling a model actually authors. Kept qualified on purpose:
 *  reducing it is the DAT-671 fix the answer path depends on, so every answer
 *  journey exercises it rather than handing the resolver a pre-reduced name. */
const QUALIFIED_RELATION = `lake.typed.${JOURNEY_RELATION}`;

/** The seeded ANSWER STATE — a proven parts-at-source declaration for "total
 *  revenue". This is what `proveAnswerSource` would have left on the canvas
 *  after a chat turn; seeding it directly keeps the suite free of any LLM call
 *  while exercising exactly the same downstream wire.
 *
 *  `snippetId` is the answer's IDENTITY (DAT-671 R2): this step declared REUSE
 *  of the curated revenue grounding, `classifyComponents` resolved the id, and
 *  the drill spends it as `snippet → concept → additivity verdict`. An answer
 *  that wrote fresh SQL carries none — that is J8, the one journey that must
 *  keep withholding. */
const REVENUE_SOURCE = {
	name: REVENUE_FIELD,
	snippetId: REVENUE_SNIPPET_ID,
	parts: {
		selectExpr: REVENUE_SELECT_EXPR,
		relation: QUALIFIED_RELATION,
		where: [REVENUE_PREDICATE],
	},
};

const COGS_SOURCE = {
	name: COGS_FIELD,
	snippetId: COGS_SNIPPET_ID,
	parts: {
		selectExpr: COGS_SELECT_EXPR,
		relation: QUALIFIED_RELATION,
		where: [COGS_PREDICATE],
	},
};

/** A declared source as the AXES wire carries it — where the number comes from,
 *  plus which grounding it reuses. Exactly what `answer-result.tsx` sends. */
const axisSource = (source: {
	snippetId: string;
	parts: { selectExpr: string; relation: string };
}) => ({
	relation: source.parts.relation,
	selectExpr: source.parts.selectExpr,
	snippetId: source.snippetId,
});

/** The answer's own base statement — the single-row scalar the practitioner
 *  read before drilling. */
const REVENUE_BASE_SQL =
	`SELECT ${REVENUE_SELECT_EXPR} AS value FROM ${QUALIFIED_RELATION} ` +
	`WHERE ${REVENUE_PREDICATE}`;

type PostHandler = (ctx: { request: Request }) => Promise<Response>;

/** Pull the POST handler off a TanStack file route.
 *
 *  Checked at runtime rather than cast: if the framework moves the handler, the
 *  suite must say so instead of silently testing nothing. */
function postHandler(mod: unknown): PostHandler {
	const handler = (
		mod as {
			Route?: { options?: { server?: { handlers?: { POST?: unknown } } } };
		}
	).Route?.options?.server?.handlers?.POST;
	if (typeof handler !== "function") {
		throw new Error("route module exposes no POST handler");
	}
	return handler as PostHandler;
}

interface Routes {
	axes: PostHandler;
	parts: PostHandler;
	node: PostHandler;
	runSql: PostHandler;
}

let routes: Routes;
let closeLake: () => Promise<void>;

/** POST a JSON body at a route, exactly as the browser would. */
async function post(
	handler: PostHandler,
	path: string,
	body: unknown,
): Promise<Response> {
	return handler({
		request: new Request(`http://cockpit.test${path}`, {
			method: "POST",
			body: JSON.stringify(body),
			headers: { "Content-Type": "application/json" },
		}),
	});
}

async function postJson<T = Record<string, unknown>>(
	handler: PostHandler,
	path: string,
	body: unknown,
): Promise<{ status: number; body: T }> {
	const res = await post(handler, path, body);
	return { status: res.status, body: (await res.json()) as T };
}

interface AxisShape {
	column: string;
	temporal: string | null;
	bucketGrain?: string;
	temporalWithheldReason?: string;
	disabledReason: string | null;
}
interface AxesResponse {
	axes: AxisShape[];
	reason?: string;
	reconciles?: { time: boolean; categorical: boolean };
	temporalGateReason?: string;
	temporalGateSource?: string;
}
interface ComposeResponse {
	ok: boolean;
	sql?: string;
	params?: unknown[];
	reason?: string;
	totals?: { sql: string };
}

/** Run SQL through the REAL `/api/run-sql` route and decode its NDJSON frame
 *  protocol into rows. A footer carrying an error is raised, so a broken
 *  statement can never read as an empty result. */
async function runSql(
	sql: string,
	params: unknown[] = [],
): Promise<Record<string, unknown>[]> {
	const res = await post(routes.runSql, "/api/run-sql", { sql, params });
	const text = await res.text();
	if (res.status !== 200) throw new Error(`run-sql ${res.status}: ${text}`);
	const frames = text
		.trim()
		.split("\n")
		.filter((line) => line.length > 0)
		.map((line) => JSON.parse(line) as Record<string, unknown>);
	const header = frames.find((f) => f.t === "h");
	const footer = frames.find((f) => f.t === "f");
	if (footer && typeof footer.error === "string") {
		throw new Error(`run-sql failed: ${footer.error}`);
	}
	if (!header) throw new Error(`run-sql produced no header frame: ${text}`);
	const columns = header.columns as string[];
	const rows: Record<string, unknown>[] = [];
	for (const frame of frames.filter((f) => f.t === "b")) {
		const cols = frame.cols as (unknown[] | null)[];
		for (let i = 0; i < (frame.n as number); i++) {
			const row: Record<string, unknown> = {};
			columns.forEach((name, ci) => {
				row[name] = cols[ci]?.[i] ?? null;
			});
			rows.push(row);
		}
	}
	return rows;
}

/** Unwrap a composer response, surfacing the REFUSAL REASON when there is one.
 *
 *  A bare `expect(body.ok).toBe(true)` reports "expected false to be true" and
 *  throws away the one thing that explains it — the same shape of silent loss
 *  this suite exists to catch. */
function composed(body: ComposeResponse): ComposeResponse {
	if (!body.ok) {
		throw new Error(`compose refused: ${body.reason ?? "(no reason given)"}`);
	}
	return body;
}

/** A breakdown as label → value, so an assertion never depends on the row order
 *  of a GROUP BY that carries no ORDER BY. The grouping order is not a promise
 *  the system makes; the VALUES are. */
const breakdown = (
	rows: Record<string, unknown>[],
	labelColumn: string,
	labelOf: (raw: string) => string = (raw) => raw,
): Record<string, number> =>
	Object.fromEntries(
		rows.map((r) => [
			labelOf(String(r[labelColumn])),
			toCents(Number(r.value)),
		]),
	);

const keyed = (
	pairs: readonly (readonly [string, number])[],
): Record<string, number> => Object.fromEntries(pairs.map(([k, v]) => [k, v]));

/** The `value` cell of a single-row result. Used for footer/totals rows, which
 *  project the operand components alongside the headline, so they are a ROW —
 *  not a bare scalar. */
const valueCell = (rows: Record<string, unknown>[]): number => {
	expect(rows).toHaveLength(1);
	return toCents(Number(rows[0].value));
};

/** The single numeric cell of a scalar result. */
const scalar = (rows: Record<string, unknown>[]): number => {
	expect(rows).toHaveLength(1);
	const values = Object.values(rows[0]);
	expect(values).toHaveLength(1);
	return toCents(Number(values[0]));
};

const axisFor = (res: AxesResponse, column: string): AxisShape | undefined =>
	res.axes.find((a) => a.column === column);

const columnsOf = (res: AxesResponse): string[] =>
	res.axes.map((a) => a.column).sort();

describe.skipIf(!jx.available)(
	jx.describeName("DAT-671 practitioner journeys (J1-J8)"),
	() => {
		beforeAll(async () => {
			routes = {
				axes: postHandler(await import("#/routes/api/drill/axes")),
				parts: postHandler(await import("#/routes/api/drill/parts")),
				node: postHandler(await import("#/routes/api/drill/node")),
				runSql: postHandler(await import("#/routes/api/run-sql")),
			};
			({ closeLake } = await import("#/duckdb/lake"));
		});

		afterAll(async () => {
			await closeLake?.();
		});

		// ────────────────────────────────────────────────────────────────────
		// J1 — a scalar additive measure: "total revenue"
		// ────────────────────────────────────────────────────────────────────
		describe("J1 · total revenue (answer path)", () => {
			it("answers the exact figure from the ledger", async () => {
				expect(scalar(await runSql(REVENUE_BASE_SQL))).toBe(TOTAL_REVENUE);
			});

			it("offers the catalogued dimensions to slice by", async () => {
				const { status, body } = await postJson<AxesResponse>(
					routes.axes,
					"/api/drill/axes",
					{
						partsSources: [axisSource(REVENUE_SOURCE)],
						baseSql: REVENUE_BASE_SQL,
					},
				);
				expect(status).toBe(200);
				// The qualified relation resolved: a failure to reduce it reports
				// "outside the current analysis" and yields NO axes at all.
				expect(body.axes.length).toBeGreaterThan(0);
				expect(columnsOf(body)).toContain(ACCOUNT_NAME_COLUMN);
				expect(columnsOf(body)).toContain(ENTRY_DATE_COLUMN);
			});

			// FLIPPED by R2. Was a red pin: the answer path withheld the time grain
			// for EVERY answer — `resolveAnswerDrillAxes` passed
			// `{target: null, carriers: new Map()}` unconditionally — and told the
			// practitioner the engine had not classified the concept, which was
			// false here (`measure|revenue|time|*` = additive is seeded, and J5
			// reads that very row through the node path). The defect was the
			// CONSULTATION, never the data.
			it("offers the time axis at MONTH grain, per the engine verdict", async () => {
				const { body } = await postJson<AxesResponse>(
					routes.axes,
					"/api/drill/axes",
					{
						partsSources: [axisSource(REVENUE_SOURCE)],
						baseSql: REVENUE_BASE_SQL,
					},
				);
				const date = axisFor(body, ENTRY_DATE_COLUMN);
				expect(date?.temporal).toBe("date");
				// Floored at the cadence the engine observed for this target.
				expect(date?.bucketGrain).toBe("month");
				expect(date?.temporalWithheldReason).toBeUndefined();
				// Named for what it is: a served verdict was read, not guessed at.
				expect(body.temporalGateSource).toBe("engine-verdict");
				// A single classified concept IS the measure, so its own verdict
				// governs — and this one reconciles on both axis classes.
				expect(body.reconciles).toEqual({ time: true, categorical: true });
			});

			// The identity is what licenses the grain, so removing it must take the
			// grain with it — the same declaration, the same relation, the same
			// seeded verdict, no `snippetId`. Without this, "consults the verdict"
			// and "offers a grain to anything with a date column" look identical
			// from the outside (J8 makes the same point for a genuinely
			// unclassified concept; this one isolates the WIRE).
			it("withholds it again when the same declaration names no grounding", async () => {
				const { relation, selectExpr } = axisSource(REVENUE_SOURCE);
				const { body } = await postJson<AxesResponse>(
					routes.axes,
					"/api/drill/axes",
					{
						partsSources: [{ relation, selectExpr }],
						baseSql: REVENUE_BASE_SQL,
					},
				);
				const date = axisFor(body, ENTRY_DATE_COLUMN);
				expect(date?.temporal).toBeNull();
				expect(date?.bucketGrain).toBeUndefined();
				expect(body.temporalGateSource).toBe("withheld-no-verdict");
				expect(date?.temporalWithheldReason).toContain(
					"has not classified for additivity",
				);
			});
		});

		// ────────────────────────────────────────────────────────────────────
		// J2 — J1 sliced by account
		// ────────────────────────────────────────────────────────────────────
		describe("J2 · revenue by account", () => {
			const slice = {
				sources: [REVENUE_SOURCE],
				expression: REVENUE_FIELD,
				steps: [{ kind: "slice", column: ACCOUNT_NAME_COLUMN }],
			};

			it("breaks the total into the five accounts with activity, to the cent", async () => {
				const res = await postJson<ComposeResponse>(
					routes.parts,
					"/api/drill/parts",
					slice,
				);
				composed(res.body);
				expect(res.status).toBe(200);

				const rows = await runSql(res.body.sql ?? "", res.body.params);
				expect(breakdown(rows, ACCOUNT_NAME_COLUMN)).toEqual(
					keyed(REVENUE_BY_ACCOUNT),
				);
			});

			it("the parts sum to the undrilled total", async () => {
				const sum = toCents(
					REVENUE_BY_ACCOUNT.reduce((acc, [, value]) => acc + value, 0),
				);
				expect(sum).toBe(TOTAL_REVENUE);
			});

			// The axis a result ALREADY breaks out is greyed WITH a reason.
			//
			// Note carefully which mechanism this exercises. The server greys an
			// axis only when the answer's own BASE statement already groups by it
			// (`markAlreadyInResult` over `existingIdentifierColumns`, a structural
			// never-executed parse). The greying a practitioner sees while LIVE
			// drilling is a different, purely client-side rule — `drillable-grid`
			// disables any column in its local `steps`, and the axes query is keyed
			// on a request that does not include `steps`, so reopening the menu
			// after a drill issues no new request at all.
			//
			// So this journey feeds a grouped BASE statement, which is what the
			// server contract actually keys on. Worth recording for R5: the
			// client-side branch disables the item with NO reason text and NO
			// tooltip (both are gated on `disabledReason`), which is the
			// "empty result is a claim" failure in miniature — the practitioner
			// sees a dead menu item and is told nothing.
			it("greys an axis the result already breaks out, and says why", async () => {
				const groupedBase =
					`SELECT ${ACCOUNT_NAME_COLUMN}, ${REVENUE_SELECT_EXPR} AS value ` +
					`FROM ${QUALIFIED_RELATION} WHERE ${REVENUE_PREDICATE} GROUP BY 1`;
				const { body } = await postJson<AxesResponse>(
					routes.axes,
					"/api/drill/axes",
					{
						partsSources: [axisSource(REVENUE_SOURCE)],
						baseSql: groupedBase,
					},
				);
				const account = axisFor(body, ACCOUNT_NAME_COLUMN);
				expect(account).toBeDefined();
				// Still OFFERED, but disabled WITH a reason — an axis that silently
				// vanished would be indistinguishable from one we never found.
				expect(account?.disabledReason).toEqual(expect.any(String));
				expect(account?.disabledReason).toContain("already");
			});

			// FLIPPED by R2 (was a red pin: the route served no `totals` on any
			// branch, so a drilled ANSWER grid had no footer at all and the
			// practitioner never saw the figure they started from — while
			// `/api/drill/node` shipped one for the identical composition).
			it("the footer prints the undrilled total (parts reconcile additively)", async () => {
				const res = await postJson<ComposeResponse>(
					routes.parts,
					"/api/drill/parts",
					slice,
				);
				// ESTABLISH THE SLICE FIRST — the premise the footer claim rests on.
				// Without it a `totals` assertion could pass against a drill that
				// composes nothing meaningful.
				expect(res.status).toBe(200);
				composed(res.body);
				expect(res.body.sql).toEqual(expect.any(String));

				expect(res.body.totals?.sql).toEqual(expect.any(String));
				expect(valueCell(await runSql(res.body.totals?.sql ?? ""))).toBe(
					TOTAL_REVENUE,
				);
			});
		});

		// ────────────────────────────────────────────────────────────────────
		// J3 — J1 sliced by month
		// ────────────────────────────────────────────────────────────────────
		describe("J3 · revenue by month", () => {
			const monthSlice = {
				sources: [REVENUE_SOURCE],
				expression: REVENUE_FIELD,
				steps: [{ kind: "slice", column: ENTRY_DATE_COLUMN, grain: "1M" }],
			};

			// FLIPPED by R2 (was a red pin, and the journey was not merely wrong on
			// the answer path but UNEXPRESSIBLE: the route's `z.strictObject`
			// rejected the `grain` key outright, so "revenue by month" could not
			// even be REQUESTED there. The schema is still strict — an off-grammar
			// grain token is refused by name — it simply now accepts the key the
			// composer has always understood).
			it("buckets the year into twelve months that sum to the total", async () => {
				const res = await postJson<ComposeResponse>(
					routes.parts,
					"/api/drill/parts",
					monthSlice,
				);
				expect(res.status).toBe(200);
				composed(res.body);

				const rows = await runSql(res.body.sql ?? "", res.body.params);
				expect(rows).toHaveLength(REVENUE_BY_MONTH.length);
				const sum = toCents(rows.reduce((acc, r) => acc + Number(r.value), 0));
				expect(sum).toBe(TOTAL_REVENUE);
			});

			// The strictness that survived the widening: a grain token outside the
			// closed grammar is REFUSED BY NAME, never stripped into a raw grouping
			// under a chip that claims a bucket width.
			it("refuses an off-grammar grain token by name", async () => {
				const { body } = await postJson<ComposeResponse>(
					routes.parts,
					"/api/drill/parts",
					{
						...monthSlice,
						steps: [
							{ kind: "slice", column: ENTRY_DATE_COLUMN, grain: "1fort" },
						],
					},
				);
				expect(body.ok).toBe(false);
				expect(body.reason).toContain("1fort");
			});

			// The engine-side arithmetic the journey depends on is independently
			// true — twelve months, summing exactly — so the pin above is about the
			// PRODUCT withholding a correct answer, not about the ledger.
			it("the ledger itself yields twelve months summing to the total", async () => {
				const rows = await runSql(
					`SELECT strftime(time_bucket(INTERVAL '1 month', ${ENTRY_DATE_COLUMN}), '%Y-%m') AS bucket,
					        SUM(credit) AS value
					   FROM ${QUALIFIED_RELATION}
					  WHERE ${REVENUE_PREDICATE}
					  GROUP BY 1 ORDER BY 1`,
				);
				expect(
					rows.map((r) => [String(r.bucket), toCents(Number(r.value))]),
				).toEqual(REVENUE_BY_MONTH.map(([p, v]) => [p, v]));
				expect(toCents(rows.reduce((acc, r) => acc + Number(r.value), 0))).toBe(
					TOTAL_REVENUE,
				);
			});
		});

		// ────────────────────────────────────────────────────────────────────
		// J4 — a grouped result can be sliced FURTHER
		// ────────────────────────────────────────────────────────────────────
		describe("J4 · drilling a grouped result further", () => {
			const groupedSql =
				`SELECT ${ACCOUNT_NAME_COLUMN}, SUM(credit) AS value ` +
				`FROM ${QUALIFIED_RELATION} WHERE ${REVENUE_PREDICATE} ` +
				`GROUP BY 1 ORDER BY 2 DESC`;

			it("the grouped result itself is correct", async () => {
				expect(
					breakdown(await runSql(groupedSql), ACCOUNT_NAME_COLUMN),
				).toEqual(keyed(REVENUE_BY_ACCOUNT));
			});

			// RETAINED (R2, owner ruling): the SAME grouped statement as an orphan
			// SQL string still offers no month — and that is a DATA difference, not
			// a path difference, which is exactly the distinction ADR-0024
			// decision 2 draws.
			//
			// A bare `resultSql` carries no identity: nothing says which concept
			// this number is, so no additivity verdict can be read for it, and
			// tier A composes by WRAPPING the result — it can only group by columns
			// the statement projects, which this one's date is not. Both halves of
			// the capability are genuinely missing. The pin stays green so a later
			// "offer grains everywhere" regression cannot slip in through tier A.
			it("still offers no month on the same result WITHOUT identity", async () => {
				const { body } = await postJson<AxesResponse>(
					routes.axes,
					"/api/drill/axes",
					{ resultSql: groupedSql },
				);
				expect(axisFor(body, ENTRY_DATE_COLUMN)).toBeUndefined();
				// Not the empty-catalog case: the account column IS found and IS
				// offered — disabled, WITH its reason. An axis that silently vanished
				// would be indistinguishable from one we never found.
				const account = axisFor(body, ACCOUNT_NAME_COLUMN);
				expect(account).toBeDefined();
				expect(account?.disabledReason).toContain("already");
				// So the menu has nothing actionable — and every item says why.
				expect(body.axes.filter((a) => a.disabledReason === null)).toHaveLength(
					0,
				);
			});

			// FLIPPED by R2 — re-pointed at the path a practitioner actually gets
			// here on (owner ruling). The grouped grid in front of them came from an
			// ANSWER: it carries the proven declaration, so the drill recomposes at
			// SOURCE instead of wrapping the result, and the month is available even
			// though the statement on screen never projected it. That is the whole
			// point of parts-at-source, and it is the same request J2 drilled — one
			// slice further along.
			it("a grouped revenue result can still be split by month", async () => {
				const { body } = await postJson<AxesResponse>(
					routes.axes,
					"/api/drill/axes",
					{
						partsSources: [axisSource(REVENUE_SOURCE)],
						baseSql: groupedSql,
					},
				);
				const date = axisFor(body, ENTRY_DATE_COLUMN);
				expect(date).toBeDefined();
				expect(date?.disabledReason).toBeNull();
				expect(date?.bucketGrain).toBe("month");
				// …while the account it is ALREADY broken out by is greyed with its
				// reason, so "slice further" never offers a tautological re-group.
				expect(axisFor(body, ACCOUNT_NAME_COLUMN)?.disabledReason).toContain(
					"already",
				);
			});
		});

		// ────────────────────────────────────────────────────────────────────
		// J5 — a ratio metric through the NODE path
		// ────────────────────────────────────────────────────────────────────
		describe("J5 · gross margin by month (metric node)", () => {
			it("offers the time axis at month grain and declares non-reconciliation", async () => {
				const { status, body } = await postJson<AxesResponse>(
					routes.axes,
					"/api/drill/axes",
					{ metricKey: GROSS_MARGIN_METRIC },
				);
				expect(status).toBe(200);
				const date = axisFor(body, ENTRY_DATE_COLUMN);
				expect(date?.temporal).toBe("date");
				// Bucketing a ratio IS honest — the composer regroups each carrier
				// per bucket and re-evaluates — so it is offered, floored at the
				// cadence the engine observed.
				expect(date?.bucketGrain).toBe("month");
				expect(date?.temporalWithheldReason).toBeUndefined();
				expect(body.temporalGateSource).toBe("engine-verdict");
				// …but the parts do NOT add up to the whole on either axis class.
				expect(body.reconciles).toEqual({ time: false, categorical: false });
			});

			it("the undrilled headline is the whole-ledger margin", async () => {
				const { body } = await postJson<ComposeResponse>(
					routes.node,
					"/api/drill/node",
					{ metricKey: GROSS_MARGIN_METRIC, steps: [] },
				);
				composed(body);
				expect(scalar(await runSql(body.sql ?? ""))).toBe(GROSS_MARGIN_PCT);
			});

			it("recomputes the margin per month rather than splitting the total", async () => {
				const { body } = await postJson<ComposeResponse>(
					routes.node,
					"/api/drill/node",
					{
						metricKey: GROSS_MARGIN_METRIC,
						steps: [{ kind: "slice", column: ENTRY_DATE_COLUMN, grain: "1M" }],
					},
				);
				composed(body);
				const rows = await runSql(body.sql ?? "", body.params);
				expect(rows).toHaveLength(GROSS_MARGIN_BY_MONTH.length);

				expect(
					breakdown(rows, ENTRY_DATE_COLUMN, (raw) => raw.slice(0, 7)),
				).toEqual(keyed(GROSS_MARGIN_BY_MONTH));
			});

			it("prints a RECOMPUTED footer total equal to the headline, not the column sum", async () => {
				const open = await postJson<ComposeResponse>(
					routes.node,
					"/api/drill/node",
					{ metricKey: GROSS_MARGIN_METRIC, steps: [] },
				);
				// The footer statement exists and is the UNRESTRICTED scalar. It
				// projects the OPERAND components alongside the value — the footer is
				// a whole row (`footerCells`), so each carrier's own total can be
				// shown under its column, not just the headline.
				expect(open.body.totals?.sql).toEqual(expect.any(String));
				const totalsRows = await runSql(open.body.totals?.sql ?? "");
				expect(totalsRows).toHaveLength(1);
				const footerRow = totalsRows[0];
				expect(Object.keys(footerRow)).toEqual(
					expect.arrayContaining(["value", REVENUE_FIELD, COGS_FIELD]),
				);
				// The carriers carry their own honest whole-ledger totals…
				expect(toCents(Number(footerRow[REVENUE_FIELD]))).toBe(TOTAL_REVENUE);
				// …and the headline cell is the RECOMPUTED margin.
				const footer = toCents(Number(footerRow.value));
				expect(footer).toBe(GROSS_MARGIN_PCT);

				// And it is emphatically NOT what adding the buckets up would give.
				const columnSum = toCents(
					GROSS_MARGIN_BY_MONTH.reduce((acc, [, v]) => acc + v, 0),
				);
				expect(columnSum).not.toBe(footer);

				// The decision to LABEL it "Total — value recomputed" is
				// `totalIsRecomputed`; assert the decision itself, since the label is
				// composed inline in the widget (its rendering is unit-covered in
				// drillable-grid.test.tsx).
				const { totalIsRecomputed } = await import("#/duckdb/drill");
				const { body: axes } = await postJson<AxesResponse>(
					routes.axes,
					"/api/drill/axes",
					{ metricKey: GROSS_MARGIN_METRIC },
				);
				expect(
					totalIsRecomputed(
						[{ kind: "slice", column: ENTRY_DATE_COLUMN, grain: "1M" }],
						axes.axes as never,
						axes.reconciles,
					),
				).toBe(true);
			});
		});

		// ────────────────────────────────────────────────────────────────────
		// J6 — the SAME ratio through the ANSWER path: capability parity with J5
		// ────────────────────────────────────────────────────────────────────
		describe("J6 · gross margin by month (answer path)", () => {
			const marginExpression = `(${REVENUE_FIELD} - ${COGS_FIELD}) / ${REVENUE_FIELD} * 100`;
			const partsSources = [
				axisSource(REVENUE_SOURCE),
				axisSource(COGS_SOURCE),
			];

			// FLIPPED by R2 — the parity claim, and the proof that the fix is ONE
			// resolution rather than three tolerable ones. Was a red pin: this
			// declaration is the same arithmetic over the same two carriers as the
			// metric node in J5, and the answer path withheld the month grain that
			// J5 offers, with no `reconciles` for the footer to key on.
			//
			// Nothing here is answer-path-specific: the two carriers' served
			// verdicts decide, through the same `decideTimeAxis` and the same
			// `reconciliation` J5 goes through.
			it("matches the node path's capability, carrier for carrier", async () => {
				const { body } = await postJson<AxesResponse>(
					routes.axes,
					"/api/drill/axes",
					{ partsSources },
				);
				const date = axisFor(body, ENTRY_DATE_COLUMN);
				expect(date?.temporal).toBe("date");
				expect(date?.bucketGrain).toBe("month");
				// J5 asserts EXACTLY this for the metric node. Two classified
				// concepts combined by the answer's own arithmetic recompute per
				// bucket, so the buckets are honest and the total is not their sum.
				expect(body.reconciles).toEqual({
					time: false,
					categorical: false,
				});

				const res = await postJson<ComposeResponse>(
					routes.parts,
					"/api/drill/parts",
					{
						sources: [REVENUE_SOURCE, COGS_SOURCE],
						expression: marginExpression,
						steps: [{ kind: "slice", column: ENTRY_DATE_COLUMN, grain: "1M" }],
					},
				);
				composed(res.body);
				const rows = await runSql(res.body.sql ?? "", res.body.params);
				expect(rows).toHaveLength(GROSS_MARGIN_BY_MONTH.length);
				// The footer prints the RECOMPUTED whole-ledger margin — the headline
				// the practitioner started from, not the sum of the twelve buckets
				// (J5's node-path assertion, reached through the answer path).
				expect(valueCell(await runSql(res.body.totals?.sql ?? ""))).toBe(
					GROSS_MARGIN_PCT,
				);
			});

			// The ANSWER-path arithmetic is right when it is allowed to run: the
			// same declaration composed WITHOUT a grain reproduces the headline. So
			// the pin above is a withheld capability, not a wrong number.
			it("the same declaration reproduces the headline undrilled", async () => {
				const rows = await runSql(
					`SELECT (SUM(CASE WHEN ${REVENUE_PREDICATE} THEN credit ELSE 0 END)
					        - SUM(CASE WHEN ${COGS_PREDICATE} THEN debit ELSE 0 END))
					        / NULLIF(SUM(CASE WHEN ${REVENUE_PREDICATE} THEN credit ELSE 0 END), 0)
					        * 100 AS value
					   FROM ${QUALIFIED_RELATION}`,
				);
				expect(scalar(rows)).toBe(GROSS_MARGIN_PCT);
			});
		});

		// ────────────────────────────────────────────────────────────────────
		// J7 — an aliased projection keeps its drill affordance
		// ────────────────────────────────────────────────────────────────────
		describe("J7 · aliased projection", () => {
			const aliasedSql =
				`SELECT ${ACCOUNT_NAME_COLUMN} AS account, SUM(credit) AS value ` +
				`FROM ${QUALIFIED_RELATION} WHERE ${REVENUE_PREDICATE} GROUP BY 1`;

			it("the aliased result is correct", async () => {
				const rows = await runSql(aliasedSql);
				expect(rows).toHaveLength(REVENUE_BY_ACCOUNT.length);
				expect(Object.keys(rows[0])).toContain("account");
			});

			// FLIPPED by R2 (was a red pin: the catalog holds `account_id__name`,
			// the result projects `account`, nothing reconciled the two — so a
			// result that visibly HAS a dimension in it reported nothing to slice
			// by, with the reason "None of this result's columns is a catalogued
			// dimension").
			it("the alias resolves back to the catalogued dimension", async () => {
				const { body } = await postJson<AxesResponse>(
					routes.axes,
					"/api/drill/axes",
					{ resultSql: aliasedSql },
				);
				// Offered in the RESULT's spelling — that is what the practitioner
				// sees and what a further compose must name (tier A can only group by
				// a column the result projects).
				expect(axisFor(body, "account")).toBeDefined();
				// …and NOT under the catalog's spelling, which this result does not
				// project: offering `account_id__name` here would name a column the
				// compose would then fail to bind.
				expect(axisFor(body, ACCOUNT_NAME_COLUMN)).toBeUndefined();
				// The dimension's curation came with it — the point of resolving the
				// alias rather than offering a bare name.
				expect(axisFor(body, "account")?.disabledReason).toEqual(
					expect.any(String),
				);
				// Already broken out by this very result (GROUP BY 1), so it is
				// offered DISABLED with a reason rather than as a fresh slice.
				expect(axisFor(body, "account")?.disabledReason).toContain("already");
			});
		});

		// ────────────────────────────────────────────────────────────────────
		// J8 — the ONLY legitimate withhold
		// ────────────────────────────────────────────────────────────────────
		describe("J8 · a genuinely unclassified concept", () => {
			// Net movement per cost centre: a real question, but not a concept the
			// engine has classified — no metric node, no standard field, no
			// additivity verdict anywhere. So the declaration names NO grounding
			// (`snippetId` absent, the shape a FRESH step produces), which is
			// precisely why it has no verdict to read: identity is what the drill
			// spends, and this answer has none to spend.
			const adHocSource = {
				selectExpr:
					"CASE WHEN COUNT(*) = 0 THEN NULL ELSE SUM(debit) - SUM(credit) END",
				relation: QUALIFIED_RELATION,
				where: [`${COST_CENTER_COLUMN} IS NOT NULL`],
			};

			it("still offers its dimensions — an unclassified measure is not an unusable one", async () => {
				const { body } = await postJson<AxesResponse>(
					routes.axes,
					"/api/drill/axes",
					{ partsSources: [adHocSource] },
				);
				expect(columnsOf(body)).toContain(ACCOUNT_NAME_COLUMN);
			});

			it("withholds the time grain WITH a stated reason, and keeps the raw date", async () => {
				const { body } = await postJson<AxesResponse>(
					routes.axes,
					"/api/drill/axes",
					{ partsSources: [adHocSource] },
				);
				const date = axisFor(body, ENTRY_DATE_COLUMN);
				expect(date).toBeDefined();
				// Withheld…
				expect(date?.temporal).toBeNull();
				expect(date?.bucketGrain).toBeUndefined();
				// …but SAID SO. A silent absence is the failure mode this whole
				// suite exists to catch; the reason is the assertion.
				expect(date?.temporalWithheldReason).toEqual(expect.any(String));
				expect(date?.temporalWithheldReason).toContain(
					"has not classified for additivity",
				);
				expect(body.temporalGateSource).toBe("withheld-no-verdict");
				// The date is still there to slice on raw — withholding the GRAIN is
				// not the same as withholding the column.
				expect(columnsOf(body)).toContain(ENTRY_DATE_COLUMN);
			});

			// WHAT THIS JOURNEY IS NOW WORTH — R1 wrote it for exactly this moment.
			//
			// Before R2 this assertion proved nothing about discrimination: the
			// answer path emitted the same withholding sentence for EVERY request,
			// classified or not, because it never read a verdict. J8 passing only
			// meant the sentence was worded honestly.
			//
			// Now J1 and J6 offer a month grain — their concepts ARE classified —
			// and J8 must still withhold, because its concept genuinely is not. So
			// this is the assertion standing between "consults the verdict" and
			// "offers a grain for everything", a regression that would otherwise
			// turn every green journey above into a lie. Kept standalone for that
			// job, never folded into J1.
			it("is the only journey that SHOULD still withhold", async () => {
				const { body } = await postJson<AxesResponse>(
					routes.axes,
					"/api/drill/axes",
					{ partsSources: [adHocSource] },
				);
				expect(axisFor(body, ENTRY_DATE_COLUMN)?.temporal).toBeNull();
			});
		});
	},
);

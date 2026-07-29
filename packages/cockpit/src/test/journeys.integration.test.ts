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
// RED PINS. Several journeys describe behaviour the product does not have yet.
// Each is a PAIR:
//   · a green `it(...)` pinning TODAY's behaviour exactly — the value, the
//     status code, the reason string;
//   · an `it.fails(...)` asserting the TARGET, which vitest reports as passing
//     while it throws and FAILS THE RUN the moment it starts succeeding.
// So R2 cannot land the fix silently: the pin goes red and must be flipped by
// deleting `.fails`. The paired green test is what stops `.fails` from hiding a
// broken fixture — if seeding breaks, the green half fails first and loudly.
//
// This file must not need product changes. Where a journey cannot be expressed
// without one, that IS the finding, and it is recorded as a pin.

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
	COST_CENTER_COLUMN,
	ENTRY_DATE_COLUMN,
	GROSS_MARGIN_METRIC,
	REVENUE_FIELD,
	REVENUE_PREDICATE,
	REVENUE_SELECT_EXPR,
} from "./seed-journey";

const jx = attachJourneyWorkspace();

/** The qualified spelling a model actually authors. Kept qualified on purpose:
 *  reducing it is the DAT-671 fix the answer path depends on, so every answer
 *  journey exercises it rather than handing the resolver a pre-reduced name. */
const QUALIFIED_RELATION = `lake.typed.${JOURNEY_RELATION}`;

/** The seeded ANSWER STATE — a proven parts-at-source declaration for "total
 *  revenue". This is what `proveAnswerSource` would have left on the canvas
 *  after a chat turn; seeding it directly keeps the suite free of any LLM call
 *  while exercising exactly the same downstream wire. */
const REVENUE_SOURCE = {
	name: REVENUE_FIELD,
	parts: {
		selectExpr: REVENUE_SELECT_EXPR,
		relation: QUALIFIED_RELATION,
		where: [REVENUE_PREDICATE],
	},
};

const COGS_SOURCE = {
	name: COGS_FIELD,
	parts: {
		selectExpr: COGS_SELECT_EXPR,
		relation: QUALIFIED_RELATION,
		where: [COGS_PREDICATE],
	},
};

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
						partsSources: [{ ...REVENUE_SOURCE.parts }],
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

			// RED PIN — CURRENT behaviour.
			it("TODAY withholds the time grain despite an additive verdict", async () => {
				const { body } = await postJson<AxesResponse>(
					routes.axes,
					"/api/drill/axes",
					{
						partsSources: [{ ...REVENUE_SOURCE.parts }],
						baseSql: REVENUE_BASE_SQL,
					},
				);
				const date = axisFor(body, ENTRY_DATE_COLUMN);
				expect(date).toBeDefined();
				// The date survives as a RAW slice, but cannot be bucketed…
				expect(date?.temporal).toBeNull();
				expect(date?.bucketGrain).toBeUndefined();
				// …and the stated reason is that no verdict exists — which is FALSE
				// here: `measure|revenue|time|*` = additive IS seeded and readable.
				// The answer path simply never consults it (`resolveAnswerDrillAxes`
				// passes `{target: null, carriers: new Map()}` unconditionally).
				expect(body.temporalGateSource).toBe("withheld-no-verdict");
				expect(date?.temporalWithheldReason).toContain(
					"has not classified for additivity",
				);
			});

			// RED PIN — TARGET behaviour. Flip by deleting `.fails` (lane R2).
			it.fails("TARGET: offers the time axis at MONTH grain, per the engine verdict", async () => {
				const { body } = await postJson<AxesResponse>(
					routes.axes,
					"/api/drill/axes",
					{
						partsSources: [{ ...REVENUE_SOURCE.parts }],
						baseSql: REVENUE_BASE_SQL,
					},
				);
				const date = axisFor(body, ENTRY_DATE_COLUMN);
				expect(date?.temporal).toBe("date");
				expect(date?.bucketGrain).toBe("month");
				expect(date?.temporalWithheldReason).toBeUndefined();
				expect(body.temporalGateSource).toBe("engine-verdict");
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
						partsSources: [{ ...REVENUE_SOURCE.parts }],
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

			// RED PIN — CURRENT behaviour. DISCOVERED while writing this suite.
			it("TODAY returns no footer total on the answer path", async () => {
				const res = await postJson<ComposeResponse>(
					routes.parts,
					"/api/drill/parts",
					slice,
				);
				// `/api/drill/node` ships a `totals` statement on its open call;
				// `/api/drill/parts` has no equivalent, and `footerCells` is supplied
				// only by the metric overlay — so a drilled ANSWER grid has no footer
				// at all, and the practitioner never sees the figure they started from.
				expect(res.body.totals).toBeUndefined();
			});

			// RED PIN — TARGET behaviour.
			it.fails("TARGET: the footer prints the undrilled total (parts reconcile additively)", async () => {
				const res = await postJson<ComposeResponse>(
					routes.parts,
					"/api/drill/parts",
					slice,
				);
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

			// RED PIN — CURRENT behaviour. DISCOVERED while writing this suite:
			// the journey is not merely wrong on the answer path, it is
			// unexpressible — the route's `z.strictObject` rejects the `grain` key
			// outright, so "revenue by month" cannot even be REQUESTED there.
			it("TODAY refuses a grained step with 400", async () => {
				const res = await post(routes.parts, "/api/drill/parts", monthSlice);
				expect(res.status).toBe(400);
				const body = (await res.json()) as { error?: string };
				expect(body.error).toEqual(expect.any(String));
			});

			// RED PIN — TARGET behaviour.
			it.fails("TARGET: buckets the year into twelve months that sum to the total", async () => {
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

			// RED PIN — CURRENT behaviour.
			it("TODAY offers no further axis on a grouped result", async () => {
				const { body } = await postJson<AxesResponse>(
					routes.axes,
					"/api/drill/axes",
					{ resultSql: groupedSql },
				);
				// Tier A can only offer COLUMNS OF THE RESULT, and the grouped
				// statement projects no date — so the month the practitioner wants
				// is not on the menu. The account column is present but already
				// broken out, which leaves nothing actionable.
				expect(axisFor(body, ENTRY_DATE_COLUMN)).toBeUndefined();
				const actionable = body.axes.filter((a) => a.disabledReason === null);
				expect(actionable).toHaveLength(0);
				// An empty menu is a CLAIM: it must say why, never render as nothing.
				const explained =
					body.reason !== undefined ||
					body.axes.every((a) => a.disabledReason !== null);
				expect(explained).toBe(true);
			});

			// RED PIN — TARGET behaviour.
			it.fails("TARGET: a grouped revenue result can still be split by month", async () => {
				const { body } = await postJson<AxesResponse>(
					routes.axes,
					"/api/drill/axes",
					{ resultSql: groupedSql },
				);
				const date = axisFor(body, ENTRY_DATE_COLUMN);
				expect(date).toBeDefined();
				expect(date?.disabledReason).toBeNull();
				expect(date?.bucketGrain).toBe("month");
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
				{ ...REVENUE_SOURCE.parts },
				{ ...COGS_SOURCE.parts },
			];

			// RED PIN — CURRENT behaviour.
			it("TODAY withholds the month grain the identical metric offers", async () => {
				const { body } = await postJson<AxesResponse>(
					routes.axes,
					"/api/drill/axes",
					{ partsSources },
				);
				const date = axisFor(body, ENTRY_DATE_COLUMN);
				expect(date).toBeDefined();
				expect(date?.temporal).toBeNull();
				expect(body.temporalGateSource).toBe("withheld-no-verdict");
				// No verdict was consulted at all, so nothing can be said about
				// reconciliation — the footer machinery has nothing to key on.
				expect(body.reconciles).toBeUndefined();
			});

			// RED PIN — TARGET behaviour: parity with J5.
			it.fails("TARGET: the answer path matches the node path's capability", async () => {
				const { body } = await postJson<AxesResponse>(
					routes.axes,
					"/api/drill/axes",
					{ partsSources },
				);
				const date = axisFor(body, ENTRY_DATE_COLUMN);
				expect(date?.temporal).toBe("date");
				expect(date?.bucketGrain).toBe("month");
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

			// RED PIN — CURRENT behaviour.
			it("TODAY loses the drill affordance behind the alias", async () => {
				const { body } = await postJson<AxesResponse>(
					routes.axes,
					"/api/drill/axes",
					{ resultSql: aliasedSql },
				);
				// The catalog holds `account_id__name`; the result projects
				// `account`. Nothing reconciles the two, so a result that visibly
				// HAS a dimension in it reports nothing to slice by.
				expect(axisFor(body, "account")).toBeUndefined();
				expect(axisFor(body, ACCOUNT_NAME_COLUMN)).toBeUndefined();
			});

			// RED PIN — TARGET behaviour.
			it.fails("TARGET: the alias resolves back to the catalogued dimension", async () => {
				const { body } = await postJson<AxesResponse>(
					routes.axes,
					"/api/drill/axes",
					{ resultSql: aliasedSql },
				);
				// Offered in the RESULT's spelling — that is what the practitioner
				// sees and what a further compose must name.
				expect(axisFor(body, "account")).toBeDefined();
			});
		});

		// ────────────────────────────────────────────────────────────────────
		// J8 — the ONLY legitimate withhold
		// ────────────────────────────────────────────────────────────────────
		describe("J8 · a genuinely unclassified concept", () => {
			// Net movement per cost centre: a real question, but not a concept the
			// engine has classified — no metric node, no standard field, no
			// additivity verdict anywhere.
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

			// WHY THIS JOURNEY IS GREEN TODAY, AND WHAT THAT IS WORTH.
			//
			// Right now this assertion is indistinguishable from J1's and J6's RED
			// pins: the answer path emits this SAME sentence for EVERY request,
			// classified or not, because it never reads a verdict. So J8 passing
			// today proves nothing about discrimination — it only proves the
			// withhold is worded honestly.
			//
			// That changes the moment R2 lands. Then J1 and J6 offer a month grain
			// (their targets ARE classified) and J8 must STILL withhold, because
			// its concept genuinely is not. At that point this test becomes the
			// only thing standing between "consults the verdict" and "offers a
			// grain for everything" — a regression that would otherwise turn every
			// green journey above into a lie. It is deliberately kept as a
			// standalone journey for that future, not folded into J1.
			it("is the only journey that SHOULD still withhold after R2", async () => {
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

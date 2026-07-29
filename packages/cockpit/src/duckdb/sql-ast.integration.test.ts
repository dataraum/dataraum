// Real-DuckDB integration: the AST read extracts the AGGREGATED base columns
// off an extract's value expression, including the engine's actual COALESCE/
// CASE-wrapped shapes (DAT-673 flow gate).

import { describe, expect, it } from "vitest";

import { REGION_NAME_COLUMN } from "#/test/seed-catalog";
import {
	aggregatedColumns,
	declaredValueExprRefusal,
	existingIdentifierColumns,
	projectedSourceColumns,
} from "./sql-ast";

describe("aggregatedColumns", () => {
	it("pulls columns from inside aggregates, ignoring bare refs", async () => {
		expect(
			[...(await aggregatedColumns("SUM(credit) - SUM(debit)"))].sort(),
		).toEqual(["credit", "debit"]);
	});

	it("handles the engine's COALESCE/CASE-wrapped extract shape", async () => {
		const expr =
			"CASE WHEN COUNT(*) = 0 THEN NULL ELSE COALESCE(SUM(credit), 0) - COALESCE(SUM(debit), 0) END";
		expect([...(await aggregatedColumns(expr))].sort()).toEqual([
			"credit",
			"debit",
		]);
	});

	it("reads a balance (stock) expression's column", async () => {
		expect(
			[
				...(await aggregatedColumns(
					"SUM(debit_balance) - SUM(credit_balance)",
				)),
			].sort(),
		).toEqual(["credit_balance", "debit_balance"]);
	});

	it("ignores a column referenced only OUTSIDE an aggregate", async () => {
		// `rate` scales the aggregate but is not itself aggregated — not a measure.
		expect([...(await aggregatedColumns("SUM(amount) * rate"))]).toEqual([
			"amount",
		]);
	});

	it("drops table qualification to the bare column", async () => {
		expect([...(await aggregatedColumns("SUM(t.credit)"))]).toEqual(["credit"]);
	});

	it("returns empty for an unparseable expression", async () => {
		expect((await aggregatedColumns("this is not sql )(")).size).toBe(0);
	});

	// --- window + FILTER grammar (DAT-868, closing the DAT-715 residue) --------
	// These used to fail closed: ANY window node returned the empty set, so a
	// windowed measure was invisible to the unit gate.

	it("reads a windowed aggregate's argument — WINDOW carries the same function_name/children as FUNCTION", async () => {
		expect([
			...(await aggregatedColumns("SUM(x) OVER (PARTITION BY y)")),
		]).toEqual(["x"]);
	});

	it("excludes the window frame's PARTITION BY / ORDER BY — they group and order, they are not aggregated", async () => {
		expect(
			[
				...(await aggregatedColumns(
					"SUM(credit) OVER (PARTITION BY acct ORDER BY booked_on)",
				)),
			].sort(),
		).toEqual(["credit"]);
	});

	it("excludes a FILTER predicate's columns — they restrict the rows, they are not the measure", async () => {
		// The old blind descent collected `flag` here: an over-collection that
		// handed the unit gate a column no measure ever summed.
		expect(
			[
				...(await aggregatedColumns("SUM(credit) FILTER (WHERE flag > 0)")),
			].sort(),
		).toEqual(["credit"]);
	});

	it("excludes a FILTER predicate on COUNT(*), which aggregates nothing at all", async () => {
		expect(
			(await aggregatedColumns("COUNT(*) FILTER (WHERE region = 'x')")).size,
		).toBe(0);
	});

	it("still finds a genuine aggregate nested inside a FILTER predicate", async () => {
		// Descending the predicate OUTSIDE the aggregate does not blind us to an
		// aggregate that sits there on its own node.
		expect(
			[
				...(await aggregatedColumns(
					"SUM(credit) FILTER (WHERE debit > (SELECT SUM(fee) FROM f))",
				)),
			].sort(),
		).toEqual(["credit", "fee"]);
	});

	it("reads row_number() as aggregating nothing — it takes no column argument", async () => {
		// NB: `row_number` IS `function_type='aggregate'` in duckdb_functions(); it
		// contributes nothing because it has no column ARGUMENTS, not because it
		// fails the name check.
		expect(
			(await aggregatedColumns("row_number() OVER (ORDER BY booked_on)")).size,
		).toBe(0);
	});

	it("collects a NAVIGATION function's measure argument — the unit gate wants it", async () => {
		// The rationale above, pinned: duckdb_functions() classifies `lead` as an
		// aggregate, so its argument is collected. A windowed read of a mixed-unit
		// measure is a real cross-unit finding, and the ORDER BY key is not.
		expect(
			[
				...(await aggregatedColumns(
					"lead(amount, 1, 0) OVER (PARTITION BY acct ORDER BY booked_on)",
				)),
			].sort(),
		).toEqual(["amount"]);
	});

	it("matches a QUOTED, mixed-case aggregate name — the lowercase fold is load-bearing", async () => {
		// DuckDB lowercases unquoted identifiers, but a quoted `"Sum"` keeps its
		// case into the AST while the catalog holds `sum` (DAT-868).
		expect([...(await aggregatedColumns('"Sum"(credit)'))]).toEqual(["credit"]);
	});

	it("reads a windowed aggregate whose FILTER and frame both carry columns", async () => {
		expect(
			[
				...(await aggregatedColumns(
					"SUM(amount) FILTER (WHERE status = 'posted') OVER (PARTITION BY acct ORDER BY d)",
				)),
			].sort(),
		).toEqual(["amount"]);
	});
});

// The declared-value-expression acceptance gate (DAT-671). Every shape below
// PARSES on its own — that is the point: DuckDB accepts `SUM(x) GROUP BY y` as
// a statement, so leaving these to the parser means they reach the composer,
// die in the binder, and get swallowed by the proof as a silent tier-A
// downgrade. The gate is what makes the failure named and repairable.
describe("declaredValueExprRefusal", () => {
	it("accepts the shapes a correct answer actually declares", async () => {
		for (const expr of [
			"SUM(amount)",
			'SUM("Betrag")',
			"SUM(credit) - SUM(debit)",
			// The house empty-aggregation rule — three aggregate calls, the normal
			// form of a real scalar, and the shape the prompt carve-out invites.
			"CASE WHEN COUNT(*) = 0 THEN NULL ELSE COALESCE(SUM(credit), 0) - COALESCE(SUM(debit), 0) END",
			// An aggregate FILTER is part of the value, not a smuggled clause.
			"SUM(amount) FILTER (WHERE posted)",
		]) {
			expect(await declaredValueExprRefusal(expr)).toBeNull();
		}
	});

	it("names the alias — the silent double-AS parse error", async () => {
		const why = await declaredValueExprRefusal("SUM(amount) AS revenue");
		expect(why).toContain("AS revenue");
	});

	it("refuses every clause a SELECT can smuggle past the projection", async () => {
		const cases: [string, RegExp][] = [
			["SUM(x) FROM orders", /FROM clause/],
			["SUM(x) FROM orders WHERE fy = 2024", /FROM clause/],
			["SUM(x) GROUP BY region", /GROUP BY/],
			["SUM(x) HAVING SUM(x) > 1", /HAVING/],
			["SUM(x) QUALIFY ROW_NUMBER() OVER () = 1", /QUALIFY/],
			["SUM(x) USING SAMPLE 10%", /SAMPLE/],
			["SUM(x) ORDER BY 1", /ORDER BY\/LIMIT/],
			["SUM(x) LIMIT 1", /ORDER BY\/LIMIT/],
		];
		for (const [expr, expected] of cases) {
			expect(await declaredValueExprRefusal(expr), expr).toMatch(expected);
		}
	});

	it("refuses a projection that is not ONE value", async () => {
		expect(await declaredValueExprRefusal("SUM(a), SUM(b)")).toMatch(
			/projects 2 values/,
		);
	});

	it("refuses an expression that is not valid SQL, quoting the parser", async () => {
		const why = await declaredValueExprRefusal("SUM(x");
		expect(why).toMatch(/not a valid SQL expression/);
		expect(why).toMatch(/syntax error/i);
	});

	it("refuses a second statement rather than composing the first", async () => {
		// `;` cannot smuggle a second statement in: json_serialize_sql refuses a
		// non-SELECT outright, and the gate reports it rather than passing it on.
		expect(await declaredValueExprRefusal("1; DROP TABLE orders")).toBeTruthy();
	});
});

// DAT-671 slice-menu curation: the one-hop structural read of a base
// statement's own outer GROUP BY / projection, deciding which of a result's
// columns are ALREADY non-measure identifiers (already-broken-out dimensions)
// rather than fresh candidates to slice by.
describe("existingIdentifierColumns", () => {
	it("reads a plain GROUP BY column, production-shaped (enriched view + aliased measure)", async () => {
		const names = await existingIdentifierColumns(
			"SELECT account_id__name, SUM(total_amount) AS revenue " +
				"FROM lake.typed.current_orders_enriched GROUP BY account_id__name",
		);
		expect(names).toEqual(new Set(["account_id__name"]));
	});

	it("resolves an ordinal GROUP BY position against the projection", async () => {
		const names = await existingIdentifierColumns(
			"SELECT account_id__name, SUM(total_amount) AS revenue " +
				"FROM lake.typed.current_orders_enriched GROUP BY 1",
		);
		expect(names).toEqual(new Set(["account_id__name"]));
	});

	it("treats every bare projected column as grouped under GROUP BY ALL", async () => {
		const names = await existingIdentifierColumns(
			"SELECT account_id__name, region_id__name, SUM(total_amount) AS revenue " +
				"FROM lake.typed.current_orders_enriched GROUP BY ALL",
		);
		expect(names).toEqual(new Set(["account_id__name", "region_id__name"]));
	});

	it("returns an EMPTY set for an ungrouped (raw detail) statement — nothing sliced yet", async () => {
		const names = await existingIdentifierColumns(
			"SELECT account_id__name, total_amount FROM lake.typed.current_orders_enriched",
		);
		expect(names).toEqual(new Set());
	});

	it("reads the outer GROUP BY over a pass-through CTE", async () => {
		const names = await existingIdentifierColumns(
			"WITH base AS (SELECT account_id__name, total_amount FROM lake.typed.current_orders_enriched) " +
				"SELECT account_id__name, SUM(total_amount) AS revenue FROM base GROUP BY account_id__name",
		);
		expect(names).toEqual(new Set(["account_id__name"]));
	});

	// DAT-671 R6. Every entry here is a SPELLING of the same already-broken-out
	// column: tier A names an axis by the result's (`account`), the parts and
	// node paths by the catalog's (`account_id__name`), and one set greys the
	// column on both paths rather than only where the two happen to coincide.
	it("returns BOTH spellings when the grouping column was aliased", async () => {
		const names = await existingIdentifierColumns(
			"SELECT account_id__name AS account, SUM(total_amount) AS revenue " +
				"FROM lake.typed.current_orders_enriched GROUP BY 1",
		);
		expect(names).toEqual(new Set(["account", "account_id__name"]));
	});

	// Senior review, DAT-671 R6. The other spellings must come from the item that
	// ESTABLISHED the grain, never from an item whose own source name collides
	// with it: here item0 groups the column `a` under the name `b`, while item1
	// projects the DIFFERENT column `b` under the name `c` and is not grouped at
	// all. Greying `c` would disable a real axis with the words "already breaks
	// out the result".
	it("does not grey a second column whose NAME collides with the grouped one's alias", async () => {
		expect(
			await existingIdentifierColumns(
				"SELECT a AS b, b AS c FROM lake.typed.current_orders_enriched GROUP BY 1",
			),
		).toEqual(new Set(["b", "a"]));
	});

	it("greys both when both are grouped, in both spellings", async () => {
		expect(
			await existingIdentifierColumns(
				"SELECT a AS b, b AS c FROM lake.typed.current_orders_enriched GROUP BY 1, 2",
			),
		).toEqual(new Set(["b", "a", "c"]));
	});

	it("resolves an aliased grouping column named by its SOURCE in the GROUP BY", async () => {
		// `GROUP BY account_id__name` over `… AS account`: the grouping names the
		// base column, the result column carries the alias. Both are the same
		// column, and greying must not depend on which one the query wrote.
		const names = await existingIdentifierColumns(
			"SELECT account_id__name AS account, SUM(total_amount) AS revenue " +
				"FROM lake.typed.current_orders_enriched GROUP BY account_id__name",
		);
		expect(names).toEqual(new Set(["account", "account_id__name"]));
	});

	// The common agent-authored shape: the aggregate lives INSIDE the CTE and the
	// outer statement only re-projects it. Read on the outer node alone this is
	// an ungrouped detail result, and `account` would be offered as a fresh slice
	// of a result that is already one row per account.
	describe("the CTE the statement itself declares (DAT-671 R6)", () => {
		const groupedCte =
			"WITH revenue AS (SELECT account_id__name AS account, SUM(total_amount) AS total " +
			"FROM lake.typed.current_orders_enriched GROUP BY 1) ";

		it("inherits a grouped CTE's grain through a pass-through projection", async () => {
			const names = await existingIdentifierColumns(
				`${groupedCte}SELECT account, total FROM revenue ORDER BY total DESC`,
			);
			expect(names).toEqual(new Set(["account", "account_id__name"]));
		});

		it("inherits it through a bare star as well", async () => {
			const names = await existingIdentifierColumns(
				`${groupedCte}SELECT * FROM revenue`,
			);
			expect(names).toEqual(new Set(["account", "account_id__name"]));
		});

		// Strict review, DAT-671 R6: dropping a grain column COLLAPSES the rows,
		// so the CTE's grain is no longer the result's. Both spellings of that
		// mistake — the explicit de-duplication and the implicit one — must
		// refuse the hop rather than grey an axis that genuinely still splits
		// the result.
		it("does NOT inherit a grain the projection collapsed", async () => {
			const twoKeyCte =
				"WITH revenue AS (SELECT account_id__name AS account, region_id__name AS region, " +
				"SUM(total_amount) AS total FROM lake.typed.current_orders_enriched GROUP BY 1, 2) ";
			expect(
				await existingIdentifierColumns(
					`${twoKeyCte}SELECT DISTINCT region FROM revenue`,
				),
			).toEqual(new Set());
			expect(
				await existingIdentifierColumns(
					`${twoKeyCte}SELECT region, total FROM revenue`,
				),
			).toEqual(new Set());
			// …and inherits it when the projection carries BOTH grain columns.
			expect(
				await existingIdentifierColumns(
					`${twoKeyCte}SELECT account, region, total FROM revenue`,
				),
			).toEqual(
				new Set(["account", "account_id__name", "region", "region_id__name"]),
			);
		});

		it("does NOT inherit it through a computed projection — that may be a rollup", async () => {
			// `SELECT SUM(total) FROM revenue` is one scalar row, broken out by
			// nothing. A parse tree cannot tell an aggregate from a scalar function
			// without bind-time classification, so any computed item stops the hop.
			expect(
				await existingIdentifierColumns(
					`${groupedCte}SELECT SUM(total) AS total FROM revenue`,
				),
			).toEqual(new Set());
			expect(
				await existingIdentifierColumns(
					`${groupedCte}SELECT account, total * 2 AS doubled FROM revenue`,
				),
			).toEqual(new Set());
		});

		it("does NOT inherit across a join — which relation the grain came from is unproven", async () => {
			expect(
				await existingIdentifierColumns(
					`${groupedCte}, budget AS (SELECT account_id__name AS account, 1 AS plan ` +
						"FROM lake.typed.current_orders_enriched GROUP BY 1) " +
						"SELECT r.account, r.total, b.plan FROM revenue r JOIN budget b USING (account)",
				),
			).toEqual(new Set());
		});

		it("keeps the outer GROUP BY when the outer statement has one of its own", async () => {
			// Aggregate outside, rename inside: the outer grouping decides, and the
			// alias still resolves down to the catalogued column.
			const names = await existingIdentifierColumns(
				"WITH base AS (SELECT account_id__name AS account, total_amount " +
					"FROM lake.typed.current_orders_enriched) " +
					"SELECT account, SUM(total_amount) AS value FROM base GROUP BY 1",
			);
			expect(names).toEqual(new Set(["account", "account_id__name"]));
		});

		it("follows a chain of CTEs, each renaming again", async () => {
			const names = await existingIdentifierColumns(
				"WITH a AS (SELECT account_id__name AS acct FROM lake.typed.current_orders_enriched), " +
					"b AS (SELECT acct AS account FROM a) " +
					"SELECT account, COUNT(*) AS n FROM b GROUP BY 1",
			);
			expect(names).toEqual(new Set(["account", "acct", "account_id__name"]));
		});
	});

	it("returns null for a set operation — not a single plain SELECT", async () => {
		const names = await existingIdentifierColumns(
			"SELECT account_id__name FROM a GROUP BY account_id__name " +
				"UNION SELECT region_id__name FROM b GROUP BY region_id__name",
		);
		expect(names).toBeNull();
	});

	it("returns null for more than one statement", async () => {
		const names = await existingIdentifierColumns(
			"SELECT a FROM t GROUP BY a; SELECT b FROM u GROUP BY b",
		);
		expect(names).toBeNull();
	});

	it("returns null for unparseable SQL rather than guessing", async () => {
		expect(await existingIdentifierColumns("this is not sql )(")).toBeNull();
	});

	// DAT-671 review round — "the rename-wrap blind spot": a MINTED child
	// report's stored SQL is exactly `SELECT * RENAME (...) FROM (<the real,
	// grouped statement>) AS _clean` (drill-sql.ts's composeDrill, DAT-671
	// drilled-projection hygiene). Read naively, that OUTER node has no GROUP
	// BY of its own, so the un-hopped read would wrongly call it ungrouped and
	// re-offer the child's own grain as a fresh, enabled slice.
	describe("the star-wrapper one-hop unwrap (OUR OWN generated shape only)", () => {
		it("hops through a RENAME wrap to find the inner GROUP BY", async () => {
			const names = await existingIdentifierColumns(
				'SELECT * RENAME ("sum(count)" AS "count") FROM ' +
					`(SELECT ${REGION_NAME_COLUMN}, SUM(total_amount) AS "sum(count)" ` +
					`FROM lake.typed.current_orders_enriched GROUP BY ${REGION_NAME_COLUMN}) AS _clean`,
			);
			expect(names).toEqual(new Set([REGION_NAME_COLUMN]));
		});

		it("hops through an EXCLUDE wrap the same way", async () => {
			const names = await existingIdentifierColumns(
				`SELECT * EXCLUDE (junk) FROM (SELECT ${REGION_NAME_COLUMN}, junk, ` +
					`SUM(total_amount) AS revenue FROM lake.typed.current_orders_enriched ` +
					`GROUP BY ${REGION_NAME_COLUMN}, junk) AS _clean`,
			);
			expect(names).toEqual(new Set([REGION_NAME_COLUMN, "junk"]));
		});

		it("does NOT hop through a bare `SELECT * FROM (subquery)` with no RENAME/EXCLUDE — that is not our generated shape", async () => {
			// Reads as an ordinary (ungrouped, at THIS level) statement instead —
			// the outer node genuinely has no GROUP BY of its own, and this shape
			// is not one composeDrill ever emits, so there is nothing to hop into.
			const names = await existingIdentifierColumns(
				`SELECT * FROM (SELECT ${REGION_NAME_COLUMN}, SUM(total_amount) AS revenue ` +
					`FROM lake.typed.current_orders_enriched GROUP BY ${REGION_NAME_COLUMN}) AS _x`,
			);
			expect(names).toEqual(new Set());
		});

		it("returns null (undecided) for a wrap-of-a-wrap — still ONE hop, never a second", async () => {
			const names = await existingIdentifierColumns(
				"SELECT * RENAME (x AS y) FROM (SELECT * RENAME (a AS b) FROM " +
					`(SELECT ${REGION_NAME_COLUMN} FROM lake.typed.current_orders_enriched ` +
					`GROUP BY ${REGION_NAME_COLUMN}) AS inner1) AS outer1`,
			);
			expect(names).toBeNull();
		});

		it("does not hop when the FROM is a bare table, not a subquery", async () => {
			const names = await existingIdentifierColumns(
				"SELECT * RENAME (a AS b) FROM lake.typed.current_orders_enriched",
			);
			expect(names).toEqual(new Set());
		});
	});
});

// DAT-671 R2: what each projected column is a projection OF. The tier-A drill
// matches result columns against the slice catalog, so an alias between the two
// silently cost an aliased result its entire drill menu.
describe("projectedSourceColumns", () => {
	it("maps an aliased dimension back to its catalogued column", async () => {
		const map = await projectedSourceColumns(
			`SELECT account_id__name AS account, SUM(total_amount) AS value ` +
				"FROM lake.typed.current_orders_enriched GROUP BY 1",
		);
		expect(map.get("account")).toBe("account_id__name");
		// The computed projection has no single source column, so it is absent
		// rather than mapped to something invented.
		expect(map.has("value")).toBe(false);
	});

	it("maps an UNALIASED column to itself, so callers have one lookup", async () => {
		const map = await projectedSourceColumns(
			`SELECT ${REGION_NAME_COLUMN}, account_id__name AS account ` +
				"FROM lake.typed.current_orders_enriched",
		);
		expect(Object.fromEntries(map)).toEqual({
			[REGION_NAME_COLUMN]: REGION_NAME_COLUMN,
			account: "account_id__name",
		});
	});

	it("drops the table qualifier from a qualified reference", async () => {
		const map = await projectedSourceColumns(
			"SELECT o.account_id__name AS account FROM lake.typed.current_orders_enriched AS o",
		);
		expect(map.get("account")).toBe("account_id__name");
	});

	// DAT-671 R6 — the live "no axes" shape: the rename happens INSIDE the CTE,
	// so one hop over the outer projection resolves `account` to the CTE's own
	// output and never to the catalogued column the whole lookup keys on.
	it("resolves an alias made inside a CTE body", async () => {
		const map = await projectedSourceColumns(
			"WITH revenue AS (SELECT account_id__name AS account, SUM(total_amount) AS total " +
				"FROM lake.typed.current_orders_enriched GROUP BY 1) " +
				"SELECT account, total FROM revenue ORDER BY total DESC",
		);
		expect(map.get("account")).toBe("account_id__name");
		// `total` IS a plain column of the outer projection, but the CTE item it
		// names is an aggregate — a projection of no single column — so the chain
		// stops there and the name speaks for itself, never for `total_amount`.
		expect(map.get("total")).toBe("total");
	});

	it("follows a chain of CTEs down to the base column", async () => {
		const map = await projectedSourceColumns(
			"WITH a AS (SELECT account_id__name AS acct FROM lake.typed.current_orders_enriched), " +
				"b AS (SELECT acct AS account FROM a) SELECT account FROM b",
		);
		expect(map.get("account")).toBe("account_id__name");
	});

	it("re-aliases on top of a CTE alias", async () => {
		const map = await projectedSourceColumns(
			"WITH revenue AS (SELECT account_id__name AS account FROM lake.typed.current_orders_enriched) " +
				"SELECT account AS customer FROM revenue",
		);
		expect(map.get("customer")).toBe("account_id__name");
	});

	it("stops at a join — a bare name is not attributable to one CTE there", async () => {
		const map = await projectedSourceColumns(
			"WITH a AS (SELECT account_id__name AS account, id FROM lake.typed.current_orders_enriched), " +
				"b AS (SELECT region_id__name AS region, id FROM lake.typed.current_orders_enriched) " +
				"SELECT a.account, b.region FROM a JOIN b USING (id)",
		);
		// Each column speaks for itself, exactly as before the CTE resolution.
		expect(Object.fromEntries(map)).toEqual({
			account: "account",
			region: "region",
		});
	});

	it("reads nothing from a shape it cannot attribute", async () => {
		// A star projection names no columns; our own RENAME wrap renames what it
		// re-projects, so reading the inner list would attribute inner names to
		// outer columns that no longer carry them. Empty = the caller matches on
		// the result's own spelling, exactly as before.
		expect(
			(
				await projectedSourceColumns(
					"SELECT * FROM lake.typed.current_orders_enriched",
				)
			).size,
		).toBe(0);
		expect(
			(
				await projectedSourceColumns(
					"SELECT * RENAME (a AS b) FROM (SELECT account_id__name AS a " +
						"FROM lake.typed.current_orders_enriched) AS _clean",
				)
			).size,
		).toBe(0);
		expect((await projectedSourceColumns("NOT SQL AT ALL")).size).toBe(0);
	});
});

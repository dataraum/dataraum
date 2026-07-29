// The journey suite's ANSWER KEY — every number a practitioner journey asserts.
//
// PROVENANCE. These are not invented and not copied from a prior run. Each was
// DERIVED from the real corpus at `dataraum-testdata/output/clean/*.csv` with
// the SQL quoted beside it, and the derivable ones cross-checked against that
// corpus's own `ground_truth.yaml`. The corpus is generated with `seed: 42` and
// `strategy: clean`, so it is reproducible: re-running the quoted SQL against a
// regenerated corpus with the same seed must yield these figures again.
//
// Why an answer key at all: the DAT-671 verdict was that 2,281 unit tests pinned
// branch logic against itself while "revenue by month" failed. A test that
// asserts whatever the code returns cannot catch that. These numbers come from
// OUTSIDE the system under test, so a journey either reproduces the ledger or it
// is wrong — there is no third outcome and no assertion to quietly adjust.

/** The fact relation every journey reads, in the catalog's own spelling. */
export const JOURNEY_RELATION = "current_journal_lines_enriched";

/**
 * Total revenue — J1's headline scalar.
 *
 * Derivation (verified 2026-07-29, DuckDB 1.5.4):
 *   SELECT round(SUM(l.credit), 2)
 *     FROM journal_lines l JOIN chart_of_accounts a USING (account_id)
 *    WHERE a.account_type = 'revenue';
 *   -- 51766199.72
 *
 * Cross-check: `ground_truth.yaml` → `annual.total_revenue: 51766199.72`. The
 * corpus is single-entry-per-side here, so SUM(credit) and SUM(credit - debit)
 * agree exactly; every journal entry is `status='posted'`, so no status filter
 * changes the figure.
 */
export const TOTAL_REVENUE = 51766199.72;

/**
 * Revenue by account — J2's breakdown, descending by value.
 *
 * Derivation:
 *   SELECT a.name, round(SUM(l.credit), 2)
 *     FROM journal_lines l JOIN chart_of_accounts a USING (account_id)
 *    WHERE a.account_type = 'revenue' GROUP BY 1 ORDER BY 2 DESC;
 *
 * FIVE rows, not ten: the chart of accounts defines 10 revenue accounts but only
 * these five carry postings. That gap is deliberate in the journey — a breakdown
 * must return the accounts with ACTIVITY, not every account in the dimension.
 */
export const REVENUE_BY_ACCOUNT: readonly (readonly [string, number])[] = [
	["International Sales", 13495114.46],
	["Domestic Sales", 13164499.89],
	["Support Contracts", 13054260.62],
	["Consulting Fees", 12028512.76],
	["Interest Income", 23811.99],
] as const;

/**
 * Revenue by calendar month — J3's breakdown, ascending.
 *
 * Derivation:
 *   SELECT strftime(e.date, '%Y-%m'), round(SUM(l.credit), 2)
 *     FROM journal_lines l JOIN chart_of_accounts a USING (account_id)
 *     JOIN journal_entries e USING (entry_id)
 *    WHERE a.account_type = 'revenue' GROUP BY 1 ORDER BY 1;
 *
 * Cross-check: identical to `ground_truth.yaml` → `monthly[].revenue`, all 12.
 *
 * TWELVE buckets, and the reason matters. The ledger runs to 2026-02-11: there
 * are journal entries in 2026-01 and 2026-02, so an UNFILTERED month bucket over
 * the fact returns FOURTEEN. Those two months carry only asset/liability lines
 * (AR/AP settlement), no revenue — so the revenue measure's own predicate
 * (`account_id__account_type = 'revenue'`) is what makes the answer 12. The
 * journey asserts 12 to hold that predicate honest: a measure that silently
 * dropped its WHERE would show up here as two extra empty buckets, not as a
 * wrong total.
 */
export const REVENUE_BY_MONTH: readonly (readonly [string, number])[] = [
	["2025-01", 3590679.27],
	["2025-02", 4155219.38],
	["2025-03", 4938419.01],
	["2025-04", 4196092.68],
	["2025-05", 4143420.23],
	["2025-06", 4696167.18],
	["2025-07", 3531932.26],
	["2025-08", 3218256.91],
	["2025-09", 3581472.81],
	["2025-10", 5288340.11],
	["2025-11", 5386118.8],
	["2025-12", 5040081.08],
] as const;

/**
 * Gross margin, whole ledger, in PERCENT — J5/J6's headline and footer total.
 *
 * DERIVATION — write this down, because the number was previously uncited
 * folklore and does NOT match the obvious candidate in `ground_truth.yaml`:
 *
 *   gross margin = (revenue - cost of goods sold) / revenue * 100
 *
 *   SELECT round(100 * (
 *            SUM(CASE WHEN a.account_type = 'revenue' THEN l.credit ELSE 0 END)
 *          - SUM(CASE WHEN a.account_id = 5100      THEN l.debit  ELSE 0 END))
 *          / SUM(CASE WHEN a.account_type = 'revenue' THEN l.credit ELSE 0 END), 2)
 *     FROM journal_lines l JOIN chart_of_accounts a USING (account_id);
 *   -- 96.56
 *
 * with revenue = 51,766,199.72 and COGS = 1,782,730.64 (account 5100, "Cost of
 * Goods Sold" — the ONLY COGS account in the chart).
 *
 * The trap: `ground_truth.yaml` carries `annual.gross_profit: 28239122.13`,
 * which nets ALL operating expense accounts and yields 54.55%, not 96.56%. That
 * figure is an operating margin under a `gross_profit` name. This metric is the
 * textbook gross margin — revenue less COGS only — so it uses account 5100
 * alone. Both are defensible; they are different metrics, and only this one is
 * the 96.56 the journey asserts.
 */
export const GROSS_MARGIN_PCT = 96.56;

/**
 * Gross margin by month, in percent — J5's per-bucket RECOMPUTE.
 *
 * Derivation: the GROSS_MARGIN_PCT expression above, grouped by
 * `strftime(e.date, '%Y-%m')` over the journal_entries join.
 *
 * The point of the journey is that these do NOT sum to the total. Their sum is
 * ~1159.5; the honest whole-ledger figure is 96.56, which is why the footer must
 * print a RECOMPUTED total and say so rather than adding the column up.
 */
export const GROSS_MARGIN_BY_MONTH: readonly (readonly [string, number])[] = [
	["2025-01", 96.22],
	["2025-02", 96.96],
	["2025-03", 98.15],
	["2025-04", 97.6],
	["2025-05", 98.24],
	["2025-06", 94.76],
	["2025-07", 93.41],
	["2025-08", 94.34],
	["2025-09", 94.4],
	["2025-10", 97.8],
	["2025-11", 98.21],
	["2025-12", 96.41],
] as const;

/** Round a DuckDB DOUBLE to cents for comparison against the key. Summing
 *  doubles yields 51766199.719999996; the ledger is denominated in cents, so
 *  the comparison is made there rather than with an epsilon whose size would
 *  itself be a judgement call. */
export const toCents = (value: number): number => Math.round(value * 100) / 100;

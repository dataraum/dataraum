// Row-limit policy for the cockpit's read verbs (`run_sql`, `probe`) — DAT-384.
//
// Both verbs LIMIT-wrap their query so a caller can't pull an unbounded result.
// Two numbers govern that:
//
//   - DEFAULT_ROW_LIMIT — applied when the caller passes no `limit`. Sized for
//     the AGENT's in-context JSON sample (a few hundred/thousand rows), NOT the
//     human-facing streaming grid (that path has its own, larger default — see
//     `plans/run-sql-streaming-design.md` §5.5).
//   - HARD_ROW_CEILING — a server-side clamp applied REGARDLESS of the requested
//     size, so an agent asking for `limit: 10_000_000` still can't trigger an
//     unbounded materialization into the chat context.
//
// The clamp is pure (no DB), so it's unit-tested directly.

/** Default row cap when a caller omits `limit` — sized for an in-context sample. */
export const DEFAULT_ROW_LIMIT = 1000;

/** Absolute server-side ceiling; any larger request is clamped down to this. */
export const HARD_ROW_CEILING = 200_000;

/**
 * Resolve the effective row limit for a read verb.
 *
 * - `undefined` (or a non-finite value) → {@link DEFAULT_ROW_LIMIT}.
 * - any finite request → clamped to `[1, HARD_ROW_CEILING]` (floored to an
 *   integer; LIMIT needs a non-negative integer and `0`/negative is never a
 *   useful agent request).
 */
export function clampRowLimit(requested?: number): number {
	if (requested === undefined || !Number.isFinite(requested)) {
		return DEFAULT_ROW_LIMIT;
	}
	return Math.min(Math.max(1, Math.floor(requested)), HARD_ROW_CEILING);
}

// Pure "stick to bottom" decision for the chat rail (DAT-527). Extracted so the
// follow-the-stream rule is unit-tested without a DOM (conventions rule 10).
//
// The rail auto-scrolls to the newest content ONLY when the user is already near
// the bottom — i.e. following the stream. Scrolled further up means they're
// reading history during a streaming turn and must NOT be yanked back down (the
// bug where scrolling up mid-load snapped you to the bottom on every token tick).

/** Within this many px of the bottom still counts as "following the stream", so a
 * new message / a widget growing its height keeps the user pinned to the latest.
 * A small slack absorbs sub-pixel rounding + the last line's leading. */
export const STICK_THRESHOLD_PX = 64;

/** True when the scroll position is within `threshold` px of the bottom (the user
 * is following the stream). Pure — operates on plain scroll metrics so it tests
 * without a DOM. */
export function isNearBottom(
	metrics: { scrollTop: number; scrollHeight: number; clientHeight: number },
	threshold: number = STICK_THRESHOLD_PX,
): boolean {
	const { scrollTop, scrollHeight, clientHeight } = metrics;
	return scrollHeight - (scrollTop + clientHeight) <= threshold;
}

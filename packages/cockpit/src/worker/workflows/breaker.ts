// Circuit-breaker reducer for the JourneyWorkflow (DAT-530 P3b.2).
//
// Pure + zero-import so it is (a) safe to import into the workflow sandbox and
// (b) unit-testable WITHOUT a Temporal test server — the project deliberately
// avoids the test-server (it stalls CI); workflow determinism is covered by the
// replay fixture, and this fold is covered by plain unit tests.
//
// The breaker governs only the AUTONOMOUS cascade (begin_session →
// operating_model). After enough CONSECUTIVE stage failures it trips `autoMode`
// off so the journey stops auto-advancing into a broken engine unattended; it is
// a MANUAL-reset breaker (stays off until an explicit resume), not a half-open
// auto-recovering one — a clean run clears the tally but does not re-arm the
// cascade. User-intentional stages (an explicit begin_session / operating_model
// signal) still run while tripped; only the auto-follow-on is gated.

import type { JourneyState } from "../contracts";

/** Consecutive stage failures that trip the breaker. */
export const BREAKER_THRESHOLD = 3;

/**
 * Fold one stage outcome into the breaker state. A success clears the failure
 * tally (but does not re-arm a tripped breaker — that needs an explicit resume).
 * A failure increments the tally and trips `autoMode` off once it reaches
 * `threshold`.
 */
export function applyOutcome(
	state: JourneyState,
	succeeded: boolean,
	threshold: number = BREAKER_THRESHOLD,
): JourneyState {
	if (succeeded) {
		return { autoMode: state.autoMode, consecutiveFailures: 0 };
	}
	const consecutiveFailures = state.consecutiveFailures + 1;
	const autoMode =
		state.autoMode && consecutiveFailures >= threshold ? false : state.autoMode;
	return { autoMode, consecutiveFailures };
}

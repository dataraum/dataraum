// Pure logic for the "Needs you" inbox (DAT-553) — the Stage-chat seed prompt.
// Extracted as a .ts with its own test (cockpit React rule 10); the panel stays a
// pure render + dispatch.

/**
 * The seed message sent into a freshly-minted Stage chat when the user clicks
 * "Resolve in Stage" (DAT-553). Framed as the user's OWN request so the stage
 * agent (teach + session toolstack) opens already working the exact grounding
 * gap, and nudged toward the teach → replay resolution path. The `awaitingNote`
 * carries the engine's reason; a null/blank note degrades to a generic-but-still-
 * actionable prompt rather than an empty seed.
 */
export function resolveSeed(note: string | null): string {
	const what = note?.trim();
	return what
		? `Onboarding flagged something that needs my judgement: ${what}\n\n` +
				"Help me resolve it — propose a teach, then replay to re-check."
		: "Onboarding flagged something that needs my judgement before it can " +
				"finish grounding the data. Help me figure out the right teach, then " +
				"replay to re-check.";
}

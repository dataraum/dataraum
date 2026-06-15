// Classify a chat stream/transport error into a user-actionable message
// (DAT-512). The raw `error.message` from useChat is a provider/transport string
// — e.g. the Anthropic SDK's `400 {"type":"error","error":{"type":
// "invalid_request_error","message":"Your credit balance is too low …"}}`. The
// chat rail used to show a flat "please try again" over all of them, hiding the
// one cause the user can act on (out of credits). This narrows the known,
// recoverable-by-the-user cases and says what to DO; everything else keeps the
// generic copy.
//
// Pure (no I/O, no React) so it's unit-testable and lives off the render path
// (React-idiom rule 10). Match on the provider's stable error `type` tokens and
// unambiguous phrases — NOT bare HTTP status numbers, which collide with request
// ids and token counts in the same string.

export interface ClassifiedChatError {
	/** Alert title — the cause in a few words. */
	title: string;
	/** One sentence: what happened and what to do next. The body itself carries
	 * the retry/no-retry guidance ("Top up credits" vs "try again") — there is no
	 * separate retry affordance to gate, so no boolean is exposed. */
	body: string;
}

export function classifyChatError(message: string): ClassifiedChatError {
	const m = message.toLowerCase();

	// Billing / credit exhaustion — a 400 invalid_request_error whose message is
	// the credit phrase. Retrying changes nothing; the user must top up. Match the
	// SDK's exact, stable phrase only — a bare "billing" substring would catch
	// unrelated errors that happen to mention the word.
	if (m.includes("credit balance is too low")) {
		return {
			title: "Out of API credits",
			body: "The Anthropic API credit balance is too low to run the assistant. Top up credits in Plans & Billing, then try again.",
		};
	}

	// Auth — the key was rejected. A server-config problem, not a user retry.
	if (
		m.includes("authentication_error") ||
		m.includes("invalid x-api-key") ||
		m.includes("x-api-key")
	) {
		return {
			title: "API key rejected",
			body: "The Anthropic API key was rejected. Check the server's ANTHROPIC_API_KEY configuration.",
		};
	}

	// Rate limited — transient; a short wait then retry is the right move.
	if (
		m.includes("rate_limit") ||
		m.includes("rate limit") ||
		m.includes("too many requests")
	) {
		return {
			title: "Rate limited",
			body: "Too many requests in a short window. Wait a few seconds, then try again.",
		};
	}

	// Anthropic-side overload — transient; give it a moment.
	if (m.includes("overloaded")) {
		return {
			title: "Service busy",
			body: "Anthropic is temporarily overloaded. Give it a moment and try again.",
		};
	}

	// Unknown — keep the generic, retry-safe copy.
	return {
		title: "Something went wrong",
		body: "The assistant couldn't finish that — please try again.",
	};
}

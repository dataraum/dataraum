import { describe, expect, it } from "vitest";
import { classifyChatError } from "#/lib/chat-error";

describe("classifyChatError (DAT-512)", () => {
	it("surfaces credit exhaustion as actionable, naming what to do", () => {
		// The real Anthropic 400 the live smoke hit.
		const e = classifyChatError(
			'400 {"type":"error","error":{"type":"invalid_request_error","message":"Your credit balance is too low to access the Anthropic API. Please go to Plans & Billing to upgrade or purchase credits."}}',
		);
		expect(e.title).toBe("Out of API credits");
		expect(e.body).toContain("Plans & Billing");
	});

	it("classifies a rejected API key as a config problem", () => {
		const e = classifyChatError(
			'401 {"type":"error","error":{"type":"authentication_error","message":"invalid x-api-key"}}',
		);
		expect(e.title).toBe("API key rejected");
	});

	it("classifies rate limiting as transient", () => {
		const e = classifyChatError(
			'429 {"type":"error","error":{"type":"rate_limit_error","message":"Number of requests has exceeded your rate limit"}}',
		);
		expect(e.title).toBe("Rate limited");
	});

	it("classifies overload as transient", () => {
		const e = classifyChatError(
			'529 {"type":"error","error":{"type":"overloaded_error","message":"Overloaded"}}',
		);
		expect(e.title).toBe("Service busy");
	});

	it("falls back to the generic message for unknown errors", () => {
		const e = classifyChatError("network error: stream closed");
		expect(e.title).toBe("Something went wrong");
		expect(e.body).toContain("please try again");
	});

	it("does not match a bare 'billing' substring (over-broad anchor removed)", () => {
		// Only the SDK's exact credit phrase classifies as out-of-credits — an
		// unrelated error that merely mentions billing must fall through.
		const e = classifyChatError(
			"failed to validate billing address column in customers",
		);
		expect(e.title).toBe("Something went wrong");
	});

	it("is case-insensitive on the matched tokens", () => {
		const e = classifyChatError("Your Credit Balance Is Too Low");
		expect(e.title).toBe("Out of API credits");
	});

	it("does not misclassify a generic message that merely contains a 3-digit number", () => {
		// Guards the decision to match type tokens/phrases, not bare HTTP codes:
		// a token count of 429 in an otherwise-unknown error must NOT read as a
		// rate limit.
		const e = classifyChatError("request used 4290 tokens and then failed");
		expect(e.title).toBe("Something went wrong");
	});
});

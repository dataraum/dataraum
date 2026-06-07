// Unit tests for the abort bridge (DAT-449) — the seam that makes a user
// stop() reach the nested synthesis chat() calls (frame induction, why_*
// narratives) instead of letting them run — and bill — to completion.

import { describe, expect, it } from "vitest";

import { linkedAbortController } from "#/lib/abort";

describe("linkedAbortController (DAT-449)", () => {
	it("returns undefined for no signal — absent context changes nothing", () => {
		expect(linkedAbortController(undefined)).toBeUndefined();
	});

	it("returns a live controller for a live signal", () => {
		const source = new AbortController();
		const linked = linkedAbortController(source.signal);
		expect(linked).toBeDefined();
		expect(linked?.signal.aborted).toBe(false);
	});

	it("aborting the source aborts the linked controller, reason propagated", () => {
		const source = new AbortController();
		const linked = linkedAbortController(source.signal);
		const reason = new Error("user stopped the run");
		source.abort(reason);
		expect(linked?.signal.aborted).toBe(true);
		expect(linked?.signal.reason).toBe(reason);
	});

	it("a signal that ALREADY aborted yields an already-aborted controller", () => {
		// The tool can be invoked after the run was stopped (the loop drains
		// in-flight work) — the nested call must see the abort immediately, not
		// only on a future event.
		const source = new AbortController();
		const reason = new Error("aborted before the tool ran");
		source.abort(reason);
		const linked = linkedAbortController(source.signal);
		expect(linked?.signal.aborted).toBe(true);
		expect(linked?.signal.reason).toBe(reason);
	});

	it("the link is one-way: aborting the linked controller leaves the source alone", () => {
		const source = new AbortController();
		const linked = linkedAbortController(source.signal);
		linked?.abort();
		expect(source.signal.aborted).toBe(false);
	});
});

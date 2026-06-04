// @vitest-environment node
//
// Regression guard. markdown.tsx sits in the SSR module graph (chat-rail →
// cockpit-view → the cockpit route), so it is EVALUATED on the server. Its
// module-level setup must not call DOM/window APIs unguarded — a module-level
// `DOMPurify.addHook(...)` once did, and DOMPurify's no-DOM stub has no addHook,
// so importing the module crashed the SSR render (HTTP 200 but a broken cockpit;
// caught only by a real browser smoke, not by the jsdom/happy-dom unit tests
// which always have a window). This test imports it with NO DOM and asserts the
// import resolves.

import { describe, expect, it } from "vitest";

describe("markdown SSR-safety", () => {
	it("imports cleanly with no DOM (window undefined) — never crashes the server", async () => {
		expect(typeof globalThis.window).toBe("undefined");
		const mod = await import("#/ui/cockpit/markdown");
		expect(mod.MarkdownMessage).toBeDefined();
	});
});

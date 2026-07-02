// Lockfile guard: @codemirror/* must resolve to ONE copy each.
//
// CodeMirror compares editor internals with instanceof, so two installed
// copies of @codemirror/state (or view/language) on one page break every
// extension set with "Unrecognized extension value in extension set" — the
// 2026-07-02 staging-hub crash. Bun writes scoped overrides like
// "@codemirror/autocomplete/@codemirror/state" into bun.lock during partial
// dependency refreshes and never garbage-collects them, so the duplication
// survives every later `bun install` / `bun update`. If this fails: delete
// the scoped @codemirror entries from bun.lock and reinstall (see PR #429).

import { readFileSync } from "node:fs";
import { fileURLToPath } from "node:url";
import { describe, expect, it } from "vitest";

const lock = readFileSync(
	fileURLToPath(new URL("../bun.lock", import.meta.url)),
	"utf8",
);

describe("codemirror lockfile singletons", () => {
	it("has no scoped @codemirror/* overrides", () => {
		const scoped = lock.match(/"[^"\n]+\/@codemirror\/[^"\n]+"\s*:/g) ?? [];
		expect(scoped).toEqual([]);
	});

	it.each([
		"state",
		"view",
		"language",
	])("resolves exactly one version of @codemirror/%s", (pkg) => {
		const versions = new Set(
			[...lock.matchAll(new RegExp(`"@codemirror/${pkg}@([^"]+)"`, "g"))].map(
				(m) => m[1],
			),
		);
		expect([...versions]).toHaveLength(1);
	});
});

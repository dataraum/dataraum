// Integration test for the frame-time LIBRARY-seed path (DAT-881/882 rework):
// `nearestSeedVertical` driven by the REAL `readShippedCycles` /
// `readShippedMetrics` (teach-cycle.ts / teach-metric.ts) against the REAL
// `packages/dataraum-config` tree — not a fake, not a mock of the reader.
//
// WHY THIS FILE EXISTS: both reviewers independently caught the first DAT-881/
// 882 cut unifying the LIBRARY reader `nearestSeedVertical` needs (a
// cross-vertical, frame-time question: "what does vertical X ship, ANY X") with
// a WORKSPACE-scoped typed-table reader that structurally cannot answer it —
// (1) the typed table is EMPTY at frame time (seeding runs in add_source, AFTER
// frame), and (2) even once seeded, the mirrored view is scoped to the
// workspace's ONE bound active_vertical, so cross-vertical is unreachable by
// design. The unit-level fake in frame-family.test.ts couldn't catch this: it
// simulated a contract production could no longer deliver, and kept "passing"
// against that fiction. This file closes the gap by running the SAME call path
// production takes, against the SAME files production reads — the automated
// counterpart of the reviewers' manual probe ("framing retail now yields
// {vertical:'', specs:[]} where the old path served finance's real cycles").
//
// WHY IT'S HERE, NOT IN frame-family.test.ts: the LIBRARY readers lazily
// `import("bun")` for YAML parsing (the house "no wrapper deps" convention —
// Bun ships a built-in YAML parser, so no npm `yaml`/`js-yaml` package exists
// in this repo). That import only resolves under the Bun runtime, which is
// exactly the boundary the `unit` vs `integration` vitest projects draw
// (vitest.config.ts's header: "This project MUST run under `bun --bun`... the
// [...] client imports `SQL` from 'bun', which cannot load under Node at
// all" — the same constraint, different import). No DB/docker fixture is
// needed here — only the bun runtime — but that's exactly what living in the
// integration project buys.

import { describe, expect, it, vi } from "vitest";

// Mock `#/config` (real `dataraumConfigPath`, everything else inert) and
// `#/db/metadata/client` (teach-cycle.ts / teach-metric.ts transitively import
// `./teach` → the metadata write client; never actually invoked here, since
// only the LIBRARY readers run — no `teach()` call). `#/config.base` is
// reached transitively via the registry/db seam. `vi.mock` factories hoist
// above every import (including a plain top-level const), so the path must be
// computed inside `vi.hoisted` (mirrors prompts/conventions.test.ts's pattern).
const { REAL_CONFIG_PATH } = vi.hoisted(() => ({
	REAL_CONFIG_PATH: new URL("../../../dataraum-config", import.meta.url)
		.pathname,
}));
vi.mock("#/config", () => ({
	config: { dataraumConfigPath: REAL_CONFIG_PATH },
}));
vi.mock("#/config.base", () => ({ baseConfig: {} }));
vi.mock("#/db/metadata/client", () => ({
	metadataDb: { insert: vi.fn(() => ({ values: vi.fn() })) },
	metadataWriteDb: { insert: vi.fn(() => ({ values: vi.fn() })) },
}));

import { nearestSeedVertical } from "./frame-family";
import { readShippedCycles } from "./teach-cycle";
import { readShippedMetrics } from "./teach-metric";

describe("nearestSeedVertical + the LIBRARY readers, against the REAL shipped tree", () => {
	it("falls back to finance's real shipped cycles when the framed vertical (retail) ships none, with correct provenance", async () => {
		// Only `finance` (+ `_adhoc`) ships a cycles.yaml today — "retail" stands
		// in for any vertical with no library, matching the reviewers' own probe
		// vocabulary.
		const result = await nearestSeedVertical(
			"retail",
			readShippedCycles,
			async () => ["finance", "retail"],
		);
		// Provenance: the specs came from finance (the richest OTHER shipped
		// vertical), never from retail (which ships nothing) and never empty.
		expect(result.vertical).toBe("finance");
		expect(result.specs.length).toBeGreaterThan(0);
		expect(result.specs.map((s) => s.name)).toContain("order_to_cash");
	});

	it("falls back to finance's real shipped metrics when the framed vertical (retail) ships none, with correct provenance", async () => {
		const result = await nearestSeedVertical(
			"retail",
			readShippedMetrics,
			async () => ["finance", "retail"],
		);
		expect(result.vertical).toBe("finance");
		expect(result.specs.length).toBeGreaterThan(0);
		expect(result.specs.map((s) => s.graph_id)).toContain("ebitda");
	});

	it("reads the framed vertical's OWN shipped cycles when it ships some (finance framing itself)", async () => {
		const result = await nearestSeedVertical(
			"finance",
			readShippedCycles,
			async () => ["finance"],
		);
		expect(result.vertical).toBe("finance");
		expect(result.specs.length).toBeGreaterThan(0);
	});
});

// The write surface is HAND-WRITTEN Drizzle over engine-owned tables (the
// DAT-592 INSERT seam) — the generated mirror regen never touches it, so an
// engine column deletion leaves it silently stale: Drizzle then emits DEFAULT
// for a column the bootstrapped schema no longer has, and the break surfaces
// only in the compose-gated CI suites that skip on every local run. That is
// not hypothetical — R6-ENG's deletion of sources.discovered_schema shipped
// exactly this way (PR #540 CI). This pin makes the drift a unit failure:
// every column the write surface declares must exist in the engine's DDL
// dump, which CI already keeps current against the models (schema-drift job).
import { readFileSync } from "node:fs";
import { join } from "node:path";
import { describe, expect, it } from "vitest";

import * as writeSurface from "./write-surface";

const ENGINE_DDL = readFileSync(
	join(__dirname, "../../../../engine/schema.sql"),
	"utf8",
);

describe("write-surface columns exist in the engine schema dump", () => {
	const tables = Object.entries(writeSurface).filter(
		([, v]) => v && typeof v === "object" && Symbol.for("drizzle:Name") in v,
	);

	it("exports at least the sources write table (guards the harness itself)", () => {
		expect(tables.map(([k]) => k)).toContain("sourcesWrite");
	});

	for (const [exportName, table] of tables) {
		it(`${exportName}: every declared column is in schema.sql`, () => {
			const t = table as unknown as Record<symbol, unknown>;
			const columns = Object.values(
				// biome-ignore lint/suspicious/noExplicitAny: drizzle internal symbol table
				(t as any)[Symbol.for("drizzle:Columns")] as Record<string, any>,
			);
			expect(columns.length).toBeGreaterThan(0);
			for (const col of columns) {
				expect(
					ENGINE_DDL.includes(col.name),
					`column "${col.name}" of ${exportName} is not in packages/engine/schema.sql — ` +
						"the engine model dropped it; drop it here in the same change",
				).toBe(true);
			}
		});
	}
});

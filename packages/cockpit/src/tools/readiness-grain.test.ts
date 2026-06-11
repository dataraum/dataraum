// Unit tests for the grain-precedence helpers (DAT-509). Pure functions — no
// DB, no mocks. These pin the rank the engine uses at every run-resolved read
// (session-grain over table-grain, latest within a grain) so a cockpit read can
// never silently show the stale add_source verdict once a session re-rolled it.
import { describe, expect, it } from "vitest";

import {
	type GrainRow,
	mergeCurrentEvidence,
	pickCurrentRow,
} from "./readiness-grain";

type Row = GrainRow & { id: string; detectorId: string | null };

function row(id: string, overrides: Partial<Omit<Row, "id">> = {}): Row {
	return {
		id,
		detectorId: null,
		viaTableHead: false,
		viaSessionHead: false,
		viaOperatingModelHead: false,
		computedAt: new Date("2026-06-01T00:00:00Z"),
		...overrides,
	};
}

describe("pickCurrentRow", () => {
	it("returns undefined on an empty set", () => {
		expect(pickCurrentRow([])).toBeUndefined();
	});

	it("prefers the session-grain row over the add_source table-head row", () => {
		// Even when the table-head row is NEWER — the session re-roll already
		// merged the table-grain detectors (engine run resolution), so it is the
		// complete verdict.
		const tableRow = row("table", {
			viaTableHead: true,
			computedAt: new Date("2026-06-10T00:00:00Z"),
		});
		const sessionRow = row("session", {
			viaSessionHead: true,
			computedAt: new Date("2026-06-05T00:00:00Z"),
		});
		expect(pickCurrentRow([tableRow, sessionRow])?.id).toBe("session");
	});

	it("treats an operating_model-head row as session-grain", () => {
		const tableRow = row("table", { viaTableHead: true });
		const omRow = row("om", { viaOperatingModelHead: true });
		expect(pickCurrentRow([tableRow, omRow])?.id).toBe("om");
	});

	it("picks the latest session-grain row across sessions", () => {
		// Multi-session workspace: one session-grain row per session survives the
		// view's per-session dedup — the cockpit read (no session input) takes the
		// most recent verdict.
		const older = row("s1", {
			viaSessionHead: true,
			computedAt: new Date("2026-06-03T00:00:00Z"),
		});
		const newer = row("s2", {
			viaSessionHead: true,
			computedAt: new Date("2026-06-09T00:00:00Z"),
		});
		expect(pickCurrentRow([older, newer])?.id).toBe("s2");
	});

	it("falls back to the table-head row when no session-grain row exists", () => {
		const tableRow = row("table", { viaTableHead: true });
		const stray = row("stray"); // no head bits at all
		expect(pickCurrentRow([stray, tableRow])?.id).toBe("table");
	});

	it("falls back to the latest row when no discriminator is set", () => {
		const older = row("old", { computedAt: new Date("2026-06-01T00:00:00Z") });
		const newer = row("new", { computedAt: new Date("2026-06-02T00:00:00Z") });
		expect(pickCurrentRow([older, newer])?.id).toBe("new");
	});

	it("sorts null computedAt as oldest and keeps the first row on ties", () => {
		const dated = row("dated", {
			viaSessionHead: true,
			computedAt: new Date("2026-06-01T00:00:00Z"),
		});
		const undated = row("undated", { viaSessionHead: true, computedAt: null });
		expect(pickCurrentRow([undated, dated])?.id).toBe("dated");

		const tieA = row("a", { viaSessionHead: true });
		const tieB = row("b", { viaSessionHead: true });
		expect(pickCurrentRow([tieA, tieB])?.id).toBe("a");
	});
});

describe("mergeCurrentEvidence", () => {
	it("keeps one row per detector, session-grain winning", () => {
		// temporal_behavior re-adjudicated by the session; null_ratio is
		// add_source-only — each detector resolves independently.
		const rows = [
			row("tb-stale", { detectorId: "temporal_behavior", viaTableHead: true }),
			row("tb-fresh", {
				detectorId: "temporal_behavior",
				viaSessionHead: true,
			}),
			row("nr", { detectorId: "null_ratio", viaTableHead: true }),
		];
		const merged = mergeCurrentEvidence(rows);
		expect(merged.map((r) => r.id)).toEqual(["tb-fresh", "nr"]);
	});

	it("keeps operating_model fan-out rows (no table-grain sibling)", () => {
		const rows = [
			row("ctc", {
				detectorId: "cross_table_consistency",
				viaOperatingModelHead: true,
			}),
			row("tf", { detectorId: "type_fidelity", viaTableHead: true }),
		];
		expect(mergeCurrentEvidence(rows).map((r) => r.id)).toEqual(["ctc", "tf"]);
	});

	it("preserves the input's first-occurrence detector order", () => {
		// Callers ORDER BY dimension — the merge must not reshuffle it.
		const rows = [
			row("b1", { detectorId: "benford", viaTableHead: true }),
			row("a1", { detectorId: "null_ratio", viaTableHead: true }),
			row("b2", { detectorId: "benford", viaSessionHead: true }),
		];
		expect(mergeCurrentEvidence(rows).map((r) => r.id)).toEqual(["b2", "a1"]);
	});

	it("returns empty for empty input", () => {
		expect(mergeCurrentEvidence([])).toEqual([]);
	});
});

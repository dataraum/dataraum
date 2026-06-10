import { describe, expect, it } from "vitest";
import { completionNote } from "#/lib/completion-note";

// A content-keyed digest (40 hex) — the shape that must never reach the model.
const D = "a1b2c3d4e5f6a7b8c9d0e1f2a3b4c5d6e7f8a9b0";
const LEAK = /src_[0-9a-f]{40}/;

/** The note's whole serialized form — the failure message rides in its text
 * part; a leak anywhere in it fails. */
const text = (note: ReturnType<typeof completionNote>) => JSON.stringify(note);

describe("completionNote (Phase 2A — name-leak protection)", () => {
	it("is a model-only user turn naming the run in plain terms", () => {
		const note = completionNote("add_source", {
			failed: false,
			failureMessage: null,
		});
		// role "user" so the transcript ends on a user turn (no-prefill model needs it).
		expect(note.role).toBe("user");
		expect(text(note)).toContain("data import");
		expect(text(note)).toContain("finished successfully");
	});

	it("strips a content-keyed src_<digest> from a failure message (DAT-433)", () => {
		const note = completionNote("add_source", {
			failed: true,
			failureMessage: `cast failed in customers (source src_${D})`,
		});
		expect(text(note)).not.toMatch(LEAK);
		// Root cause stays readable — only the digest is neutralized.
		expect(text(note)).toContain("cast failed in customers (source upload)");
	});

	it("strips the BARE digest from a staged-upload s3 URI in a failure", () => {
		// Import failures realistically quote the source's s3 URI, where the digest
		// appears bare (`uploads/<digest>/`, no `src_` prefix).
		const note = completionNote("add_source", {
			failed: true,
			failureMessage: `CSV header mismatch in 's3://dataraum-lake/uploads/${D}/orders.csv'`,
		});
		expect(text(note)).not.toContain(D);
		// The trailing filename survives — the root cause stays readable.
		expect(text(note)).toContain("CSV header mismatch in 'orders.csv'");
	});

	it("labels each run stage in the user's terms (never the internal id)", () => {
		const note = (stage: Parameters<typeof completionNote>[0]) =>
			text(completionNote(stage, { failed: false, failureMessage: null }));
		expect(note("begin_session")).toContain("analysis session");
		expect(note("operating_model")).toContain("operating-model run");
		// No raw stage ids leak into the note.
		expect(note("begin_session")).not.toContain("begin_session");
	});
});

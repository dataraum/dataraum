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

describe("completionNote (DAT-510 — narration boundary)", () => {
	const body = (note: ReturnType<typeof completionNote>) =>
		(note.parts[0] as { content: string }).content;

	it("anchors to THIS run when nothing else is in flight", () => {
		const note = completionNote("add_source", {
			failed: false,
			failureMessage: null,
		});
		expect(body(note)).toContain(
			"Narrate ONLY the data import — do not state or imply any other run finished.",
		);
	});

	it("names a single still-running stage as off-limits", () => {
		// add_source landed while begin_session is still running — the bug was the
		// agent narrating the session as done. The note must forbid that.
		const note = completionNote(
			"add_source",
			{ failed: false, failureMessage: null },
			["begin_session"],
		);
		expect(body(note)).toContain("the analysis session is still running");
		expect(body(note)).toContain("narrate ONLY the data import");
		expect(body(note)).toContain("do NOT say or imply it has finished");
	});

	it("lists multiple still-running stages with plural agreement", () => {
		const note = completionNote(
			"add_source",
			{ failed: false, failureMessage: null },
			["begin_session", "operating_model"],
		);
		expect(body(note)).toContain(
			"the analysis session and operating-model run are still running",
		);
		expect(body(note)).toContain("do NOT say or imply they have finished");
	});

	it("excludes this run's own stage and dedups the in-flight set", () => {
		// The finished run may still appear in a racy snapshot; it must never be
		// listed as "still running", and duplicates collapse.
		const note = completionNote(
			"begin_session",
			{ failed: false, failureMessage: null },
			["begin_session", "add_source", "add_source"],
		);
		expect(body(note)).toContain("the data import is still running");
		expect(body(note)).not.toContain("analysis session is still running");
		// Single remaining stage → singular agreement.
		expect(body(note)).toContain("do NOT say or imply it has finished");
	});

	it("falls back to the solo-run boundary when in-flight collapses to empty", () => {
		const note = completionNote(
			"operating_model",
			{ failed: false, failureMessage: null },
			["operating_model"],
		);
		expect(body(note)).toContain(
			"Narrate ONLY the operating-model run — do not state or imply any other run finished.",
		);
	});
});

// Unit tests for the upload → agent handoff (DAT-423) — the clean bubble vs. the
// model-only refs split that kills the s3:// leak. The marker/skip machinery is
// the shared lib/agent-refs helper (DAT-437); these tests cover the
// upload-specific composition.

import { describe, expect, it } from "vitest";

import { isAgentRefsPart } from "#/lib/agent-refs";
import { uploadBubbleText, uploadTurn } from "#/ui/cockpit/upload-handoff";

const A = "s3://dataraum-lake/uploads/aaa111/invoices.csv";
const B = "s3://dataraum-lake/uploads/bbb222/payments.csv";

describe("uploadBubbleText — the visible chat bubble", () => {
	it("names a single file and shows NO s3:// path (AC1)", () => {
		const t = uploadBubbleText([A]);
		expect(t).toBe("Uploaded invoices.csv.");
		expect(t).not.toContain("s3://");
	});

	it("names a multi-file batch by filename, still no paths", () => {
		const t = uploadBubbleText([A, B]);
		expect(t).toBe("Uploaded 2 files: invoices.csv, payments.csv.");
		expect(t).not.toContain("s3://");
	});
});

describe("uploadTurn — the two-part turn", () => {
	it("carries the clean bubble first; the uris ride only in the refs part", () => {
		const turn = uploadTurn([A, B]);
		expect(turn.content).toHaveLength(2);
		const [bubble, refs] = turn.content;
		expect(bubble?.content).toBe(
			"Uploaded 2 files: invoices.csv, payments.csv.",
		);
		expect(bubble?.content).not.toContain("s3://");
		expect(refs && isAgentRefsPart(refs.content)).toBe(true);
		expect(refs?.content).toContain(A);
		expect(refs?.content).toContain(B);
	});

	it("preserves batch order in the refs part (numbered, in selection order — AC3)", () => {
		const refs = uploadTurn([A, B]).content[1]?.content ?? "";
		expect(refs.indexOf("1. invoices.csv")).toBeLessThan(
			refs.indexOf("2. payments.csv"),
		);
	});

	it("the bubble is never flagged as a refs part", () => {
		const bubble = uploadTurn([A]).content[0]?.content ?? "";
		expect(isAgentRefsPart(bubble)).toBe(false);
	});
});

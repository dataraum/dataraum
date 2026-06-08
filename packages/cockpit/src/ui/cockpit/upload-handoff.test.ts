// Unit tests for the upload → agent handoff (DAT-423, refs flip DAT-462) — the
// clean bubble vs. the model-only refs body that kills the s3:// leak. The bubble
// is the visible message; the refs body rides as forwardedProps (never in the
// visible content). These tests cover the upload-specific composition.

import { describe, expect, it } from "vitest";

import { uploadBubbleText, uploadRefs } from "#/ui/cockpit/upload-handoff";

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

describe("uploadRefs — the model-only refs body (forwardedProps)", () => {
	it("carries the uris (the bubble never does)", () => {
		const refs = uploadRefs([A, B]);
		expect(refs).toContain(A);
		expect(refs).toContain(B);
	});

	it("preserves batch order, numbered in selection order (AC3)", () => {
		const refs = uploadRefs([A, B]);
		expect(refs.indexOf("1. invoices.csv")).toBeLessThan(
			refs.indexOf("2. payments.csv"),
		);
	});
});

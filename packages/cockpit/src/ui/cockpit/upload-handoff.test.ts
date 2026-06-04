// Unit tests for the upload → agent handoff (DAT-423) — the clean bubble vs. the
// model-only refs split that kills the s3:// leak.

import { describe, expect, it } from "vitest";

import {
	isUploadRefsPart,
	UPLOAD_REFS_MARKER,
	uploadBubbleText,
	uploadRefsBlock,
} from "#/ui/cockpit/upload-handoff";

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

describe("uploadRefsBlock — the model-only refs part", () => {
	it("carries the ordered uris and starts with the skip marker", () => {
		const block = uploadRefsBlock([A, B]);
		expect(block.startsWith(UPLOAD_REFS_MARKER)).toBe(true);
		expect(block).toContain(A);
		expect(block).toContain(B);
	});

	it("preserves batch order (numbered, in selection order — AC3)", () => {
		const block = uploadRefsBlock([A, B]);
		expect(block.indexOf("1. invoices.csv")).toBeLessThan(
			block.indexOf("2. payments.csv"),
		);
	});
});

describe("isUploadRefsPart — what the chat rail must skip", () => {
	it("flags the refs block but not a normal bubble", () => {
		expect(isUploadRefsPart(uploadRefsBlock([A]))).toBe(true);
		expect(isUploadRefsPart(uploadBubbleText([A]))).toBe(false);
		expect(isUploadRefsPart("Uploaded invoices.csv.")).toBe(false);
	});
});

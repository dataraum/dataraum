import { describe, expect, it } from "vitest";

import { fileIdSegment, fileName } from "#/lib/file-uri";

describe("file-uri display helpers", () => {
	const uri = "s3://dataraum-lake/uploads/abc123/people.csv";

	it("takes the filename from the last path segment", () => {
		expect(fileName(uri)).toBe("people.csv");
	});

	it("takes the upload id from the second-to-last segment", () => {
		expect(fileIdSegment(uri)).toBe("abc123");
	});

	it("degrades gracefully for a bare name (no path)", () => {
		expect(fileName("people.csv")).toBe("people.csv");
		expect(fileIdSegment("people.csv")).toBeNull();
	});
});

// Unit tests for the upload policy + handle shape (DAT-386). Pure — no I/O.
//
// The handle shape is a contract DAT-389 reads, so these lock it precisely:
// `s3://<bucket>/uploads/<uuid>/<filename>`, lake stays at `lake/`.

import { describe, expect, it } from "vitest";

import {
	ALLOWED_EXTENSIONS,
	buildUploadKey,
	buildUploadUri,
	fileExtension,
	isAllowedExtension,
	sanitizeFilename,
	UPLOAD_PREFIX,
} from "./policy";

describe("fileExtension", () => {
	it("returns the lowercased extension", () => {
		expect(fileExtension("people.CSV")).toBe("csv");
		expect(fileExtension("a.b.parquet")).toBe("parquet");
	});
	it("returns null when there is no usable extension", () => {
		expect(fileExtension("noext")).toBeNull();
		expect(fileExtension(".hidden")).toBeNull(); // leading dot, no name
		expect(fileExtension("trailing.")).toBeNull();
	});
});

describe("isAllowedExtension", () => {
	it("accepts every sniffable extension", () => {
		for (const ext of ALLOWED_EXTENSIONS) {
			expect(isAllowedExtension(`f.${ext}`)).toBe(true);
		}
	});
	it("accepts case-insensitively", () => {
		expect(isAllowedExtension("F.Csv")).toBe(true);
	});
	it("rejects unsupported and extension-less files", () => {
		expect(isAllowedExtension("data.xlsx")).toBe(false);
		expect(isAllowedExtension("data.exe")).toBe(false);
		expect(isAllowedExtension("data")).toBe(false);
	});
});

describe("sanitizeFilename", () => {
	it("strips directory parts to a single leaf", () => {
		expect(sanitizeFilename("../../etc/passwd.csv")).toBe("passwd.csv");
		expect(sanitizeFilename("C:\\\\data\\\\x.csv")).toBe("x.csv");
	});
	it("collapses unsafe chars and drops leading dots", () => {
		expect(sanitizeFilename("my file (1).csv")).toBe("my_file__1_.csv");
		expect(sanitizeFilename("...hidden.csv")).toBe("hidden.csv");
	});
	it("falls back to a stable name when nothing safe remains", () => {
		expect(sanitizeFilename("///")).toBe("upload");
	});
});

describe("buildUploadKey / buildUploadUri (locked contract for DAT-389)", () => {
	it("lays the key out as uploads/<uuid>/<filename>", () => {
		expect(
			buildUploadKey("11111111-2222-3333-4444-555555555555", "sales.csv"),
		).toBe("uploads/11111111-2222-3333-4444-555555555555/sales.csv");
		expect(UPLOAD_PREFIX).toBe("uploads");
	});
	it("sanitizes the filename inside the key", () => {
		expect(buildUploadKey("u", "../evil.csv")).toBe("uploads/u/evil.csv");
	});
	it("sanitizes a non-UUID uuid so it cannot inject `/` or `..`", () => {
		expect(buildUploadKey("../../lake", "x.csv")).toBe("uploads/lake/x.csv");
	});
	it("builds the s3:// handle in the same bucket as the lake", () => {
		const key = buildUploadKey("u", "x.parquet");
		expect(buildUploadUri("dataraum-lake", key)).toBe(
			"s3://dataraum-lake/uploads/u/x.parquet",
		);
	});
});

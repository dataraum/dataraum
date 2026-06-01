// Unit tests for the multi-file upload batch gate (DAT-391). Pure — no I/O.

import { describe, expect, it } from "vitest";

import { validateUploadBatch } from "./batch";
import { MAX_UPLOAD_FILES } from "./policy";

describe("validateUploadBatch", () => {
	it("accepts a homogeneous batch within the cap", () => {
		expect(validateUploadBatch(["a.csv", "b.csv", "c.tsv"])).toBeNull();
		expect(validateUploadBatch(["x.parquet", "y.pq"])).toBeNull();
		expect(
			validateUploadBatch(["one.json", "two.jsonl", "three.ndjson"]),
		).toBeNull();
		expect(validateUploadBatch(["solo.csv"])).toBeNull();
	});

	it("rejects an empty selection", () => {
		expect(validateUploadBatch([])).toMatch(/at least one/i);
	});

	it(`rejects more than ${MAX_UPLOAD_FILES} files`, () => {
		const tooMany = Array.from(
			{ length: MAX_UPLOAD_FILES + 1 },
			(_, i) => `f${i}.csv`,
		);
		const err = validateUploadBatch(tooMany);
		expect(err).toMatch(new RegExp(`Up to ${MAX_UPLOAD_FILES}`));
		expect(err).toContain(String(MAX_UPLOAD_FILES + 1));
	});

	it("accepts exactly the cap", () => {
		const atCap = Array.from(
			{ length: MAX_UPLOAD_FILES },
			(_, i) => `f${i}.csv`,
		);
		expect(validateUploadBatch(atCap)).toBeNull();
	});

	it("rejects an unsupported extension", () => {
		expect(validateUploadBatch(["data.csv", "notes.md"])).toMatch(
			/unsupported/i,
		);
	});

	it("rejects a mixed-kind batch (csv + parquet → two source_types)", () => {
		const err = validateUploadBatch(["a.csv", "b.parquet"]);
		expect(err).toMatch(/same kind/i);
		expect(err).toContain("csv");
		expect(err).toContain("parquet");
	});

	it("treats csv/tsv/txt as one kind (no mixed-kind error)", () => {
		expect(validateUploadBatch(["a.csv", "b.tsv", "c.txt"])).toBeNull();
	});
});

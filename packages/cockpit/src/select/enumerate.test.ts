// Unit tests for select-stage prefix enumeration (DAT-378).
//
// The ListObjectsV2 I/O is injected (the pure mapper `keysToUris` carries the
// filter/sort logic and is exercised directly; the driver `enumeratePrefixUris`
// is tested with a stubbed `list`), so no live SeaweedFS or @aws-lite is needed.
// enumerate.ts imports `../upload/policy` (pure) and `../upload/s3-upload` (boots
// config + @aws-lite at module load) — mock config at the `#/` alias so this
// stays a pure unit.

import { describe, expect, it, vi } from "vitest";

vi.mock("#/config", () => ({
	config: {
		s3Endpoint: "seaweedfs:8333",
		s3Region: "us-east-1",
		s3UseSsl: false,
		s3AccessKeyId: "k",
		s3SecretAccessKey: "s",
		s3Bucket: "dataraum-lake",
	},
}));

import {
	buildFileUrisConfig,
	enumeratePrefixUris,
	isLoadableKey,
	keysToUris,
} from "./enumerate";

const BUCKET = "dataraum-lake";

describe("isLoadableKey", () => {
	it("accepts supported data extensions", () => {
		for (const k of [
			"uploads/a/orders.csv",
			"p/x.tsv",
			"p/y.parquet",
			"p/z.jsonl",
			"p/q.JSON",
		]) {
			expect(isLoadableKey(k)).toBe(true);
		}
	});

	it("rejects folder markers, extensionless, and unsupported types", () => {
		for (const k of [
			"uploads/sub/", // folder marker
			"uploads/readme", // no extension
			"uploads/notes.md", // unsupported
			"uploads/logo.png",
		]) {
			expect(isLoadableKey(k)).toBe(false);
		}
	});
});

describe("keysToUris", () => {
	it("filters to loadable files, maps to s3:// URIs, and sorts", () => {
		const keys = [
			"sel/orders.csv",
			"sel/sub/", // dropped (folder marker)
			"sel/readme.md", // dropped (unsupported)
			"sel/customers.parquet",
			"sel/events.jsonl",
		];
		expect(keysToUris(BUCKET, keys)).toEqual([
			`s3://${BUCKET}/sel/customers.parquet`,
			`s3://${BUCKET}/sel/events.jsonl`,
			`s3://${BUCKET}/sel/orders.csv`,
		]);
	});

	it("returns an empty list when nothing under the prefix is loadable", () => {
		expect(keysToUris(BUCKET, ["sel/", "sel/notes.md"])).toEqual([]);
	});
});

describe("enumeratePrefixUris", () => {
	it("enumerates a prefix into an explicit, sorted URI list", async () => {
		const list = vi
			.fn()
			.mockResolvedValue(["sel/b.csv", "sel/a.csv", "sel/skip.md"]);
		const uris = await enumeratePrefixUris(BUCKET, "sel/", list);
		expect(list).toHaveBeenCalledWith(BUCKET, "sel/");
		expect(uris).toEqual([
			`s3://${BUCKET}/sel/a.csv`,
			`s3://${BUCKET}/sel/b.csv`,
		]);
	});

	it("throws when the prefix has no loadable objects", async () => {
		const list = vi.fn().mockResolvedValue(["sel/", "sel/notes.md"]);
		await expect(enumeratePrefixUris(BUCKET, "sel/", list)).rejects.toThrow(
			/No loadable objects/,
		);
	});
});

describe("buildFileUrisConfig", () => {
	it("wraps the URI list under the distinct file_uris key", () => {
		const uris = [`s3://${BUCKET}/sel/a.csv`, `s3://${BUCKET}/sel/b.csv`];
		expect(buildFileUrisConfig(uris)).toEqual({ file_uris: uris });
	});
});

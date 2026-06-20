// Unit tests for persistFileSources (DAT-594) — the import-set producer that
// writes ONE content-keyed `src_<digest>` source per staged upload.
//
// The write seam (`#/select/source-write`: upsertSource) is mocked so the row
// SHAPE + the content-keying are asserted without a live Postgres. The mock is on
// the `#/` alias so it intercepts file-source's relative `./source-write` import
// (same resolved module — the cockpit vitest mock-alias rule).

import { beforeEach, describe, expect, it, vi } from "vitest";

// file-source pulls `sourceTypeForUri` from `#/select/mappers` (→ upload/policy),
// which is crypto-bearing but config-free; no config stub needed. The content
// digest lives in the upload URI shape, so no env is required.

const h = vi.hoisted(() => ({
	upserts: [] as Array<{
		name: string;
		sourceType: string;
		backend: string | null;
		connectionConfig: Record<string, unknown>;
	}>,
}));

vi.mock("#/select/source-write", () => ({
	STAGE_AFTER_SELECT: "add_source",
	INITIAL_STATUS: "configured",
	upsertSource: vi.fn(
		async (v: {
			name: string;
			sourceType: string;
			backend: string | null;
			connectionConfig: Record<string, unknown>;
		}) => {
			h.upserts.push(v);
			return `id_${v.name}`;
		},
	),
}));

import { persistFileSources } from "./file-source";

// A staged upload URI is `s3://<bucket>/<ws>/uploads/<digest>/<file>` (DAT-505) —
// the digest segment is what file-source content-keys the source on.
const WS = "00000000-0000-0000-0000-000000000001";
const A = `s3://dataraum-lake/${WS}/uploads/aaa111/orders.csv`;
const B = `s3://dataraum-lake/${WS}/uploads/bbb222/customers.parquet`;

beforeEach(() => {
	h.upserts.length = 0;
});

describe("persistFileSources (DAT-594) — one content-keyed source per file", () => {
	it("mints ONE content-keyed source per uploaded file", async () => {
		const result = await persistFileSources([{ file_uri: A }, { file_uri: B }]);

		expect(h.upserts).toHaveLength(2);
		const byName = Object.fromEntries(
			h.upserts.map((r) => [r.name, r]),
		) as Record<string, (typeof h.upserts)[number]>;
		expect(Object.keys(byName).sort()).toEqual(["src_aaa111", "src_bbb222"]);
		expect(byName.src_aaa111.connectionConfig).toEqual({ file_uris: [A] });
		expect(byName.src_aaa111.sourceType).toBe("csv");
		expect(byName.src_bbb222.sourceType).toBe("parquet");
		// file_uris and tables never cross-contaminate.
		expect(byName.src_aaa111.connectionConfig).not.toHaveProperty("tables");

		expect(result.map((p) => p.source_id).sort()).toEqual([
			"id_src_aaa111",
			"id_src_bbb222",
		]);
	});

	it("dedups a repeated URI to ONE UPSERT (same content key)", async () => {
		const result = await persistFileSources([{ file_uri: A }, { file_uri: A }]);
		expect(h.upserts).toHaveLength(1);
		expect(result).toHaveLength(1);
		expect(result[0].file_uri).toBe(A);
	});

	it("fails loud on a non-upload URI BEFORE persisting (not content-addressed)", async () => {
		await expect(
			persistFileSources([
				{ file_uri: "s3://dataraum-lake/data/2024/sales.csv" },
			]),
		).rejects.toThrow(/must be a staged upload/);
		expect(h.upserts).toHaveLength(0);
	});

	it("persists nothing for an empty list", async () => {
		const result = await persistFileSources([]);
		expect(result).toEqual([]);
		expect(h.upserts).toHaveLength(0);
	});
});

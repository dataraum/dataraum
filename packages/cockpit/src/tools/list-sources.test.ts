// Unit tests for list_sources — the available-inputs inventory. The env scan
// (configured databases) and the S3 prefix list (uploaded files) are mocked via
// the `#/` alias so the assembly is tested without a live SeaweedFS or real env.

import { describe, expect, it, vi } from "vitest";

vi.mock("#/config", () => ({ config: { s3Bucket: "dataraum-lake" } }));
const { listConfiguredDatabasesMock, listPrefixObjectsMock } = vi.hoisted(
	() => ({
		listConfiguredDatabasesMock: vi.fn(),
		listPrefixObjectsMock: vi.fn(),
	}),
);
vi.mock("#/duckdb/credentials", () => ({
	listConfiguredDatabases: listConfiguredDatabasesMock,
}));
vi.mock("#/upload/s3-upload", () => ({
	listPrefixObjects: listPrefixObjectsMock,
}));
// list_sources resolves the active workspace (DAT-505) to scope the uploads
// prefix to `<ws>/uploads/`; mock the registry seam so no live cockpit_db loads.
const WS = "00000000-0000-0000-0000-000000000001";
vi.mock("#/db/cockpit/registry", () => ({
	resolveActiveWorkspaceRow: vi.fn(async () => ({
		id: WS,
		taskQueue: `engine-${WS}`,
		vertical: "_adhoc",
	})),
}));

import { listSources } from "./list-sources";

describe("listSources", () => {
	it("unifies configured databases and uploaded files as available inputs", async () => {
		listConfiguredDatabasesMock.mockReturnValue([
			{ name: "finance", backend: "postgres" },
		]);
		listPrefixObjectsMock.mockResolvedValue([
			{ key: `${WS}/uploads/abc123/orders.csv`, size: 2048 },
		]);

		const sources = await listSources();

		// Listed under the workspace's own prefix (DAT-505).
		expect(listPrefixObjectsMock).toHaveBeenCalledWith(
			"dataraum-lake",
			`${WS}/uploads/`,
		);
		expect(sources).toEqual([
			{
				kind: "database",
				name: "finance",
				backend: "postgres",
				uri: null,
				size_bytes: null,
			},
			{
				kind: "file",
				name: "orders.csv",
				backend: null,
				uri: `s3://dataraum-lake/${WS}/uploads/abc123/orders.csv`,
				size_bytes: 2048,
			},
		]);
	});

	it("is empty when nothing is configured or uploaded", async () => {
		listConfiguredDatabasesMock.mockReturnValue([]);
		listPrefixObjectsMock.mockResolvedValue([]);
		expect(await listSources()).toEqual([]);
	});
});

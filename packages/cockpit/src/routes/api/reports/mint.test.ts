// Unit tests for the mint-report handler (DAT-624; lineage DAT-627).
//
// `handleMint` takes injected deps (the `handleUpload` convention) so the
// gates — chart-config shape guard, fingerprint best-effort, parentId
// validation — are asserted with no real cockpit_db and no live lake
// connection. The freeze itself (defaulting, workspace scope) is covered by
// reports.test.ts; here we assert the ROUTE's own logic.
//
// Importing `./mint` transitively pulls in the cockpit_db client (bun-only
// `SQL` from "bun", via #/db/cockpit/reports) and the lake connection (via
// #/duckdb/report-fingerprint-read) purely to WIRE the real functions into
// `Route` — `handleMint` itself never touches them (every dep is injected
// below), so these are mocked only to break the import graph under node
// (mirrors upload.test.ts / chat.test.ts).

import { describe, expect, it, vi } from "vitest";

vi.mock("#/config", () => ({ config: {} }));
vi.mock("#/config.base", () => ({ baseConfig: {} }));
vi.mock("#/db/cockpit/client", () => ({ cockpitDb: {} }));
vi.mock("#/duckdb/report-fingerprint-read", () => ({
	computeReportFingerprint: vi.fn(),
}));

import type { AnswerConfidence } from "#/ui/cockpit/canvas-state";
import { handleMint, type MintBody } from "./mint";

const confidence: AnswerConfidence = {
	band: "ready",
	groundedRatio: 1,
	reuse: { exactReuse: 1, adapted: 0, fresh: 0 },
	assumptions: [],
	conceptsUsed: ["revenue"],
};

function baseBody(overrides: Partial<MintBody> = {}): MintBody {
	return {
		sql: "SELECT 1",
		summary: "s",
		title: "t",
		confidence,
		...overrides,
	};
}

function deps(overrides: Partial<Parameters<typeof handleMint>[1]> = {}) {
	return {
		workspaceId: "ws-1",
		getReport: vi.fn(async () => null),
		createReport: vi.fn(async () => "new-id"),
		fingerprint: vi.fn(
			async () => ({ fingerprint: "fp-1", result: {} }) as never,
		),
		...overrides,
	};
}

describe("handleMint", () => {
	it("freezes the report and returns its id", async () => {
		const d = deps();
		const result = await handleMint(baseBody(), d);
		expect(result).toEqual({ id: "new-id" });
		expect(d.createReport).toHaveBeenCalledWith(
			expect.objectContaining({
				workspaceId: "ws-1",
				sql: "SELECT 1",
				confidence,
				parentId: null,
				sqlParams: null,
			}),
		);
	});

	it("passes sqlParams through for a PINNED drill (DAT-627)", async () => {
		const d = deps();
		await handleMint(
			baseBody({ sql: "SELECT 1 WHERE region = $1", sqlParams: ["EU"] }),
			d,
		);
		expect(d.createReport).toHaveBeenCalledWith(
			expect.objectContaining({ sqlParams: ["EU"] }),
		);
	});

	it("carries a null confidence through for a drilled mint (DAT-627)", async () => {
		const d = deps();
		await handleMint(baseBody({ confidence: null }), d);
		expect(d.createReport).toHaveBeenCalledWith(
			expect.objectContaining({ confidence: null }),
		);
	});

	it("validates parentId against the workspace's live reports and forwards it when it resolves (DAT-627)", async () => {
		const d = deps({
			getReport: vi.fn(async (id: string) =>
				id === "parent-1"
					? {
							id: "parent-1",
							workspaceId: "ws-1",
							parentId: null,
							title: "Parent",
							summary: "s",
							summaryFingerprint: null,
							sql: "SELECT 1",
							sqlParams: null,
							confidence,
							chartConfig: null,
							createdAt: new Date(),
						}
					: null,
			),
		});
		await handleMint(baseBody({ parentId: "parent-1" }), d);
		expect(d.createReport).toHaveBeenCalledWith(
			expect.objectContaining({ parentId: "parent-1" }),
		);
	});

	it("drops a parentId that doesn't resolve in this workspace — never trusted onto the row", async () => {
		const d = deps({ getReport: vi.fn(async () => null) });
		const warn = vi.spyOn(console, "warn").mockImplementation(() => {});
		await handleMint(baseBody({ parentId: "someone-elses-report" }), d);
		expect(d.createReport).toHaveBeenCalledWith(
			expect.objectContaining({ parentId: null }),
		);
		expect(warn).toHaveBeenCalled();
		warn.mockRestore();
	});

	it("drops a malformed chart config instead of failing the mint", async () => {
		const d = deps();
		const error = vi.spyOn(console, "error").mockImplementation(() => {});
		await handleMint(
			baseBody({ chartConfig: { not: "a valid config" } as never }),
			d,
		);
		expect(d.createReport).toHaveBeenCalledWith(
			expect.objectContaining({ chartConfig: null }),
		);
		error.mockRestore();
	});

	it("mints with a null fingerprint when the fingerprint call fails — never blocks the mint", async () => {
		const d = deps({
			fingerprint: vi.fn(async () => {
				throw new Error("lake unreachable");
			}),
		});
		const error = vi.spyOn(console, "error").mockImplementation(() => {});
		const result = await handleMint(baseBody(), d);
		expect(result).toEqual({ id: "new-id" });
		expect(d.createReport).toHaveBeenCalledWith(
			expect.objectContaining({ summaryFingerprint: null }),
		);
		error.mockRestore();
	});
});

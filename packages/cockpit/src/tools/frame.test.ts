// Unit coverage for frame()'s conventions DECLARE loop (DAT-789). writeConvention's
// DB correctness is proven in convention-write.integration.test.ts; this pins the
// orchestration frame() wraps it in — the sentinel fold-back ([]/{} → NULL) on the
// write, and the FrameResult.conventions shape returned to the caller. Every
// side-effecting seam is stubbed via the `#/` alias so the loop is testable without a
// DB, an LLM, or the overlay teach families.

import { describe, expect, it, vi } from "vitest";

vi.mock("#/config", () => ({ config: { dataraumWorkspaceId: "ws-test" } }));
vi.mock("#/config.base", () => ({ baseConfig: {} }));
vi.mock("#/db/metadata/client", () => ({
	metadataDb: {},
	metadataWriteDb: {},
}));
vi.mock("#/db/cockpit/registry", () => ({
	setActiveWorkspaceVertical: vi.fn(async () => {}),
}));
vi.mock("../duckdb/connect", () => ({
	ConnectSchema: { parse: (x: unknown) => x },
}));

// The two typed-home writers — spied so we assert the loop's fold-back + shape.
// concept-write is import-original (frame.ts also pulls CONCEPT_KINDS / DIMENSION_ORDERINGS
// from it for the ProposedConcept schema); only writeConcept is stubbed.
const writeConventionMock = vi.hoisted(() =>
	vi.fn(async (_input?: unknown) => ({ convention_id: "vid" })),
);
vi.mock("./concept-write", async (orig) => ({
	...(await orig<typeof import("./concept-write")>()),
	writeConcept: vi.fn(async () => ({ concept_id: "cid" })),
}));
vi.mock("./convention-write", () => ({ writeConvention: writeConventionMock }));

// The overlay families are out of scope here — stub frameFamily to write nothing.
vi.mock("./frame-family", () => ({
	frameFamily: async () => ({ written: [], items: [] }),
	induceNative: async () => ({}),
	nearestSeedVertical: async () => ({ vertical: "x", specs: [] }),
	stripUndefined: (o: unknown) => o,
	formatSeedExamples: () => "",
}));
vi.mock("../prompts", () => ({
	getFrameInstructions: () => "",
	getFrameValidationsInstructions: () => "",
	getFrameCyclesInstructions: () => "",
	getFrameMetricsInstructions: () => "",
}));

import { frame } from "./frame";

const CONCEPT = {
	name: "revenue",
	kind: "measure" as const,
	description: "d",
	indicators: [],
	exclude_patterns: [],
	unit_from_concept: "",
	ordering: "nominal" as const,
};

describe("frame() conventions declare loop (DAT-789)", () => {
	it("writes declared conventions verbatim, folds empty sentinels to NULL, returns the id", async () => {
		writeConventionMock.mockClear();
		writeConventionMock
			.mockResolvedValueOnce({ convention_id: "vid-a" })
			.mockResolvedValueOnce({ convention_id: "vid-b" });

		const result = await frame({
			schema: { tables: [] } as never,
			vertical_name: "testvert",
			concepts: [CONCEPT],
			validations: [],
			cycles: [],
			metrics: [],
			conventions: [
				{
					name: "sign",
					statement: "rule A",
					targets: ["extraction", "qa"],
					concept_groups: { g: ["revenue"] },
				},
				{ name: "bare", statement: "rule B", targets: [], concept_groups: {} },
			],
		});

		expect(writeConventionMock).toHaveBeenCalledTimes(2);
		// A populated convention is written verbatim under the resolved vertical.
		expect(writeConventionMock).toHaveBeenNthCalledWith(1, {
			vertical: "testvert",
			name: "sign",
			statement: "rule A",
			targets: ["extraction", "qa"],
			concept_groups: { g: ["revenue"] },
		});
		// The empty sentinels fold to undefined (⇒ NULL), never [] / {}.
		expect(writeConventionMock).toHaveBeenNthCalledWith(2, {
			vertical: "testvert",
			name: "bare",
			statement: "rule B",
			targets: undefined,
			concept_groups: undefined,
		});
		// The result carries the (parsed, model-facing) rows + their minted ids.
		expect(result.conventions).toEqual([
			{
				name: "sign",
				statement: "rule A",
				targets: ["extraction", "qa"],
				concept_groups: { g: ["revenue"] },
				convention_id: "vid-a",
			},
			{
				name: "bare",
				statement: "rule B",
				targets: [],
				concept_groups: {},
				convention_id: "vid-b",
			},
		]);
	});

	it("writes no conventions when the set is absent (declare-only, never induced)", async () => {
		writeConventionMock.mockClear();
		const result = await frame({
			schema: { tables: [] } as never,
			vertical_name: "testvert",
			concepts: [CONCEPT],
			validations: [],
			cycles: [],
			metrics: [],
			// no conventions key
		});
		expect(writeConventionMock).not.toHaveBeenCalled();
		expect(result.conventions).toEqual([]);
	});
});

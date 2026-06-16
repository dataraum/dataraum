// Unit tests for the use_vertical tool (DAT-523) — adopt an existing vertical
// onto the workspace. The vertical catalogue (`listVerticals`) is injected so the
// test exercises the adopt/born-loud logic without the config-fs scan or the
// metadata DB; the workspace write is mocked at the registry boundary (no
// cockpit_db). The real SQL write is covered by registry.test.ts.

import { beforeEach, describe, expect, it, vi } from "vitest";

import type { Vertical } from "./list-verticals";

// The static `list-verticals` import transitively pulls config.ts + the Postgres
// metadata client; mock both so the test needs no env and opens no connection
// (the catalogue is injected below, so neither is actually exercised).
vi.mock("#/config", () => ({ config: { dataraumConfigPath: "/nonexistent" } }));
vi.mock("#/db/metadata/client", () => ({ metadataDb: {} }));

// The workspace-vertical write — mocked so no cockpit_db is touched. We assert it
// fires with the adopted name (and NOT on the born-loud rejection path).
const setVerticalMock = vi.fn();
vi.mock("#/db/cockpit/registry", () => ({
	setActiveWorkspaceVertical: (...args: unknown[]) => setVerticalMock(...args),
}));

import { useVertical } from "./use-vertical";

const vertical = (name: string, kind: "builtin" | "framed"): Vertical => ({
	name,
	kind,
	description: kind === "builtin" ? "Shipped domain" : null,
	concept_count: 12,
	has_cycles: kind === "builtin",
	has_validations: kind === "builtin",
	has_metrics: kind === "builtin",
});

const CATALOGUE: Vertical[] = [
	vertical("finance", "builtin"),
	vertical("sales", "framed"),
];
const stubList = async () => CATALOGUE;

beforeEach(() => {
	setVerticalMock.mockClear();
});

describe("useVertical (DAT-523)", () => {
	it("adopts a builtin vertical onto the workspace", async () => {
		const result = await useVertical("finance", stubList);
		expect(result).toEqual({ vertical: "finance", kind: "builtin" });
		expect(setVerticalMock).toHaveBeenCalledWith("finance");
	});

	it("adopts an already-framed vertical onto the workspace", async () => {
		const result = await useVertical("sales", stubList);
		expect(result).toEqual({ vertical: "sales", kind: "framed" });
		expect(setVerticalMock).toHaveBeenCalledWith("sales");
	});

	it("is born-loud on an unknown vertical — no workspace write", async () => {
		await expect(useVertical("typo_vertical", stubList)).rejects.toThrow(
			/not available to adopt/,
		);
		// The typo never pins a non-resolving vertical (DAT-479 conflation guard).
		expect(setVerticalMock).not.toHaveBeenCalled();
	});
});

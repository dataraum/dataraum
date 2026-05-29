import { afterEach, describe, expect, it, vi } from "vitest";

import { resolveCredential } from "./credentials";

afterEach(() => {
	vi.unstubAllEnvs();
});

describe("resolveCredential (DAT-367)", () => {
	it("resolves DATARAUM_<NAME>_URL by upper-cased source name", () => {
		vi.stubEnv("DATARAUM_ORDERS_URL", "postgres://u:p@host:5432/db");
		const resolved = resolveCredential("orders");
		expect(resolved).toEqual({
			url: "postgres://u:p@host:5432/db",
			source: "env",
		});
	});

	it("upper-cases a mixed-case source name", () => {
		vi.stubEnv("DATARAUM_SALESDB_URL", "mysql://localhost/sales");
		expect(resolveCredential("salesDb")?.url).toBe("mysql://localhost/sales");
	});

	it("returns null when no env var is set (caller fails loud)", () => {
		vi.stubEnv("DATARAUM_MISSING_URL", undefined as unknown as string);
		expect(resolveCredential("missing")).toBeNull();
	});

	it("treats an empty-string env var as unset", () => {
		vi.stubEnv("DATARAUM_BLANK_URL", "");
		expect(resolveCredential("blank")).toBeNull();
	});
});

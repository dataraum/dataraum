// Unit tests for the upload content digest. Pure — uses WebCrypto (global in
// Node), no I/O. Locks the two properties dedup relies on: determinism (same
// bytes + workspace → same digest) and workspace-scoping (salt = workspace id).

import { describe, expect, it } from "vitest";

import { digestBytes } from "./digest";

const bytes = (s: string) => new TextEncoder().encode(s);

describe("digestBytes", () => {
	it("is deterministic for the same bytes + salt (and is SHA-1 hex)", async () => {
		const a = await digestBytes(bytes("id,name\n1,Ada\n"), "ws-1");
		const b = await digestBytes(bytes("id,name\n1,Ada\n"), "ws-1");
		expect(a).toBe(b);
		expect(a).toMatch(/^[0-9a-f]{40}$/);
	});

	it("is workspace-scoped: a different salt yields a different digest", async () => {
		const a = await digestBytes(bytes("same bytes"), "ws-1");
		const b = await digestBytes(bytes("same bytes"), "ws-2");
		expect(a).not.toBe(b);
	});

	it("distinguishes different content", async () => {
		const a = await digestBytes(bytes("aaa"), "ws-1");
		const b = await digestBytes(bytes("bbb"), "ws-1");
		expect(a).not.toBe(b);
	});

	it("hashes the chunked (>8MB) path deterministically", async () => {
		// 9 MB > the 8e6-byte slice threshold → the Merkle roll-up path.
		const big = new Uint8Array(9 * 1024 * 1024).fill(7);
		const a = await digestBytes(big, "ws-1");
		const b = await digestBytes(big, "ws-1");
		expect(a).toBe(b);
		expect(a).toMatch(/^[0-9a-f]{40}$/);
	});
});

// Real-Postgres integration test for the provisioner's per-workspace advisory
// lock (DAT-820) — the serialization the unit-test harness fakes away. Proves
// on a live cockpit_db that a second lifecycle op for the SAME workspace is
// refused while the first holds the lock, that DIFFERENT workspaces don't
// contend, and that release (via connection close) actually frees the key.
//
// Requires the compose Postgres. Self-skips when COCKPIT_DATABASE_URL is
// unset so unit CI without the stack stays green.

import { describe, expect, it } from "vitest";

const STACK_AVAILABLE = !!process.env.COCKPIT_DATABASE_URL;
if (!process.env.BETTER_AUTH_SECRET) {
	// config.base (loaded by the module under test) requires it; the value is
	// irrelevant to the lock.
	process.env.BETTER_AUTH_SECRET = "dataraum-dev-secret";
}

// Unique per run — the dev database is shared state.
const WS = `lock-test-${Date.now().toString(36)}`;

describe.skipIf(!STACK_AVAILABLE)("provisioner advisory lock (DAT-820)", () => {
	it("refuses a concurrent op for the same workspace, frees on release", async () => {
		const { withWorkspaceLock } = await import("./lifecycle-deps");

		let release!: () => void;
		const held = new Promise<void>((r) => {
			release = r;
		});
		let firstEntered!: () => void;
		const entered = new Promise<void>((r) => {
			firstEntered = r;
		});

		const first = withWorkspaceLock(WS, async () => {
			firstEntered();
			await held;
			return "first";
		});
		await entered;

		// Same workspace: refused while held.
		await expect(withWorkspaceLock(WS, async () => "second")).rejects.toThrow(
			/already in flight/,
		);

		// A different workspace does not contend.
		await expect(
			withWorkspaceLock(`${WS}-other`, async () => "other"),
		).resolves.toBe("other");

		release();
		await expect(first).resolves.toBe("first");

		// Released: the same key locks again.
		await expect(withWorkspaceLock(WS, async () => "again")).resolves.toBe(
			"again",
		);
	});
});

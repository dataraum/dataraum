// create-tracker (DAT-821): terminal-state semantics of the in-process
// create-run record — success deletes (the registry's `ready` says
// everything), failure keeps the message, retry overwrites.

import { describe, expect, it } from "vitest";
import { createRunFor, trackCreateRun } from "#/portal/create-tracker";

const settle = () => new Promise((resolve) => setImmediate(resolve));

describe("trackCreateRun", () => {
	it("records a running op and deletes the entry on success", async () => {
		let finish: (value: unknown) => void = () => {};
		trackCreateRun(
			"ws-ok",
			"user-1",
			new Promise((resolve) => {
				finish = resolve;
			}),
		);
		expect(createRunFor("ws-ok")).toEqual({
			userId: "user-1",
			status: "running",
		});
		finish(undefined);
		await settle();
		expect(createRunFor("ws-ok")).toBeNull();
	});

	it("keeps the failure message verbatim", async () => {
		trackCreateRun(
			"ws-fail",
			"user-1",
			Promise.reject(new Error("subdomain 'x' is already claimed")),
		);
		await settle();
		expect(createRunFor("ws-fail")).toEqual({
			userId: "user-1",
			status: "failed",
			error: "subdomain 'x' is already claimed",
		});
	});

	it("a retry overwrites a failed record with the new run", async () => {
		trackCreateRun("ws-retry", "user-1", Promise.reject(new Error("died")));
		await settle();
		trackCreateRun("ws-retry", "user-2", new Promise(() => {}));
		expect(createRunFor("ws-retry")).toEqual({
			userId: "user-2",
			status: "running",
		});
	});

	it("returns null for an untracked workspace", () => {
		expect(createRunFor("ws-unknown")).toBeNull();
	});

	it("a stale run's settlement never clobbers the newer run's record", async () => {
		// Double-submit race (TOCTOU past the "already running" guard's awaits):
		// the LOSER hits the advisory lock and rejects near-instantly while the
		// real run is live — its failure must not overwrite the live record,
		// and its success must not delete it either.
		let failLoser: (err: Error) => void = () => {};
		trackCreateRun(
			"ws-race",
			"u1",
			new Promise((_, reject) => {
				failLoser = reject;
			}),
		);
		// The newer (winning) run replaces the entry.
		trackCreateRun("ws-race", "u1", new Promise(() => {}));
		failLoser(new Error("a lifecycle operation is already in flight"));
		await settle();
		expect(createRunFor("ws-race")).toEqual({
			userId: "u1",
			status: "running",
		});
	});
});

// Unit tests for the JourneyWorkflow entry trigger (DAT-529). Mock #/config and
// @temporalio/client at the seam (no Temporal in units).

import { beforeEach, describe, expect, it, vi } from "vitest";

const h = vi.hoisted(() => ({
	config: {
		temporalHost: "localhost:7233",
		temporalNamespace: "default",
		cockpitOrchestrationTaskQueue: "cockpit-orchestration",
	} as Record<string, unknown>,
	signalWithStart: vi.fn(async () => ({})),
	close: vi.fn(async () => {}),
}));

vi.mock("#/config", () => ({
	get config() {
		return h.config;
	},
}));
vi.mock("@temporalio/client", () => ({
	Connection: { connect: vi.fn(async () => ({ close: h.close })) },
	Client: vi.fn(function Client() {
		return { workflow: { signalWithStart: h.signalWithStart } };
	}),
}));

import { signalVerticalEstablished } from "./journey-trigger";

beforeEach(() => {
	h.config = {
		temporalHost: "localhost:7233",
		temporalNamespace: "default",
		cockpitOrchestrationTaskQueue: "cockpit-orchestration",
	};
	h.signalWithStart.mockClear();
	h.close.mockClear();
});

describe("signalVerticalEstablished (DAT-529)", () => {
	it("signals verticalEstablished with-start on the per-workspace journey id", async () => {
		const wfId = await signalVerticalEstablished("ws-1", "finance");

		expect(wfId).toBe("journey-ws-1");
		expect(h.signalWithStart).toHaveBeenCalledWith(
			"journeyWorkflow",
			expect.objectContaining({
				taskQueue: "cockpit-orchestration",
				workflowId: "journey-ws-1",
				args: ["ws-1"],
				signal: "verticalEstablished",
				signalArgs: [{ vertical: "finance" }],
			}),
		);
		expect(h.close).toHaveBeenCalled();
	});

	it("fails loud when Temporal isn't configured", async () => {
		h.config = {};
		await expect(signalVerticalEstablished("ws-1", "finance")).rejects.toThrow(
			/not configured/,
		);
		expect(h.signalWithStart).not.toHaveBeenCalled();
	});
});

// Unit tests for the add_source TRIGGER (DAT-352; one-call select DAT-436; routed
// through the journey in DAT-551). The trigger no longer starts the workflow
// directly — it signals the per-workspace JourneyWorkflow (`runAddSource`), which
// records the run + starts the engine child. So the unit asserts the SIGNAL
// payload, not a workflow.start. Mocked seams: #/config, the registry (workspace +
// vertical + queue), the journey trigger (signalRunAddSource), and the ALS
// conversation context.

import { beforeEach, describe, expect, it, vi } from "vitest";

const WS = "00000000-0000-0000-0000-000000000001";

const h = vi.hoisted(() => ({
	config: {
		dataraumWorkspaceId: "00000000-0000-0000-0000-000000000001",
		temporalHost: "localhost:7233",
		temporalNamespace: "default",
	} as Record<string, unknown>,
	vertical: "_adhoc" as string,
	conversationId: "conv-1" as string | null,
	signalled: null as {
		workspaceId: string;
		req: Record<string, unknown>;
	} | null,
	signalRunAddSource: vi.fn(
		async (workspaceId: string, req: Record<string, unknown>) => {
			h.signalled = { workspaceId, req };
			return req.workflowId as string;
		},
	),
}));

// Live getter (the unconfigured-guard test reassigns h.config).
vi.mock("#/config", () => ({
	get config() {
		return h.config;
	},
}));
vi.mock("#/db/cockpit/registry", () => ({
	resolveActiveWorkspaceRow: vi.fn(async () => ({
		id: h.config.dataraumWorkspaceId,
		// Per-workspace engine queue (DAT-505) — the journey runs the child here.
		taskQueue: `engine-${h.config.dataraumWorkspaceId}`,
		vertical: h.vertical,
	})),
}));
vi.mock("#/temporal/journey-trigger", () => ({
	signalRunAddSource: h.signalRunAddSource,
}));
vi.mock("#/lib/run-context", () => ({
	currentConversationId: () => h.conversationId,
}));

import { triggerAddSource } from "./trigger-add-source";

beforeEach(() => {
	h.config = {
		dataraumWorkspaceId: WS,
		temporalHost: "localhost:7233",
		temporalNamespace: "default",
	};
	h.vertical = "_adhoc";
	h.conversationId = "conv-1";
	h.signalled = null;
	h.signalRunAddSource.mockClear();
});

describe("triggerAddSource (DAT-352, one-call DAT-436, routed via the journey — DAT-551)", () => {
	it("signals the journey with the source SET / queue / verticals + kind onboarding", async () => {
		h.vertical = "financial_reporting";
		const result = await triggerAddSource({ sources: ["src-1"] });
		const cockpitSessionId = result.cockpit_session_id;

		expect(h.signalled?.workspaceId).toBe(WS);
		expect(h.signalled?.req).toEqual({
			sessionId: cockpitSessionId,
			workflowId: `addsource-${WS}-${cockpitSessionId}`,
			engineTaskQueue: `engine-${WS}`,
			sources: ["src-1"],
			verticals: ["financial_reporting"],
			kind: "onboarding",
			conversationId: "conv-1",
		});
		// The trigger returns the deterministic workflow id (run_id mirrors it — the
		// journey owns the real execution id; progress resolves latest by id).
		expect(result).toEqual({
			workflow_id: `addsource-${WS}-${cockpitSessionId}`,
			run_id: `addsource-${WS}-${cockpitSessionId}`,
			sources: ["src-1"],
			cockpit_session_id: cockpitSessionId,
		});
	});

	it("threads a NULL conversationId when outside a chat turn (non-narrating run)", async () => {
		h.conversationId = null;
		await triggerAddSource({ sources: ["src-1"] });
		expect(h.signalled?.req.conversationId).toBeNull();
	});

	it("throws when Temporal is unconfigured and signals nothing", async () => {
		h.config = { dataraumWorkspaceId: WS };
		await expect(triggerAddSource({ sources: ["src-1"] })).rejects.toThrow(
			/Temporal client is not configured/,
		);
		expect(h.signalRunAddSource).not.toHaveBeenCalled();
	});
});

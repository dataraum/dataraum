// Unit tests for the add_source TRIGGER (DAT-352; one-call select DAT-436; DAT-609).
// The trigger starts the per-workspace `groundingLoopWorkflow` (import + autonomous
// teach loop). So the unit asserts the START payload, not a journey signal. Mocked
// seams: #/config, the registry (workspace + vertical + queue), the orchestration
// trigger (startGroundingLoop), and the ALS conversation context.

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
	started: null as Record<string, unknown> | null,
	startGroundingLoop: vi.fn(async (input: Record<string, unknown>) => {
		h.started = input;
	}),
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
		// Per-workspace engine queue (DAT-505) — the workflow runs the child here.
		taskQueue: `engine-${h.config.dataraumWorkspaceId}`,
		vertical: h.vertical,
	})),
}));
vi.mock("#/temporal/orchestration-trigger", () => ({
	startGroundingLoop: h.startGroundingLoop,
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
	h.started = null;
	h.startGroundingLoop.mockClear();
});

describe("triggerAddSource (DAT-352, one-call DAT-436, grounding loop — DAT-609)", () => {
	it("starts the grounding loop with the source SET / queue / verticals", async () => {
		h.vertical = "financial_reporting";
		const result = await triggerAddSource({ sources: ["src-1"] });

		// One workflow id per workspace (DAT-562) — no minted session segment.
		expect(h.started).toEqual({
			workspaceId: WS,
			workflowId: `addsource-${WS}`,
			engineTaskQueue: `engine-${WS}`,
			sources: ["src-1"],
			verticals: ["financial_reporting"],
			conversationId: "conv-1",
		});
		// The trigger returns the deterministic engine workflow id (run_id mirrors it —
		// the workflow owns the real execution id; progress resolves latest by id).
		expect(result).toEqual({
			workflow_id: `addsource-${WS}`,
			run_id: `addsource-${WS}`,
			sources: ["src-1"],
		});
	});

	it("threads a NULL conversationId when outside a chat turn (non-narrating run)", async () => {
		h.conversationId = null;
		await triggerAddSource({ sources: ["src-1"] });
		expect(h.started?.conversationId).toBeNull();
	});

	it("throws when Temporal is unconfigured and starts nothing", async () => {
		h.config = { dataraumWorkspaceId: WS };
		await expect(triggerAddSource({ sources: ["src-1"] })).rejects.toThrow(
			/Temporal client is not configured/,
		);
		expect(h.startGroundingLoop).not.toHaveBeenCalled();
	});
});

// Pin the hand-mirrored cockpit queue convention (DAT-818). The engine derives
// the identical name in `dataraum.worker.contracts.cockpit_task_queue_for`
// (pinned by test_workflow_ids.py); the literal here makes a drift on this side
// fail a test, not strand live callbacks on an unpolled queue.

import { describe, expect, it } from "vitest";

import { cockpitTaskQueueFor } from "./task-queue";

describe("cockpitTaskQueueFor (DAT-818)", () => {
	it("derives cockpit-<workspace_id>", () => {
		const WS = "00000000-0000-0000-0000-000000000001";
		expect(cockpitTaskQueueFor(WS)).toBe(`cockpit-${WS}`);
	});

	it("keeps two workspaces on distinct queues", () => {
		expect(cockpitTaskQueueFor("ws-1")).not.toBe(cockpitTaskQueueFor("ws-2"));
	});
});

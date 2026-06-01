import { describe, expect, it } from "vitest";

import { addSourceWorkflowId } from "./workflow-id";

// The parent workflow ID encodes workspace_id as its first segment (DAT-364),
// and MUST agree with the Python worker's `add_source_workflow_id` (the worker
// builds child IDs off the same prefix). These pin the format + the
// cross-workspace distinctness the convention exists to guarantee.

const WS_A = "12345678-1234-1234-1234-123456789abc";
const WS_B = "00000000-0000-0000-0000-000000000001";
const SOURCE = "src-7";

describe("addSourceWorkflowId (DAT-364)", () => {
	it("encodes workspace then source", () => {
		expect(addSourceWorkflowId(WS_A, SOURCE)).toBe(
			`addsource-${WS_A}-${SOURCE}`,
		);
	});

	it("does not collide across workspaces sharing a source_id", () => {
		expect(addSourceWorkflowId(WS_A, SOURCE)).not.toBe(
			addSourceWorkflowId(WS_B, SOURCE),
		);
	});
});

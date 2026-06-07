import { describe, expect, it } from "vitest";

import { addSourceWorkflowId, operatingModelWorkflowId } from "./workflow-id";

// The parent workflow ID encodes workspace_id as its first segment (DAT-364) and
// is keyed by the run's session_id (DAT-422 — a run is over a SET of objects from
// 1–N sources, not one source). It MUST agree with the Python worker's
// `add_source_workflow_id` (the worker builds child IDs off the same prefix).
// These pin the format + the cross-workspace distinctness the convention exists
// to guarantee.

const WS_A = "12345678-1234-1234-1234-123456789abc";
const WS_B = "00000000-0000-0000-0000-000000000001";
const SESSION = "sess-7";

describe("addSourceWorkflowId (DAT-364, DAT-422)", () => {
	it("encodes workspace then session", () => {
		expect(addSourceWorkflowId(WS_A, SESSION)).toBe(
			`addsource-${WS_A}-${SESSION}`,
		);
	});

	it("does not collide across workspaces sharing a session_id", () => {
		expect(addSourceWorkflowId(WS_A, SESSION)).not.toBe(
			addSourceWorkflowId(WS_B, SESSION),
		);
	});
});

describe("operatingModelWorkflowId (DAT-438)", () => {
	it("encodes workspace then session", () => {
		expect(operatingModelWorkflowId(WS_A, SESSION)).toBe(
			`operatingmodel-${WS_A}-${SESSION}`,
		);
	});

	it("does not collide across workspaces sharing a session_id", () => {
		expect(operatingModelWorkflowId(WS_A, SESSION)).not.toBe(
			operatingModelWorkflowId(WS_B, SESSION),
		);
	});
});

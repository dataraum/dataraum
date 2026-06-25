import { describe, expect, it } from "vitest";

import {
	addSourceWorkflowId,
	beginSessionWorkflowId,
	groundingLoopWorkflowId,
	operatingModelWorkflowId,
	sessionCascadeWorkflowId,
} from "./workflow-id";

// The parent workflow ID is `<stage>-<workspace_id>` — ONE id per stage per
// workspace (DAT-562 retired the per-import session segment). It MUST agree with the
// Python worker's prefix convention (the worker builds child IDs off the same
// prefix). These pin the format + the cross-workspace distinctness the convention
// exists to guarantee.

const WS_A = "12345678-1234-1234-1234-123456789abc";
const WS_B = "00000000-0000-0000-0000-000000000001";

describe("addSourceWorkflowId (DAT-364, DAT-562)", () => {
	it("encodes the stage then the workspace", () => {
		expect(addSourceWorkflowId(WS_A)).toBe(`addsource-${WS_A}`);
	});

	it("does not collide across workspaces", () => {
		expect(addSourceWorkflowId(WS_A)).not.toBe(addSourceWorkflowId(WS_B));
	});
});

describe("beginSessionWorkflowId (DAT-409, DAT-562)", () => {
	it("encodes the stage then the workspace", () => {
		expect(beginSessionWorkflowId(WS_A)).toBe(`beginsession-${WS_A}`);
	});
});

describe("operatingModelWorkflowId (DAT-438, DAT-562)", () => {
	it("encodes the stage then the workspace", () => {
		expect(operatingModelWorkflowId(WS_A)).toBe(`operatingmodel-${WS_A}`);
	});

	it("does not collide across workspaces", () => {
		expect(operatingModelWorkflowId(WS_A)).not.toBe(
			operatingModelWorkflowId(WS_B),
		);
	});
});

describe("stage ids are distinct within one workspace", () => {
	it("never collide across stages", () => {
		const ids = new Set([
			addSourceWorkflowId(WS_A),
			beginSessionWorkflowId(WS_A),
			operatingModelWorkflowId(WS_A),
		]);
		expect(ids.size).toBe(3);
	});
});

describe("orchestration workflow ids (DAT-609)", () => {
	it("encode the orchestration stage then the workspace", () => {
		expect(groundingLoopWorkflowId(WS_A)).toBe(`grounding-${WS_A}`);
		expect(sessionCascadeWorkflowId(WS_A)).toBe(`session-${WS_A}`);
	});

	it("do not collide across workspaces", () => {
		expect(groundingLoopWorkflowId(WS_A)).not.toBe(
			groundingLoopWorkflowId(WS_B),
		);
		expect(sessionCascadeWorkflowId(WS_A)).not.toBe(
			sessionCascadeWorkflowId(WS_B),
		);
	});

	it("are distinct from the engine ids they wrap (no orchestration↔child clash)", () => {
		const ids = new Set([
			groundingLoopWorkflowId(WS_A),
			sessionCascadeWorkflowId(WS_A),
			addSourceWorkflowId(WS_A),
			beginSessionWorkflowId(WS_A),
			operatingModelWorkflowId(WS_A),
		]);
		expect(ids.size).toBe(5);
	});
});

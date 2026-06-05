// Unit tests for the shared model-only refs helper (DAT-437) — the clean bubble
// vs. marked refs split that keeps internal ids out of every visible bubble.

import { describe, expect, it } from "vitest";

import {
	AGENT_REFS_MARKER,
	agentRefsBlock,
	isAgentRefsPart,
	turnWithRefs,
} from "#/lib/agent-refs";

describe("agentRefsBlock / isAgentRefsPart", () => {
	it("marks a refs body and the predicate flags it", () => {
		const block = agentRefsBlock('column_id for "amount": c_123');
		expect(block.startsWith(AGENT_REFS_MARKER)).toBe(true);
		expect(block).toContain("c_123");
		expect(isAgentRefsPart(block)).toBe(true);
	});

	it("does not flag a normal bubble or human-typed text", () => {
		expect(isAgentRefsPart('Explain the readiness for column "amount".')).toBe(
			false,
		);
		expect(isAgentRefsPart("what does [[dataraum:refs]] mean?")).toBe(false);
	});
});

describe("turnWithRefs — the two-part turn", () => {
	it("carries a clean bubble first and the marked refs part second", () => {
		const turn = turnWithRefs(
			'Explain the readiness for column "amount" using the why_column tool.',
			'column_id for "amount": c_123',
		);
		expect(turn.content).toHaveLength(2);
		const [bubble, refs] = turn.content;
		expect(bubble?.type).toBe("text");
		expect(bubble?.content).not.toContain("c_123");
		expect(bubble?.content).not.toContain(AGENT_REFS_MARKER);
		expect(refs?.type).toBe("text");
		expect(refs && isAgentRefsPart(refs.content)).toBe(true);
		expect(refs?.content).toContain("c_123");
	});
});

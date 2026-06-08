// Tests for the shared agent-actionable error envelope (consistency pass 2).

import { describe, expect, it } from "vitest";
import { z } from "zod";

import {
	AgentActionableError,
	asAgentError,
	catchActionable,
	isAgentError,
	withAgentError,
} from "./agent-error";

describe("isAgentError", () => {
	it("narrows the { error: string } shape only", () => {
		expect(isAgentError({ error: "bad SQL" })).toBe(true);
		expect(isAgentError({ error: 42 })).toBe(false);
		expect(isAgentError({ columns: [], rows: [] })).toBe(false);
		expect(isAgentError(null)).toBe(false);
		expect(isAgentError("nope")).toBe(false);
	});
});

describe("withAgentError", () => {
	it("accepts the success shape OR the error branch", () => {
		const schema = withAgentError(z.object({ ok: z.boolean() }));
		expect(schema.safeParse({ ok: true }).success).toBe(true);
		expect(schema.safeParse({ error: "stale id" }).success).toBe(true);
		expect(schema.safeParse({ nope: 1 }).success).toBe(false);
	});
});

describe("asAgentError (read/query tools — catch-all)", () => {
	it("passes a success through unchanged", async () => {
		await expect(asAgentError(async () => ({ rows: 3 }))).resolves.toEqual({
			rows: 3,
		});
	});

	it("converts ANY throw into { error } (turn survives a bad query)", async () => {
		const out = await asAgentError(async () => {
			throw new Error('Parser Error: syntax error at "FROM"');
		});
		expect(out).toEqual({ error: 'Parser Error: syntax error at "FROM"' });
	});
});

describe("catchActionable (write/compute tools — selective)", () => {
	it("converts AgentActionableError into { error }", async () => {
		const out = await catchActionable(async () => {
			throw new AgentActionableError("validation_id already declared");
		});
		expect(out).toEqual({ error: "validation_id already declared" });
	});

	it("lets infra errors propagate (throws)", async () => {
		await expect(
			catchActionable(async () => {
				throw new Error("ECONNREFUSED postgres");
			}),
		).rejects.toThrow("ECONNREFUSED postgres");
	});
});

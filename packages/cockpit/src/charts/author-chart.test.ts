// Mock the env config so importing the module (which reads the Anthropic key for
// the live emit path) doesn't parse real env — the tests inject `emit`, so the
// live path never runs (cockpit vitest rule: a module pulling #/config must mock it).
import { describe, expect, it, vi } from "vitest";

vi.mock("#/config", () => ({ config: { anthropicApiKey: "sk-ant-test" } }));
// Mode-shared base config (DAT-819) — reached transitively via the
// registry/db seam; parsing the real one needs env this test does not set.
vi.mock("#/config.base", () => ({ baseConfig: {} }));

import {
	type AuthorMessage,
	authorChart,
	CHART_AUTHOR_MAX_ATTEMPTS,
	type EmitFn,
} from "./author-chart";
import type { ChartConfig } from "./chart-config";

const COLUMNS = [
	{ name: "month", type: "temporal" },
	{ name: "revenue", type: "quantitative" },
];

const validConfig: ChartConfig = {
	mark: "bar",
	encoding: {
		x: { field: "month", type: "temporal" },
		y: { field: "revenue", type: "quantitative", aggregate: "sum" },
	},
};

/** An emit that returns a scripted sequence of emissions, recording the messages
 * it was handed each call (to assert the breaker fed errors back). */
function scriptedEmit(sequence: unknown[]): {
	emit: EmitFn;
	calls: AuthorMessage[][];
} {
	const calls: AuthorMessage[][] = [];
	let i = 0;
	const emit: EmitFn = async (_prompts, messages) => {
		// Snapshot the conversation as handed to this attempt.
		calls.push(messages.map((m) => ({ ...m })));
		return sequence[i++];
	};
	return { emit, calls };
}

describe("authorChart circuit breaker", () => {
	it("returns the config on a first valid emission", async () => {
		const { emit, calls } = scriptedEmit([validConfig]);
		const res = await authorChart({
			columns: COLUMNS,
			instruction: "revenue by month",
			emit,
		});
		expect(res.ok).toBe(true);
		if (res.ok) expect(res.config.mark).toBe("bar");
		expect(calls).toHaveLength(1);
	});

	it("retries with the validation error fed back, then succeeds", async () => {
		// First emission references a non-existent column → rejected by the gate;
		// second is valid.
		const bad = {
			mark: "bar",
			encoding: {
				x: { field: "month", type: "temporal" },
				y: { field: "profit", type: "quantitative" },
			},
		};
		const { emit, calls } = scriptedEmit([bad, validConfig]);
		const res = await authorChart({
			columns: COLUMNS,
			instruction: "revenue by month",
			emit,
		});
		expect(res.ok).toBe(true);
		expect(calls).toHaveLength(2);
		// The retry conversation carries the prior (bad) emission + the rejection,
		// so the model can correct it.
		const retryMessages = calls[1];
		expect(retryMessages.length).toBeGreaterThan(1);
		expect(retryMessages.at(-1)?.content).toMatch(/rejected/);
		expect(retryMessages.at(-1)?.content).toContain("profit");
	});

	it("gives up after the attempt ceiling with an actionable error", async () => {
		const bad = { mark: "pie", encoding: {} }; // never valid (bad mark, no axes)
		const { emit, calls } = scriptedEmit([bad, bad, bad, bad]);
		const res = await authorChart({
			columns: COLUMNS,
			instruction: "make it pretty",
			emit,
		});
		expect(res.ok).toBe(false);
		if (!res.ok) {
			expect(res.error).toContain(String(CHART_AUTHOR_MAX_ATTEMPTS));
			expect(res.error).toMatch(/manually/);
		}
		expect(calls).toHaveLength(CHART_AUTHOR_MAX_ATTEMPTS);
	});

	it("treats a no-tool-call emission as a failed attempt", async () => {
		const { emit, calls } = scriptedEmit([undefined, validConfig]);
		const res = await authorChart({
			columns: COLUMNS,
			instruction: "revenue by month",
			emit,
		});
		expect(res.ok).toBe(true);
		expect(calls).toHaveLength(2);
	});
});

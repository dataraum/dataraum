import { describe, expect, it } from "vitest";
import type { ConversationKind } from "#/db/cockpit/conversations";
import { JOURNEY_STAGES } from "#/journey/stages";
import { getInstructions } from "#/prompts/orchestrator";

const KINDS: ReadonlyArray<ConversationKind> = ["connect", "stage", "analyse"];

describe("per-type instructions (DAT-532)", () => {
	it("is byte-stable per kind (the prompt-cache invariant)", () => {
		for (const kind of KINDS) {
			expect(getInstructions(kind)).toBe(getInstructions(kind));
		}
	});

	it("shares mission/workspace_model/canvas/naming/voice byte-for-byte across kinds", () => {
		// Only <journey> + <tools> differ; the shared sections must be identical, so
		// the cached prefix is the same shape per kind and the naming rules can't
		// drift between types.
		const section = (s: string, tag: string) =>
			s.slice(s.indexOf(`<${tag}>`), s.indexOf(`</${tag}>`) + tag.length + 3);
		for (const tag of [
			"mission",
			"workspace_model",
			"canvas",
			"naming",
			"voice",
		]) {
			const connect = section(getInstructions("connect"), tag);
			expect(connect.length).toBeGreaterThan(0);
			for (const kind of KINDS) {
				expect(section(getInstructions(kind), tag)).toBe(connect);
			}
		}
	});

	it("names every journey stage across the kinds; each kind names its own arc", () => {
		const union = KINDS.map(getInstructions).join("\n");
		for (const stage of JOURNEY_STAGES) {
			expect(union).toContain(stage.id);
		}
		// Each kind owns its stages and not the others' acting stages.
		expect(getInstructions("connect")).toContain("add_source");
		expect(getInstructions("stage")).toContain("operating_model");
		expect(getInstructions("analyse")).toContain("answer");
	});

	it("fences the toolstack per kind in the prompt (journey/tools differ)", () => {
		// Connect drives select/import but not the session; Analyse is answer-only,
		// no raw run_sql; Stage owns begin_session + run_sql, not answer/select.
		const connect = getInstructions("connect");
		const stage = getInstructions("stage");
		const analyse = getInstructions("analyse");

		expect(connect).toContain("Calling select STARTS the import");
		expect(connect).not.toContain("begin_session");
		// One-step select: no retired "Add source" button, no approval/confirm step.
		expect(connect).not.toContain("Add source button");
		expect(connect).not.toContain('"Add source"');
		expect(connect).not.toContain("wait for confirmation");

		expect(stage).toContain("begin_session");
		expect(stage).toContain("run_sql");
		expect(stage).not.toContain("answer —");

		expect(analyse).toContain("answer —");
		// answer is the analytical surface — analyse has no raw run_sql.
		expect(analyse).toContain("you do NOT have raw run_sql");
		expect(analyse).not.toContain("select STARTS");
		expect(analyse).not.toContain("begin_session");
	});

	it("keeps the naming rules (physical_name / src_ guard) in every kind", () => {
		for (const kind of KINDS) {
			const prompt = getInstructions(kind);
			expect(prompt).toContain("physical_name");
			expect(prompt).toContain("lake.<layer>.<physical_name>");
			expect(prompt).toContain('"src_" followed by 40 hex characters');
			expect(prompt).toContain("name the FILE");
		}
	});
});

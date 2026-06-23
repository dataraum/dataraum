import { describe, expect, it } from "vitest";
import type { ConversationKind } from "#/db/cockpit/conversations";
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

	it("each kind names the stages it drives (DAT-597: onboarding is hub-driven, not chat-narrated)", () => {
		// The onboarding stages (connect/frame/select/add_source) are driven by the
		// staging-hub widget, not narrated as chat stages — so each chat kind names
		// only the stages IT drives. Stage owns begin_session + operating_model;
		// Analyse owns answer; Connect is the teach surface (teach + replay).
		expect(getInstructions("stage")).toContain("begin_session");
		expect(getInstructions("stage")).toContain("operating_model");
		expect(getInstructions("analyse")).toContain("answer");
		const connect = getInstructions("connect");
		expect(connect).toContain("teach");
		expect(connect).toContain("replay");
	});

	it("fences the toolstack per kind in the prompt (journey/tools differ)", () => {
		// Connect is the TEACH surface (DAT-597) — teach + replay, no acquisition
		// tools, no session; Analyse is answer-only, no raw run_sql; Stage owns
		// begin_session + run_sql, not answer.
		const connect = getInstructions("connect");
		const stage = getInstructions("stage");
		const analyse = getInstructions("analyse");

		expect(connect).toContain("teach");
		expect(connect).toContain("replay");
		// The ONE retained opener — re-mounts the staging hub on the canvas (DAT-597
		// follow-up); the acquisition LOGIC tools stay removed.
		expect(connect).toContain("open_staging_hub");
		expect(connect).not.toContain("begin_session");
		// The removed acquisition surface must not be advertised any more.
		expect(connect).not.toContain("Calling select STARTS the import");
		for (const removed of ["use_vertical", "list_verticals", "open_probe"]) {
			expect(connect).not.toContain(removed);
		}

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

// open_probe tool (DAT-576) — a UI tool: it opens an editable SQL probe panel in
// the workspace canvas so the user can pick a configured database source and write
// + run read-only SQL against it BEFORE importing. It does no server work; the
// result projects the probe widget (tool-result-to-canvas), and the panel runs
// queries on its own streaming channel (/api/probe-sql).
//
// Mirrors `upload`: a no-arg opener. To pre-fill the panel with a specific query,
// the agent uses `probe` instead (it runs a sample AND seeds the same panel).

import { toolDefinition } from "@tanstack/ai";
import { z } from "zod";

export const openProbeTool = toolDefinition({
	name: "open_probe",
	description:
		"Open an editable SQL probe panel so the user can write and run read-only SQL " +
		"against a configured database source BEFORE importing it. Call this whenever the " +
		"user wants to explore or query a connected database directly, or write SQL " +
		"themselves. The user picks the source and writes the query in the panel — you " +
		"don't run it here. To pre-fill a specific query for them, use `probe` instead " +
		"(it seeds the same panel).",
	inputSchema: z.object({}),
	outputSchema: z.object({ ready: z.boolean() }),
}).server(() => ({ ready: true }));

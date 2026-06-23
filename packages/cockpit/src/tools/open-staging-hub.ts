// open_staging_hub tool (DAT-597 follow-up) — a UI opener: (re)mounts the staging
// hub on the Connect canvas.
//
// The hub is the Connect chat's DEFAULT canvas (cockpit-state), but ANY projecting
// tool — list_sources, look_table, why_column — replaces it, and DAT-597 removed
// the acquisition openers, leaving no way back. This restores ONE opener so the
// agent can bring the hub back when the user wants to add/import data. It does no
// server work: the result projects the `probe` canvas (tool-result-to-canvas), and
// the hub runs its own probe/import channels. It is NOT an acquisition tool — the
// chat still doesn't assemble/frame/import (that's the hub itself); it only opens it.

import { toolDefinition } from "@tanstack/ai";
import { z } from "zod";

export const openStagingHubTool = toolDefinition({
	name: "open_staging_hub",
	description:
		"Open the staging hub on the canvas — the panel where the user browses configured " +
		"database sources and uploaded files, writes/edits read-only SQL, frames or adopts a " +
		"model, and clicks Import. The hub is this chat's default canvas; call this to BRING " +
		"IT BACK whenever the user wants to add or import data, or after another widget " +
		"(list_sources, look_table, …) replaced the canvas. You don't import here — the user " +
		"does it in the hub; this just makes it visible again.",
	inputSchema: z.object({}),
	outputSchema: z.object({ opened: z.boolean() }),
}).server(() => ({ opened: true }));

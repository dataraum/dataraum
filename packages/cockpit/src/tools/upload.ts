// upload tool (redesign) — a UI tool: it opens a file-upload area in the
// workspace canvas so the user can add files from their computer. It does no
// server work; the result projects the upload-area widget
// (tool-result-to-canvas), the user drops files there, and the dropzone drives
// the existing connect flow. Kept OFF the chat rail so uploads aren't a permanent
// fixture — most data comes from configured external systems, not local files.

import { toolDefinition } from "@tanstack/ai";
import { z } from "zod";

export const uploadTool = toolDefinition({
	name: "upload",
	description:
		"Open a file-upload area in the workspace so the user can drop CSV / Parquet / " +
		"JSON files from their computer to import. Call this whenever the user asks to " +
		"upload, add, or import files they have locally. You don't receive the files " +
		"here — the user drops them and the connect/import flow proceeds from there.",
	inputSchema: z.object({}),
	outputSchema: z.object({ ready: z.boolean() }),
}).server(() => ({ ready: true }));

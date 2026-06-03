// The agent-tier tool registry (DAT-353) — the array passed to
// `chat({ tools })` on the server route. Hand-written and explicit: adding a
// tool is one import + one entry here, no auto-discovery.
//
// Read tools (list_*) run unattended; write/compute tools (frame, select, teach,
// replay) declare `needsApproval` and are gated by the user in the UI before
// they run.

import { connectTool } from "./connect";
import { frameTool } from "./frame";
import { listSourcesTool } from "./list-sources";
import { listTablesTool } from "./list-tables";
import { lookTableTool } from "./look-table";
import { probeTool } from "./probe";
import { replayTool } from "./replay";
import { runSqlTool } from "./run_sql";
import { selectTool } from "./select";
import { teachTool } from "./teach";
import { whyColumnTool } from "./why-column";
import { workflowStatusTool } from "./workflow-status";

export const tools = [
	listSourcesTool,
	listTablesTool,
	lookTableTool,
	whyColumnTool,
	runSqlTool,
	probeTool,
	connectTool,
	frameTool,
	selectTool,
	teachTool,
	replayTool,
	workflowStatusTool,
] as const;

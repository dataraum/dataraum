// The agent-tier tool registry (DAT-353) — the array passed to
// `chat({ tools })` on the server route. Hand-written and explicit: adding a
// tool is one import + one entry here, no auto-discovery.
//
// Read tools (list_*) run unattended; write/compute tools (teach, replay)
// declare `needsApproval` and are gated by the user in the UI before they run.

import { listSourcesTool } from "./list-sources";
import { listTablesTool } from "./list-tables";
import { probeTool } from "./probe";
import { replayTool } from "./replay";
import { runSqlTool } from "./run_sql";
import { teachTool } from "./teach";

export const tools = [
	listSourcesTool,
	listTablesTool,
	runSqlTool,
	probeTool,
	teachTool,
	replayTool,
] as const;

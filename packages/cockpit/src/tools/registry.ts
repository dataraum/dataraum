// The agent-tier tool registry (DAT-353) — the array passed to
// `chat({ tools })` on the server route. Hand-written and explicit: adding a
// tool is one import + one entry here, no auto-discovery.
//
// Read tools (list_*) run unattended; write/compute tools (frame, select, teach,
// replay) declare `needsApproval` and are gated by the user in the UI before
// they run.

import { beginSessionTool } from "./begin-session";
import { connectTool } from "./connect";
import { frameTool } from "./frame";
import { listSourcesTool } from "./list-sources";
import { listTablesTool } from "./list-tables";
import { listVerticalsTool } from "./list-verticals";
import { lookCycleTool } from "./look-cycle";
import { lookRelationshipsTool } from "./look-relationships";
import { lookTableTool } from "./look-table";
import { lookValidationTool } from "./look-validation";
import { operatingModelTool } from "./operating-model";
import { probeTool } from "./probe";
import { replayTool } from "./replay";
import { runSqlTool } from "./run_sql";
import { selectTool } from "./select";
import { teachTool } from "./teach";
import { teachCycleTool } from "./teach-cycle";
import { teachValidationTool } from "./teach-validation";
import { uploadTool } from "./upload";
import { whyColumnTool } from "./why-column";
import { whyCycleTool } from "./why-cycle";
import { whyRelationshipTool } from "./why-relationship";
import { whyTableTool } from "./why-table";
import { whyValidationTool } from "./why-validation";
import { workflowStatusTool } from "./workflow-status";

export const tools = [
	listSourcesTool,
	listTablesTool,
	listVerticalsTool,
	uploadTool,
	lookTableTool,
	whyColumnTool,
	whyTableTool,
	lookRelationshipsTool,
	whyRelationshipTool,
	runSqlTool,
	probeTool,
	connectTool,
	frameTool,
	selectTool,
	teachTool,
	beginSessionTool,
	teachValidationTool,
	teachCycleTool,
	operatingModelTool,
	lookValidationTool,
	whyValidationTool,
	lookCycleTool,
	whyCycleTool,
	replayTool,
	workflowStatusTool,
] as const;

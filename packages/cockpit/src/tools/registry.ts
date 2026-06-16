// The agent-tier tool registry (DAT-353) — the array passed to
// `chat({ tools })` on the server route. Hand-written and explicit: adding a
// tool is one import + one entry here, no auto-discovery.
//
// Read tools and write/compute tools (frame, select, teach, replay) alike run
// directly when the agent calls them — there is no approval gate; the user's
// instruction is the consent.

import { beginSessionTool } from "./begin-session";
import { connectTool } from "./connect";
import { frameTool } from "./frame";
import { listSourcesTool } from "./list-sources";
import { listTablesTool } from "./list-tables";
import { listVerticalsTool } from "./list-verticals";
import { lookCycleTool } from "./look-cycle";
import { lookMetricTool } from "./look-metric";
import { lookProfileTool } from "./look-profile";
import { lookRelationshipsTool } from "./look-relationships";
import { lookTableTool } from "./look-table";
import { lookValidationTool } from "./look-validation";
import { operatingModelTool } from "./operating-model";
import { probeTool } from "./probe";
import { answerTool } from "./query";
import { replayTool } from "./replay";
import { runSqlTool } from "./run_sql";
import { selectTool } from "./select";
import { teachTool } from "./teach";
import { teachCycleTool } from "./teach-cycle";
import { teachMetricTool } from "./teach-metric";
import { teachValidationTool } from "./teach-validation";
import { uploadTool } from "./upload";
import { useVerticalTool } from "./use-vertical";
import { whyColumnTool } from "./why-column";
import { whyCycleTool } from "./why-cycle";
import { whyMetricTool } from "./why-metric";
import { whyRelationshipTool } from "./why-relationship";
import { whyTableTool } from "./why-table";
import { whyValidationTool } from "./why-validation";

export const tools = [
	listSourcesTool,
	listTablesTool,
	listVerticalsTool,
	useVerticalTool,
	uploadTool,
	lookTableTool,
	lookProfileTool,
	whyColumnTool,
	whyTableTool,
	lookRelationshipsTool,
	whyRelationshipTool,
	runSqlTool,
	answerTool,
	probeTool,
	connectTool,
	frameTool,
	selectTool,
	teachTool,
	beginSessionTool,
	teachValidationTool,
	teachCycleTool,
	teachMetricTool,
	operatingModelTool,
	lookValidationTool,
	whyValidationTool,
	lookCycleTool,
	whyCycleTool,
	lookMetricTool,
	whyMetricTool,
	replayTool,
] as const;

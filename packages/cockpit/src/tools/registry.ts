// The agent-tier tool registry (DAT-353) — the tools passed to `chat({ tools })`
// on the server route. Hand-written and explicit: adding a tool is one import +
// one entry here, no auto-discovery.
//
// Read tools and write/compute tools (frame, select, teach, replay) alike run
// directly when the agent calls them — there is no approval gate; the user's
// instruction is the consent.
//
// Per-type skills (DAT-532): the toolstack is FENCED per chat `kind` —
// `toolsByKind[kind]` is what `buildChatOptions(kind)` passes to chat(). Overlap
// is intentional (the look_*/why_* read+explain set is in BOTH stage and
// analyse). There is deliberately NO flat-union export in the end state: the
// routing-contract test derives the union from `toolsByKind`, so it can't drift
// from what's reachable, and `ConversationKind` stays exactly the three real
// kinds.

import type { ConversationKind } from "#/db/cockpit/conversations";
import { beginSessionTool } from "./begin-session";
import { connectTool } from "./connect";
import { frameTool } from "./frame";
import { listSourcesTool } from "./list-sources";
import { listTablesTool } from "./list-tables";
import { listVerticalsTool } from "./list-verticals";
import { lookCycleTool } from "./look-cycle";
import { lookDriversTool } from "./look-drivers";
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

// The read + explain set — table/relationship inspection plus the operating-model
// artifact look/why. Shared by BOTH stage and analyse (overlap is by design).
const inspectTools = [
	lookTableTool,
	lookProfileTool,
	lookRelationshipsTool,
	lookValidationTool,
	lookCycleTool,
	lookMetricTool,
	lookDriversTool,
	whyColumnTool,
	whyTableTool,
	whyRelationshipTool,
	whyValidationTool,
	whyCycleTool,
	whyMetricTool,
] as const;

/**
 * The fenced toolstack per chat kind (DAT-532). `connect` acquires data (peek /
 * vertical / select / upload); `stage` teaches + runs the session over typed
 * tables (the inspect set + teach* + begin_session/operating_model/replay + a raw
 * run_sql peek); `analyse` answers questions — `answer` is the analytical surface
 * (grounded SQL + validated snippets), NOT raw run_sql — plus the inspect set to
 * explain a result or a quality band. `satisfies` enforces all three kinds are
 * present without widening the precise tuple types chat() needs.
 */
export const toolsByKind = {
	connect: [
		listSourcesTool,
		listTablesTool,
		listVerticalsTool,
		connectTool,
		probeTool,
		useVerticalTool,
		frameTool,
		selectTool,
		uploadTool,
	],
	stage: [
		listTablesTool,
		...inspectTools,
		teachTool,
		teachValidationTool,
		teachCycleTool,
		teachMetricTool,
		beginSessionTool,
		operatingModelTool,
		replayTool,
		runSqlTool,
	],
	analyse: [listTablesTool, ...inspectTools, answerTool],
} as const satisfies Record<ConversationKind, ReadonlyArray<{ name: string }>>;

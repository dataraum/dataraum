// Agent-tier prompts (TS-owned, DD/27688962). Pipeline prompts stay in
// dataraum-config; agent/conversational prompts live here, versioned and tested
// with the agents that use them. Per-agent prompt builders export from their own
// module (orchestrator now; frame / why-column land as those tickets arrive).

export { getFrameInstructions } from "./frame";
export { getOrchestratorInstructions } from "./orchestrator";

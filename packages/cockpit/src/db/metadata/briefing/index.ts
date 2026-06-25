// Workspace Briefing — public surface (DAT-632, epic DAT-574). See ./types for
// the shape and the per-concern split (assemble / next-actions / project / build).

export { assembleBriefing } from "./assemble";
export { buildWorkspaceBriefing } from "./build";
export { computeNextActions } from "./next-actions";
export { projectBriefing } from "./project";
export type * from "./types";

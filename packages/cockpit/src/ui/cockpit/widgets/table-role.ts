// Display vocabulary for the engine's persisted table role (DAT-728): the role
// string ∈ {fact, periodic_snapshot, dimension} → the human chip label. One home
// so the table-readiness header and the workspace-inventory badge never drift
// (cockpit idiom #13 — shared visual vocabulary is shared code, not per-widget
// copies). A null/unknown role maps to `undefined`, so the caller renders NO chip
// rather than a misleading one — and never folds two roles into one boolean (a
// periodic snapshot is a fact subtype, but the two carry different additivity
// consequences, so they stay distinct wherever a human or the agent reads them).
export const TABLE_ROLE_LABEL: Record<string, string> = {
	fact: "Fact table",
	periodic_snapshot: "Periodic snapshot",
	dimension: "Dimension table",
};

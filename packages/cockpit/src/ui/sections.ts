// The top-level sections of the cockpit, rendered as the left app rail.
// Order here is the order in the rail. Routes are flat (DAT-822): one cockpit
// per workspace behind a subdomain (DD/51740673), so no URL segment carries a
// workspace id.
//
// Icons are lucide-react component references — the rail reads `icon` and the
// section route reads `label`. Single source so the rail and any breadcrumbs
// can never drift.

import {
	Boxes,
	Database,
	LayoutDashboard,
	Library,
	type LucideIcon,
	Network,
	Settings,
	ShieldCheck,
	Workflow,
} from "lucide-react";

export interface Section {
	id: string;
	label: string;
	icon: LucideIcon;
	/**
	 * Typed router `to` for the section. Keep these literal so TanStack Router
	 * type-checks the rail links.
	 */
	to:
		| "/cockpit"
		| "/reports"
		| "/library"
		| "/workflows"
		| "/metadata"
		| "/operating-model"
		| "/governance"
		| "/settings";
}

export const sections: readonly Section[] = [
	{
		id: "cockpit",
		label: "Cockpit",
		icon: LayoutDashboard,
		to: "/cockpit",
	},
	{
		// The minted-report library (DAT-624) — a workspace's saved widgets, each a
		// frozen query re-run live on open. Takes the `Library` icon: it is the
		// genuine "library", whereas the `library` section below is really Sources.
		id: "reports",
		label: "Reports",
		icon: Library,
		to: "/reports",
	},
	{
		// The data-sources browser (route path stays `/library` — was `/sources`,
		// DAT-339). Relabeled "Sources" with a source-fitting icon now that Reports
		// owns the "library" identity (DAT-624).
		id: "library",
		label: "Sources",
		icon: Boxes,
		to: "/library",
	},
	{
		// Native run monitor (DAT-550). Route path stays `/workflows`; the label is
		// "Runs" — it's a cockpit_db-backed view of stage runs, not the raw Temporal UI.
		id: "workflows",
		label: "Runs",
		icon: Workflow,
		to: "/workflows",
	},
	{
		id: "metadata",
		label: "Metadata",
		icon: Database,
		to: "/metadata",
	},
	{
		// The operating-model canvas (DAT-591): the workspace's concept-spine DAG —
		// ontology concepts grounded into columns, with the metrics/cycles/validations
		// /drivers built on them. A standing xyflow page, not a chat widget.
		id: "operating-model",
		label: "Model",
		icon: Network,
		to: "/operating-model",
	},
	{
		id: "governance",
		label: "Governance",
		icon: ShieldCheck,
		to: "/governance",
	},
	{
		id: "settings",
		label: "Settings",
		icon: Settings,
		to: "/settings",
	},
] as const;

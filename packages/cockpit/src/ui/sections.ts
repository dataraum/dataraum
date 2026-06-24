// The top-level sections of the cockpit, rendered as the left app rail.
// Order here is the order in the rail. Each section is workspace-scoped
// (/workspace/$wsId/<id>) except `settings`, which is global (/settings).
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
	 * Typed router `to` for the section. Workspace sections use the `$wsId`
	 * param template (the rail supplies the value); the global section is a
	 * fixed path. Keep these literal so TanStack Router type-checks the links.
	 */
	to:
		| "/workspace/$wsId/cockpit"
		| "/workspace/$wsId/reports"
		| "/workspace/$wsId/library"
		| "/workspace/$wsId/workflows"
		| "/workspace/$wsId/metadata"
		| "/workspace/$wsId/operating-model"
		| "/workspace/$wsId/governance"
		| "/settings";
	/** Global sections live at a fixed path; workspace sections nest under wsId. */
	global?: boolean;
}

export const sections: readonly Section[] = [
	{
		id: "cockpit",
		label: "Cockpit",
		icon: LayoutDashboard,
		to: "/workspace/$wsId/cockpit",
	},
	{
		// The minted-report library (DAT-624) — a workspace's saved widgets, each a
		// frozen query re-run live on open. Takes the `Library` icon: it is the
		// genuine "library", whereas the `library` section below is really Sources.
		id: "reports",
		label: "Reports",
		icon: Library,
		to: "/workspace/$wsId/reports",
	},
	{
		// The data-sources browser (route path stays `/library` — was `/sources`,
		// DAT-339). Relabeled "Sources" with a source-fitting icon now that Reports
		// owns the "library" identity (DAT-624).
		id: "library",
		label: "Sources",
		icon: Boxes,
		to: "/workspace/$wsId/library",
	},
	{
		// Native run monitor (DAT-550). Route path stays `/workflows`; the label is
		// "Runs" — it's a cockpit_db-backed view of stage runs, not the raw Temporal UI.
		id: "workflows",
		label: "Runs",
		icon: Workflow,
		to: "/workspace/$wsId/workflows",
	},
	{
		id: "metadata",
		label: "Metadata",
		icon: Database,
		to: "/workspace/$wsId/metadata",
	},
	{
		// The operating-model canvas (DAT-591): the workspace's concept-spine DAG —
		// ontology concepts grounded into columns, with the metrics/cycles/validations
		// /drivers built on them. A standing xyflow page, not a chat widget.
		id: "operating-model",
		label: "Model",
		icon: Network,
		to: "/workspace/$wsId/operating-model",
	},
	{
		id: "governance",
		label: "Governance",
		icon: ShieldCheck,
		to: "/workspace/$wsId/governance",
	},
	{
		id: "settings",
		label: "Settings",
		icon: Settings,
		to: "/settings",
		global: true,
	},
] as const;

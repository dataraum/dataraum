// The bus matrix as a fact x conformed-dimension grid (DAT-740).
//
// Kimball's bus matrix says which facts share which dimensions. It is what makes a
// cross-fact question answerable at all: `enriched_views` is unique per fact, so two
// facts are NEVER joined to each other — a comparison composes one subquery per fact
// and merges them on a shared conformed dimension (DAT-809).
//
// This module is PURE (no DB, no IO) and carries the tests; `bus-matrix-load.ts` is
// the thin Drizzle glue. Three rules it exists to enforce:
//
//  1. Axes are keyed on the engine's `conformed_group`, never on `concept_label`. A
//     label drifts between runs and collides across unrelated groups, so keying on it
//     would split one axis in two and merge two into one. Folded cells that were never
//     conformed carry no group, so they fall back to the cell's `signature` — which is
//     FACT-scoped, keeping each fact's own fold a separate axis. That COALESCE is the
//     engine's own (`og_dim_members.axis_identity`), mirrored here deliberately.
//  2. Drillable-across is a claim about CONFIRMED conformance, and it is the same bar
//     the engine composes on: a group present on 2+ facts where every cell names a
//     confirmation source other than `unconfirmed` and none awaits review. Same-named
//     FK roles conform STRUCTURALLY, with nobody having confirmed the underlying
//     relationship — rendering that as drillable would invite exactly the join the
//     engine refuses.
//  3. Every not-drillable axis carries a REASON. "No axes" rendered as blankness is
//     indistinguishable from "nothing analyzed yet", which is the failure this grid
//     exists to prevent.

/** One `current_bus_matrix` row, as the Drizzle mirror returns it. */
export interface BusMatrixRow {
	factTableId: string;
	attachment: string;
	conceptLabel: string;
	dimensionTableId: string | null;
	roles: unknown;
	attributes: unknown;
	confirmationSource: string;
	conformedGroup: string | null;
	needsConfirmation: boolean;
	signature: string;
}

export interface TableRow {
	tableId: string;
	tableName: string;
}

export interface BusMatrixInput {
	cells: BusMatrixRow[];
	tables: TableRow[];
}

/** Confirmation sources that may back a cross-fact join — mirrors the engine's set. */
const CONFIRMED_SOURCES = new Set(["judge", "keeper", "user"]);

export interface BusMatrixCell {
	factTableId: string;
	factName: string;
	/** `folded` = the dimension is inlined on the fact; `referenced` = a real FK. */
	attachment: string;
	confirmationSource: string;
	needsConfirmation: boolean;
	roles: string[];
	attributes: string[];
}

export interface BusMatrixAxis {
	/** The `conformed_group`, or the fact-scoped signature when never conformed. */
	identity: string;
	/** Display only — never a grouping key. */
	label: string;
	/** True when this axis is a real conformed dimension, not an unconformed fold. */
	conformed: boolean;
	cells: BusMatrixCell[];
	/** Whether a cross-fact drill-across may compose on this axis. */
	drillable: boolean;
	/** Why not, when `drillable` is false. Always present in that case. */
	blockedReason: string | null;
}

export interface BusMatrix {
	/** Fact table names, the grid's rows, sorted. */
	facts: string[];
	/** The grid's columns, drillable axes first then by label. */
	axes: BusMatrixAxis[];
}

function toStringArray(value: unknown): string[] {
	if (!Array.isArray(value)) return [];
	return value.filter((v): v is string => typeof v === "string");
}

/**
 * Why this axis cannot carry a cross-fact drill, or null when it can.
 *
 * Order matters: the checks run cheapest-and-most-fundamental first, so a
 * single-fact axis is reported as such rather than as an unconfirmed one.
 */
function blockedReason(
	cells: BusMatrixCell[],
	conformed: boolean,
): string | null {
	if (cells.length < 2) {
		return "only one fact carries this dimension — there is nothing to drill across";
	}
	if (!conformed) {
		return "this dimension is folded into each fact and was never conformed across them, so the values cannot be assumed to mean the same thing";
	}
	const unconfirmed = cells.filter(
		(c) => !CONFIRMED_SOURCES.has(c.confirmationSource),
	);
	if (unconfirmed.length > 0) {
		const names = unconfirmed.map((c) => c.factName).join(", ");
		return `the conformance is unconfirmed on ${names} — confirming it would enable the drill-across; merging on it now would assert an identity nobody established`;
	}
	const review = cells.filter((c) => c.needsConfirmation);
	if (review.length > 0) {
		return `awaiting review on ${review.map((c) => c.factName).join(", ")}`;
	}
	return null;
}

export function buildBusMatrix(input: BusMatrixInput): BusMatrix {
	const nameOf = new Map(input.tables.map((t) => [t.tableId, t.tableName]));

	// The engine's own axis identity: the conformed group when one exists, else the
	// cell's fact-scoped signature so two unconformed folds never collapse into one.
	const grouped = new Map<
		string,
		{ conformed: boolean; label: string; cells: BusMatrixCell[] }
	>();
	for (const cell of input.cells) {
		const identity = cell.conformedGroup ?? cell.signature;
		const entry = grouped.get(identity) ?? {
			conformed: cell.conformedGroup !== null,
			label: cell.conceptLabel,
			cells: [],
		};
		entry.cells.push({
			factTableId: cell.factTableId,
			factName: nameOf.get(cell.factTableId) ?? cell.factTableId,
			attachment: cell.attachment,
			confirmationSource: cell.confirmationSource,
			needsConfirmation: cell.needsConfirmation,
			roles: toStringArray(cell.roles),
			attributes: toStringArray(cell.attributes),
		});
		grouped.set(identity, entry);
	}

	const axes: BusMatrixAxis[] = [...grouped.entries()].map(
		([identity, entry]) => {
			const cells = [...entry.cells].sort((a, b) =>
				a.factName.localeCompare(b.factName),
			);
			const reason = blockedReason(cells, entry.conformed);
			return {
				identity,
				label: entry.label,
				conformed: entry.conformed,
				cells,
				drillable: reason === null,
				blockedReason: reason,
			};
		},
	);

	// Drillable axes first — they are the ones a practitioner can act on — then by
	// label, then by identity so the order is total and cannot flip between renders.
	axes.sort(
		(a, b) =>
			Number(b.drillable) - Number(a.drillable) ||
			a.label.localeCompare(b.label) ||
			a.identity.localeCompare(b.identity),
	);

	const facts = [
		...new Set(
			input.cells.map((c) => nameOf.get(c.factTableId) ?? c.factTableId),
		),
	];
	facts.sort((a, b) => a.localeCompare(b));

	return { facts, axes };
}

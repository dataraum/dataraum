// Bus matrix loader (DAT-740) — the SERVER-ONLY half. Reads the already-mirrored
// `current_bus_matrix` view (plus `current_tables` for fact names) and hands the rows
// to the pure `buildBusMatrix`. Imports the metadata DB client, so it must NEVER be
// imported by a client component — the same split `concept-graph-load.ts` documents.
//
// No new mirror needed: `currentBusMatrix` has been in the Drizzle schema since
// DAT-762 and, until this lane, had ZERO query sites anywhere in the cockpit.
//
// Workspace scoping: no explicit filter, like every other `current_*` read here — the
// read-role connection's search_path already resolves to this workspace's `ws_<id>_read`
// schema (DAT-816, ADR-0008). Both views are head-gated engine-side, so an unpromoted
// run is invisible rather than half-shown.
//
// tsc-bounded, test-unexecuted (the convention `operating-model-load.ts` sets): this is
// thin DB-read glue with no branching of its own; `buildBusMatrix` carries the
// behaviour and the tests.

import { metadataDb } from "../db/metadata/client";
import { currentBusMatrix, currentTables } from "../db/metadata/schema";
import { type BusMatrix, buildBusMatrix } from "./bus-matrix";

export async function loadBusMatrix(): Promise<BusMatrix> {
	const [cellRows, tableRows] = await Promise.all([
		metadataDb
			.select({
				factTableId: currentBusMatrix.factTableId,
				attachment: currentBusMatrix.attachment,
				conceptLabel: currentBusMatrix.conceptLabel,
				dimensionTableId: currentBusMatrix.dimensionTableId,
				roles: currentBusMatrix.roles,
				attributes: currentBusMatrix.attributes,
				confirmationSource: currentBusMatrix.confirmationSource,
				conformedGroup: currentBusMatrix.conformedGroup,
				needsConfirmation: currentBusMatrix.needsConfirmation,
				signature: currentBusMatrix.signature,
			})
			.from(currentBusMatrix),
		metadataDb
			.select({
				tableId: currentTables.tableId,
				tableName: currentTables.tableName,
			})
			.from(currentTables),
	]);

	return buildBusMatrix({
		cells: cellRows.map((r) => ({
			factTableId: r.factTableId ?? "",
			attachment: r.attachment ?? "",
			conceptLabel: r.conceptLabel ?? "",
			dimensionTableId: r.dimensionTableId,
			roles: r.roles,
			attributes: r.attributes,
			confirmationSource: r.confirmationSource ?? "unconfirmed",
			conformedGroup: r.conformedGroup,
			needsConfirmation: r.needsConfirmation ?? false,
			signature: r.signature ?? "",
		})),
		tables: tableRows.map((t) => ({
			tableId: t.tableId ?? "",
			tableName: t.tableName ?? "",
		})),
	});
}

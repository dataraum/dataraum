import { describe, expect, it } from "vitest";

import { type BusMatrixRow, buildBusMatrix } from "./bus-matrix";

// FIXTURE PROVENANCE: shapes are taken from the engine's ONE writer,
// analysis/hierarchies/bus_matrix.py::derive_bus_matrix — a REFERENCED cell always
// carries a conformed_group (`ref:{dim}:{roles}`) and a non-null dimension_table_id;
// a FOLDED cell carries dimension_table_id NULL, exactly one entry in roles, and a
// conformed_group ONLY when the conform judge returned a verdict. Not idealized.

function ref(
	fact: string,
	role: string,
	group: string,
	over: Partial<BusMatrixRow> = {},
): BusMatrixRow {
	return {
		factTableId: fact,
		attachment: "referenced",
		conceptLabel: "accounts",
		dimensionTableId: "t_dim",
		roles: [role],
		attributes: [],
		confirmationSource: "judge",
		conformedGroup: group,
		needsConfirmation: false,
		signature: `bus:referenced:${fact}:t_dim:${role}`,
		...over,
	};
}

function folded(
	fact: string,
	key: string,
	over: Partial<BusMatrixRow> = {},
): BusMatrixRow {
	return {
		factTableId: fact,
		attachment: "folded",
		conceptLabel: "region",
		dimensionTableId: null,
		roles: [key],
		attributes: [],
		confirmationSource: "unconfirmed",
		conformedGroup: null,
		needsConfirmation: false,
		signature: `bus:folded:${fact}:${key}`,
		...over,
	};
}

const TABLES = [
	{ tableId: "t_gl", tableName: "gl_entries" },
	{ tableId: "t_ap", tableName: "ap_balances" },
];

describe("buildBusMatrix (DAT-740)", () => {
	it("is empty for a workspace with no cells", () => {
		expect(buildBusMatrix({ cells: [], tables: [] })).toEqual({
			facts: [],
			axes: [],
		});
	});

	it("groups two facts onto ONE axis by conformed_group, not by column name", () => {
		// The crossing: the facts spell the dimension differently. A name-keyed grid
		// would show two axes of one fact each and hide the drill entirely.
		const matrix = buildBusMatrix({
			cells: [
				ref("t_gl", "account_id", "ref:t_dim:account_id"),
				ref("t_ap", "acct", "ref:t_dim:account_id"),
			],
			tables: TABLES,
		});
		expect(matrix.axes).toHaveLength(1);
		expect(matrix.axes[0].identity).toBe("ref:t_dim:account_id");
		expect(matrix.axes[0].cells.map((c) => c.factName)).toEqual([
			"ap_balances",
			"gl_entries",
		]);
		expect(matrix.axes[0].drillable).toBe(true);
		expect(matrix.axes[0].blockedReason).toBeNull();
		expect(matrix.facts).toEqual(["ap_balances", "gl_entries"]);
	});

	it("does not merge two axes that merely share a label", () => {
		const matrix = buildBusMatrix({
			cells: [
				ref("t_gl", "billto", "ref:t_dim:billto", { conceptLabel: "accounts" }),
				ref("t_ap", "shipto", "ref:t_dim:shipto", { conceptLabel: "accounts" }),
			],
			tables: TABLES,
		});
		// Role-playing FKs the judge kept apart: same label, two identities, and
		// neither is drillable because each covers one fact.
		expect(matrix.axes).toHaveLength(2);
		expect(matrix.axes.every((a) => !a.drillable)).toBe(true);
	});

	it("refuses an unconfirmed pairing and names the facts", () => {
		const matrix = buildBusMatrix({
			cells: [
				ref("t_gl", "account_id", "ref:t_dim:account_id", {
					confirmationSource: "unconfirmed",
				}),
				ref("t_ap", "acct", "ref:t_dim:account_id"),
			],
			tables: TABLES,
		});
		expect(matrix.axes[0].drillable).toBe(false);
		expect(matrix.axes[0].blockedReason).toContain("unconfirmed on gl_entries");
		expect(matrix.axes[0].blockedReason).toContain("nobody established");
	});

	it("refuses a pairing awaiting review", () => {
		const matrix = buildBusMatrix({
			cells: [
				ref("t_gl", "account_id", "ref:t_dim:account_id", {
					needsConfirmation: true,
				}),
				ref("t_ap", "acct", "ref:t_dim:account_id"),
			],
			tables: TABLES,
		});
		expect(matrix.axes[0].drillable).toBe(false);
		expect(matrix.axes[0].blockedReason).toContain(
			"awaiting review on gl_entries",
		);
	});

	it.each([
		"judge",
		"keeper",
		"user",
	])("accepts %s as a confirmed source", (source) => {
		const matrix = buildBusMatrix({
			cells: [
				ref("t_gl", "account_id", "ref:t_dim:account_id", {
					confirmationSource: source,
				}),
				ref("t_ap", "acct", "ref:t_dim:account_id", {
					confirmationSource: source,
				}),
			],
			tables: TABLES,
		});
		expect(matrix.axes[0].drillable).toBe(true);
	});

	it("keeps two unconformed folds apart via the fact-scoped signature", () => {
		// Both facts fold a `region` column. Nothing conformed them, so they are two
		// axes — collapsing them would assert the values mean the same thing.
		const matrix = buildBusMatrix({
			cells: [folded("t_gl", "region"), folded("t_ap", "region")],
			tables: TABLES,
		});
		expect(matrix.axes).toHaveLength(2);
		expect(matrix.axes.every((a) => !a.conformed)).toBe(true);
		expect(matrix.axes[0].blockedReason).toContain("only one fact");
	});

	it("merges a judge-conformed fold and explains a single-fact one", () => {
		const group = "conform:t_ap:region|t_gl:region";
		const matrix = buildBusMatrix({
			cells: [
				folded("t_gl", "region", {
					conformedGroup: group,
					confirmationSource: "judge",
				}),
				folded("t_ap", "region", {
					conformedGroup: group,
					confirmationSource: "judge",
				}),
			],
			tables: TABLES,
		});
		expect(matrix.axes).toHaveLength(1);
		expect(matrix.axes[0].drillable).toBe(true);
	});

	it("gives EVERY non-drillable axis a reason", () => {
		const matrix = buildBusMatrix({
			cells: [
				ref("t_gl", "account_id", "ref:t_dim:account_id"),
				folded("t_ap", "region"),
				ref("t_ap", "acct", "ref:t_dim:account_id"),
			],
			tables: TABLES,
		});
		for (const axis of matrix.axes) {
			if (!axis.drillable) expect(axis.blockedReason).toBeTruthy();
		}
	});

	it("orders drillable axes first and is stable", () => {
		const cells = [
			folded("t_gl", "zzz_region"),
			ref("t_gl", "account_id", "ref:t_dim:account_id"),
			ref("t_ap", "acct", "ref:t_dim:account_id"),
		];
		const first = buildBusMatrix({ cells, tables: TABLES });
		const second = buildBusMatrix({
			cells: [...cells].reverse(),
			tables: TABLES,
		});
		expect(first.axes[0].drillable).toBe(true);
		expect(first.axes.map((a) => a.identity)).toEqual(
			second.axes.map((a) => a.identity),
		);
	});

	it("counts distinct FACTS, not cells, when judging conformance", () => {
		// A folded group can transitively hold two components of the SAME fact. Two
		// cells then look like two participants, and a cell-count check would call a
		// single-fact axis conformed-across — claiming a merge the engine refuses.
		const group = "conform:t_gl:region|t_gl:region_name";
		const matrix = buildBusMatrix({
			cells: [
				folded("t_gl", "region", {
					conformedGroup: group,
					confirmationSource: "judge",
				}),
				folded("t_gl", "region_name", {
					conformedGroup: group,
					confirmationSource: "judge",
					signature: "bus:folded:t_gl:region_name",
				}),
			],
			tables: TABLES,
		});
		expect(matrix.axes).toHaveLength(1);
		expect(matrix.axes[0].cells).toHaveLength(2);
		expect(matrix.axes[0].drillable).toBe(false);
		expect(matrix.axes[0].blockedReason).toContain("only one fact");
	});

	it("falls back to the table id when a fact name is unresolvable", () => {
		const matrix = buildBusMatrix({
			cells: [ref("t_ghost", "account_id", "ref:t_dim:account_id")],
			tables: [],
		});
		expect(matrix.facts).toEqual(["t_ghost"]);
	});

	it("tolerates non-array roles/attributes from the json column", () => {
		const matrix = buildBusMatrix({
			cells: [
				ref("t_gl", "account_id", "g", { roles: null, attributes: "oops" }),
			],
			tables: TABLES,
		});
		expect(matrix.axes[0].cells[0].roles).toEqual([]);
		expect(matrix.axes[0].cells[0].attributes).toEqual([]);
	});
});

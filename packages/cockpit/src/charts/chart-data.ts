// Pure bridges between the columnar result store and the chart layer (DAT-626).
// No React, no vega — so the materialization + type-suggestion rules are unit-
// tested directly (cockpit idiom rule 10); the renderer stays a thin DOM shell.

import type { Json } from "@duckdb/node-api";
import { columnFilterKind } from "#/duckdb/cell-format";
import type { GridView } from "#/duckdb/ndjson-stream";
import type { FieldType } from "./chart-config";

/** One chart datum — a column-name → cell-value record, the row shape Vega binds
 * to the named `table` data source. */
export type ChartRow = Record<string, Json | null>;

/**
 * Materialize a (capped) columnar {@link GridView} into row objects for Vega.
 *
 * The store is already bounded — the chart fetch caps at HARD_ROW_CEILING and
 * surfaces `truncated` — so building one object per row is safe here (a chart
 * over more marks than that is the cap-warning case, handled in the modal). The
 * grid itself never does this (it reads cells by index to stay virtualized); the
 * chart genuinely needs whole rows, so this is the one place we rematerialize.
 */
export function gridViewToRows(view: GridView): ChartRow[] {
	const { columns, rowCount } = view;
	const rows: ChartRow[] = new Array(rowCount);
	for (let r = 0; r < rowCount; r++) {
		const row: ChartRow = {};
		for (let c = 0; c < columns.length; c++) {
			row[columns[c]] = view.cell(c, r);
		}
		rows[r] = row;
	}
	return rows;
}

/**
 * The Vega-Lite measurement type to default a column to in the manual mapper,
 * read off its DuckDB type via the SAME classifier the grid's filters use
 * ({@link columnFilterKind}) so the affordances agree: numeric → quantitative,
 * temporal → temporal, everything else → nominal. The user can override per
 * field; this is only the starting guess.
 */
export function suggestFieldType(duckdbType: Json | undefined): FieldType {
	switch (columnFilterKind(duckdbType)) {
		case "numeric":
			return "quantitative";
		case "temporal":
			return "temporal";
		default:
			return "nominal";
	}
}

/** A result column paired with its suggested field type — the option list the
 * manual mapper's encoding selects render from. */
export interface ColumnOption {
	name: string;
	suggestedType: FieldType;
}

/** Pair each result column with its suggested field type, in result order. */
export function columnOptions(columns: string[], types: Json): ColumnOption[] {
	const typeList = Array.isArray(types) ? (types as Json[]) : [];
	return columns.map((name, i) => ({
		name,
		suggestedType: suggestFieldType(typeList[i]),
	}));
}

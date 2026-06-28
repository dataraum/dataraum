import type { Json } from "@duckdb/node-api";
import { describe, expect, it } from "vitest";
import type { GridView } from "#/duckdb/ndjson-stream";
import { columnOptions, gridViewToRows, suggestFieldType } from "./chart-data";

/** A minimal columnar GridView over `cols[colIndex][rowIndex]`. */
function fakeView(
	columns: string[],
	cols: (Json | null)[][],
	types: Json = null,
): GridView {
	return {
		columns,
		types,
		rowCount: cols[0]?.length ?? 0,
		status: "done",
		truncated: false,
		cell: (c, r) => cols[c]?.[r] ?? null,
	};
}

describe("gridViewToRows", () => {
	it("materializes column-major cells into row objects", () => {
		const view = fakeView(
			["month", "revenue"],
			[
				["jan", "feb"],
				[10, 20],
			],
		);
		expect(gridViewToRows(view)).toEqual([
			{ month: "jan", revenue: 10 },
			{ month: "feb", revenue: 20 },
		]);
	});

	it("returns an empty array for a zero-row result", () => {
		expect(gridViewToRows(fakeView(["a"], [[]]))).toEqual([]);
	});
});

describe("suggestFieldType", () => {
	it("maps numeric DuckDB types to quantitative", () => {
		expect(suggestFieldType({ typeId: 4 } as Json)).toBe("quantitative"); // INTEGER
		expect(suggestFieldType({ typeId: 11 } as Json)).toBe("quantitative"); // DOUBLE
	});

	it("maps date/timestamp types to temporal", () => {
		expect(suggestFieldType({ typeId: 13 } as Json)).toBe("temporal"); // DATE
		expect(suggestFieldType({ typeId: 12 } as Json)).toBe("temporal"); // TIMESTAMP
	});

	it("falls back to nominal for text / unknown types", () => {
		expect(suggestFieldType(undefined)).toBe("nominal");
		expect(suggestFieldType({ typeId: 999 } as Json)).toBe("nominal");
	});
});

describe("columnOptions", () => {
	it("pairs each column with its suggested type, in result order", () => {
		const columns = ["region", "revenue"];
		const types = [{ typeId: 17 }, { typeId: 11 }] as Json; // VARCHAR-ish, DOUBLE
		expect(columnOptions(columns, types)).toEqual([
			{ name: "region", suggestedType: "nominal" },
			{ name: "revenue", suggestedType: "quantitative" },
		]);
	});
});

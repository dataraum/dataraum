// Unit tests for the synthetic-ConnectSchema assembly (DAT-594) — the pure
// builder that unions a heterogeneous staging set (probed queries + sniffed files)
// into ONE ConnectSchema for `frame` to induce from. No driver: the per-item
// schemas are handed in.

import { describe, expect, it, vi } from "vitest";

import type { ConnectSchema } from "../duckdb/connect";
import type { ProbeSchema } from "../duckdb/probe";

// stage-schema pulls `collectSampleValues` from `#/duckdb/connect`, whose module
// graph imports config at load — stub it so the pure-assembly unit test needs no
// real env (the cockpit config-free-test idiom; the server fn wires the real one).
vi.mock("#/config", () => ({ config: { s3Bucket: "dataraum-lake" } }));

import { assembleStagingSchema, queryToConnectTable } from "./stage-schema";

const ordersSchema: ProbeSchema = {
	columns: [
		{ name: "order_id", type: "BIGINT" },
		{ name: "status", type: "VARCHAR" },
	],
	sampleRows: [
		{ order_id: 1, status: "open" },
		{ order_id: 2, status: "open" },
		{ order_id: 3, status: "shipped" },
	],
};

const customersFile: ConnectSchema = {
	sourceKind: "file",
	source: "s3://dataraum-lake/ws/uploads/aaa/customers.csv",
	tables: [
		{
			name: "customers.csv",
			rowCountEstimate: 2,
			columns: [
				{
					name: "customer_id",
					position: 1,
					sourceType: "BIGINT",
					nullable: false,
					sampleValues: [10, 11],
				},
			],
		},
	],
};

describe("queryToConnectTable (DAT-594)", () => {
	it("maps a probed query's DESCRIBE + sample to one named ConnectTable", () => {
		const table = queryToConnectTable({
			source_name: "wwi_orders",
			schema: ordersSchema,
		});
		expect(table.name).toBe("wwi_orders");
		expect(table.rowCountEstimate).toBeNull();
		expect(table.columns.map((c) => c.name)).toEqual(["order_id", "status"]);
		expect(table.columns[0]).toMatchObject({
			position: 1,
			sourceType: "BIGINT",
			nullable: true,
		});
		// Sample values are derived from the sample rows (distinct, capped).
		expect(table.columns[1].sampleValues).toEqual(["open", "shipped"]);
	});
});

describe("assembleStagingSchema (DAT-594)", () => {
	it("unions a query + a file into one schema (one table per staged item)", () => {
		const schema = assembleStagingSchema({
			queries: [{ source_name: "wwi_orders", schema: ordersSchema }],
			files: [customersFile],
		});
		// Queries first, then files (matching the import-set union order).
		expect(schema.tables.map((t) => t.name)).toEqual([
			"wwi_orders",
			"customers.csv",
		]);
		// A mixed set with a query reads as a database-kind schema.
		expect(schema.sourceKind).toBe("database");
		expect(schema.source).toContain("2 items");
	});

	it("reads as file-kind when the set is files only", () => {
		const schema = assembleStagingSchema({
			queries: [],
			files: [customersFile],
		});
		expect(schema.sourceKind).toBe("file");
		expect(schema.tables).toHaveLength(1);
		expect(schema.source).toContain("1 item");
	});

	it("flattens multi-table file sniffs into the union", () => {
		const twoTableFile: ConnectSchema = {
			sourceKind: "file",
			source: "x",
			tables: [
				{ name: "a", rowCountEstimate: null, columns: [] },
				{ name: "b", rowCountEstimate: null, columns: [] },
			],
		};
		const schema = assembleStagingSchema({
			queries: [{ source_name: "q", schema: ordersSchema }],
			files: [twoTableFile],
		});
		expect(schema.tables.map((t) => t.name)).toEqual(["q", "a", "b"]);
	});

	it("throws on an empty staging set (nothing to frame)", () => {
		expect(() => assembleStagingSchema({ queries: [], files: [] })).toThrow(
			/empty staging set/,
		);
	});
});

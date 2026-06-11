// Property-style leak tests (DAT-433): no content-keyed `src_<digest>` name
// reaches the agent through ANY tool projection. Fixtures are ENGINE-SHAPED —
// they mirror what the engine actually persists:
//   - every evidence dict carries `_column_name` + `_table_name` (stamped by
//     `entropy/detectors/base.py` create_entropy_object),
//   - relationship detectors add explicit `from_table`/`to_table`
//     (`entropy/detectors/structural/relations.py`),
//   - slice detectors add `slice_table_name` with the underscore-collapsed
//     slice name (`entropy/detectors/value/slice_variance.py` +
//     `analysis/slicing/slice_runner.py` naming),
//   - workflow failures embed raw physical names in engine-built message text
//     (`worker.contracts.ProgressFailure`).
//
// The property: JSON.stringify of the FULL projected result never matches
// /src_[0-9a-f]{40}/ — with ONE sanctioned exception: list_tables/look_table
// deliberately carry the raw DuckDB name in `physical_name` (the run_sql
// round-trip key; the agent cannot reconstruct the digest from a display
// name). For those two, the property holds on the result MINUS that field,
// and the orchestrator prompt forbids echoing it in prose.

import { describe, expect, it, vi } from "vitest";

// Importing the tools pulls config.ts + the Postgres metadata client; mock both
// (with the `#/` alias — relative specifiers silently don't intercept) so the
// pure projections run with no env and no connection.
vi.mock("#/config", () => ({ config: { anthropicApiKey: "test" } }));
vi.mock("#/db/metadata/client", () => ({ metadataDb: {} }));

import { buildInventory, type InventoryTableRow } from "./list-tables";
import {
	type ColumnNameLookup,
	projectRelationshipReadiness,
	type RelationshipReadinessRow,
} from "./look-relationships";
import { projectColumnReadiness, projectLookTable } from "./look-table";
import { projectValidationOverview } from "./look-validation";
import {
	projectWhyData,
	type WhyEvidenceRow,
	type WhyReadinessRow,
} from "./why-column";
import {
	projectWhyRelationship,
	type RelEndpoints,
	type WhyRelEvidenceRow,
} from "./why-relationship";
import { projectWhyTable, type WhyTableEvidenceRow } from "./why-table";
import { projectWhyValidation } from "./why-validation";

const LEAK = /src_[0-9a-f]{40}/;

// Two distinct content digests (sha-1 hex), as the upload sources mint them.
const D1 = "204bc8e118543a6c35654c1f68c43539a2e226f2";
const D2 = "3cb4f3325aa757324f32b2dbe30b4ca5a55a8b50";
const RAW_ORDERS = `src_${D1}__orders`;
const RAW_CUSTOMERS = `src_${D2}__customers`;
const RAW_SLICE = `slice_src_${D1}_orders_region_emea`;

// Engine-shaped per-column evidence: detector payload + the stamped `_` keys.
const columnEvidence: WhyEvidenceRow[] = [
	{
		layer: "semantic",
		dimension: "units",
		subDimension: "unit_declaration",
		score: 0.8,
		detectorId: "unit_entropy",
		evidence: [
			{
				metric: "undeclared_ratio",
				value: 0.8,
				_column_name: "amount",
				_table_name: RAW_ORDERS,
			},
		],
	},
	{
		layer: "value",
		dimension: "distribution",
		subDimension: "slice_variance",
		score: 0.6,
		detectorId: "slice_variance",
		evidence: [
			{
				null_ratio: 0.1,
				distinct_count: 5,
				row_count: 100,
				slice_table_name: RAW_SLICE,
				outlier_ratio: 0.0,
				benford_p_value: null,
				_column_name: "amount",
				_table_name: RAW_ORDERS,
			},
		],
	},
];

// Engine-shaped relationship evidence (relations.py): explicit from/to tables.
const relationshipEvidence = [
	{
		path_status: "ambiguous",
		from_table: RAW_ORDERS,
		to_table: RAW_CUSTOMERS,
		distinct_join_paths: 2,
		resolved_by_overlay: false,
		_column_name: "customer_id",
		_table_name: RAW_ORDERS,
	},
];

describe("agent name-leak property (DAT-433)", () => {
	it("why_column: full result is digest-free", () => {
		const readiness: WhyReadinessRow = {
			columnId: "c_amount",
			columnName: "amount",
			tableName: RAW_ORDERS,
			band: "investigate",
			bandStage: "add_source",
			bandComputedAt: null,
			worstIntentRisk: 0.42,
			intents: [],
		};
		const out = projectWhyData(readiness, columnEvidence, 1);
		expect(JSON.stringify(out)).not.toMatch(LEAK);
		// The display name survives — sanitization is not erasure.
		expect(out.table_name).toBe("orders");
		expect(out.evidence[1]?.detail).toContain("slice_orders_region_emea");
	});

	it("why_table: full result is digest-free", () => {
		const evidenceRows: WhyTableEvidenceRow[] = columnEvidence;
		const out = projectWhyTable(
			"t_orders",
			RAW_ORDERS,
			{
				band: "investigate",
				bandStage: "session_detect",
				bandComputedAt: null,
				worstIntentRisk: 0.42,
				intents: [],
			},
			evidenceRows,
			0,
		);
		expect(JSON.stringify(out)).not.toMatch(LEAK);
		expect(out.table_name).toBe("orders");
	});

	it("why_relationship: full result is digest-free", () => {
		const endpoints: RelEndpoints = {
			fromTableName: RAW_ORDERS,
			fromColumnName: "customer_id",
			toTableName: RAW_CUSTOMERS,
			toColumnName: "id",
		};
		const evidenceRows: WhyRelEvidenceRow[] = [
			{
				layer: "structural",
				dimension: "relationships",
				subDimension: "join_paths",
				score: 0.5,
				detectorId: "relation_ambiguity",
				evidence: relationshipEvidence,
			},
		];
		const out = projectWhyRelationship(
			"c_from",
			"c_to",
			endpoints,
			{
				band: "investigate",
				bandStage: "session_detect",
				bandComputedAt: null,
				worstIntentRisk: 0.3,
				intents: [],
			},
			evidenceRows,
			0,
		);
		expect(JSON.stringify(out)).not.toMatch(LEAK);
		expect(out.from_table_name).toBe("orders");
		expect(out.to_table_name).toBe("customers");
		// The relationship detector's explicit name keys are display-mapped.
		expect(out.evidence[0]?.detail).toContain('"from_table":"orders"');
		expect(out.evidence[0]?.detail).toContain('"to_table":"customers"');
	});

	it("look_relationships: full result is digest-free", () => {
		const row: RelationshipReadinessRow = {
			target: "relationship:c_from::c_to",
			band: "ready",
			worstIntentRisk: 0.1,
			intents: [],
			topDrivers: [],
		};
		const names: ColumnNameLookup = new Map([
			["c_from", { columnName: "customer_id", tableName: RAW_ORDERS }],
			["c_to", { columnName: "id", tableName: RAW_CUSTOMERS }],
		]);
		const out = projectRelationshipReadiness(row, names);
		expect(JSON.stringify(out)).not.toMatch(LEAK);
		expect(out?.from_table_name).toBe("orders");
	});

	it("list_tables: result minus the sanctioned physical_name is digest-free", () => {
		const rows: InventoryTableRow[] = [
			{
				tableId: "t_orders",
				tableName: RAW_ORDERS,
				layer: "typed",
				rowCount: 100,
				sourceId: "s1",
				sourceName: `src_${D1}`,
				sourceType: "csv",
				sourceBackend: null,
				sourceConnectionConfig: {
					file_uris: [`s3://lake/uploads/${D1}/orders.csv`],
				},
			},
		];
		const [out] = buildInventory(rows, []);
		const { physical_name, ...rest } = out;
		expect(JSON.stringify(rest)).not.toMatch(LEAK);
		// The round-trip key still carries the raw DuckDB name.
		expect(physical_name).toBe(RAW_ORDERS);
		expect(out.table_name).toBe("orders");
		expect(out.source_name).toBe("orders.csv");
	});

	it("look_table: result minus the sanctioned physical_name is digest-free", () => {
		const out = projectLookTable(
			"t_orders",
			RAW_ORDERS,
			[
				projectColumnReadiness({
					columnId: "c_amount",
					columnName: "amount",
					resolvedType: "DECIMAL(18,2)",
					band: "ready",
					worstIntentRisk: 0.1,
					intents: [],
					topDrivers: [],
				}),
			],
			null,
			0,
		);
		const { physical_name, ...rest } = out;
		expect(JSON.stringify(rest)).not.toMatch(LEAK);
		expect(physical_name).toBe(RAW_ORDERS);
		expect(out.table_name).toBe("orders");
	});

	it("look_validation: full result is digest-free, reason stays readable", () => {
		// Engine-built lifecycle reasons + result messages embed raw physical
		// names (the validation binder works on lake tables).
		const out = projectValidationOverview(
			{
				artifactKey: "gl_invoice_match",
				state: "declared",
				stateReason: `Missing required tables: ${RAW_ORDERS} and ${RAW_CUSTOMERS}`,
			},
			{
				status: "executed",
				severity: "error",
				passed: false,
				message: `12 rows in ${RAW_ORDERS} have no match`,
				columnsUsed: [`${RAW_ORDERS}.customer_id`],
			},
		);
		expect(JSON.stringify(out)).not.toMatch(LEAK);
		expect(out.state_reason).toBe(
			"Missing required tables: orders and customers",
		);
		expect(out.message).toBe("12 rows in orders have no match");
	});

	it("why_validation: full result is digest-free, SQL + grounding sanitized", () => {
		const out = projectWhyValidation(
			"gl_invoice_match",
			{
				state: "executed",
				stateReason: null,
				strictness: 0.8,
				groundedAgainst: {
					from_table: RAW_ORDERS,
					to_table: RAW_CUSTOMERS,
					_table_name: RAW_ORDERS,
				},
			},
			{
				status: "executed",
				severity: "error",
				passed: false,
				message: `12 rows in ${RAW_ORDERS} have no match`,
				sqlUsed: `SELECT count(*) FROM lake.typed.${RAW_ORDERS} o LEFT JOIN lake.typed.${RAW_CUSTOMERS} c ON o.customer_id = c.id`,
				executedAt: new Date("2026-06-07T12:00:00Z"),
				details: { table: RAW_ORDERS, failing_rows: 12 },
				columnsUsed: [`${RAW_ORDERS}.customer_id`],
			},
			0,
		);
		expect(JSON.stringify(out)).not.toMatch(LEAK);
		// The SQL stays readable as evidence — digest prefixes dropped.
		expect(out.sql_used).toContain("lake.typed.orders");
		expect(out.sql_used).toContain("lake.typed.customers");
		// The grounding render display-maps known table-name keys and drops
		// engine-internal `_` keys (shared evidence sanitizer).
		expect(out.grounded_against).toContain('"from_table":"orders"');
		expect(out.grounded_against).not.toContain("_table_name");
	});
});

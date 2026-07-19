// Property-style leak tests (DAT-433): no content-keyed `src_<digest>` name
// reaches the agent through ANY tool projection. Post-DAT-639 physical table
// names are NARROW (no `src_<digest>__` prefix), so the digest only survives on
// the upload SOURCE name (`sources.name` = `src_<digest>`) and the staged-upload
// s3 URI — those are the live leak vectors these tests pin. Fixtures are
// ENGINE-SHAPED — they mirror what the engine actually persists:
//   - every evidence dict carries `_column_name` + `_table_name` (stamped by
//     `entropy/detectors/base.py` create_entropy_object),
//   - relationship detectors add explicit `from_table`/`to_table`
//     (`entropy/detectors/structural/relations.py`),
//   - workflow failures embed physical names in engine-built message text
//     (`worker.contracts.ProgressFailure`),
//   - an upload source's row carries `src_<digest>` as its name + the digest s3
//     URI in `connection_config` (the list_tables case).
//
// The property: JSON.stringify of the FULL projected result never matches
// /src_[0-9a-f]{40}/. list_tables/look_table carry the raw DuckDB name in
// `physical_name` (the run_sql round-trip key) — now narrow, so the property
// holds on the full result.

import { describe, expect, it, vi } from "vitest";

// Importing the tools pulls config.ts + the Postgres metadata client; mock both
// (with the `#/` alias — relative specifiers silently don't intercept) so the
// pure projections run with no env and no connection.
vi.mock("#/config", () => ({ config: { anthropicApiKey: "test" } }));
// Mode-shared base config (DAT-819) — reached transitively via the
// registry/db seam; parsing the real one needs env this test does not set.
vi.mock("#/config.base", () => ({ baseConfig: {} }));
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

// A content digest (sha-1 hex), as the upload sources mint them. Lives on the
// SOURCE name + URI now — never on a table name (DAT-639).
const D1 = "204bc8e118543a6c35654c1f68c43539a2e226f2";
// Narrow, workspace-unique table names (DAT-639): the physical name IS the
// display name — there is no digest to strip out of it.
const ORDERS = "orders";
const CUSTOMERS = "customers";

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
				_table_name: ORDERS,
			},
		],
	},
];

// Engine-shaped relationship evidence (relations.py): explicit from/to tables.
const relationshipEvidence = [
	{
		path_status: "ambiguous",
		from_table: ORDERS,
		to_table: CUSTOMERS,
		distinct_join_paths: 2,
		resolved_by_overlay: false,
		_column_name: "customer_id",
		_table_name: ORDERS,
	},
];

describe("agent name-leak property (DAT-433)", () => {
	it("why_column: full result is digest-free", () => {
		const readiness: WhyReadinessRow = {
			columnId: "c_amount",
			columnName: "amount",
			tableName: ORDERS,
			band: "investigate",
			bandStage: "add_source",
			bandComputedAt: null,
			worstIntentRisk: 0.42,
			intents: [],
		};
		const out = projectWhyData(readiness, columnEvidence, 1);
		expect(JSON.stringify(out)).not.toMatch(LEAK);
		expect(out.table_name).toBe("orders");
	});

	it("why_table: full result is digest-free", () => {
		const evidenceRows: WhyTableEvidenceRow[] = columnEvidence;
		const out = projectWhyTable(
			"t_orders",
			ORDERS,
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
			fromTableName: ORDERS,
			fromColumnName: "customer_id",
			toTableName: CUSTOMERS,
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
		// The relationship detector's explicit name keys survive verbatim (narrow).
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
			["c_from", { columnName: "customer_id", tableName: ORDERS }],
			["c_to", { columnName: "id", tableName: CUSTOMERS }],
		]);
		const out = projectRelationshipReadiness(row, names);
		expect(JSON.stringify(out)).not.toMatch(LEAK);
		expect(out?.from_table_name).toBe("orders");
	});

	it("list_tables: a content-keyed source name + upload URI never leak", () => {
		// The live digest vector post-DAT-639: the upload's source row carries
		// `src_<digest>` as its name and the digest s3 URI in connection_config.
		// The projection must surface the human filename, never the digest.
		const rows: InventoryTableRow[] = [
			{
				tableId: "t_orders",
				tableName: ORDERS,
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
		expect(JSON.stringify(out)).not.toMatch(LEAK);
		// The round-trip key carries the (now narrow) physical name.
		expect(out.physical_name).toBe(ORDERS);
		expect(out.table_name).toBe("orders");
		expect(out.source_name).toBe("orders.csv");
	});

	it("look_table: full result is digest-free", () => {
		const out = projectLookTable(
			"t_orders",
			ORDERS,
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
		expect(JSON.stringify(out)).not.toMatch(LEAK);
		expect(out.physical_name).toBe(ORDERS);
		expect(out.table_name).toBe("orders");
	});

	it("look_validation: full result is digest-free, reason stays readable", () => {
		const out = projectValidationOverview(
			{
				artifactKey: "gl_invoice_match",
				state: "declared",
				stateReason: `Missing required tables: ${ORDERS} and ${CUSTOMERS}`,
			},
			{
				sqlUsed: `SELECT count(*) FROM ${ORDERS}`,
				columnsUsed: [`${ORDERS}.customer_id`],
			},
			{
				status: "failed",
				passed: false,
				deviation: 12,
				magnitude: 100,
				message: `12 rows in ${ORDERS} have no match`,
			},
			{ tolerance: 0.01, severity: "error" },
		);
		expect(JSON.stringify(out)).not.toMatch(LEAK);
		expect(out.state_reason).toBe(
			"Missing required tables: orders and customers",
		);
		expect(out.message).toBe("12 rows in orders have no match");
	});

	it("why_validation: full result is digest-free, SQL + grounding readable", () => {
		const out = projectWhyValidation(
			"gl_invoice_match",
			{
				state: "executed",
				stateReason: null,
				strictness: 0.8,
				groundedAgainst: {
					from_table: ORDERS,
					to_table: CUSTOMERS,
					_table_name: ORDERS,
				},
			},
			{
				sqlUsed: `SELECT count(*) FROM lake.typed.${ORDERS} o LEFT JOIN lake.typed.${CUSTOMERS} c ON o.customer_id = c.id`,
				executedAt: new Date("2026-06-07T12:00:00Z"),
				columnsUsed: [`${ORDERS}.customer_id`],
			},
			{
				status: "failed",
				passed: false,
				deviation: 12,
				magnitude: 100,
				message: `12 rows in ${ORDERS} have no match`,
			},
			{ tolerance: 0.01, severity: "error" },
			0,
		);
		expect(JSON.stringify(out)).not.toMatch(LEAK);
		expect(out.sql_used).toContain("lake.typed.orders");
		expect(out.sql_used).toContain("lake.typed.customers");
		// The grounding render keeps narrow table-name keys + drops engine `_` keys.
		expect(out.grounded_against).toContain('"from_table":"orders"');
		expect(out.grounded_against).not.toContain("_table_name");
	});
});

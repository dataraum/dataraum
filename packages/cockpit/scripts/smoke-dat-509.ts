// Lane smoke for DAT-509 — the multi-grain read alignment against a REAL
// materialized ws schema (scratch Postgres; run via scripts/smoke-dat-509.sh).
//
// The unit tests pin the pure pick/merge logic; this exercises the actual SQL
// the changed tools emit against the real views (current_entropy_readiness /
// current_entropy_objects / current_semantic_annotations /
// current_validation_results), with rows seeded at BOTH grains:
//
//   - column c-1 carries an add_source table-head readiness row (blocked) AND
//     a later session-head re-roll (ready) → every surface must report READY;
//   - column c-2 carries a contested point_in_time annotation → the query
//     sub-agent's schema block must tag it;
//   - one executed validation result carries digest-prefixed columns_used →
//     why_validation must surface them stripped.
//
// No LLM is touched: the asserted paths are the non-synthesis ones (and
// why_column runs on the readiness-less column, where analysis is skipped).
//
// Run from packages/cockpit:  bash scripts/smoke-dat-509.sh

import assert from "node:assert/strict";
import { SQL } from "bun";

import { config } from "#/config";
import { listTables } from "#/tools/list-tables";
import { lookTable } from "#/tools/look-table";
import { lookValidation } from "#/tools/look-validation";
import { buildSchemaBlock } from "#/tools/query-context";
import { whyColumn } from "#/tools/why-column";
import { whyTable } from "#/tools/why-table";
import { whyValidation } from "#/tools/why-validation";

const WS = `ws_${config.dataraumWorkspaceId.replaceAll("-", "_")}`;
const DIGEST = "204bc8e118543a6c35654c1f68c43539a2e226f2";

async function seed(): Promise<void> {
	const sql = new SQL(config.metadataDatabaseUrl);
	// Raw inserts into the ws schema; the tools read the _read views on top.
	await sql.unsafe(`
		INSERT INTO ${WS}.investigation_sessions
			(session_id, status, started_at, intent, step_count)
		VALUES ('sess-1', 'active', '2026-06-11T08:00:00', 'exploratory', 0);

		INSERT INTO ${WS}.sources
			(source_id, name, source_type, created_at, updated_at)
		VALUES ('src-1', 'erp', 'csv', '2026-06-11T08:00:00', '2026-06-11T08:00:00');

		INSERT INTO ${WS}.tables
			(table_id, source_id, table_name, layer, created_at, row_count)
		VALUES ('t-1', 'src-1', 'orders', 'typed', '2026-06-11T08:00:00', 10);

		INSERT INTO ${WS}.columns
			(column_id, table_id, column_name, column_position, resolved_type)
		VALUES
			('c-1', 't-1', 'amount', 1, 'DECIMAL'),
			('c-2', 't-1', 'balance', 2, 'DECIMAL');

		INSERT INTO ${WS}.metadata_snapshot_head
			(head_id, target, stage, run_id, promoted_at, version)
		VALUES
			('h-add', 'table:t-1',    'detect',              'run-add', '2026-06-11T09:00:00', 1),
			('h-ses', 'session:sess-1', 'detect',            'run-ses', '2026-06-11T10:00:00', 1),
			('h-sem', 'table:t-1',    'semantic_per_column', 'run-sem', '2026-06-11T09:30:00', 1),
			('h-om',  'session:sess-1', 'operating_model',   'run-om',  '2026-06-11T11:00:00', 1);

		-- The fault line: same column, table-head verdict (blocked) + a later
		-- session re-roll (ready). The session-grain row must win everywhere.
		INSERT INTO ${WS}.entropy_readiness
			(readiness_id, session_id, target, table_id, column_id, run_id,
			 band, worst_intent_risk, intents, computed_at)
		VALUES
			('r-add', 'sess-1', 'column:c-1', 't-1', 'c-1', 'run-add',
			 'blocked', 0.9, '[]'::jsonb, '2026-06-11T09:00:00'),
			('r-ses', 'sess-1', 'column:c-1', 't-1', 'c-1', 'run-ses',
			 'ready', 0.1, '[]'::jsonb, '2026-06-11T10:00:00');

		-- Contested stock/flow annotation for c-2 (semantic_per_column head).
		INSERT INTO ${WS}.semantic_annotations
			(annotation_id, session_id, column_id, run_id, business_concept,
			 temporal_behavior, temporal_behavior_contested, annotated_at)
		VALUES ('a-1', 'sess-1', 'c-2', 'run-sem', 'account_balance',
			 'point_in_time', true, '2026-06-11T09:30:00');

		-- Executed validation with digest-prefixed columns_used (operating_model head).
		INSERT INTO ${WS}.validation_results
			(result_id, session_id, run_id, validation_id, table_ids, columns_used,
			 status, severity, passed, message, executed_at)
		VALUES ('v-1', 'sess-1', 'run-om', 'tb_gl_reconciliation',
			 '["t-1"]'::json, '["src_${DIGEST}__orders.amount"]'::json,
			 'executed', 'critical', false, 'TB does not reconcile',
			 '2026-06-11T11:00:00');
	`);
	await sql.close();
}

async function main(): Promise<void> {
	await seed();

	// 1. look_table: the per-column grid picks the session re-roll over the
	//    stale add_source verdict, and carries the semantic join.
	const grid = await lookTable({ table_id: "t-1" });
	const amount = grid.columns.find((c) => c.column_name === "amount");
	const balance = grid.columns.find((c) => c.column_name === "balance");
	assert.equal(amount?.band, "ready", "look_table: session re-roll must win");
	assert.equal(balance?.band, null, "look_table: no readiness → unanalyzed");
	assert.equal(balance?.semantic?.business_concept, "account_balance");

	// 2. list_tables: the band tally counts the picked grain, not both rows.
	const inventory = await listTables();
	const orders = inventory.find((t) => t.table_name === "orders");
	assert.equal(orders?.readiness.ready, 1, "list_tables: ready=1 (c-1 picked)");
	assert.equal(orders?.readiness.blocked, 0, "list_tables: stale row not counted");
	assert.equal(orders?.readiness.unanalyzed, 1, "list_tables: c-2 unanalyzed");
	assert.equal(orders?.worst_band, "ready");

	// 3. why_column on the readiness-less column: SQL executes, no LLM call
	//    (analyzed=false), found=true.
	const why = await whyColumn({ column_id: "c-2" });
	assert.equal(why.found, true);
	assert.equal(why.analyzed, false, "why_column: c-2 has no readiness row");
	assert.equal(why.analysis, "", "why_column: no synthesis without readiness");

	// 4. why_table: no table-target readiness seeded → unanalyzed shell, both
	//    queries execute (pick + merge over the real views).
	const whyT = await whyTable({ session_id: "sess-1", table_id: "t-1" });
	assert.equal(whyT.analyzed, false);

	// 5. The query sub-agent's schema block: stock/flow + contested markers.
	const block = await buildSchemaBlock();
	assert.ok(
		block.includes(
			'"balance" :: DECIMAL  [concept: account_balance] (point_in_time)  [stock/flow contested]',
		),
		`schema block must tag the contested stock — got:\n${block}`,
	);

	// 6. why_validation: columns_used surfaced, digest-stripped.
	const whyV = await whyValidation({
		session_id: "sess-1",
		validation_id: "tb_gl_reconciliation",
	});
	assert.equal(whyV.found, true);
	assert.deepEqual(whyV.columns_used, ["orders.amount"]);

	// 7. look_validation: the promoted operating_model head reads as analyzed.
	const lookV = await lookValidation({ session_id: "sess-1" });
	assert.equal(lookV.analyzed, true);

	console.log("smoke-dat-509: ALL GREEN (7 surfaces, multi-grain seed)");
}

await main();
process.exit(0);

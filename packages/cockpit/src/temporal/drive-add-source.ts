// E4a integration driver (DAT-344, P3): prove the TS workflow → Python activity
// path end-to-end against a running compose stack (temporal + engine-worker +
// workflow-worker + postgres).
//
// In production the addSourceWorkflow's caller seeds the Source +
// InvestigationSession (E4b). Here this script does it directly, then starts the
// workflow and asserts both phases completed and import produced raw tables —
// which means the cross-language dispatch + the Python substrate both worked.
//
// Run against the published compose ports, e.g.:
//   TEMPORAL_HOST=localhost:7233 TEMPORAL_NAMESPACE=default \
//   TEMPORAL_TASK_QUEUE=dataraum-pipeline \
//   METADATA_DATABASE_URL=postgresql://dataraum:dataraum@localhost:5432/dataraum \
//   DATARAUM_WORKSPACE_ID=00000000-0000-0000-0000-000000000001 \
//   SOURCE_PATH=/var/lib/dataraum/sources/orders.csv \
//   bun run src/temporal/drive-add-source.ts

import { randomUUID } from "node:crypto";
import { Client, Connection } from "@temporalio/client";
import postgres from "postgres";
import { z } from "zod";
import type { PhaseActivityInput, PhaseActivityResult } from "./types";

const env = z
	.object({
		TEMPORAL_HOST: z.string().min(1),
		TEMPORAL_NAMESPACE: z.string().min(1),
		TEMPORAL_TASK_QUEUE: z.string().min(1),
		METADATA_DATABASE_URL: z.string().min(1),
		DATARAUM_WORKSPACE_ID: z.string().min(1),
		// Container path the engine-worker sees (mounted sources dir).
		SOURCE_PATH: z.string().default("/var/lib/dataraum/sources/orders.csv"),
	})
	.parse(process.env);

const schema = `ws_${env.DATARAUM_WORKSPACE_ID.replaceAll("-", "_")}`;

async function seed(sourceId: string, sessionId: string): Promise<void> {
	// Source.name is UNIQUE — keep it unique per run so the driver is repeatable.
	const name = `orders_${sourceId.slice(0, 8)}`;
	const sql = postgres(env.METADATA_DATABASE_URL, { onnotice: () => {} });
	try {
		await sql.begin(async (tx) => {
			await tx.unsafe(`SET LOCAL search_path TO "${schema}", public`);
			await tx`
				INSERT INTO sources (source_id, name, source_type, connection_config, status, created_at, updated_at)
				VALUES (${sourceId}, ${name}, 'csv', ${sql.json({ path: env.SOURCE_PATH })}, 'configured', now(), now())
				ON CONFLICT (source_id) DO NOTHING`;
			await tx`
				INSERT INTO investigation_sessions (session_id, source_id, intent, status, started_at, step_count)
				VALUES (${sessionId}, ${sourceId}, 'e4a drive', 'active', now(), 0)
				ON CONFLICT (session_id) DO NOTHING`;
		});
	} finally {
		await sql.end();
	}
}

async function main(): Promise<void> {
	const sourceId = randomUUID();
	const sessionId = randomUUID();
	await seed(sourceId, sessionId);

	const connection = await Connection.connect({ address: env.TEMPORAL_HOST });
	try {
		const client = new Client({
			connection,
			namespace: env.TEMPORAL_NAMESPACE,
		});
		const input: PhaseActivityInput = {
			workspace_id: env.DATARAUM_WORKSPACE_ID,
			source_id: sourceId,
			session_id: sessionId,
		};
		const results = await client.workflow.execute<
			(input: PhaseActivityInput) => Promise<PhaseActivityResult[]>
		>("addSourceWorkflow", {
			taskQueue: env.TEMPORAL_TASK_QUEUE,
			workflowId: `addsource-${sourceId}`,
			args: [input],
		});

		const [importResult, typingResult] = results;
		console.log("import:", JSON.stringify(importResult));
		console.log("typing:", JSON.stringify(typingResult));

		const rawTables = (importResult?.outputs?.raw_tables as unknown[]) ?? [];
		if (importResult?.status !== "completed" || rawTables.length === 0) {
			throw new Error(
				`import did not complete with raw tables: ${importResult?.error}`,
			);
		}
		if (typingResult?.status !== "completed") {
			throw new Error(`typing did not complete: ${typingResult?.error}`);
		}
		console.log(
			"✓ addSourceWorkflow completed: import + typing landed via Temporal",
		);
	} finally {
		await connection.close();
	}
}

main().catch((err) => {
	console.error("✗ drive failed:", err);
	process.exit(1);
});

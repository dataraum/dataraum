// SPIKE: resume the re-injection from begin_session (add_source already typed
// the 8 tables; the first begin_session died on an unrelated semantic_per_table
// LLM shape flake). Mirrors smoke-operating-model.ts stages 2+3 verbatim.
//
// Run:  TEMPORAL_TASK_QUEUE=engine-<wsid> TYPED_TABLE_IDS=<comma list> \
//         bun spikes/resume-om.spike.ts

import { Client, Connection } from "@temporalio/client";
import { z } from "zod";

import type {
	BeginSessionInput,
	BeginSessionResult,
	OperatingModelInput,
	OperatingModelResult,
} from "#/temporal/types";
import {
	beginSessionWorkflowId,
	operatingModelWorkflowId,
} from "#/temporal/workflow-id";

const env = z
	.object({
		TEMPORAL_HOST: z.string().min(1),
		TEMPORAL_NAMESPACE: z.string().min(1),
		TEMPORAL_TASK_QUEUE: z.string().min(1),
		DATARAUM_WORKSPACE_ID: z.string().min(1),
		TYPED_TABLE_IDS: z.string().min(1),
	})
	.parse(process.env);

const VERTICAL = "finance";
const tableIds = env.TYPED_TABLE_IDS.split(",")
	.map((t) => t.trim())
	.filter(Boolean);

async function main(): Promise<void> {
	const connection = await Connection.connect({ address: env.TEMPORAL_HOST });
	try {
		const client = new Client({ connection, namespace: env.TEMPORAL_NAMESPACE });

		const beginInput: BeginSessionInput = {
			workspace_id: env.DATARAUM_WORKSPACE_ID,
			tables: tableIds,
			verticals: [VERTICAL],
		};
		const beginHandle = await client.workflow.start<
			(p: BeginSessionInput) => Promise<BeginSessionResult>
		>("beginSessionWorkflow", {
			taskQueue: env.TEMPORAL_TASK_QUEUE,
			workflowId: beginSessionWorkflowId(env.DATARAUM_WORKSPACE_ID),
			args: [beginInput],
		});
		const beginResult = (await beginHandle.result()) as BeginSessionResult;
		console.log(`✓ begin_session completed: run_id=${beginResult.run_id}`);

		const input: OperatingModelInput = {
			workspace_id: env.DATARAUM_WORKSPACE_ID,
			verticals: [VERTICAL],
		};
		const handle = await client.workflow.start<
			(p: OperatingModelInput) => Promise<OperatingModelResult>
		>("operatingModelWorkflow", {
			taskQueue: env.TEMPORAL_TASK_QUEUE,
			workflowId: operatingModelWorkflowId(env.DATARAUM_WORKSPACE_ID),
			args: [input],
		});
		const omResult = (await handle.result()) as OperatingModelResult;
		console.log(
			`✓ operating_model completed: run_id=${omResult.run_id ?? "?"}`,
		);
		console.log(JSON.stringify(omResult, null, 2).slice(0, 2000));
	} finally {
		await connection.close();
	}
}

await main();

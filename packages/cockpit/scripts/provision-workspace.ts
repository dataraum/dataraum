// Workspace lifecycle trigger (DAT-820) — the minimal programmatic surface
// over src/portal/lifecycle.ts. The create-workspace UI/UX is DAT-821; this
// script is how a workspace is provisioned/archived until then (and the lane
// smoke's driver).
//
//   bun run workspace:create -- --name "Dept 3" --subdomain ws3 \
//       --vertical finance --member dev@dataraum.dev [--id <uuid>]
//   bun run workspace:archive -- --id <workspace-id>
//
// Re-running EITHER op with the same id converges (ADR-0010): a create that
// died mid-way resumes from the registry row; an archive re-sweeps whatever
// remains. `--id` on create is exactly that resume handle.
//
// Runs on the HOST against the dev compose stack (`bun run` from
// packages/cockpit, like the other smoke scripts) — the provisioner env then
// points at the published ports and the host docker socket:
//
//   COCKPIT_DATABASE_URL=postgresql://dataraum:dataraum@localhost:5432/cockpit_db \
//   BETTER_AUTH_SECRET=dataraum-dev-secret \
//   DATARAUM_PORTAL_ORIGIN=http://dataraum.localhost \
//   PROVISIONER_DATABASE_URL=postgresql://dataraum:dataraum@localhost:5432/dataraum \
//   DUCKLAKE_CATALOG_URL=postgresql://dataraum:dataraum@localhost:5432/dataraum_lake_catalog \
//   CADDY_ADMIN_URL=http://localhost:2019 \
//   S3_ENDPOINT=localhost:8333 S3_BUCKET=dataraum-lake S3_USE_SSL=false \
//   S3_ACCESS_KEY_ID=dataraum S3_SECRET_ACCESS_KEY=dataraum-s3-secret \
//   bun run workspace:create -- --name "Dept 3" --subdomain ws3 --vertical finance
//
// In-container (the portal service carries this env — see the portal service
// in packages/infra/docker-compose.yml) the same functions run inside the
// portal server process; DAT-821 exposes them as server fns.

import { parseArgs } from "node:util";
import { inArray } from "drizzle-orm";
import { cockpitDb } from "#/db/cockpit/client";
import { users } from "#/db/cockpit/schema";
import { archiveWorkspace, createWorkspace } from "#/portal/lifecycle";
import { runLifecycle } from "#/portal/lifecycle-deps";

function usage(): never {
	console.error(
		"usage:\n" +
			'  provision-workspace.ts create --name <name> --subdomain <label> --vertical <vertical> [--member <email>]... [--id <uuid>]\n' +
			"  provision-workspace.ts archive --id <workspace-id>",
	);
	process.exit(2);
}

async function resolveMemberIds(emails: string[]): Promise<string[]> {
	if (emails.length === 0) {
		return [];
	}
	const rows = await cockpitDb
		.select({ id: users.id, email: users.email })
		.from(users)
		.where(inArray(users.email, emails));
	const found = new Map(rows.map((r) => [r.email, r.id]));
	const missing = emails.filter((email) => !found.has(email));
	if (missing.length > 0) {
		throw new Error(
			`no cockpit_db user for: ${missing.join(", ")} — members must exist ` +
				"(sign up through the portal, or bring the stack up so the dev " +
				"user is seeded)",
		);
	}
	return [...found.values()];
}

async function main(): Promise<void> {
	const { values, positionals } = parseArgs({
		args: process.argv.slice(2),
		allowPositionals: true,
		options: {
			id: { type: "string" },
			name: { type: "string" },
			subdomain: { type: "string" },
			vertical: { type: "string" },
			member: { type: "string", multiple: true },
		},
	});
	const command = positionals[0];

	if (command === "create") {
		if (!values.name || !values.subdomain || !values.vertical) {
			usage();
		}
		const memberUserIds = await resolveMemberIds(values.member ?? []);
		const result = await runLifecycle((deps) =>
			createWorkspace(
				{
					workspaceId: values.id,
					name: values.name as string,
					subdomain: values.subdomain as string,
					vertical: values.vertical as string,
					memberUserIds,
				},
				deps,
			),
		);
		console.log(
			result.already
				? `workspace ${result.workspaceId} already ready — no-op`
				: `workspace ${result.workspaceId} is ${result.state}`,
		);
		console.log(
			`  subdomain: ${values.subdomain} (route ws-${result.workspaceId})`,
		);
		return;
	}

	if (command === "archive") {
		if (!values.id) {
			usage();
		}
		const result = await runLifecycle((deps) =>
			archiveWorkspace(values.id as string, deps),
		);
		console.log(
			result.already
				? `workspace ${result.workspaceId} already archived — no-op`
				: `workspace ${result.workspaceId} is ${result.state}`,
		);
		return;
	}

	usage();
}

main()
	.then(() => process.exit(0))
	.catch((err) => {
		console.error(err instanceof Error ? err.message : err);
		process.exit(1);
	});

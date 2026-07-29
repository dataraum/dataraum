// The fixture workspace: a throwaway Postgres carrying the ENGINE's real
// generated schema, so cockpit code can be exercised against the shape the
// engine actually produces instead of against a mock's idea of it.
//
// Why this exists (DAT-671 retro): every high-value cockpit defect of the
// wave was invisible to unit tests, tsc and build. Those gates are
// structural; the cockpit's truth is UX-semantic — an inert code path, a
// check dropped before the render, a report stored in a shape its own reader
// cannot render. None of that is a type error. Catching it needs the real
// composers running against a real seeded schema.
//
// WHY RAW DOCKER AND NOT TESTCONTAINERS
// The integration project must run under `bun --bun`, because the metadata
// Drizzle client imports `SQL` from "bun" and simply cannot load under Node.
// The JS testcontainers library hangs indefinitely under the Bun runtime
// (probed: fine under Node, no output and no container after 7 minutes under
// Bun). So the container is driven through the docker CLI directly — which is
// also exactly what scripts/pull-metadata.sh already does in CI, with the
// same image and the same seeding recipe. No new dependency.
//
// SCHEMA LAYOUT mirrors pull-metadata.sh, because the generated Drizzle
// mirror in src/db/metadata/ was introspected from precisely that layout:
//   engine  — the raw run-stamped tables (schema.sql, applied via search_path)
//   public  — the promoted-read views (schema_read.sql, __WS__/__READ__
//             substituted); this is what the reader role sees at runtime, so
//             unqualified names in the mirror resolve here.
// Writes in tests therefore target `engine.<table>`; reads go through the
// views, exactly as in production (ADR-0008).

import { spawnSync } from "node:child_process";
import { readFileSync } from "node:fs";
import { fileURLToPath } from "node:url";

// Keep in lockstep with packages/infra/docker-compose.yml + pull-metadata.sh.
const PG_IMAGE = "postgres:19beta1";
const RAW_SCHEMA = "engine";
const SCRATCH_DB = "scratch";
const COCKPIT_DB = "cockpit_db";

const enginePath = (name: string) =>
	fileURLToPath(new URL(`../../../engine/${name}`, import.meta.url));

export interface FixtureWorkspace {
	/** DSN for the engine metadata surface (read views in `public`). */
	metadataUrl: string;
	/** DSN for cockpit_db (hand-written Drizzle schema, real migrations). */
	cockpitUrl: string;
	/** Docker container id, for teardown. */
	containerId: string;
	/** Run arbitrary SQL against a database in the fixture. */
	psql(sql: string, database?: string): void;
}

function docker(args: string[], input?: string) {
	return spawnSync("docker", args, {
		input,
		encoding: "utf8",
		// Image pull on a cold machine dominates; everything else is fast.
		timeout: 300_000,
	});
}

/** Is a docker daemon actually usable here? Returns a reason when not. */
export function dockerUnavailableReason(): string | null {
	const probe = docker(["info", "--format", "{{.ServerVersion}}"]);
	if (probe.error) return `docker CLI not runnable: ${probe.error.message}`;
	if (probe.status !== 0) {
		const err = (probe.stderr || "").trim().split("\n")[0] || "unknown error";
		return `docker daemon not available: ${err}`;
	}
	return null;
}

function applySql(containerId: string, database: string, sql: string): void {
	const res = docker(
		[
			"exec",
			"-i",
			containerId,
			"psql",
			"-U",
			"postgres",
			"-d",
			database,
			"-v",
			"ON_ERROR_STOP=1",
			"-f",
			"-",
		],
		sql,
	);
	if (res.status !== 0) {
		throw new Error(
			`fixture psql failed (db=${database}):\n${(res.stderr || "").slice(-2000)}`,
		);
	}
}

/**
 * Boot a fixture workspace. Throws on failure — callers that want a skip
 * instead should check `dockerUnavailableReason()` first.
 */
export function startFixtureWorkspace(): FixtureWorkspace {
	const run = docker([
		"run",
		"--rm",
		"-d",
		// Teardown removes this container, but a SIGKILLed run cannot. The label
		// makes any orphan identifiable and bulk-removable:
		//   docker rm -f $(docker ps -aq --filter label=dataraum-fixture=1)
		"--label",
		"dataraum-fixture=1",
		"-e",
		"POSTGRES_PASSWORD=scratch",
		"-e",
		`POSTGRES_DB=${SCRATCH_DB}`,
		"-p",
		"127.0.0.1::5432",
		PG_IMAGE,
	]);
	if (run.status !== 0) {
		throw new Error(`failed to start fixture Postgres: ${run.stderr}`);
	}
	const containerId = run.stdout.trim();

	try {
		// TCP probe on purpose (same reasoning as pull-metadata.sh): the
		// entrypoint's init-phase server answers the unix socket while running
		// with listen_addresses='' — only the real server passes this.
		let ready = false;
		for (let i = 0; i < 150; i++) {
			const probe = docker([
				"exec",
				containerId,
				"pg_isready",
				"-h",
				"127.0.0.1",
				"-U",
				"postgres",
				"-q",
			]);
			if (probe.status === 0) {
				ready = true;
				break;
			}
			Atomics.wait(new Int32Array(new SharedArrayBuffer(4)), 0, 0, 200);
		}
		if (!ready) throw new Error("fixture Postgres never became ready");

		const portRes = docker(["port", containerId, "5432/tcp"]);
		const port = portRes.stdout.trim().split("\n")[0]?.split(":").pop();
		if (!port)
			throw new Error(`could not read fixture port: ${portRes.stdout}`);

		// --- engine metadata surface: raw tables + promoted-read views ---
		const rawDdl = readFileSync(enginePath("schema.sql"), "utf8");
		const readDdl = readFileSync(enginePath("schema_read.sql"), "utf8")
			.replaceAll("__READ__", "public")
			.replaceAll("__WS__", RAW_SCHEMA);
		applySql(
			containerId,
			SCRATCH_DB,
			[
				`CREATE SCHEMA ${RAW_SCHEMA};`,
				`SET search_path TO ${RAW_SCHEMA};`,
				rawDdl,
				readDdl,
			].join("\n"),
		);

		// --- cockpit_db: its own database, same instance ---
		applySql(containerId, SCRATCH_DB, `CREATE DATABASE ${COCKPIT_DB};`);

		const base = `postgresql://postgres:scratch@127.0.0.1:${port}`;
		return {
			metadataUrl: `${base}/${SCRATCH_DB}`,
			cockpitUrl: `${base}/${COCKPIT_DB}`,
			containerId,
			psql: (sql: string, database = SCRATCH_DB) =>
				applySql(containerId, database, sql),
		};
	} catch (err) {
		docker(["rm", "-f", containerId]);
		throw err;
	}
}

/**
 * Bring cockpit_db up to date using the REAL checked-in migrations, driven by
 * the same tool the repo's own `db:migrate:cockpit` script uses.
 *
 * Deliberately not a snapshot of the schema: lanes land migrations
 * concurrently, and a hand-copied DDL would silently test yesterday's shape.
 * Running the migration folder means a sibling's new migration is picked up
 * the moment this branch rebases onto it.
 */
export function applyCockpitMigrations(cockpitUrl: string): void {
	const res = spawnSync(
		"bun",
		[
			"--bun",
			"drizzle-kit",
			"migrate",
			"--config",
			"drizzle.config.cockpit.ts",
		],
		{
			encoding: "utf8",
			timeout: 180_000,
			cwd: fileURLToPath(new URL("../..", import.meta.url)),
			env: { ...process.env, COCKPIT_DATABASE_URL: cockpitUrl },
		},
	);
	if (res.status !== 0) {
		throw new Error(
			`cockpit_db migrations failed:\n${res.stdout}\n${res.stderr}`,
		);
	}
}

export function stopFixtureWorkspace(containerId: string): void {
	docker(["rm", "-f", containerId]);
}

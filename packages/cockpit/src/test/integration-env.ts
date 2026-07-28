// The ONE env stub every integration suite uses to make `config` parse.
//
// Why this module exists: `src/config.ts` and `src/config.base.ts` parse their
// Zod schemas at MODULE EVAL and throw on the first missing field. Any suite
// that imports a module which transitively reaches either one must therefore
// populate the full required set BEFORE that import.
//
// This used to be a hand-copied `REQUIRED_DEFAULTS` block in each integration
// test — ELEVEN copies (twelve counting portal/lock's ad-hoc one-field stub)
// that drifted apart. When DAT-819 added the required `BETTER_AUTH_SECRET` to
// config.base, three of them (duckdb connect/probe/probe-stream) started
// failing at import with "Invalid cockpit base configuration"; the rest only
// escaped because their stack gate skipped them first. The copies had also
// drifted among themselves — connect.integration was missing
// DATARAUM_CONFIG_PATH as well.
//
// So: one home. A new required config field is added HERE, once, and every
// integration suite keeps parsing.
//
// These are PLACEHOLDERS, not infrastructure. They exist so the schema
// validates; nothing here points at a reachable service. Suites that need real
// infrastructure take it from the fixture workspace (./fixture-workspace.ts)
// and pass it through `overrides`.
//
// EVERY placeholder must be a value no real environment would ever hold. That
// is not tidiness — `providedByEnvironment` (below) decides whether real
// infrastructure is configured, and a placeholder that COLLIDES with a real
// documented value makes that gate answer "no" for exactly the developers who
// followed the setup instructions, silently skipping the suites written for
// their stack. The S3 credential placeholders used to be `dataraum` /
// `dataraum-s3-secret`, byte-identical to .env.example — so `cp .env.example
// .env` (the documented dev setup) skipped the SeaweedFS round-trip. Hence the
// `integration-env.invalid` posture everywhere, not just on the DSNs.

const UNREACHABLE = "postgresql://placeholder@integration-env.invalid:5432";

/** Marker shared by every non-DSN placeholder, for the same collision reason. */
const INVALID = "integration-env-invalid";

/** Workspace identity every fixture-backed suite shares. */
export const TEST_WORKSPACE_ID = "00000000-0000-0000-0000-000000000001";

/** Keys THIS module wrote — never "provided by the environment". */
const INJECTED = new Set<string>();

/**
 * The complete set of env vars `config.ts` + `config.base.ts` require to parse.
 * Keep this in lockstep with those two schemas — that is the whole point of
 * the module. Optional-in-schema vars are deliberately absent.
 */
function requiredDefaults(): Record<string, string> {
	return {
		// --- config.base.ts (both roles) ---
		COCKPIT_DATABASE_URL: `${UNREACHABLE}/cockpit_db`,
		BETTER_AUTH_SECRET: "integration-test-auth-secret",

		// --- config.ts (workspace role) ---
		METADATA_DATABASE_URL: `${UNREACHABLE}/metadata_read`,
		METADATA_WRITER_DATABASE_URL: `${UNREACHABLE}/metadata_write`,
		DATARAUM_WORKSPACE_ID: TEST_WORKSPACE_ID,
		DATARAUM_CONFIG_PATH: "/opt/dataraum/config",
		DATARAUM_LAKE_PATH: "s3://dataraum-lake/lake",
		DUCKLAKE_CATALOG_URL: `${UNREACHABLE}/lake_catalog`,
		ANTHROPIC_API_KEY: `sk-ant-${INVALID}`,
		// NOT .env.example's values — see the collision note above. All three
		// S3 keys gate the live-object-store suite, so all three must be
		// unmistakably fake.
		S3_ENDPOINT: `${INVALID}.localdomain:8333`,
		S3_ACCESS_KEY_ID: `${INVALID}-access-key`,
		S3_SECRET_ACCESS_KEY: `${INVALID}-secret-key`,
		S3_BUCKET: `${INVALID}-bucket`,
	};
}

/**
 * Populate `process.env` so the config schemas parse.
 *
 * Precedence, lowest to highest: built-in placeholder < ambient `process.env`
 * < `overrides`. Ambient values win over placeholders so a developer pointing
 * at their own stack keeps working; `overrides` win over everything so the
 * fixture workspace can inject its real, container-assigned DSNs.
 *
 * MUST be called before the first import of any module that reaches `config`
 * — in practice: call it at test-module top level, then `await import(...)`
 * the subject inside `beforeAll`.
 */
export function applyIntegrationEnv(
	overrides: Record<string, string> = {},
): void {
	for (const [key, value] of Object.entries(requiredDefaults())) {
		if (!process.env[key]) {
			process.env[key] = value;
			INJECTED.add(key);
		}
	}
	for (const [key, value] of Object.entries(overrides)) {
		process.env[key] = value;
		INJECTED.add(key);
	}
}

/**
 * Did the ENVIRONMENT supply this var, as opposed to our placeholder?
 *
 * This is the canonical gate for "is real infrastructure configured". Read it
 * instead of `process.env` directly: vitest reuses a worker process across
 * test files, so once ANY sibling suite has called `applyIntegrationEnv()`,
 * a bare `!!process.env.COCKPIT_DATABASE_URL` is true even on a bare checkout
 * — and the suite would run against an unreachable placeholder instead of
 * skipping. Comparing against the known placeholder is order-independent, so
 * it holds no matter which file loaded first.
 *
 * Reachability is NOT enough on its own for credentialed services. This repo
 * runs several lanes at once, so a sibling's compose stack may well be
 * listening on the expected port with DIFFERENT credentials — a port probe
 * passes and the first authenticated call then fails. Gate on credentials
 * being real, then (optionally) on reachability.
 *
 * Two independent checks, because neither alone is sufficient:
 *  · WE WROTE IT — covers the fixture DSNs, which are real, reachable values
 *    that are nonetheless not the developer's infrastructure. Without this a
 *    legacy stack-gated suite would read the fixture as "real infra" and hard
 *    fail against a Postgres that has no ws_<id> schema.
 *  · IT EQUALS A PLACEHOLDER — covers cross-file leakage, which the first
 *    check cannot see: vitest reuses a worker process across files, and this
 *    module's state is per-module-instance, so a sibling's write is visible in
 *    `process.env` while its INJECTED entry is not. Value comparison is
 *    order-independent and holds regardless of which file loaded first.
 */
export function providedByEnvironment(key: string): boolean {
	const value = process.env[key];
	if (!value) return false;
	if (INJECTED.has(key)) return false;
	return value !== requiredDefaults()[key];
}

/**
 * Suite title carrying a skip reason, so a skipped run SAYS why in the
 * reporter. `describe.skipIf` renders an unexplained "skipped" otherwise —
 * indistinguishable from a suite nobody meant to run, which is how a silently
 * skipped round-trip goes unnoticed for a wave.
 */
export function suiteTitle(title: string, skipReason: string | null): string {
	return skipReason ? `${title} [SKIPPED: ${skipReason}]` : title;
}

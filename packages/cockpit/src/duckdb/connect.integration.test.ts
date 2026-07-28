// Real in-process DuckDB integration for connect's FILE path (DAT-386/DAT-381).
//
// connect's file path now accepts ONLY an `s3://<bucket>/<key>` URI — local
// paths are an arbitrary-file-read hole and are refused (DAT-386). So the file
// round-trip is proven against a real object staged in SeaweedFS (gated below);
// the rejection of a local path is asserted hermetically. The database path is
// unit-tested with a mocked probe (connect.test.ts); probe's own ATTACH
// round-trip is probe.integration.
//
// Importing connect transitively boots config.ts (via probe), so we stub the
// required env before the dynamic import — same approach as teach.integration.

import { beforeAll, describe, expect, it } from "vitest";

import {
	applyIntegrationEnv,
	providedByEnvironment,
	suiteTitle,
} from "#/test/integration-env";

applyIntegrationEnv();

// biome-ignore lint/suspicious/noExplicitAny: dynamic-imported module shape
let connect: any;

beforeAll(async () => {
	// Dynamic import so the env stub above is in place before config.ts loads.
	({ connect } = await import("./connect"));
});

describe("connect file path rejects non-bucket paths (DAT-386)", () => {
	// Real boot of connect() (config.ts loaded): a local path must be refused
	// before any DuckDB work — proves the validator is wired into the live module,
	// not just the unit mock. This is the arbitrary-file-read fix.
	it.each([
		"/etc/passwd",
		"/app/.env",
		"../foo.csv",
		"file:///etc/passwd",
	])("rejects connect(file, path=%s)", async (path: string) => {
		await expect(connect({ source_kind: "file", path })).rejects.toThrow();
	});
});

// connect's s3:// FILE path (DAT-386): stage a CSV to the SAME SeaweedFS bucket
// via the real @aws-lite PutObject, then sniff it over `s3://` through the SAME
// ConnectSchema — proving the upload→bucket→connect round-trip end-to-end and
// that connectFile registers the S3 secret for s3:// paths. Gated on a reachable
// SeaweedFS S3 gateway (compose stack up); self-skips otherwise so the default
// integration run on a bare checkout stays green.
const S3_ENDPOINT = process.env.S3_ENDPOINT ?? "127.0.0.1:8333";
const S3_BUCKET = process.env.S3_BUCKET ?? "dataraum-lake";

// Reachability alone is NOT a sufficient gate. This repo runs several lanes
// concurrently, so a SIBLING's compose stack is routinely listening on 8333
// with its own credentials: the port probe passes, and PutObject then fails
// with an opaque S3Error. Require the environment to have supplied real
// credentials (not integration-env's placeholders) before touching the store.
//
// All three keys, not just the two credentials: a developer with creds but no
// S3_BUCKET would otherwise stage into integration-env's placeholder bucket
// and fail on a bucket that was never meant to exist.
const S3_CREDENTIALED =
	providedByEnvironment("S3_ACCESS_KEY_ID") &&
	providedByEnvironment("S3_SECRET_ACCESS_KEY") &&
	providedByEnvironment("S3_BUCKET");

// A named reason, because an unexplained skip is how this suite went quiet.
const S3_SKIP_REASON = S3_CREDENTIALED
	? null
	: "no real S3 credentials in the environment (S3_ACCESS_KEY_ID / S3_SECRET_ACCESS_KEY / S3_BUCKET) — `cp .env.example .env` and bring up the compose stack's SeaweedFS";

async function seaweedReachable(): Promise<boolean> {
	try {
		const res = await fetch(`http://${S3_ENDPOINT}/`, {
			method: "GET",
			signal: AbortSignal.timeout(1000),
		});
		// Any HTTP response (even 403/404) means the gateway is up.
		return res.status > 0;
	} catch {
		return false;
	}
}

describe.skipIf(!S3_CREDENTIALED)(
	suiteTitle(
		"connect s3:// path against live SeaweedFS (DAT-386)",
		S3_SKIP_REASON,
	),
	() => {
		it("stages a CSV to the bucket and sniffs it over s3://", async () => {
			if (!(await seaweedReachable())) {
				// Credentials configured but no gateway up — skip rather than fail
				// (mirrors the DB-gated suites).
				return;
			}

			const { putObject } = await import("../upload/s3-upload");
			const { buildUploadKey, buildUploadUri } = await import(
				"../upload/policy"
			);

			// DAT-505: uploads stage under the workspace's `<ws>/uploads/` prefix.
			const key = buildUploadKey(
				"00000000-0000-0000-0000-000000000001",
				crypto.randomUUID(),
				"people.csv",
			);
			await putObject(
				S3_BUCKET,
				key,
				Buffer.from("id,name,active\n1,Ada,true\n2,Grace,false\n3,Ada,true\n"),
				"text/csv",
			);

			const uri = buildUploadUri(S3_BUCKET, key);
			const schema = await connect({ source_kind: "file", path: uri });

			expect(schema.sourceKind).toBe("file");
			expect(schema.source).toBe(uri);
			expect(schema.tables).toHaveLength(1);
			const table = schema.tables[0];
			expect(table.name).toBe("people.csv");
			expect(table.columns.map((c: { name: string }) => c.name)).toEqual([
				"id",
				"name",
				"active",
			]);
			const name = table.columns.find(
				(c: { name: string }) => c.name === "name",
			);
			expect(name.sampleValues).toEqual(["Ada", "Grace"]);
		});
	},
);

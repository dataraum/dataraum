// Object-store PUT for staged uploads (DAT-386).
//
// The upload entry-mode stages a dragged file to the SAME SeaweedFS/S3 bucket
// the lake lives in (NO shared filesystem post-DAT-388), under the `uploads/`
// prefix, so the cockpit's DuckDB httpfs reader can sniff it over `s3://` and
// DAT-389 can ingest + clean it up. The DuckLake reader (`s3-secret.ts`) reads
// via DuckDB's own S3 client; this WRITE path needs a real S3 PUT, so it uses
// `@aws-lite/client` + the `@aws-lite/s3` plugin (the lib the object-store seam
// picked — DD/28278785; NOT aws-sdk).
//
// SeaweedFS is a non-AWS, path-style endpoint: the s3 plugin builds the request
// path as `/<Bucket>/<Key>` (path-style), which is what SeaweedFS expects — no
// virtual-host bucket subdomain. config.s3Endpoint is `host:port` (no scheme,
// the engine + DuckDB ENDPOINT form); @aws-lite's `endpoint` wants a FULL URL
// it parses with `new URL()` (host + port + protocol), so we prefix the scheme
// off `s3UseSsl` — passing a bare `host:port` makes it treat the whole string
// as a hostname (getaddrinfo ENOTFOUND `host:port`).

import awsLite from "@aws-lite/client";
import awsLiteS3 from "@aws-lite/s3";

import { config } from "../config";

// @aws-lite/s3 ships no types; the only surface we touch is S3.PutObject.
interface S3Client {
	S3: {
		PutObject(params: {
			Bucket: string;
			Key: string;
			Body: Buffer;
			ContentType?: string;
		}): Promise<unknown>;
	};
}

let clientPromise: Promise<S3Client> | null = null;

/** Full-URL endpoint @aws-lite parses: `http[s]://host:port` (scheme off SSL). */
export function s3EndpointUrl(endpoint: string, useSsl: boolean): string {
	return `${useSsl ? "https" : "http"}://${endpoint}`;
}

// One memoized client per process — the upload route reuses it across requests.
function getS3Client(): Promise<S3Client> {
	if (!clientPromise) {
		// The base AwsLiteClient is a callable request fn; the s3 plugin augments it
		// with the `S3.*` namespace at runtime, which its untyped ESM can't express.
		// Cast through unknown to the minimal S3 surface we actually call.
		clientPromise = awsLite({
			accessKeyId: config.s3AccessKeyId,
			secretAccessKey: config.s3SecretAccessKey,
			region: config.s3Region,
			endpoint: s3EndpointUrl(config.s3Endpoint, config.s3UseSsl),
			plugins: [awsLiteS3],
		}) as unknown as Promise<S3Client>;
	}
	return clientPromise;
}

/**
 * PUT `body` to `bucket/key` on the object store.
 *
 * Thin wrapper over the memoized @aws-lite S3 client so the route stays an I/O
 * shell and unit tests mock THIS function (not the network). Fails loud — a
 * rejected PUT surfaces to the route, which maps it to a 502.
 */
export async function putObject(
	bucket: string,
	key: string,
	body: Buffer,
	contentType?: string,
): Promise<void> {
	const client = await getS3Client();
	await client.S3.PutObject({
		Bucket: bucket,
		Key: key,
		Body: body,
		ContentType: contentType,
	});
}

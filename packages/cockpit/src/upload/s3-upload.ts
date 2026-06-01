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

// @aws-lite/s3 ships no types; the surface we touch is S3.PutObject (upload) and
// S3.ListObjectsV2 (select-time prefix enumeration, DAT-378).
interface S3ListObjectsV2Response {
	// @aws-lite returns the parsed XML: Contents is the per-object list, absent
	// when the prefix is empty. ContinuationToken paginates a >1000-object prefix.
	Contents?: { Key?: string; Size?: number }[];
	IsTruncated?: boolean;
	NextContinuationToken?: string;
}

interface S3Client {
	S3: {
		PutObject(params: {
			Bucket: string;
			Key: string;
			Body: Buffer;
			ContentType?: string;
		}): Promise<unknown>;
		ListObjectsV2(params: {
			Bucket: string;
			Prefix?: string;
			ContinuationToken?: string;
		}): Promise<S3ListObjectsV2Response>;
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

/**
 * List every object key under `prefix` in `bucket`, paginating to completion.
 *
 * The select-time enumeration primitive (DAT-378): the engine NEVER globs, so
 * the cockpit lists the prefix here and hands the engine an EXPLICIT URI list.
 * Returns raw keys (e.g. `uploads/<uuid>/orders.csv`); `enumeratePrefixUris`
 * filters + maps them to `s3://<bucket>/<key>` URIs. A trailing-slash "directory
 * marker" key (Size 0, ends in `/`) is dropped — it is not a real object.
 *
 * Pagination is followed via ContinuationToken so a prefix with >1000 objects
 * is fully enumerated (S3 caps a page at 1000); `IsTruncated` gates the loop.
 */
export async function listPrefixKeys(
	bucket: string,
	prefix: string,
): Promise<string[]> {
	const client = await getS3Client();
	const keys: string[] = [];
	let continuationToken: string | undefined;
	do {
		const res = await client.S3.ListObjectsV2({
			Bucket: bucket,
			Prefix: prefix,
			ContinuationToken: continuationToken,
		});
		for (const obj of res.Contents ?? []) {
			const key = obj.Key;
			// Skip the directory-marker object S3 returns for a prefix written as a
			// folder (zero-byte key ending in `/`); it is not loadable data.
			if (key && !(key.endsWith("/") && (obj.Size ?? 0) === 0)) {
				keys.push(key);
			}
		}
		continuationToken = res.IsTruncated ? res.NextContinuationToken : undefined;
	} while (continuationToken);
	return keys;
}

// Object-store access for staged uploads — Bun's built-in S3 client.
//
// The cockpit runs on Bun (dev `bun --bun vite dev`, prod
// `bun run .output/server/index.mjs`), so S3 access uses `Bun.S3Client`. It is
// PATH-STYLE by default (`endpoint/<bucket>/<key>`), which is exactly what the
// self-hosted SeaweedFS gateway requires — it has no virtual-host bucket
// subdomain, so a `<bucket>.<host>` request fails to resolve. (This replaced
// @aws-lite, whose ListObjectsV2 defaulted to virtual-host addressing and 500'd
// against SeaweedFS; PutObject happened to be path-style, hiding it until the
// upload path started listing for dedup.)
//
// The DuckLake reader (`s3-secret.ts`) reads parquet via DuckDB's OWN S3 client;
// this module owns the cockpit's bucket WRITE + LIST. config.s3Endpoint is
// `host:port` (no scheme — the engine + DuckDB ENDPOINT form); Bun's client
// wants a full URL, so we prefix the scheme off `s3UseSsl`.

import { config } from "../config";

let client: Bun.S3Client | null = null;

/** Full-URL endpoint Bun's S3 client parses: `http[s]://host:port`. */
export function s3EndpointUrl(endpoint: string, useSsl: boolean): string {
	return `${useSsl ? "https" : "http"}://${endpoint}`;
}

// One memoized client per process — the upload route reuses it across requests.
// The bucket is passed per call (there is one configured bucket today, but
// keeping it per-call leaves the client bucket-agnostic).
function getS3Client(): Bun.S3Client {
	if (!client) {
		client = new Bun.S3Client({
			accessKeyId: config.s3AccessKeyId,
			secretAccessKey: config.s3SecretAccessKey,
			region: config.s3Region,
			endpoint: s3EndpointUrl(config.s3Endpoint, config.s3UseSsl),
		});
	}
	return client;
}

/**
 * PUT `body` to `bucket/key` on the object store.
 *
 * Thin wrapper over the memoized Bun S3 client so the route stays an I/O shell
 * and unit tests mock THIS function. Fails loud — a rejected write surfaces to
 * the route, which maps it to a 502.
 */
export async function putObject(
	bucket: string,
	key: string,
	body: Buffer,
	contentType?: string,
): Promise<void> {
	await getS3Client().write(key, body, { bucket, type: contentType });
}

/** One real object under a prefix: its key and byte size. */
export interface PrefixObject {
	key: string;
	size: number;
}

/**
 * List every object (key + size) under `prefix` in `bucket`, paginating to
 * completion. A trailing-slash "directory marker" key (Size 0, ends in `/`) is
 * dropped — it is not a real object.
 *
 * Pagination follows the continuation token so a prefix with >1000 objects is
 * fully enumerated (S3 caps a page at 1000); `isTruncated` gates the loop.
 */
export async function listPrefixObjects(
	bucket: string,
	prefix: string,
): Promise<PrefixObject[]> {
	const s3 = getS3Client();
	const objects: PrefixObject[] = [];
	let continuationToken: string | undefined;
	do {
		// `bucket` rides in the second arg (S3Options); the first is the list query.
		const res = await s3.list(
			{ prefix, maxKeys: 1000, continuationToken },
			{ bucket },
		);
		for (const obj of res.contents ?? []) {
			const size = obj.size ?? 0;
			// Skip the directory-marker object S3 returns for a prefix written as a
			// folder (zero-byte key ending in `/`); it is not loadable data.
			if (!(obj.key.endsWith("/") && size === 0)) {
				objects.push({ key: obj.key, size });
			}
		}
		continuationToken = res.isTruncated ? res.nextContinuationToken : undefined;
	} while (continuationToken);
	return objects;
}

/**
 * List every object KEY under `prefix` in `bucket`, paginating to completion.
 *
 * Returns raw keys (e.g. `<ws>/uploads/<digest>/orders.csv`). Injected into the
 * upload route as `deps.listPrefix` (`routes/api/upload.ts`): a non-empty
 * listing under the content digest's directory means those bytes are already
 * staged, so the route returns the existing handle and skips the re-PUT.
 */
export async function listPrefixKeys(
	bucket: string,
	prefix: string,
): Promise<string[]> {
	return (await listPrefixObjects(bucket, prefix)).map((o) => o.key);
}

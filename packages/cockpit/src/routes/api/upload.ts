// Upload entry-mode route — stage a dragged file to the object store (DAT-386).
//
// A TanStack Start file-route (the `createFileRoute(...).server.handlers`
// pattern from run-sql.ts / chat.ts, NOT a createServerFn): it reads the
// multipart `request.formData()`, enforces the size + extension limits, streams
// the bytes to the SeaweedFS bucket via @aws-lite PutObject under the `uploads/`
// prefix, and returns the locked `{ path: "s3://<bucket>/uploads/<uuid>/<name>" }`
// handle. The UI then drives the EXISTING `connect` tool over that `s3://` path
// (same ConnectSchema, same canvas) — there is no new sniff path here.
//
// Staging is BUCKET-only: post-DAT-388 there is no shared filesystem between the
// cockpit and the engine, so the s3:// handle is the one thing both sides see.
//
// The handler is split out of `Route` as `handleUpload` so the size/type gates
// and the handle shape are unit-testable with a mocked putObject, without
// booting the router. The route is then a one-line delegate.

import { createFileRoute } from "@tanstack/react-router";

import { config } from "../../config";
import { resolveActiveWorkspaceRow } from "../../db/cockpit/registry";
import { digestBytes } from "../../upload/digest";
import {
	buildUploadKey,
	buildUploadUri,
	isAllowedExtension,
	MAX_UPLOAD_BYTES,
	workspaceUploadPrefix,
} from "../../upload/policy";
import { listPrefixKeys, putObject } from "../../upload/s3-upload";

function jsonError(message: string, status: number): Response {
	return new Response(JSON.stringify({ error: message }), {
		status,
		headers: { "Content-Type": "application/json" },
	});
}

/**
 * Core upload handler: parse the multipart body, gate on extension + size,
 * content-digest the bytes, and stage to `<ws>/uploads/<digest>/<name>` — UNLESS
 * that content is already staged (this workspace), in which case the existing
 * handle is returned and the PUT skipped (dedup). Returns the locked s3:// handle
 * plus a `deduped` flag.
 *
 * `bucket`, `workspaceId`, `put`, `digest`, and `listPrefix` are injected so the
 * unit test can assert the flow without a live SeaweedFS; the route passes the
 * real config bucket, the registry-resolved workspace id (DAT-505 — uploads stage
 * under the workspace's `<ws>/` prefix and the digest is salted with it), and
 * @aws-lite putObject/listPrefixKeys.
 */
export async function handleUpload(
	request: Request,
	deps: {
		bucket: string;
		workspaceId: string;
		put: (
			bucket: string,
			key: string,
			body: Buffer,
			contentType?: string,
		) => Promise<void>;
		digest: (bytes: Uint8Array) => Promise<string>;
		listPrefix: (bucket: string, prefix: string) => Promise<string[]>;
	},
): Promise<Response> {
	let form: FormData;
	try {
		form = await request.formData();
	} catch {
		return jsonError("Request body must be multipart/form-data.", 400);
	}

	const file = form.get("file");
	if (!(file instanceof File)) {
		return jsonError("Field 'file' is required and must be a file.", 400);
	}
	if (!isAllowedExtension(file.name)) {
		return jsonError(
			"Unsupported file type. Supported: .csv/.tsv/.txt, .parquet, .json/.ndjson/.jsonl.",
			415,
		);
	}
	// `file.size` is the declared length; verify the materialized buffer against
	// the same cap so a lying size header can't slip a huge body in.
	if (file.size > MAX_UPLOAD_BYTES) {
		return jsonError(`File is too large (max ${MAX_UPLOAD_BYTES} bytes).`, 413);
	}

	const body = Buffer.from(await file.arrayBuffer());
	if (body.byteLength > MAX_UPLOAD_BYTES) {
		return jsonError(`File is too large (max ${MAX_UPLOAD_BYTES} bytes).`, 413);
	}

	// Content-address the upload so identical bytes dedup instead of piling up a
	// fresh copy per upload (the "S3 sink-hole" + no-dedup bug). If the digest
	// directory already holds an object, this content is already staged — reuse
	// its handle and skip the re-PUT.
	const digest = await deps.digest(body);
	const existing = await deps.listPrefix(
		deps.bucket,
		`${workspaceUploadPrefix(deps.workspaceId)}/${digest}/`,
	);
	if (existing.length > 0) {
		return new Response(
			JSON.stringify({
				path: buildUploadUri(deps.bucket, existing[0]),
				deduped: true,
			}),
			{ status: 200, headers: { "Content-Type": "application/json" } },
		);
	}

	const key = buildUploadKey(deps.workspaceId, digest, file.name);
	try {
		await deps.put(deps.bucket, key, body, file.type || undefined);
	} catch (err) {
		console.error("upload PutObject failed", err);
		return jsonError("Failed to stage the upload to the object store.", 502);
	}

	return new Response(
		JSON.stringify({ path: buildUploadUri(deps.bucket, key), deduped: false }),
		{
			status: 200,
			headers: { "Content-Type": "application/json" },
		},
	);
}

export const Route = createFileRoute("/api/upload")({
	server: {
		handlers: {
			POST: async ({ request }) => {
				// The active workspace id from the cockpit_db registry (DAT-505), NOT
				// the bare env var — uploads stage under this workspace's `<ws>/`
				// prefix and the digest is salted with it.
				const { id: workspaceId } = await resolveActiveWorkspaceRow();
				return handleUpload(request, {
					bucket: config.s3Bucket,
					workspaceId,
					put: putObject,
					// Workspace-scoped content digest (salt = workspace id) so the same
					// bytes dedup within a workspace, not across.
					digest: (bytes) => digestBytes(bytes, workspaceId),
					listPrefix: listPrefixKeys,
				});
			},
		},
	},
});

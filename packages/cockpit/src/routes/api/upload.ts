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
import {
	buildUploadKey,
	buildUploadUri,
	isAllowedExtension,
	MAX_UPLOAD_BYTES,
} from "../../upload/policy";
import { putObject } from "../../upload/s3-upload";

function jsonError(message: string, status: number): Response {
	return new Response(JSON.stringify({ error: message }), {
		status,
		headers: { "Content-Type": "application/json" },
	});
}

/**
 * Core upload handler: parse the multipart body, gate on extension + size, PUT
 * to the bucket under `uploads/<uuid>/<name>`, and return the locked s3:// handle.
 *
 * `bucket` and `put` are injected so the unit test can assert the call without a
 * live SeaweedFS; the route passes the real config bucket + @aws-lite putObject.
 */
export async function handleUpload(
	request: Request,
	deps: {
		bucket: string;
		put: (
			bucket: string,
			key: string,
			body: Buffer,
			contentType?: string,
		) => Promise<void>;
		uuid: () => string;
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

	// One uuid directory per upload → no cross-upload filename collision.
	const key = buildUploadKey(deps.uuid(), file.name);
	try {
		await deps.put(deps.bucket, key, body, file.type || undefined);
	} catch (err) {
		console.error("upload PutObject failed", err);
		return jsonError("Failed to stage the upload to the object store.", 502);
	}

	return new Response(
		JSON.stringify({ path: buildUploadUri(deps.bucket, key) }),
		{
			status: 200,
			headers: { "Content-Type": "application/json" },
		},
	);
}

export const Route = createFileRoute("/api/upload")({
	server: {
		handlers: {
			POST: ({ request }: { request: Request }) =>
				handleUpload(request, {
					bucket: config.s3Bucket,
					put: putObject,
					uuid: () => crypto.randomUUID(),
				}),
		},
	},
});

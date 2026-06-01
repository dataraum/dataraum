// Upload entry-mode dropzone (DAT-386; multi-file DAT-391).
//
// Drag-drop (or pick) one OR several files → validate the batch client-side →
// POST each to /api/upload → collect the staged `s3://` handles → hand the LIST
// to the caller (`onUploaded`), which drives the agent loop (connect per file
// for a preview, then select the batch as ONE `file_uris` source). The dropzone
// owns NO sniff/canvas wiring.
//
// The hard cap (MAX_UPLOAD_FILES) + same-kind homogeneity are a CLIENT-SIDE UX
// gate (validateUploadBatch): the upload route stays one-file-per-request and
// uncapped. Native drag/drop + a `multiple` file <input> (no @mantine/dropzone
// dep) — the input is what Playwright's browser_file_upload targets in the smoke.

import { Box, Button, Group, Stack, Text } from "@mantine/core";
import { Upload } from "lucide-react";
import { type ChangeEvent, type DragEvent, useRef, useState } from "react";
import { tokens } from "#/ui/theme";
import { validateUploadBatch } from "#/upload/batch";
import { MAX_UPLOAD_FILES } from "#/upload/policy";

// Accept attribute mirrors the route's ALLOWED_EXTENSIONS (the route is the
// authority; this is just a UX hint that pre-filters the OS picker).
const ACCEPT = ".csv,.tsv,.txt,.parquet,.pq,.json,.ndjson,.jsonl";

interface UploadResponse {
	path: string;
}

async function uploadFile(file: File): Promise<string> {
	const form = new FormData();
	form.append("file", file);
	const res = await fetch("/api/upload", { method: "POST", body: form });
	if (!res.ok) {
		let message = `Upload failed (${res.status}).`;
		try {
			const body = (await res.json()) as { error?: string };
			if (body.error) message = body.error;
		} catch {
			// Non-JSON error body — keep the status-based message.
		}
		throw new Error(message);
	}
	const body = (await res.json()) as UploadResponse;
	return body.path;
}

/**
 * `onUploaded` receives the staged `s3://` paths (one per file, in selection
 * order); the caller composes them — connect each for a preview, then select the
 * batch as ONE `file_uris` source.
 */
export function UploadDropzone({
	onUploaded,
	disabled = false,
}: {
	onUploaded: (s3Paths: string[]) => void;
	// True while the agent turn is running (ChatRail passes `isLoading`): the
	// dropzone goes inert so an upload can't complete INTO a busy agent loop and
	// get silently dropped, and so it reads as "unavailable" rather than broken.
	disabled?: boolean;
}) {
	const inputRef = useRef<HTMLInputElement>(null);
	// Synchronous re-entrancy guard: `busy` state is stale across two rapid drops
	// in one tick, so gate on a ref, not the rendered `busy`.
	const inFlightRef = useRef(false);
	const [total, setTotal] = useState(0);
	const [busy, setBusy] = useState(false);
	const [dragOver, setDragOver] = useState(false);
	const [error, setError] = useState<string | null>(null);

	const handleFiles = async (files: File[]) => {
		if (inFlightRef.current || disabled) return;
		setError(null);
		// Client-side UX gate: count ≤ cap, supported + same-kind. The route stays
		// uncapped — this just stops a bad batch before it stages anything.
		const invalid = validateUploadBatch(files.map((f) => f.name));
		if (invalid) {
			setError(invalid);
			return;
		}
		inFlightRef.current = true;
		setTotal(files.length);
		setBusy(true);
		try {
			const results = await Promise.allSettled(files.map(uploadFile));
			const failed = results.flatMap((r, i) =>
				r.status === "rejected"
					? [
							`${files[i].name}: ${r.reason instanceof Error ? r.reason.message : String(r.reason)}`,
						]
					: [],
			);
			if (failed.length > 0) {
				// All-or-nothing: a partial file_uris source is confusing, so don't
				// proceed — surface which failed (already-staged files are cleaned up
				// automatically by the engine, DAT-389) and let the user retry.
				setError(
					`Some files failed to upload (already-staged files are cleaned up automatically):\n${failed.join("\n")}`,
				);
				return;
			}
			const paths = results.map(
				(r) => (r as PromiseFulfilledResult<string>).value,
			);
			onUploaded(paths);
		} finally {
			inFlightRef.current = false;
			setBusy(false);
		}
	};

	const onInputChange = (e: ChangeEvent<HTMLInputElement>) => {
		const files = Array.from(e.currentTarget.files ?? []);
		// Reset so re-selecting the SAME files fires change again.
		e.currentTarget.value = "";
		if (files.length > 0) void handleFiles(files);
	};

	const onDrop = (e: DragEvent<HTMLDivElement>) => {
		e.preventDefault();
		setDragOver(false);
		const files = Array.from(e.dataTransfer.files ?? []);
		if (files.length > 0) void handleFiles(files);
	};

	// Inert while an upload is in flight (progress) or the agent turn is running.
	const blocked = busy || disabled;

	return (
		<Stack gap="xs" p="xs" data-testid="upload-dropzone">
			<Box
				onDragOver={(e) => {
					e.preventDefault();
					setDragOver(true);
				}}
				onDragLeave={() => setDragOver(false)}
				onDrop={onDrop}
				onClick={() => {
					if (!blocked) inputRef.current?.click();
				}}
				data-testid="upload-dropzone-target"
				style={{
					borderWidth: 1,
					borderStyle: "dashed",
					borderColor: dragOver ? tokens.colors.text : tokens.colors.border,
					borderRadius: tokens.radii.sm,
					padding: tokens.spacing.sm,
					cursor: busy ? "progress" : disabled ? "not-allowed" : "pointer",
					backgroundColor: dragOver ? tokens.colors.surface : undefined,
				}}
			>
				<Group gap="xs" justify="center" wrap="nowrap">
					<Upload size={16} />
					<Text size="xs" c="dimmed">
						{busy
							? `Uploading ${total} file${total === 1 ? "" : "s"}…`
							: disabled
								? "Agent is working — upload when it's done"
								: `Drop CSV/Parquet/JSON files (up to ${MAX_UPLOAD_FILES}), or click to pick`}
					</Text>
				</Group>
			</Box>
			<input
				ref={inputRef}
				type="file"
				accept={ACCEPT}
				multiple
				onChange={onInputChange}
				disabled={blocked}
				data-testid="upload-input"
				style={{ display: "none" }}
			/>
			{error && (
				<Text
					size="xs"
					c="red"
					data-testid="upload-error"
					style={{ whiteSpace: "pre-wrap" }}
				>
					{error}
				</Text>
			)}
			<Button
				size="compact-xs"
				variant="subtle"
				onClick={() => inputRef.current?.click()}
				disabled={blocked}
				data-testid="upload-pick"
			>
				Choose files
			</Button>
		</Stack>
	);
}

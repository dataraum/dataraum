// Upload entry-mode dropzone (DAT-386).
//
// Drag-drop (or pick) a file → POST /api/upload → get the staged `s3://` handle
// → drive the EXISTING `connect` tool over that path so its result projects onto
// the existing schema-preview canvas via the existing tool→canvas bridge. The
// dropzone owns NO sniff logic and NO canvas wiring: it hands the `s3://` path to
// the agent loop (`onConnect`), which runs `connect(source_kind='file', path)`,
// and the chat rail's canvas effect renders the ConnectSchema as it always has.
//
// Native drag/drop + a file <input> (no @mantine/dropzone dep) — the file input
// is what Playwright's browser_file_upload targets in the lane smoke.

import { Box, Button, Group, Stack, Text } from "@mantine/core";
import { Upload } from "lucide-react";
import { type ChangeEvent, type DragEvent, useRef, useState } from "react";
import { tokens } from "#/ui/theme";

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
 * `onConnect` receives the staged `s3://` path; the caller drives the existing
 * connect tool with it (e.g. by sending a chat message to the agent loop).
 */
export function UploadDropzone({
	onConnect,
}: {
	onConnect: (s3Path: string) => void;
}) {
	const inputRef = useRef<HTMLInputElement>(null);
	const [busy, setBusy] = useState(false);
	const [dragOver, setDragOver] = useState(false);
	const [error, setError] = useState<string | null>(null);

	const handleFile = async (file: File) => {
		setError(null);
		setBusy(true);
		try {
			const path = await uploadFile(file);
			onConnect(path);
		} catch (err) {
			setError(err instanceof Error ? err.message : String(err));
		} finally {
			setBusy(false);
		}
	};

	const onInputChange = (e: ChangeEvent<HTMLInputElement>) => {
		const file = e.currentTarget.files?.[0];
		// Reset so re-selecting the SAME file fires change again.
		e.currentTarget.value = "";
		if (file) void handleFile(file);
	};

	const onDrop = (e: DragEvent<HTMLDivElement>) => {
		e.preventDefault();
		setDragOver(false);
		const file = e.dataTransfer.files?.[0];
		if (file) void handleFile(file);
	};

	return (
		<Stack gap="xs" p="xs" data-testid="upload-dropzone">
			<Box
				onDragOver={(e) => {
					e.preventDefault();
					setDragOver(true);
				}}
				onDragLeave={() => setDragOver(false)}
				onDrop={onDrop}
				onClick={() => inputRef.current?.click()}
				data-testid="upload-dropzone-target"
				style={{
					borderWidth: 1,
					borderStyle: "dashed",
					borderColor: dragOver ? tokens.colors.text : tokens.colors.border,
					borderRadius: tokens.radii.sm,
					padding: tokens.spacing.sm,
					cursor: busy ? "progress" : "pointer",
					backgroundColor: dragOver ? tokens.colors.surface : undefined,
				}}
			>
				<Group gap="xs" justify="center" wrap="nowrap">
					<Upload size={16} />
					<Text size="xs" c="dimmed">
						{busy
							? "Uploading…"
							: "Drop a CSV/Parquet/JSON file, or click to pick"}
					</Text>
				</Group>
			</Box>
			<input
				ref={inputRef}
				type="file"
				accept={ACCEPT}
				onChange={onInputChange}
				disabled={busy}
				data-testid="upload-input"
				style={{ display: "none" }}
			/>
			{error && (
				<Text size="xs" c="red" data-testid="upload-error">
					{error}
				</Text>
			)}
			<Button
				size="compact-xs"
				variant="subtle"
				onClick={() => inputRef.current?.click()}
				disabled={busy}
				data-testid="upload-pick"
			>
				Choose file
			</Button>
		</Stack>
	);
}

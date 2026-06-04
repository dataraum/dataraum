// Upload-area widget (redesign) — the focus-canvas surface the `upload` tool
// opens. Owns the dropzone and drives the EXISTING connect flow on upload (one
// connect per file for a schema preview; a batch registers as ONE `file_uris`
// source). Reads only the stable actions context, so it doesn't re-render while a
// turn streams. This is where uploads live now — they're no longer a permanent
// fixture in the chat rail.

import { Stack, Text } from "@mantine/core";
import type { CanvasState } from "#/ui/cockpit/canvas-state";
import { useCockpitActions } from "#/ui/cockpit/cockpit-state";
import { UploadDropzone } from "#/ui/cockpit/upload-dropzone";

export function UploadAreaWidget(_props: {
	state: Extract<CanvasState, { kind: "upload-area" }>;
}) {
	const { sendMessage } = useCockpitActions();

	// Staged `s3://` path(s) drive the connect tool through the agent loop — one
	// connect per file for a preview, then a single select registering a batch as
	// ONE `file_uris` source. The tool results project back onto the canvas via the
	// provider's derivation — no canvas wiring here.
	const onUploaded = (s3Paths: string[]) => {
		if (s3Paths.length === 0) return;
		if (s3Paths.length === 1) {
			sendMessage(
				`Connect to the uploaded file at ${s3Paths[0]} (source_kind=file) and show me its schema.`,
				{ label: "Reading the file…" },
			);
			return;
		}
		const list = s3Paths.map((p) => `- ${p}`).join("\n");
		sendMessage(
			`I uploaded ${s3Paths.length} files to import together as ONE source:\n${list}\n\n` +
				`Connect to each file (source_kind=file) so I can preview its schema, then ` +
				`register them as a single source with the select tool — pass all ${s3Paths.length} ` +
				`as file_uris.`,
			{ label: "Reading the files…" },
		);
	};

	return (
		<Stack gap="sm" data-testid="canvas-upload-area">
			<Text size="sm" fw={600}>
				Add data
			</Text>
			<Text size="xs" c="dimmed">
				Drop files from your computer to import them. Most workspaces pull from
				connected systems — this is for quick local files.
			</Text>
			<UploadDropzone onUploaded={onUploaded} />
		</Stack>
	);
}

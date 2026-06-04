// Upload-area widget (redesign) — the focus-canvas surface the `upload` tool
// opens. Owns the dropzone and drives the EXISTING connect flow on upload (one
// connect per file for a schema preview; a batch registers as ONE `file_uris`
// source). This is where uploads live now — they're no longer a permanent
// fixture in the chat rail.

import { Stack, Text } from "@mantine/core";
import type { CanvasState } from "#/ui/cockpit/canvas-state";
import { useCockpitActions, useCockpitState } from "#/ui/cockpit/cockpit-state";
import { UploadDropzone } from "#/ui/cockpit/upload-dropzone";
import { uploadBubbleText, uploadRefsBlock } from "#/ui/cockpit/upload-handoff";

export function UploadAreaWidget(_props: {
	state: Extract<CanvasState, { kind: "upload-area" }>;
}) {
	const { sendMessage } = useCockpitActions();
	// Gate the dropzone while a turn is in flight: an upload that completes during
	// a running loop would dispatch its connect message INTO a busy useChat and get
	// dropped. The widget unmounts once the canvas swaps to the connect result, so
	// reading isLoading here costs no extra per-token renders in practice.
	const { isLoading } = useCockpitState();

	// The upload turn carries TWO text parts (DAT-423): a CLEAN bubble (filenames
	// only) the rail renders, and a model-only REFS block (the ordered objects +
	// their `s3://` uris) the rail skips. So the agent gets the exact ordered batch
	// structurally — drives connect → vertical → select as before — with NO path in
	// any chat bubble. The tool results project back onto the canvas via the
	// provider's derivation — no canvas wiring here.
	const onUploaded = (s3Paths: string[]) => {
		if (s3Paths.length === 0) return;
		sendMessage(
			{
				content: [
					{ type: "text", content: uploadBubbleText(s3Paths) },
					{ type: "text", content: uploadRefsBlock(s3Paths) },
				],
			},
			{
				label:
					s3Paths.length === 1 ? "Reading the file…" : "Reading the files…",
			},
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
			<UploadDropzone onUploaded={onUploaded} disabled={isLoading} />
		</Stack>
	);
}

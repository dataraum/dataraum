// Frame / Vertical modal (DAT-594) — the staging hub's "declare a business model"
// step. Two paths, both DIRECT (no LLM round-trip), mirroring the agent's frame vs
// use_vertical fork:
//   - FRAME a NEW vertical: induce concepts (+ the executable knowledge) from the
//     staged set's UNIONED schemas (frameStagingSet sniffs each item server-side),
//     under a user-named vertical.
//   - USE an existing vertical: adopt a builtin (or already-framed) onto the
//     workspace (useVerticalForStaging) — a builtin ships its own concepts.
//
// Either path writes the workspace's vertical; on success the caller invalidates
// the active-vertical-status query so the Start gate flips immediately. Frame is an
// ACTING step here (it writes overlay rows now) — the Start button, not this modal,
// gates the import (DAT-598 tracks a propose/commit split for frame itself).

import {
	Alert,
	Button,
	Group,
	Loader,
	Modal,
	Radio,
	Stack,
	Text,
	TextInput,
} from "@mantine/core";
import { useMutation, useQuery } from "@tanstack/react-query";
import { useState } from "react";
import {
	adoptVerticalForStaging,
	frameStagingSet,
	listAdoptableVerticals,
} from "#/server/stage-frame";

/** The staged-set shape the modal needs to seed a frame — only the query identity
 * (sniffed server-side) and file URIs; the full item union lives in the widget. */
export interface StagedForFrame {
	kind: "query" | "file";
	source_name?: string;
	credential_source?: string;
	backend?: string;
	sql?: string;
	file_uri?: string;
}

const VERTICAL_NAME_RE = /^[a-z][a-z0-9_]{1,48}$/;

export function ModelModal({
	opened,
	onClose,
	importSet,
	onModelDeclared,
}: {
	opened: boolean;
	onClose: () => void;
	importSet: StagedForFrame[];
	/** Called after a frame / use_vertical succeeds — the caller invalidates the
	 * active-vertical-status query so the Start gate flips. */
	onModelDeclared: () => void;
}) {
	const [mode, setMode] = useState<"frame" | "adopt">("frame");
	const [verticalName, setVerticalName] = useState("");
	const [adopt, setAdopt] = useState<string | null>(null);

	const verticals = useQuery({
		queryKey: ["adoptable-verticals"],
		queryFn: () => listAdoptableVerticals(),
		// Only fetched while the modal is open (it offers the adopt list).
		enabled: opened,
	});

	const frameMutation = useMutation({
		mutationFn: () =>
			frameStagingSet({
				data: {
					queries: importSet
						.filter((x) => x.kind === "query")
						.map((x) => ({
							source_name: x.source_name ?? "",
							credential_source: x.credential_source ?? "",
							backend: x.backend ?? "",
							sql: x.sql ?? "",
						})),
					files: importSet
						.filter((x) => x.kind === "file")
						.map((x) => ({ file_uri: x.file_uri ?? "" })),
					vertical_name: verticalName.trim() || null,
				},
			}),
		onSuccess: onModelDeclared,
	});

	const adoptMutation = useMutation({
		mutationFn: (name: string) => adoptVerticalForStaging({ data: { name } }),
		onSuccess: onModelDeclared,
	});

	const error =
		(frameMutation.error as Error | null)?.message ??
		(adoptMutation.error as Error | null)?.message;
	const pending = frameMutation.isPending || adoptMutation.isPending;

	const nameValid =
		verticalName.trim() === "" || VERTICAL_NAME_RE.test(verticalName.trim());
	const emptySet = importSet.length === 0;
	const canFrame = !emptySet && nameValid && !pending;
	const canAdopt = adopt !== null && !pending;

	return (
		<Modal
			opened={opened}
			onClose={onClose}
			centered
			size="lg"
			title="Declare a business model"
			data-testid="model-modal"
		>
			<Stack gap="md">
				<Text size="xs" c="dimmed">
					An imported source grounds against the workspace's business model.
					Frame a new one from your staged set, or adopt an existing vertical.
				</Text>

				{error && (
					<Alert color="red" data-testid="model-error">
						{error}
					</Alert>
				)}

				<Radio.Group
					value={mode}
					onChange={(v) => setMode(v as "frame" | "adopt")}
				>
					<Stack gap="xs">
						<Radio
							value="frame"
							label="Frame a new vertical from the staged set"
							data-testid="model-mode-frame"
						/>
						<Radio
							value="adopt"
							label="Adopt an existing vertical (builtin or framed)"
							data-testid="model-mode-adopt"
						/>
					</Stack>
				</Radio.Group>

				{mode === "frame" ? (
					<Stack gap="sm">
						{emptySet && (
							<Alert color="yellow" data-testid="model-empty-set">
								Stage at least one query or file first — frame induces the model
								from the staged set's schemas.
							</Alert>
						)}
						<TextInput
							size="xs"
							label="Vertical name (optional)"
							placeholder="e.g. sales, logistics — lowercase; blank → _adhoc"
							value={verticalName}
							onChange={(e) => setVerticalName(e.currentTarget.value)}
							error={
								!nameValid
									? "lowercase, start with a letter, [a-z0-9_], 2–49 chars"
									: undefined
							}
							data-testid="model-vertical-name"
						/>
						<Group justify="flex-end">
							<Button
								size="xs"
								onClick={() => frameMutation.mutate()}
								disabled={!canFrame}
								loading={frameMutation.isPending}
								data-testid="model-frame-run"
							>
								Frame the model
							</Button>
						</Group>
					</Stack>
				) : (
					<Stack gap="sm">
						{verticals.isLoading ? (
							<Group gap="xs">
								<Loader size="sm" />
								<Text size="sm" c="dimmed">
									Loading verticals…
								</Text>
							</Group>
						) : (verticals.data ?? []).length === 0 ? (
							<Text size="sm" c="dimmed" data-testid="model-no-verticals">
								No adoptable verticals — frame a new one instead.
							</Text>
						) : (
							<Radio.Group
								value={adopt}
								onChange={setAdopt}
								data-testid="model-vertical-list"
							>
								<Stack gap="xs">
									{(verticals.data ?? []).map((v) => (
										<Radio
											key={v.name}
											value={v.name}
											label={
												<Text size="sm">
													{v.name}{" "}
													<Text span c="dimmed" size="xs">
														({v.kind}, {v.concept_count} concept
														{v.concept_count === 1 ? "" : "s"}
														{v.description ? ` — ${v.description}` : ""})
													</Text>
												</Text>
											}
										/>
									))}
								</Stack>
							</Radio.Group>
						)}
						<Group justify="flex-end">
							<Button
								size="xs"
								onClick={() => adopt && adoptMutation.mutate(adopt)}
								disabled={!canAdopt}
								loading={adoptMutation.isPending}
								data-testid="model-adopt-run"
							>
								Adopt vertical
							</Button>
						</Group>
					</Stack>
				)}
			</Stack>
		</Modal>
	);
}

// Probe widget (DAT-576) — the editable Connect-phase surface: pick a configured
// DB source, write/edit read-only SQL, and run it against the external DB BEFORE
// ingest. Runs stream through /api/probe-sql into the SAME virtualized result grid
// the lake uses (StreamingGrid), so a large external result never floods the DOM.
//
// The agent only SEEDS this surface: a `probe` tool call projects its source + sql
// into the editor (out of CHIP_ONLY, Phase 3) for the user to edit and re-run. The
// run itself is a direct fetch — no agent round-trip — exactly like the run_sql
// result grid.

import {
	Alert,
	Badge,
	Button,
	Group,
	Select,
	Stack,
	Text,
} from "@mantine/core";
import { useQuery } from "@tanstack/react-query";
import { useMemo, useState } from "react";
import { getConfiguredDatabases } from "#/server/configured-databases";
import type { CanvasState } from "#/ui/cockpit/canvas-state";
import { StreamingGrid } from "#/ui/cockpit/widgets/result-grid";
import { SqlEditor } from "#/ui/cockpit/widgets/sql-editor";

/** The probe-sql request a Run submits — mirrors the /api/probe-sql body. A `type`
 * (not `interface`) so it's assignable to the grid's `Record<string, unknown>` body. */
type ProbeRun = {
	source_name: string;
	backend: string;
	sql: string;
};

export function ProbeWidget({
	state,
}: {
	state: Extract<CanvasState, { kind: "probe" }>;
}) {
	// Remount the panel whenever the agent SEED (the source + sql projected onto the
	// canvas) changes, so a repeated probe / open_probe in a later turn re-seeds the
	// editor + picker via fresh state init — never a reset effect (idiom rule 5, the
	// ResultGridWidget → StreamingGrid pattern). User edits don't touch the seed, so
	// typing never remounts.
	const seedKey = `${state.source?.name ?? ""}|${state.sql ?? ""}`;
	return <ProbePanel key={seedKey} state={state} />;
}

function ProbePanel({
	state,
}: {
	state: Extract<CanvasState, { kind: "probe" }>;
}) {
	const sources = useQuery({
		queryKey: ["configured-databases"],
		queryFn: () => getConfiguredDatabases(),
	});
	const list = useMemo(() => sources.data ?? [], [sources.data]);

	// Seed from the canvas state (agent-generate): a projected source + sql preload
	// the picker + editor as INITIAL values; the user then edits freely.
	const [selected, setSelected] = useState<string | null>(
		state.source?.name ?? null,
	);
	const [sqlText, setSqlText] = useState<string>(state.sql ?? "");
	const [submitted, setSubmitted] = useState<ProbeRun | null>(null);
	// Bumped per Run so an identical re-run still remounts StreamingGrid (fresh
	// stream + sort reset), not just a different query.
	const [runId, setRunId] = useState(0);

	const selectedSource = useMemo(
		() => list.find((s) => s.name === selected) ?? null,
		[list, selected],
	);
	const canRun = selectedSource !== null && sqlText.trim().length > 0;

	const run = () => {
		if (!selectedSource || sqlText.trim().length === 0) return;
		setSubmitted({
			source_name: selectedSource.name,
			backend: selectedSource.backend,
			sql: sqlText,
		});
		setRunId((r) => r + 1);
	};

	const noSources = !sources.isLoading && list.length === 0;

	return (
		<Stack gap="sm" data-testid="canvas-probe">
			<Group justify="space-between" wrap="nowrap">
				<Text size="sm" fw={600}>
					Probe a database source
				</Text>
				{selectedSource && (
					<Badge variant="light" size="sm" tt="none">
						{selectedSource.backend}
					</Badge>
				)}
			</Group>
			<Text size="xs" c="dimmed">
				Read-only DuckDB SQL against a configured source (use LIMIT, not TOP) —
				no data is imported. ⌘/Ctrl+Enter to run.
			</Text>

			<Select
				data-testid="probe-source-select"
				placeholder={
					sources.isLoading
						? "Loading sources…"
						: list.length
							? "Pick a source"
							: "No configured sources"
				}
				data={list.map((s) => ({
					value: s.name,
					label: `${s.name} (${s.backend})`,
				}))}
				value={selected}
				onChange={setSelected}
				disabled={sources.isLoading || list.length === 0}
				searchable
			/>

			{noSources && (
				<Alert color="gray" data-testid="probe-no-sources">
					No configured database sources. Set a{" "}
					<Text span ff="monospace" size="xs">
						DATARAUM_&lt;NAME&gt;_URL
					</Text>{" "}
					and bring the source up (see{" "}
					<Text span ff="monospace" size="xs">
						docker-compose.sources.yml
					</Text>
					).
				</Alert>
			)}

			<SqlEditor
				value={sqlText}
				onChange={setSqlText}
				onRun={run}
				placeholder="SELECT * FROM my_schema.my_table LIMIT 100"
			/>

			<Group>
				<Button
					size="xs"
					onClick={run}
					disabled={!canRun}
					data-testid="probe-run"
				>
					Run
				</Button>
			</Group>

			{submitted && (
				<StreamingGrid key={runId} endpoint="/api/probe-sql" body={submitted} />
			)}
		</Stack>
	);
}

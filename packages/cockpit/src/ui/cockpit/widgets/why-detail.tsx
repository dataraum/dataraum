// Shared why-detail vocabulary (DAT-434) — the blocks every readiness
// explanation surface renders from: per-intent drivers, the detector-evidence
// table, the signals caption, and the pending-teach note. Extracted from the
// column-why / table-why / relationship-why triplication before it drifts
// (rule 13; the BandBadge lesson — per-widget copies diverged until DAT-451
// pulled them into band-badge.tsx).
//
// Prop types are STRUCTURAL: the three why_* tool results share these shapes
// field-for-field, so each widget's own result type satisfies them without a
// cast — and a tool whose shape diverges fails tsc here, loudly.

import { Alert, Group, Stack, Table, Text } from "@mantine/core";
import { humanizeIdentifier } from "#/lib/display-names";
import { BandBadge, INTENT_LABEL } from "#/ui/cockpit/widgets/band-badge";
import { EvidenceDetail } from "#/ui/cockpit/widgets/evidence-detail";

/** "table.column" endpoint label from nullable display names — NEVER an id. */
export function relationshipEndpointLabel(
	tableName: string | null,
	columnName: string | null,
): string {
	const table = tableName ?? "(unknown table)";
	const column = columnName ?? "(unknown column)";
	return `${table}.${column}`;
}

interface IntentExplanationLike {
	intent: string;
	band: string;
	drivers: ReadonlyArray<{ node: string; label: string; state: string }>;
}

interface EvidenceSignalLike {
	dimension_path: string;
	detector_id: string;
	score: number;
	detail: string;
}

/** "Based on N signals" caption; zero reads as not-yet-characterised. */
export function SignalsCaption({
	count,
	testId,
}: {
	count: number;
	testId: string;
}) {
	return (
		<Text size="xs" c="dimmed" data-testid={testId}>
			Based on {count} signal{count === 1 ? "" : "s"}
			{count === 0 ? " — not yet characterised" : ""}.
		</Text>
	);
}

/** The pending-teach advisory — identical wording on every readiness surface. */
export function PendingTeachAlert({
	count,
	testId,
}: {
	count: number;
	testId: string;
}) {
	if (count <= 0) return null;
	return (
		<Alert color="blue" data-testid={testId}>
			{count} pending teach
			{count === 1 ? "" : "es"} may affect this view — consider a replay before
			trusting it.
		</Alert>
	);
}

/** Per-intent drivers — the pre-computed diagnosis, ranked by impact. */
export function IntentDriversBlock({
	intents,
}: {
	intents: ReadonlyArray<IntentExplanationLike>;
}) {
	return (
		<Stack gap={4}>
			{intents.map((i) => (
				<Group key={i.intent} gap="xs" wrap="wrap" align="center">
					<Text size="xs" fw={500} w={92}>
						{INTENT_LABEL[i.intent] ?? i.intent}
					</Text>
					<BandBadge band={i.band} />
					{i.drivers.length === 0 ? (
						<Text span size="xs" c="dimmed">
							no drivers
						</Text>
					) : (
						i.drivers.map((d) => (
							<Text key={d.node} span size="xs" c="dimmed">
								{d.label} ({d.state})
							</Text>
						))
					)}
				</Group>
			))}
		</Stack>
	);
}

/** The detector-evidence table: readable dimension leaf (dotted path stays a
 * hover tooltip), detector, score, and the sanitized key→value detail through
 * the shared EvidenceDetail renderer. Renders nothing for empty evidence. */
export function EvidenceTable({
	evidence,
	testId,
}: {
	evidence: ReadonlyArray<EvidenceSignalLike>;
	testId: string;
}) {
	if (evidence.length === 0) return null;
	return (
		<Table.ScrollContainer minWidth={360}>
			<Table striped data-testid={testId}>
				<Table.Thead>
					<Table.Tr>
						<Table.Th>Dimension</Table.Th>
						<Table.Th>Detector</Table.Th>
						<Table.Th>Score</Table.Th>
						<Table.Th>Detail</Table.Th>
					</Table.Tr>
				</Table.Thead>
				<Table.Tbody>
					{evidence.map((e) => {
						const dimLeaf = e.dimension_path.split(".").at(-1) ?? "";
						return (
							<Table.Tr key={`${e.dimension_path}-${e.detector_id}`}>
								<Table.Td>
									<Text span size="xs" title={e.dimension_path || undefined}>
										{humanizeIdentifier(dimLeaf) || "—"}
									</Text>
								</Table.Td>
								<Table.Td>
									<Text span size="xs" c="dimmed">
										{humanizeIdentifier(e.detector_id) || e.detector_id}
									</Text>
								</Table.Td>
								<Table.Td>{e.score.toFixed(2)}</Table.Td>
								<Table.Td>
									<EvidenceDetail detail={e.detail} />
								</Table.Td>
							</Table.Tr>
						);
					})}
				</Table.Tbody>
			</Table>
		</Table.ScrollContainer>
	);
}

// --- Verdict provenance (DAT-513) — the pick made visible. The shown band is
// ONE of possibly several coexisting snapshot rows (add_source → session →
// operating_model); this caption names which one, and the history lists them
// all (oldest first) so a practitioner sees the verdict evolve as evidence
// accrued. Later stages are computed over strictly more evidence — they
// supersede earlier ones, they don't disagree with them.

/** One readiness snapshot in a target's history — mirrors the tools' shape. */
export interface VerdictHistoryRow {
	stage: string;
	band: string;
	worst_intent_risk: number | null;
	computed_at: string | null;
	run_id: string | null;
	signals: number | null;
}

const STAGE_LABEL: Record<string, string> = {
	add_source: "import",
	catalog: "session analysis",
	operating_model: "operating model",
};

/** Human label for a pipeline stage; falls through to the raw stage string. */
export function stageLabel(stage: string | null): string | null {
	return stage === null ? null : (STAGE_LABEL[stage] ?? stage);
}

function historyTime(iso: string | null): string {
	return iso === null ? "—" : new Date(iso).toLocaleString();
}

/** "as of <stage> · <time>" caption + the snapshot history (shown only when
 * more than one snapshot coexists — a single row adds nothing). */
export function VerdictProvenance({
	stage,
	computedAt,
	history,
	testId,
}: {
	stage: string | null;
	computedAt: string | null;
	history: VerdictHistoryRow[];
	testId: string;
}) {
	if (stage === null) return null;
	return (
		<Stack gap={4} data-testid={testId}>
			<Text size="xs" c="dimmed" data-testid={`${testId}-stage`}>
				as of {stageLabel(stage)}
				{computedAt !== null && ` · ${historyTime(computedAt)}`}
			</Text>
			{history.length > 1 && (
				<Stack gap={2} data-testid={`${testId}-history`}>
					{history.map((h) => (
						<Group
							gap="xs"
							wrap="nowrap"
							key={h.run_id ?? `${h.stage}-${h.computed_at ?? ""}`}
						>
							<Text size="xs" c="dimmed" w={140}>
								{stageLabel(h.stage)}
							</Text>
							<BandBadge band={h.band} />
							<Text size="xs" c="dimmed">
								{historyTime(h.computed_at)}
								{h.signals !== null && ` · ${h.signals} signals`}
							</Text>
						</Group>
					))}
				</Stack>
			)}
		</Stack>
	);
}

// Column-profile widget (DAT-475) — renders the `look_profile` result: one
// column's full descriptive deep-dive (semantic annotation, statistical profile,
// type candidates + decision, statistical quality, temporal profile, derived
// columns). Each block renders only when its stage produced a row; the values
// are the engine's persisted profile — this widget only formats them.
//
// Reads theme/tokens only; the row type is a type-only import (erased — no server
// code in the client bundle). Bounded: top_values / outlier_samples are already
// capped (≤10) by the tool projection, and type_candidates / histogram are short
// per-column lists — no virtualization needed.

import { Alert, Badge, Group, Stack, Table, Text } from "@mantine/core";
import type { ReactNode } from "react";
import type { ProfileQuality, ProfileStats } from "#/tools/look-profile";
import type { CanvasState } from "#/ui/cockpit/canvas-state";

/** Compact numeric display — fixed-ish without forcing trailing zeros on ints. */
function num(v: number | null | undefined): string {
	if (v === null || v === undefined) return "—";
	if (Number.isInteger(v)) return String(v);
	return v.toFixed(4).replace(/\.?0+$/, "");
}

function pct(v: number | null | undefined): string {
	if (v === null || v === undefined) return "—";
	return `${(v * 100).toFixed(1)}%`;
}

/** Format a histogram bucket edge — number (numeric bucket) or string
 * (categorical bucket); null degrades to the em-dash. */
function edge(v: number | string | null | undefined): string {
	if (v === null || v === undefined) return "—";
	return typeof v === "number" ? num(v) : v;
}

function Field({ label, value }: { label: string; value: ReactNode }) {
	return (
		<Group gap={6} align="baseline">
			<Text span size="xs" c="dimmed">
				{label}
			</Text>
			<Text span size="xs" fw={500}>
				{value}
			</Text>
		</Group>
	);
}

function StatsBlock({ stats }: { stats: ProfileStats }) {
	const { numeric_stats: n, string_stats: s } = stats;
	return (
		<Stack gap={4} data-testid="canvas-column-profile-stats">
			<Text size="xs" fw={600}>
				Statistics
			</Text>
			<Group gap="md" wrap="wrap">
				<Field label="rows" value={num(stats.total_count)} />
				<Field label="nulls" value={num(stats.null_count)} />
				<Field label="null ratio" value={pct(stats.null_ratio)} />
				<Field label="distinct" value={num(stats.distinct_count)} />
				<Field label="cardinality" value={pct(stats.cardinality_ratio)} />
				<Field
					label="unique"
					value={
						stats.is_unique === null ? "—" : stats.is_unique ? "yes" : "no"
					}
				/>
			</Group>
			{n && (
				<Group gap="md" wrap="wrap" data-testid="canvas-column-profile-numeric">
					<Field label="min" value={num(n.min_value)} />
					<Field label="max" value={num(n.max_value)} />
					<Field label="mean" value={num(n.mean)} />
					<Field label="stddev" value={num(n.stddev)} />
					<Field label="skew" value={num(n.skewness)} />
					<Field label="kurtosis" value={num(n.kurtosis)} />
					<Field label="cv" value={num(n.cv)} />
				</Group>
			)}
			{s && (
				<Group gap="md" wrap="wrap" data-testid="canvas-column-profile-string">
					<Field label="min len" value={num(s.min_length)} />
					<Field label="max len" value={num(s.max_length)} />
					<Field label="avg len" value={num(s.avg_length)} />
				</Group>
			)}
			{stats.top_values.length > 0 && (
				<Group
					gap={4}
					wrap="wrap"
					data-testid="canvas-column-profile-topvalues"
				>
					<Text span size="xs" c="dimmed">
						top values
					</Text>
					{stats.top_values.map((tv, i) => (
						<Badge
							// biome-ignore lint/suspicious/noArrayIndexKey: static parsed JSON, capped + never reordered
							key={i}
							size="xs"
							variant="light"
							color="gray"
						>
							{String(tv.value ?? "∅")} · {num(tv.count)}
						</Badge>
					))}
				</Group>
			)}
			{stats.histogram.length > 0 && (
				<Group
					gap={4}
					wrap="wrap"
					data-testid="canvas-column-profile-histogram"
				>
					<Text span size="xs" c="dimmed">
						histogram
					</Text>
					{stats.histogram.map((h, i) => (
						<Badge
							// biome-ignore lint/suspicious/noArrayIndexKey: static parsed JSON, bucket order is stable
							key={i}
							size="xs"
							variant="light"
							color="blue"
						>
							{edge(h.bucket_min)}–{edge(h.bucket_max)} · {num(h.count)}
						</Badge>
					))}
				</Group>
			)}
		</Stack>
	);
}

function QualityBlock({ quality }: { quality: ProfileQuality }) {
	return (
		<Stack gap={4} data-testid="canvas-column-profile-quality">
			<Text size="xs" fw={600}>
				Quality
			</Text>
			<Group gap="md" wrap="wrap">
				<Field
					label="outliers"
					value={
						quality.has_outliers === null
							? "—"
							: quality.has_outliers
								? "yes"
								: "no"
					}
				/>
				<Field
					label="IQR outlier ratio"
					value={pct(quality.iqr_outlier_ratio)}
				/>
				<Field
					label="z-score outlier ratio"
					value={pct(quality.zscore_outlier_ratio)}
				/>
				<Field
					label="Benford"
					value={
						quality.benford_compliant === null
							? "—"
							: quality.benford_compliant
								? "compliant"
								: "non-compliant"
					}
				/>
			</Group>
			{quality.benford?.interpretation && (
				<Text size="xs" c="dimmed">
					{quality.benford.interpretation}
				</Text>
			)}
			{quality.outlier_samples.length > 0 && (
				<Group gap={4} wrap="wrap" data-testid="canvas-column-profile-outliers">
					<Text span size="xs" c="dimmed">
						outlier samples
					</Text>
					{quality.outlier_samples.map((o, i) => (
						<Badge
							// biome-ignore lint/suspicious/noArrayIndexKey: static parsed JSON, capped + never reordered
							key={i}
							size="xs"
							variant="light"
							color="red"
						>
							{String(o)}
						</Badge>
					))}
				</Group>
			)}
		</Stack>
	);
}

export function ColumnProfileWidget({
	state,
}: {
	state: Extract<CanvasState, { kind: "column-profile" }>;
}) {
	const { profile } = state;

	if (!profile.found) {
		return (
			<Stack gap="xs" data-testid="canvas-column-profile">
				<Text size="sm" fw={600}>
					look_profile
				</Text>
				<Alert color="gray" data-testid="canvas-column-profile-notfound">
					No such column.
				</Alert>
			</Stack>
		);
	}

	const {
		semantic,
		stats,
		type_candidates,
		type_decision,
		quality,
		temporal,
		derived,
	} = profile;

	const nothingProfiled =
		!semantic &&
		!stats &&
		type_candidates.length === 0 &&
		!type_decision &&
		!quality &&
		!temporal &&
		derived.length === 0;

	return (
		<Stack gap="sm" data-testid="canvas-column-profile">
			<Group justify="space-between" wrap="nowrap">
				<Text size="sm" fw={600}>
					{profile.column_name}{" "}
					<Text span c="dimmed">
						{/* table_name arrives in display form (projected in the tool). */}·{" "}
						{profile.table_name}
					</Text>
				</Text>
				<Text span size="xs" c="dimmed">
					{profile.resolved_type ?? "—"}
				</Text>
			</Group>

			{nothingProfiled && (
				<Alert color="gray" data-testid="canvas-column-profile-unprofiled">
					This column hasn't been profiled yet — run the source through
					add_source to compute its profile.
				</Alert>
			)}

			{semantic && (
				<Stack gap={4} data-testid="canvas-column-profile-semantic">
					<Text size="xs" fw={600}>
						Semantic
					</Text>
					<Group gap="md" wrap="wrap">
						<Field label="meaning" value={semantic.meaning ?? "—"} />
						<Field label="role" value={semantic.semantic_role ?? "—"} />
						<Field
							label="business name"
							value={semantic.business_name ?? "—"}
						/>
						<Field label="entity" value={semantic.entity_type ?? "—"} />
						<Field
							label="temporal behavior"
							value={semantic.temporal_behavior ?? "—"}
						/>
						<Field
							label="unit source"
							value={semantic.unit_source_column ?? "—"}
						/>
					</Group>
				</Stack>
			)}

			{stats && <StatsBlock stats={stats} />}

			{type_decision && (
				<Stack gap={4} data-testid="canvas-column-profile-decision">
					<Text size="xs" fw={600}>
						Type decision
					</Text>
					<Group gap="md" wrap="wrap">
						<Field label="decided" value={type_decision.decided_type ?? "—"} />
						<Field
							label="source"
							value={type_decision.decision_source ?? "—"}
						/>
						<Field
							label="previous"
							value={type_decision.previous_type ?? "—"}
						/>
					</Group>
					{type_decision.decision_reason && (
						<Text size="xs" c="dimmed">
							{type_decision.decision_reason}
						</Text>
					)}
				</Stack>
			)}

			{type_candidates.length > 0 && (
				<Stack gap={4} data-testid="canvas-column-profile-candidates">
					<Text size="xs" fw={600}>
						Type candidates
					</Text>
					<Table.ScrollContainer minWidth={360}>
						<Table striped>
							<Table.Thead>
								<Table.Tr>
									<Table.Th>Type</Table.Th>
									<Table.Th>Confidence</Table.Th>
									<Table.Th>Parse rate</Table.Th>
									<Table.Th>Pattern</Table.Th>
									<Table.Th>Unit</Table.Th>
									<Table.Th>Quarantine</Table.Th>
								</Table.Tr>
							</Table.Thead>
							<Table.Tbody>
								{type_candidates.map((c, i) => (
									// biome-ignore lint/suspicious/noArrayIndexKey: static row list, never reordered after render
									<Table.Tr key={i}>
										<Table.Td>{c.data_type ?? "—"}</Table.Td>
										<Table.Td>{pct(c.confidence)}</Table.Td>
										<Table.Td>{pct(c.parse_success_rate)}</Table.Td>
										<Table.Td>
											<Text span size="xs" c="dimmed">
												{c.detected_pattern ?? "—"}
											</Text>
										</Table.Td>
										<Table.Td>
											<Text span size="xs" c="dimmed">
												{c.detected_unit ?? "—"}
											</Text>
										</Table.Td>
										<Table.Td>{pct(c.quarantine_rate)}</Table.Td>
									</Table.Tr>
								))}
							</Table.Tbody>
						</Table>
					</Table.ScrollContainer>
				</Stack>
			)}

			{quality && <QualityBlock quality={quality} />}

			{temporal && (
				<Stack gap={4} data-testid="canvas-column-profile-temporal">
					<Text size="xs" fw={600}>
						Temporal
					</Text>
					<Group gap="md" wrap="wrap">
						<Field label="from" value={temporal.min_timestamp ?? "—"} />
						<Field label="to" value={temporal.max_timestamp ?? "—"} />
						<Field
							label="span"
							value={
								temporal.span_days === null
									? "—"
									: `${Math.round(temporal.span_days)}d`
							}
						/>
						<Field label="granularity" value={temporal.granularity ?? "—"} />
						<Field label="completeness" value={pct(temporal.completeness)} />
						<Field
							label="gaps"
							value={
								temporal.gap_count === null
									? "—"
									: temporal.largest_gap_days
										? `${temporal.gap_count} (largest ${Math.round(temporal.largest_gap_days)}d)`
										: String(temporal.gap_count)
							}
						/>
						<Field
							label="stale"
							value={
								temporal.is_stale === null
									? "—"
									: temporal.is_stale
										? "yes"
										: "no"
							}
						/>
					</Group>
				</Stack>
			)}

			{derived.length > 0 && (
				<Stack gap={4} data-testid="canvas-column-profile-derived">
					<Text size="xs" fw={600}>
						Derived from
					</Text>
					{derived.map((d, i) => (
						<Group
							// biome-ignore lint/suspicious/noArrayIndexKey: static row list, never reordered after render
							key={i}
							gap="md"
							wrap="wrap"
						>
							<Field label="type" value={d.derivation_type ?? "—"} />
							<Field label="formula" value={d.formula ?? "—"} />
							<Field label="match" value={pct(d.match_rate)} />
						</Group>
					))}
				</Stack>
			)}
		</Stack>
	);
}

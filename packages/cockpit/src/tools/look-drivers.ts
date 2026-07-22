// look_drivers tool (DAT-546) — the persisted driver rankings for the workspace's
// promoted begin_session run. For a measure, which dimensions and slices most
// explain its variation: a ranked, significance-gated answer the engine pre-computed
// (the variance-reduction tree + within-dataset permutation null, DAT-545/561/563),
// so the answer agent narrates a real driver story instead of guessing a GROUP BY.
//
// Pure read of the `current_driver_rankings` view: catalog-grain, so the view already
// resolves to the single promoted begin_session catalog head (one row per measure
// column — no session axis, no TS-side latest pick needed). The grain labels are
// surfaced, NOT flattened: the primary family's `grain`/`entity` plus a
// `secondary_dimensions` list where each item keeps its OWN grain + entity (the
// per-entity families are not cross-comparable — DAT-563). Read-only → no approval.
//
// The DB read is integration-smoke-covered; the pure row→shape projection (JSON
// narrowing + digest sanitization, grain labels preserved) is unit-tested via
// `projectDriverRanking`.

import { toolDefinition } from "@tanstack/ai";
import { and, eq } from "drizzle-orm";
import { z } from "zod";

import { metadataDb } from "../db/metadata/client";
import { catalogHeadTarget } from "../db/metadata/relationship-target";
import {
	currentDriverRankings,
	metadataSnapshotHead,
} from "../db/metadata/schema";
import { stripSrcDigests } from "../lib/display-names";

// --- The persisted JSON shapes (narrowed at the boundary; fail soft to []). ---

const RankedDim = z.object({ dimension: z.string(), gain: z.number() });
const Slice = z.object({
	dimension: z.string(),
	value: z.string(),
	effect: z.number(),
	support: z.number(),
});
const Secondary = z.object({
	dimension: z.string(),
	gain: z.number(),
	// "entity" or "row" — the exchangeable unit this dim's null used.
	grain: z.string(),
	// Which identity column the entity grain belongs to; null for the row family.
	entity: z.string().nullable(),
});

// --- The tool's output: one entry per ranked measure. ---

const DriverRanking = z.object({
	// The measure the ranking explains (the column / ratio label).
	measure: z.string(),
	target_type: z.string(), // flow | stock | ratio; "" when abstained with no resolved type
	// The PRIMARY family's exchangeable grain: "row", or "entity" when the measure
	// clusters within an identity; `entity` then names WHICH identity (else null).
	grain: z.string(),
	entity: z.string().nullable(),
	// Effective sample size the power scales with (rows, or entities at entity grain)
	// — so "no significant driver" on few entities is honestly attributable.
	n_rows: z.number(),
	// DAT-859: "measured" (the engine actually ranked it) or "abstained" (a measure
	// whose temporal_behavior was NULL/undetermined, or one of the honest-empty
	// construction sites — no enriched view, too few candidates, no usable measure
	// value). Surfaced honestly here — this raw tool never drops it — even though
	// the answer-agent's `<drivers>` context filters abstained rankings out.
	status: z.string(),
	// Closed vocabulary (missing_inputs | insufficient_candidates | insufficient_data);
	// null exactly when status is "measured".
	abstain_reason: z.string().nullable(),
	// The primary family's significant dims, strongest first.
	ranked_dimensions: z.array(RankedDim),
	// Surviving drill vectors (e.g. ["region","channel"]) of the primary tree.
	driver_paths: z.array(z.array(z.string())),
	// Sharp-deviation slices across the tree, strongest first.
	interesting_slices: z.array(Slice),
	// Every NON-primary grain family's significant dims, each labeled with its own
	// grain + entity — never merged into ranked_dimensions (the grains aren't comparable).
	secondary_dimensions: z.array(Secondary),
});
export type DriverRanking = z.infer<typeof DriverRanking>;

const LookDriversResult = z.object({
	// False when the workspace has no promoted begin_session run yet — the widget
	// should say "not run" rather than imply zero measures.
	analyzed: z.boolean(),
	rankings: z.array(DriverRanking),
});
export type LookDriversResult = z.infer<typeof LookDriversResult>;

/** A raw `current_driver_rankings` row (JSON columns are `unknown` from Drizzle). */
export interface DriverRankingRow {
	measureLabel: string | null;
	targetType: string | null;
	grain: string | null;
	entity: string | null;
	nRows: number | null;
	status: string | null;
	abstainReason: string | null;
	rankedDimensions: unknown;
	driverPaths: unknown;
	interestingSlices: unknown;
	secondaryDimensions: unknown;
}

/**
 * Project one persisted ranking row to the tool's shape. Pure (no DB) so the JSON
 * narrowing + sanitization is unit-testable. Every dimension/entity string is run
 * through the digest backstop (an enriched dimension can carry a raw
 * `src_<digest>__` physical prefix); a malformed JSON blob degrades to `[]` rather
 * than throwing — born-loud absence, never a crash. Grain labels are preserved
 * verbatim, per family.
 */
export function projectDriverRanking(row: DriverRankingRow): DriverRanking {
	const ranked = RankedDim.array().safeParse(row.rankedDimensions);
	const paths = z.array(z.array(z.string())).safeParse(row.driverPaths);
	const slices = Slice.array().safeParse(row.interestingSlices);
	const secondary = Secondary.array().safeParse(row.secondaryDimensions);
	return {
		measure: stripSrcDigests(row.measureLabel ?? ""),
		target_type: row.targetType ?? "",
		grain: row.grain ?? "row",
		entity: row.entity === null ? null : stripSrcDigests(row.entity),
		n_rows: row.nRows ?? 0,
		// The DB column is NOT NULL (CHECK-enforced); the view mirror types it
		// nullable regardless (view-column nullability isn't introspected) — this
		// default is defensive, not a real fallback path.
		status: row.status ?? "measured",
		abstain_reason: row.abstainReason ?? null,
		ranked_dimensions: ranked.success
			? ranked.data.map((d) => ({
					dimension: stripSrcDigests(d.dimension),
					gain: d.gain,
				}))
			: [],
		driver_paths: paths.success
			? paths.data.map((p) => p.map(stripSrcDigests))
			: [],
		interesting_slices: slices.success
			? slices.data.map((s) => ({
					...s,
					dimension: stripSrcDigests(s.dimension),
				}))
			: [],
		secondary_dimensions: secondary.success
			? secondary.data.map((s) => ({
					...s,
					dimension: stripSrcDigests(s.dimension),
					entity: s.entity === null ? null : stripSrcDigests(s.entity),
				}))
			: [],
	};
}

/** The promoted begin_session catalog run, or null when none is promoted yet. */
async function readBeginSessionHead(): Promise<string | null> {
	const [head] = await metadataDb
		.select({ runId: metadataSnapshotHead.runId })
		.from(metadataSnapshotHead)
		.where(
			and(
				eq(metadataSnapshotHead.target, catalogHeadTarget()),
				eq(metadataSnapshotHead.stage, "catalog"),
			),
		)
		.limit(1);
	return head?.runId ?? null;
}

/** Persisted driver rankings for the promoted begin_session run, optionally filtered. */
export async function lookDrivers(input: {
	measure?: string;
}): Promise<LookDriversResult> {
	// `analyzed` = a begin_session run is promoted — distinct from "promoted but zero
	// measure columns" (a fact with no measures), which must not read as never-ran.
	const head = await readBeginSessionHead();
	if (!head) {
		return { analyzed: false, rankings: [] };
	}

	// The view IS the promoted run (one row per measure column, catalog head). The set
	// is small (a handful of measures per fact), so the optional measure filter is a
	// case-insensitive substring match in JS — no brittle SQL name resolution.
	const rows = await metadataDb
		.select({
			measureLabel: currentDriverRankings.measureLabel,
			targetType: currentDriverRankings.targetType,
			grain: currentDriverRankings.grain,
			entity: currentDriverRankings.entity,
			nRows: currentDriverRankings.nRows,
			status: currentDriverRankings.status,
			abstainReason: currentDriverRankings.abstainReason,
			rankedDimensions: currentDriverRankings.rankedDimensions,
			driverPaths: currentDriverRankings.driverPaths,
			interestingSlices: currentDriverRankings.interestingSlices,
			secondaryDimensions: currentDriverRankings.secondaryDimensions,
		})
		.from(currentDriverRankings);

	const needle = input.measure?.trim().toLowerCase();
	const rankings = rows
		.map(projectDriverRanking)
		.filter((r) => !needle || r.measure.toLowerCase().includes(needle));

	return { analyzed: true, rankings };
}

export const lookDriversTool = toolDefinition({
	name: "look_drivers",
	description:
		"Show which dimensions and slices most explain a measure's variation — the " +
		"pre-computed driver rankings for the workspace's begin_session run. For each " +
		"measure: ranked_dimensions (the dims that best explain it, significance-gated), " +
		"driver_paths (drill vectors like region→channel), and interesting_slices (where " +
		"the measure deviates sharply, with effect + support). grain/entity say at what " +
		"level the story holds (row-level, or clustered within an identity like customer); " +
		"n_rows is the effective sample size. secondary_dimensions are drivers found at a " +
		"DIFFERENT grain (e.g. a second identity) — kept separate, not comparable to the " +
		"primary ranking. status is 'measured' or 'abstained' — an abstained ranking " +
		"(its measure's temporal behavior was undetermined, or it had no enriched " +
		"view/too few candidates/no usable value) carries no ranked content; say so " +
		"plainly rather than reporting 'no driver found', which implies a ranking was " +
		"actually attempted. abstain_reason names why. Pass `measure` to filter to one " +
		"(substring match); omit it for all. Read-only; reflects the promoted " +
		"begin_session run (run begin_session first).",
	inputSchema: z.object({
		measure: z.string().optional().meta({
			description:
				"Filter to measures whose name contains this (case-insensitive).",
		}),
	}),
	outputSchema: LookDriversResult,
}).server((input) => lookDrivers(input));

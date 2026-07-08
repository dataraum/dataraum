// SPIKE (measurement, not a test): enumerate EVERY drillable node × EVERY
// axis on the live workspace through the PRODUCTION drill modules —
// resolveNodeSteps → resolveDrillAxes → composeNodeQuery → describeColumns →
// execution. Enumerate-don't-sample: this is the full finite matrix the
// browser can reach, run on real data before the lead clicks it.
//
// Checks per node:
//   scalar   — composes, binds, executes; revenue == ground truth.
//   per axis — grouped composition binds + executes; additive metrics must
//              have Σ(groups) == scalar (signed contributions); NULL counted.
//   pin      — the first grouped row's value must be reproduced exactly by
//              pinning that row (pin ≡ group, the coherence the browser
//              smoke clicks).
//
// Run:  cd packages/cockpit && bun spikes/drill-node.spike.ts

import { and, asc, eq, like } from "drizzle-orm";

import { config } from "#/config";
import { metadataDb } from "#/db/metadata/client";
import { currentLifecycleArtifacts, sqlSnippets } from "#/db/metadata/schema";
import type { DrillPinValue } from "#/duckdb/drill";
import { describeColumns } from "#/duckdb/drill-sql";
import { applyEngineScope, closeLake, withLakeConnection } from "#/duckdb/lake";
import { composeNodeQuery, flattenAdditive, type NodeStep } from "#/duckdb/parts";
import { resolveDrillAxes } from "#/tools/drill-axes";
import { resolveNodeSteps } from "#/tools/drill-metric";

import { num } from "./spike-lib";

const GROUND_TRUTH: Record<string, number> = { revenue: 51766199.72 };

/** The production classification (doctrine v2): additive nodes decompose via
 *  signed contributions and must satisfy Σ(groups) == scalar on every axis. */
function isAdditive(steps: NodeStep[]): boolean {
	const byId = new Map(steps.map((s) => [s.stepId, s]));
	const output =
		steps.find((s) => s.outputStep) ?? steps[steps.length - 1] ?? null;
	return output !== null && flattenAdditive(output, byId) !== null;
}

const toPin = (v: unknown): DrillPinValue | undefined =>
	v === null || ["string", "number", "boolean"].includes(typeof v)
		? (v as DrillPinValue)
		: undefined;

async function main(): Promise<void> {
	const metricRows = await metadataDb
		.select({ key: currentLifecycleArtifacts.artifactKey })
		.from(currentLifecycleArtifacts)
		.where(eq(currentLifecycleArtifacts.artifactType, "metric"))
		.orderBy(asc(currentLifecycleArtifacts.artifactKey));

	// The measure nodes: every standard field the graph agent grounded (the
	// canvas keys measure nodes by standardField).
	const fieldRows = await metadataDb
		.select({ standardField: sqlSnippets.standardField })
		.from(sqlSnippets)
		.where(
			and(
				eq(sqlSnippets.schemaMappingId, config.dataraumWorkspaceId),
				like(sqlSnippets.source, "graph:%"),
				eq(sqlSnippets.snippetType, "extract"),
			),
		);
	const measureFields = [
		...new Set(
			fieldRows
				.map((r) => r.standardField)
				.filter((f): f is string => Boolean(f)),
		),
	].sort();

	const report: string[] = [];
	let fails = 0;
	const flag = (line: string) => {
		fails++;
		report.push(`  ✗ ${line}`);
	};

	await withLakeConnection(async (conn) => {
		await applyEngineScope(conn);

		const exec = async (sql: string, params: DrillPinValue[]) => {
			await describeColumns(conn, sql, params); // the route's binder gate
			const r =
				params.length > 0
					? await conn.runAndReadAll(sql, params)
					: await conn.runAndReadAll(sql);
			return r.getRowObjectsJson() as Record<string, unknown>[];
		};

		const drillNode = async (
			label: string,
			ref: { metricKey: string } | { standardField: string },
		) => {
			const resolved = await resolveNodeSteps(ref);
			if ("missing" in resolved) {
				flag(`${label}: resolve MISSING — ${resolved.missing}`);
				return;
			}

			// 1) scalar
			const scalarQ = composeNodeQuery(resolved.steps, undefined, {
				slices: [],
				pins: [],
			});
			if ("refusal" in scalarQ) {
				report.push(`  · ${label}: scalar REFUSED — ${scalarQ.refusal}`);
				return; // named refusal is a legitimate surface (holes)
			}
			let scalar: number | null = null;
			try {
				scalar = num((await exec(scalarQ.sql, []))[0]?.value);
			} catch (err) {
				flag(`${label}: scalar ERR ${String(err).split("\n")[0]}`);
				return;
			}
			const gtField = "standardField" in ref ? ref.standardField : ref.metricKey;
			const gt = GROUND_TRUTH[gtField];
			const gtNote =
				gt !== undefined
					? scalar !== null && Math.abs(scalar - gt) < 0.01
						? " GT✓"
						: ` GT✗ expected ${gt}`
					: "";
			if (gtNote.includes("✗")) fails++;

			// 2) axes
			const { axes, reason } = await resolveDrillAxes(ref);
			const additive = isAdditive(resolved.steps);
			report.push(
				`## ${label} — scalar=${String(scalar)}${gtNote} | ${axes.length} axes${
					axes.length === 0 ? ` (${reason ?? "?"})` : ""
				} | ${additive ? "additive" : "non-additive"}`,
			);

			let pinChecked = false;
			for (const axis of axes) {
				const q = composeNodeQuery(resolved.steps, undefined, {
					slices: [{ column: axis.column }],
					pins: [],
				});
				if ("refusal" in q) {
					flag(`${label} by ${axis.column}: compose REFUSED — ${q.refusal}`);
					continue;
				}
				let rows: Record<string, unknown>[];
				try {
					rows = await exec(q.sql, []);
				} catch (err) {
					flag(`${label} by ${axis.column}: ERR ${String(err).split("\n")[0]}`);
					continue;
				}
				const nulls = rows.filter((r) => r.value === null).length;
				let sumNote = "";
				if (additive && scalar !== null) {
					const sum = rows.reduce((s, r) => s + (num(r.value) ?? 0), 0);
					if (Math.abs(sum - scalar) < 1e-6) sumNote = " Σ=scalar";
					else {
						sumNote = ` Σ≠scalar (Σ=${sum})`;
						fails++;
					}
				}
				report.push(
					`  [${axis.priority === Number.MAX_SAFE_INTEGER ? "substrate" : "curated"}] ${axis.column}: ${rows.length} groups${nulls ? `, ${nulls} NULL` : ""}${sumNote}`,
				);

				// 3) pin ≡ group, once per node on the first pinnable row
				if (!pinChecked) {
					const row = rows.find((r) => toPin(r[axis.column]) !== undefined);
					const pinValue = row ? toPin(row[axis.column]) : undefined;
					if (row && pinValue !== undefined) {
						pinChecked = true;
						const pq = composeNodeQuery(resolved.steps, undefined, {
							slices: [],
							pins: [{ column: axis.column, value: pinValue }],
						});
						if ("refusal" in pq) {
							flag(`${label} pin ${axis.column}: REFUSED — ${pq.refusal}`);
						} else {
							try {
								const pv = num((await exec(pq.sql, pq.params))[0]?.value);
								const gv = num(row.value);
								const same =
									(pv === null && gv === null) ||
									(pv !== null &&
										gv !== null &&
										Math.abs(pv - gv) < 1e-6);
								if (same) {
									report.push(
										`  pin ${axis.column}=${String(pinValue)}: ${String(pv)} ≡ group ✓`,
									);
								} else {
									flag(
										`${label} pin ${axis.column}=${String(pinValue)}: pin=${String(pv)} ≠ group=${String(gv)}`,
									);
								}
							} catch (err) {
								flag(
									`${label} pin ${axis.column}: ERR ${String(err).split("\n")[0]}`,
								);
							}
						}
					}
				}
			}
		};

		for (const m of metricRows) {
			if (m.key) await drillNode(`metric ${m.key}`, { metricKey: m.key });
		}
		for (const field of measureFields) {
			await drillNode(`measure ${field}`, { standardField: field });
		}
	});
	await closeLake();

	report.push(`\n=== ${fails === 0 ? "ALL CHECKS PASS" : `${fails} FAILURES`}`);
	console.log(report.join("\n"));
	if (fails > 0) process.exitCode = 1;
}

await main();

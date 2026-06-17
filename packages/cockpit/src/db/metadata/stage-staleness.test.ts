import { describe, expect, it } from "vitest";
import {
	collapseHeads,
	deriveStaleness,
	type RawHead,
	type StageHead,
} from "#/db/metadata/stage-staleness";

const t = (iso: string) => new Date(iso);

describe("collapseHeads (DAT-531)", () => {
	it("collapses add_source to the MAX of its per-table generation heads", () => {
		const heads: RawHead[] = [
			{
				target: "table:a",
				stage: "generation",
				promotedAt: t("2026-06-17T10:00:00Z"),
			},
			{
				target: "table:b",
				stage: "generation",
				promotedAt: t("2026-06-17T12:00:00Z"),
			},
		];
		const add = collapseHeads(heads).find((h) => h.stage === "add_source");
		expect(add?.promotedAt?.toISOString()).toBe("2026-06-17T12:00:00.000Z");
	});

	it("reads begin_session from the catalog head and operating_model from its own", () => {
		const heads: RawHead[] = [
			{
				target: "catalog",
				stage: "catalog",
				promotedAt: t("2026-06-17T11:00:00Z"),
			},
			{
				target: "catalog",
				stage: "operating_model",
				promotedAt: t("2026-06-17T11:30:00Z"),
			},
		];
		const out = collapseHeads(heads);
		expect(
			out.find((h) => h.stage === "begin_session")?.promotedAt?.toISOString(),
		).toBe("2026-06-17T11:00:00.000Z");
		expect(
			out.find((h) => h.stage === "operating_model")?.promotedAt?.toISOString(),
		).toBe("2026-06-17T11:30:00.000Z");
	});

	it("reports a stage with no head as null (unrun)", () => {
		expect(collapseHeads([]).every((h) => h.promotedAt === null)).toBe(true);
	});
});

describe("deriveStaleness (DAT-531)", () => {
	const heads = (
		a: string | null,
		b: string | null,
		o: string | null,
	): StageHead[] => [
		{ stage: "add_source", promotedAt: a ? t(a) : null },
		{ stage: "begin_session", promotedAt: b ? t(b) : null },
		{ stage: "operating_model", promotedAt: o ? t(o) : null },
	];
	const stale = (r: ReturnType<typeof deriveStaleness>, s: string) =>
		r.find((x) => x.stage === s);

	it("flags nothing stale when every stage ran in upstream→downstream order", () => {
		const r = deriveStaleness(
			heads(
				"2026-06-17T10:00:00Z",
				"2026-06-17T11:00:00Z",
				"2026-06-17T12:00:00Z",
			),
			[],
		);
		expect(r.every((x) => !x.stale)).toBe(true);
	});

	it("flags downstream stale when an upstream head is newer (upstream-newer)", () => {
		// add_source re-ran at 13:00, after begin_session (11:00) + operating_model (12:00).
		const r = deriveStaleness(
			heads(
				"2026-06-17T13:00:00Z",
				"2026-06-17T11:00:00Z",
				"2026-06-17T12:00:00Z",
			),
			[],
		);
		expect(stale(r, "add_source")?.stale).toBe(false);
		expect(stale(r, "begin_session")).toMatchObject({
			stale: true,
			reason: "upstream-newer",
		});
		expect(stale(r, "operating_model")).toMatchObject({
			stale: true,
			reason: "upstream-newer",
		});
	});

	it("flags a stage teach-pending when an overlay post-dates its head", () => {
		// A metric teach (→ operating_model) written after operating_model last ran.
		const r = deriveStaleness(
			heads(
				"2026-06-17T10:00:00Z",
				"2026-06-17T11:00:00Z",
				"2026-06-17T12:00:00Z",
			),
			[{ type: "metric", createdAt: t("2026-06-17T12:30:00Z") }],
		);
		expect(stale(r, "operating_model")).toMatchObject({
			stale: true,
			reason: "teach-pending",
		});
		// The unrelated stages stay fresh.
		expect(stale(r, "add_source")?.stale).toBe(false);
		expect(stale(r, "begin_session")?.stale).toBe(false);
	});

	it("ignores an overlay written BEFORE the stage ran (already applied)", () => {
		const r = deriveStaleness(
			heads(
				"2026-06-17T10:00:00Z",
				"2026-06-17T11:00:00Z",
				"2026-06-17T12:00:00Z",
			),
			[{ type: "metric", createdAt: t("2026-06-17T09:00:00Z") }],
		);
		expect(r.every((x) => !x.stale)).toBe(true);
	});

	it("prefers teach-pending over upstream-newer when both hold", () => {
		// begin_session is behind add_source AND has a pending relationship teach.
		const r = deriveStaleness(
			heads("2026-06-17T13:00:00Z", "2026-06-17T11:00:00Z", null),
			[{ type: "relationship", createdAt: t("2026-06-17T14:00:00Z") }],
		);
		expect(stale(r, "begin_session")).toMatchObject({
			stale: true,
			reason: "teach-pending",
		});
	});

	it("never flags an unrun stage stale", () => {
		const r = deriveStaleness(heads("2026-06-17T10:00:00Z", null, null), []);
		expect(stale(r, "begin_session")?.stale).toBe(false);
		expect(stale(r, "operating_model")?.stale).toBe(false);
	});
});

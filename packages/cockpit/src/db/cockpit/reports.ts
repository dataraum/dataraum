// Server-owned report persistence (DAT-624) — cockpit_db is the source of truth
// for minted reports. A report is a frozen { SQL + summary (+ chart config) } widget
// over LIVE data: the SQL is captured once from an `answer` and RE-RUN on every open
// (no result snapshot — numbers stay current). Workspace-owned and session-
// independent: it outlives the chat it was minted from (`conversationId` /
// `messageId` are nullable provenance, never owners).
//
// Mutation surface, mirroring conversations.ts: createReport (mint), listReports
// (gallery), getReport (detail; null when missing OR soft-deleted), renameReport
// (the one editable field), softDeleteReport. The analytical content (sql / summary
// / confidence) is immutable — there is no editReport by design.

import { randomUUID } from "node:crypto";
import { and, desc, eq, isNull } from "drizzle-orm";
import type { AnswerConfidence } from "#/ui/cockpit/canvas-state";
import { cockpitDb } from "./client";
import { reports } from "./schema";

/** How many reports the gallery lists. Bounded (cockpit "bound every data surface")
 * — newest-first; pagination is a follow-up when a workspace outgrows this. */
export const REPORTS_LIMIT = 200;

/** The fields captured when an answer is minted into a report. `conversationId` /
 * `messageId` / `parentId` are optional provenance/lineage; the rest is the frozen
 * artifact. `title` defaults from the answer at the call site. */
export interface CreateReportInput {
	workspaceId: string;
	conversationId?: string | null;
	messageId?: string | null;
	parentId?: string | null;
	title: string;
	summary: string;
	sql: string;
	confidence: AnswerConfidence;
	/** Headline result fingerprint at mint (DAT-625). Drives drift detection on open;
	 * null only if fingerprinting failed (best-effort), then lazy-backfilled later. */
	summaryFingerprint?: string | null;
}

/** A report as the gallery + detail render it. `chartConfig` is reserved (DAT-626).
 * `summaryFingerprint` is the result fingerprint at last summary-gen (DAT-625) — the
 * detail loader compares it to a fresh fingerprint to flag the summary outdated. */
export interface ReportRow {
	id: string;
	workspaceId: string;
	parentId: string | null;
	title: string;
	summary: string;
	summaryFingerprint: string | null;
	sql: string;
	confidence: AnswerConfidence;
	createdAt: Date;
}

/**
 * Mint a report from an answer and return its id (DAT-624). The SQL + summary +
 * confidence are frozen here; everything else about the report is derived live on
 * open. Provenance is best-effort — `workspaceId` is the only owner.
 */
export async function createReport(input: CreateReportInput): Promise<string> {
	const id = randomUUID();
	await cockpitDb.insert(reports).values({
		id,
		workspaceId: input.workspaceId,
		conversationId: input.conversationId ?? null,
		messageId: input.messageId ?? null,
		parentId: input.parentId ?? null,
		title: input.title,
		summary: input.summary,
		summaryFingerprint: input.summaryFingerprint ?? null,
		sql: input.sql,
		confidence: input.confidence,
	});
	return id;
}

/**
 * A workspace's live reports, newest first, BOUNDED — the gallery list. Soft-deleted
 * rows (`deletedAt IS NOT NULL`) are excluded.
 */
export async function listReports(
	workspaceId: string,
	limit: number = REPORTS_LIMIT,
): Promise<Array<ReportRow>> {
	return cockpitDb
		.select({
			id: reports.id,
			workspaceId: reports.workspaceId,
			parentId: reports.parentId,
			title: reports.title,
			summary: reports.summary,
			summaryFingerprint: reports.summaryFingerprint,
			sql: reports.sql,
			confidence: reports.confidence,
			createdAt: reports.createdAt,
		})
		.from(reports)
		.where(and(eq(reports.workspaceId, workspaceId), isNull(reports.deletedAt)))
		.orderBy(desc(reports.createdAt))
		.limit(limit);
}

/**
 * Hydrate a report by id (the detail loader). Null when the id is unknown OR the
 * report is soft-deleted — either way the route 404s rather than mounting a ghost.
 */
export async function getReport(reportId: string): Promise<ReportRow | null> {
	const [row] = await cockpitDb
		.select({
			id: reports.id,
			workspaceId: reports.workspaceId,
			parentId: reports.parentId,
			title: reports.title,
			summary: reports.summary,
			summaryFingerprint: reports.summaryFingerprint,
			sql: reports.sql,
			confidence: reports.confidence,
			createdAt: reports.createdAt,
		})
		.from(reports)
		.where(and(eq(reports.id, reportId), isNull(reports.deletedAt)))
		.limit(1);
	return row ?? null;
}

/**
 * Rename a report (the only editable field). Scoped to live rows so a soft-deleted
 * report can't be renamed back into view.
 */
export async function renameReport(
	reportId: string,
	title: string,
): Promise<void> {
	await cockpitDb
		.update(reports)
		.set({ title })
		.where(and(eq(reports.id, reportId), isNull(reports.deletedAt)));
}

/**
 * Refresh a report's summary + its result fingerprint together (DAT-625 regenerate).
 * This is the ONLY path that mutates `summary` — `sql` stays immutable. Atomic so the
 * stored fingerprint always matches the summary it was generated against (no window
 * where the prose is new but the fingerprint still flags it outdated). Live-rows only.
 */
export async function updateReportSummary(
	reportId: string,
	summary: string,
	summaryFingerprint: string,
): Promise<void> {
	await cockpitDb
		.update(reports)
		.set({ summary, summaryFingerprint })
		.where(and(eq(reports.id, reportId), isNull(reports.deletedAt)));
}

/**
 * Lazy-backfill a report's fingerprint (DAT-625) — for reports minted before this
 * existed, or when mint-time fingerprinting failed. Written on first open so the
 * report starts tracking drift WITHOUT touching `summary` (the prose is unchanged, so
 * it's not "outdated" — we just had nothing to compare against yet). Live-rows only.
 */
export async function setReportFingerprint(
	reportId: string,
	summaryFingerprint: string,
): Promise<void> {
	await cockpitDb
		.update(reports)
		.set({ summaryFingerprint })
		.where(and(eq(reports.id, reportId), isNull(reports.deletedAt)));
}

/**
 * Soft-delete a report — it drops out of the gallery; its children (`parentId`)
 * remain (lineage is provenance, not ownership). Idempotent: a re-delete is a no-op
 * via the `deletedAt IS NULL` guard.
 */
export async function softDeleteReport(reportId: string): Promise<void> {
	await cockpitDb
		.update(reports)
		.set({ deletedAt: new Date() })
		.where(and(eq(reports.id, reportId), isNull(reports.deletedAt)));
}

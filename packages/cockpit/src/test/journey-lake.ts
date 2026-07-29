// The journey LAKE: real corpus rows in a real DuckLake, readable by the
// cockpit's own `lake.ts` with nothing mocked.
//
// WHY THIS SHAPE, AND WHY NOT A MOCK
// The existing lake-backed suites `vi.mock("./lake")` and hand a connection to
// a local file-catalog DuckLake. That is fine for testing `runSql` in
// isolation, but a JOURNEY has to prove the route the practitioner actually
// hits — and `lake.ts`'s bootstrap (extension load, S3 secret, the ATTACH
// itself) is part of what can break. `buildDucklakeAttachSql` hardcodes a
// `ducklake:postgres:` connection string, so a file catalog cannot be reached
// through config at all.
//
// So the lake is built the way production's is: a POSTGRES DuckLake catalog —
// hosted in the fixture container that is already running — with a local
// directory as DATA_PATH. The cockpit then reaches it through nothing but
// config (`DUCKLAKE_CATALOG_URL`, `DATARAUM_LAKE_PATH`), running its real
// bootstrap. Verified end to end before this was written: a writer instance
// commits, a SEPARATE reader process ATTACHes READ_ONLY with the same
// METADATA_SCHEMA and reads every figure back.
//
// The WRITER here stands in for the engine — it is the only part that is not
// the shipped code path, which is correct: the engine owns the lake, and the
// cockpit is a reader. Its ATTACH is hand-built rather than borrowed from
// `buildDucklakeAttachSql`, because that builder is the READER's and pins
// READ_ONLY.

import { existsSync } from "node:fs";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";

import { DuckDBInstance } from "@duckdb/node-api";

import {
	ducklakeMetadataSchemaFor,
	escapeSqlLiteral,
} from "../duckdb/sql-escape";
import { JOURNEY_RELATION } from "./journey-answer-key";
import { JOURNEY_WORKSPACE_ID } from "./seed-journey";

/** CSVs the enriched view is built from. */
const REQUIRED_FILES = [
	"journal_lines.csv",
	"journal_entries.csv",
	"chart_of_accounts.csv",
] as const;

/**
 * Locate the `dataraum-testdata` corpus.
 *
 * `DATARAUM_TESTDATA_PATH` wins when set (CI, or a checkout somewhere else).
 * Otherwise walk UP from this file looking for a sibling `dataraum-testdata`.
 * The walk is not decoration: this repo is worked in git WORKTREES nested
 * several levels inside the project dir, so a fixed `../../../..` hop resolves
 * correctly in a bare checkout and to nothing in a worktree — silently skipping
 * the whole suite for exactly the setup the team uses.
 */
export function findCorpusDir(): string | null {
	const fromEnv = process.env.DATARAUM_TESTDATA_PATH;
	if (fromEnv) {
		const dir = fromEnv.endsWith("clean")
			? fromEnv
			: join(fromEnv, "output", "clean");
		return hasCorpus(dir) ? dir : null;
	}
	// The bound is generous on purpose. Worktrees nest inside the project dir
	// (`.claude/worktrees/<epic>/.claude/worktrees/<lane>/…`), so this file can
	// sit 15+ levels below the directory holding the sibling corpus — a
	// plausible-looking cap of 12 stops one level short of a real lane checkout
	// and skips the whole suite. Walking to the filesystem root costs nothing.
	let dir = dirname(fileURLToPath(import.meta.url));
	for (let i = 0; i < 40; i++) {
		const candidate = join(dir, "dataraum-testdata", "output", "clean");
		if (hasCorpus(candidate)) return candidate;
		const parent = dirname(dir);
		if (parent === dir) break;
		dir = parent;
	}
	return null;
}

function hasCorpus(dir: string): boolean {
	return REQUIRED_FILES.every((f) => existsSync(join(dir, f)));
}

export interface JourneyLakeSpec {
	/** libpq connection string for the DuckLake catalog database. */
	catalogLibpq: string;
	/** Local directory holding the lake's parquet. */
	dataPath: string;
	/** Directory holding the corpus CSVs. */
	corpusDir: string;
}

/**
 * Build `lake.typed.<JOURNEY_RELATION>` from the real corpus and commit it.
 *
 * The relation is the enriched view the catalog advertises: the fact's own
 * columns plus the enrichment's `<fk>__<attr>` dimension attributes. It is
 * materialised as a TABLE — DuckLake stores tables, and what the cockpit reads
 * is a relation of the catalogued name either way.
 *
 * Nothing is filtered on the way in. The revenue predicate lives in the
 * MEASURE's clause parts, which is where production keeps it and what makes
 * J3's twelve-bucket assertion meaningful (the raw fact spans fourteen months).
 */
export async function buildJourneyLake(spec: JourneyLakeSpec): Promise<void> {
	const schema = ducklakeMetadataSchemaFor(JOURNEY_WORKSPACE_ID);
	const attach =
		`ATTACH 'ducklake:postgres:${escapeSqlLiteral(spec.catalogLibpq)}' AS lake ` +
		`(DATA_PATH '${escapeSqlLiteral(spec.dataPath)}', METADATA_SCHEMA '${escapeSqlLiteral(schema)}')`;

	const csv = (name: string) =>
		`read_csv_auto('${escapeSqlLiteral(join(spec.corpusDir, name))}')`;

	const instance = await DuckDBInstance.create(":memory:");
	const conn = await instance.connect();
	try {
		try {
			await conn.run("INSTALL ducklake");
		} catch {
			// Already present offline — LOAD surfaces a genuine absence.
		}
		await conn.run("LOAD ducklake");
		await conn.run(attach);
		await conn.run("CREATE SCHEMA IF NOT EXISTS lake.typed");
		await conn.run(
			`CREATE TABLE lake.typed.${JOURNEY_RELATION} AS
			 SELECT l.line_id, l.entry_id, l.account_id, l.debit, l.credit,
			        l.currency, l.cost_center,
			        a.name         AS account_id__name,
			        a.account_type AS account_id__account_type,
			        e.date         AS entry_id__date,
			        e.status       AS entry_id__status
			   FROM ${csv("journal_lines.csv")} l
			   JOIN ${csv("chart_of_accounts.csv")} a ON l.account_id = a.account_id
			   JOIN ${csv("journal_entries.csv")} e ON l.entry_id = e.entry_id`,
		);
	} finally {
		conn.closeSync();
		instance.closeSync();
	}
}

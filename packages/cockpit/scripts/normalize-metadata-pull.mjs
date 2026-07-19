#!/usr/bin/env node
// Post-process drizzle-kit pull output for the metadata schema.
//
// The pull introspects the scratch `public` schema (pull-metadata.sh), so
// drizzle already emits plain unqualified exports — there is nothing to
// rename. Two jobs remain:
//
//   1. Enforce the DAT-816 invariant at generation time: the mirror must be
//      workspace-neutral. Any `pgSchema(` or `ws_` literal in the generated
//      files means the scratch materialization regressed (e.g. the read views
//      were applied into a named schema again) — fail loud here, before the
//      schema-drift CI job ever sees a bad artifact.
//   2. Delete drizzle's pull-only migration artifacts (one timestamped dir per
//      pull; we never push from the metadata config, so they are pure noise).
//
// Run after pull (or via `bun run db:pull:metadata`, which chains them).

import { readdir, readFile, rm } from 'node:fs/promises'

const targets = [
  'src/db/metadata/schema.ts',
  'src/db/metadata/relations.ts',
]

// Word-boundary `ws_`: the runtime reader role's search_path resolves the
// ws_<id>_read schema, so no generated identifier or SQL body may name one.
const FORBIDDEN = [/pgSchema\(/, /\bws_/]

let violations = false
for (const file of targets) {
  const content = await readFile(file, 'utf-8')
  for (const pattern of FORBIDDEN) {
    if (pattern.test(content)) {
      console.error(
        `  ${file} ERROR: generated output matches ${pattern} — the mirror ` +
          'must be workspace-neutral (DAT-816). Did the scratch read views ' +
          'land outside `public`?',
      )
      violations = true
    }
  }
  if (!violations) console.log(`  ${file} clean (workspace-neutral)`)
}
if (violations) process.exit(1)

// Drizzle pull also writes a timestamped migration dir (migration.sql +
// snapshot.json) into the out folder. We never push from the metadata
// config, so the migration artifacts are noise that would re-accumulate
// (one new timestamped dir per pull). Delete them.
const metadataDir = 'src/db/metadata'
const entries = await readdir(metadataDir, { withFileTypes: true })
const TIMESTAMP_PREFIX = /^\d{14}_/
for (const entry of entries) {
  if (entry.isDirectory() && TIMESTAMP_PREFIX.test(entry.name)) {
    await rm(`${metadataDir}/${entry.name}`, { recursive: true, force: true })
    console.log(`  ${metadataDir}/${entry.name} removed (pull-only artifact)`)
  }
}

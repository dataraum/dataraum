#!/usr/bin/env node
// Post-process drizzle-kit pull output for the metadata schema.
//
// drizzle-kit pull encodes the introspected schema name into every TS
// identifier (e.g. activeSessionInWs00000000000000000000000000000001 +
// const ws00000000000000000000000000000001 = pgSchema(...)) so multi-schema
// pulls don't collide. For the cockpit's single workspace schema, those
// identifiers are noise that leak the workspace_id into every call site.
//
// This script strips the suffix from table exports and renames the schema
// const to `metadataSchema`, while leaving the pgSchema("ws_<id>") argument
// intact so emitted SQL still qualifies by the real Postgres schema.
//
// Run after pull (or via `bun run db:pull:metadata`, which chains them).

import { readdir, readFile, rm, writeFile } from 'node:fs/promises'

const workspaceId = process.env.DATARAUM_WORKSPACE_ID
if (!workspaceId) {
  console.error(
    'DATARAUM_WORKSPACE_ID is not set. Set it to the same value that was ' +
      'used for drizzle-kit pull so this script can compute the identifier ' +
      'patterns to strip.',
  )
  process.exit(1)
}

const flatId = workspaceId.replaceAll('-', '')

// The mirror introspects the READ schema ws_<id>_read (ADR-0008/DAT-453), so
// drizzle's identifier suffix is InWs<id>Read and the schema const ws<id>Read.
const inWsPattern = new RegExp(`InWs${flatId}Read`, 'g')
const wsConstPattern = new RegExp(`\\bws${flatId}Read\\b`, 'g')

const targets = [
  'src/db/metadata/schema.ts',
  'src/db/metadata/relations.ts',
]

const UNMANGLED_DRIZZLE_OUTPUT = /InWs[0-9a-f]{32}(Read)?/i

let stalePullDetected = false
for (const file of targets) {
  const original = await readFile(file, 'utf-8')
  const normalized = original
    .replace(inWsPattern, '')
    .replace(wsConstPattern, 'metadataSchema')
  if (normalized === original) {
    // Either the file is already normalized (good) or the patterns missed
    // because DATARAUM_WORKSPACE_ID doesn't match what pull saw (bad: the
    // committed schema.ts would still reference a different workspace).
    if (UNMANGLED_DRIZZLE_OUTPUT.test(original)) {
      console.error(
        `  ${file} ERROR: contains un-normalized InWs<id> identifiers but ` +
          `none matched DATARAUM_WORKSPACE_ID=${workspaceId}. Did pull run ` +
          'against a different workspace?',
      )
      stalePullDetected = true
    } else {
      console.log(`  ${file} (already normalized)`)
    }
    continue
  }
  await writeFile(file, normalized)
  console.log(`  ${file} normalized`)
}

if (stalePullDetected) {
  process.exit(1)
}

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

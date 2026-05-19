import { drizzle } from 'drizzle-orm/postgres-js'
import postgres from 'postgres'

import * as schema from './schema'

const connectionString = process.env.COCKPIT_DATABASE_URL

if (!connectionString) {
  throw new Error(
    'COCKPIT_DATABASE_URL is not set. Point it at the cockpit_db database in the shared Postgres instance.',
  )
}

const client = postgres(connectionString, { prepare: false })

export const db = drizzle(client, { schema })

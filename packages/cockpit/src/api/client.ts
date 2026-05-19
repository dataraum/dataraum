import createClient from 'openapi-fetch'

import type { paths } from './types'

/**
 * Typed fetch client for the DataRaum engine REST surface.
 *
 * - **Dev (`pnpm dev`):** `VITE_ENGINE_API_URL` is unset → baseUrl is empty
 *   → fetch goes to the same origin → Vite proxies `/api/*` to the engine
 *   on `:8000`.
 * - **Docker (`docker compose up`):** the Dockerfile sets
 *   `VITE_ENGINE_API_URL=http://localhost:8000` as a build arg, so the
 *   compiled bundle does direct cross-origin fetches from the browser to
 *   the engine. CORS on the engine side allows it.
 *
 * Types are regenerated from dataraum-api via `pnpm sync-contracts`; this
 * file consumes whatever is in `./types`.
 */
const baseUrl = import.meta.env.VITE_ENGINE_API_URL ?? ''

export const apiClient = createClient<paths>({ baseUrl })

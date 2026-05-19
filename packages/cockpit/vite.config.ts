import { defineConfig } from 'vite'
import { devtools } from '@tanstack/devtools-vite'

import { tanstackStart } from '@tanstack/react-start/plugin/vite'
import { nitro } from 'nitro/vite'

import viteReact from '@vitejs/plugin-react'
import tailwindcss from '@tailwindcss/vite'

const config = defineConfig({
  resolve: { tsconfigPaths: true },
  plugins: [devtools(), tailwindcss(), tanstackStart(), nitro(), viteReact()],
  server: {
    proxy: {
      // Dev-only: same-origin /api requests on :3000 proxy to the Python
      // engine REST at :8000. In production the cockpit talks cross-origin
      // (CORS is configured on the engine side); the proxy is just to make
      // dev hot-reload nicer.
      '/api': {
        target: 'http://localhost:8000',
        changeOrigin: true,
      },
    },
  },
})

export default config

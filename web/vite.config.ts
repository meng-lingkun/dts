import { readFileSync } from 'node:fs'
import { fileURLToPath } from 'node:url'
import { defineConfig } from 'vite'
import vue from '@vitejs/plugin-vue'

const appVersion = readFileSync(fileURLToPath(new URL('../VERSION', import.meta.url)), 'utf8').trim()

export default defineConfig({
  plugins: [vue()],
  define: {__APP_VERSION__: JSON.stringify(appVersion)},
  server: {
    port: 5173,
    proxy: {
      '/api': {target: 'http://127.0.0.1:8080', changeOrigin: true},
      '/healthz': {target: 'http://127.0.0.1:8080'},
    },
  },
})

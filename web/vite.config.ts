import { readFileSync } from 'node:fs'
import { fileURLToPath } from 'node:url'
import { defineConfig } from 'vite'
import vue from '@vitejs/plugin-vue'
import Components from 'unplugin-vue-components/vite'
import { ElementPlusResolver } from 'unplugin-vue-components/resolvers'

const appVersion = readFileSync(fileURLToPath(new URL('../VERSION', import.meta.url)), 'utf8').trim()

export default defineConfig({
  plugins: [vue(), Components({
    dts: false,
    dirs: [],
    resolvers: [ElementPlusResolver()],
  })],
  define: {__APP_VERSION__: JSON.stringify(appVersion)},
  server: {
    port: 5173,
    proxy: {
      '/api': {target: 'http://127.0.0.1:8080', changeOrigin: false, ws:true},
      '/healthz': {target: 'http://127.0.0.1:8080'},
    },
  },
})

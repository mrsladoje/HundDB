import { defineConfig } from 'vite'
import { fileURLToPath } from "node:url";
import react from '@vitejs/plugin-react'
import path from 'path'

const __dirname = path.dirname(fileURLToPath(import.meta.url));

// https://vite.dev/config/
export default defineConfig({
  plugins: [react()],
  server: {
    host: '127.0.0.1',
    port: 5173,
    strictPort: true,
    hmr: {
      port: 5174
    }
  },
  resolve: {
    alias: {
      "@": path.resolve(__dirname, "./src"),
      '@wails': path.resolve(__dirname, './wailsjs/wailsjs/go').replace(/\\/g, '/'),
    },
  },
  clearScreen: false
})
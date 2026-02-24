import { defineConfig, loadEnv } from 'vite'
import vue from '@vitejs/plugin-vue'
import path from 'path'

const envDir = path.resolve(__dirname, '..')

export default defineConfig(({ mode }) => {
  const env = loadEnv(mode, envDir, 'VITE_')
  const devHost = env.VITE_DEV_HOST || process.env.VITE_DEV_HOST || '0.0.0.0'
  const devPort = Number(env.VITE_DEV_PORT || process.env.VITE_DEV_PORT || 3000)
  const apiProxyTarget =
    env.VITE_API_PROXY_TARGET ||
    process.env.VITE_API_PROXY_TARGET ||
    'http://localhost:8000'

  return {
    envDir,
    plugins: [vue()],
    resolve: {
      alias: {
        '@': path.resolve(__dirname, 'src')
      }
    },
    server: {
      host: devHost,
      port: Number.isFinite(devPort) ? devPort : 3000,
      proxy: {
        '/api': {
          target: apiProxyTarget,
          changeOrigin: true,
          ws: true,
          rewrite: (path) => path
        }
      }
    },
    build: {
      outDir: 'dist',
      sourcemap: false,
      chunkSizeWarningLimit: 1500
    }
  }
})


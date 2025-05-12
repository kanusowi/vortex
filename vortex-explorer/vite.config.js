import { defineConfig } from 'vite'
import react from '@vitejs/plugin-react'
// Removed incorrect Tailwind CSS import

// https://vite.dev/config/
export default defineConfig({
  plugins: [react()], // Removed Tailwind CSS plugin
  server: { 
    proxy: {
      // Proxy /api requests to the backend server
      '/api': {
        target: 'http://localhost:3000', // Assuming vortex-server runs here
        changeOrigin: true, // Recommended for virtual hosted sites
        rewrite: (path) => path.replace(/^\/api/, ''), // Remove /api prefix before forwarding
      },
    }
  }
})

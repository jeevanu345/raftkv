import { defineConfig } from "vite";
import react from "@vitejs/plugin-react";

export default defineConfig({
  plugins: [react()],
  server: {
    port: 5173,
    host: "127.0.0.1",
    // Preserve the browser-facing Host so the API can validate Origin.
    // Rewriting Host to :8080 while leaving Origin at :5173 rejects mutations.
    proxy: {
      "/api": {
        target: "http://127.0.0.1:8080",
        changeOrigin: false
      },
      "/health": {
        target: "http://127.0.0.1:8080",
        changeOrigin: false
      },
      "/metrics": {
        bypass(request) {if(request.headers.accept?.includes("text/html"))return "/index.html";},
        target: "http://127.0.0.1:8080",
        changeOrigin: false
      },
      "/lab": {
        target: "http://127.0.0.1:8090",
        changeOrigin: false
      }
    }
  }
});

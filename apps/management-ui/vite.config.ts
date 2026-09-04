import { fileURLToPath, URL } from "node:url";
import { defineConfig } from "vite";
import react from "@vitejs/plugin-react";
import tailwindcss from "@tailwindcss/vite";

export default defineConfig({
  plugins: [react(), tailwindcss()],
  resolve: { alias: { "@": fileURLToPath(new URL("./src/client", import.meta.url)) } },
  build: { outDir: "dist/client", emptyOutDir: false },
  server: { proxy: { "/ui-api": "http://127.0.0.1:8080", "/health": "http://127.0.0.1:8080" } }
});

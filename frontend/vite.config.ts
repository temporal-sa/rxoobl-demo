import { defineConfig } from "vite";
import react from "@vitejs/plugin-react";

export default defineConfig({
  // Vite serves the React console in development. The API stays separate on
  // port 8000 so the same frontend build can point at local FastAPI or a
  // deployed API by changing VITE_API_BASE_URL.
  plugins: [react()],
  server: {
    port: 5173,
  },
});

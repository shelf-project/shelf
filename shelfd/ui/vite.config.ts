import { defineConfig } from "vite";
import react from "@vitejs/plugin-react";

// Dev target: the shelfd admin port. The smoke harness
// (`benchmarks/smoke/docker-compose.yml`) exposes shelfd on 9090; the
// compiled binary defaults to the same. Override with `SHELFD_ORIGIN`
// (e.g. `SHELFD_ORIGIN=http://127.0.0.1:9091 pnpm dev`) if you run the
// daemon on a different port.
const shelfdOrigin = process.env.SHELFD_ORIGIN ?? "http://127.0.0.1:9090";

export default defineConfig({
  // The Rust side mounts the SPA at `/ui`; asset URLs must be rooted
  // there so `<script src="/ui/assets/...">` resolves correctly once
  // baked into the binary.
  base: "/ui/",
  plugins: [react()],
  server: {
    port: 5173,
    proxy: {
      "/stats": shelfdOrigin,
      "/metrics": shelfdOrigin,
      "/admin": shelfdOrigin,
      "/healthz": shelfdOrigin,
      "/readyz": shelfdOrigin,
    },
  },
  build: {
    // Write directly to the folder rust-embed reads from.
    outDir: "dist",
    emptyOutDir: true,
    // Keep the bundle greppable in `shelfd` images and easy to
    // eyeball in `du -sh`. ~60 KB gzipped is the target.
    sourcemap: false,
  },
});

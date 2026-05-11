# Nexus Analytics — BD Frontend

React + Vite + Tailwind v4 (CSS-only theme) + recharts + tanstack-query.
Two pages matching the BD requirements doc:

- `/bridge` — Bridge Flow Analytics
- `/spikes` — Spike Detection

Talks to the FastAPI service in `../api/`. Dev: Vite proxies `/api/*` to
`http://localhost:8000`. Prod: same-origin (nginx serves both static
bundle and reverse-proxied API).

## Develop

```bash
# Terminal 1: API
cd ..
PYTHONPATH=. python -m uvicorn api.main:app --port 8000

# Terminal 2: frontend
cd frontend
pnpm install
pnpm dev
# Open http://localhost:5173/
```

## Build for production

```bash
pnpm build
# Output → dist/  (rsync to /var/www on the VPS)
```

The bundle is ~650 kB raw / ~195 kB gzipped (one chunk for now; can be
code-split if it grows).

### Override API base

If the frontend is served from a different origin than the API, set
`VITE_API_BASE` at build time:

```bash
VITE_API_BASE=https://api.example.com pnpm build
```

Defaults to same-origin (empty string), so `/api/*` lands at whatever
serves the bundle.

## Layout

```
src/
  App.tsx            Router + query-client
  main.tsx           React entry
  index.css          Tailwind theme + globals
  lib/
    api.ts           Typed fetch client (mirrors api/queries.py)
    format.ts        Number / duration / address formatters
  components/
    Layout.tsx       Top nav + outlet
    Filters.tsx      Chain + time-window selectors
    ui.tsx           Card / Kpi / Section / Banner / Table / Loading / ErrorBox
  pages/
    BridgeFlow.tsx   /bridge — 7 queries, 4 KPIs, 3 charts, 4 tables
    Spikes.tsx       /spikes — 6 queries, 3 KPIs, 1 chart, 5 tables
```

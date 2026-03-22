# Workspace

## Overview

pnpm workspace monorepo using TypeScript. Each package manages its own dependencies.

## Stack

- **Monorepo tool**: pnpm workspaces
- **Node.js version**: 24
- **Package manager**: pnpm
- **TypeScript version**: 5.9
- **API framework**: Express 5
- **Database**: PostgreSQL + Drizzle ORM
- **Validation**: Zod (`zod/v4`), `drizzle-zod`
- **API codegen**: Orval (from OpenAPI spec)
- **Build**: esbuild (CJS bundle)

## Structure

```text
artifacts-monorepo/
├── artifacts/              # Deployable applications
│   └── api-server/         # Express API server
├── lib/                    # Shared libraries
│   ├── api-spec/           # OpenAPI spec + Orval codegen config
│   ├── api-client-react/   # Generated React Query hooks
│   ├── api-zod/            # Generated Zod schemas from OpenAPI
│   └── db/                 # Drizzle ORM schema + DB connection
├── scripts/                # Utility scripts (single workspace package)
│   └── src/                # Individual .ts scripts, run via `pnpm --filter @workspace/scripts run <script>`
├── pnpm-workspace.yaml     # pnpm workspace (artifacts/*, lib/*, lib/integrations/*, scripts)
├── tsconfig.base.json      # Shared TS options (composite, bundler resolution, es2022)
├── tsconfig.json           # Root TS project references
└── package.json            # Root package with hoisted devDeps
```

## TypeScript & Composite Projects

Every package extends `tsconfig.base.json` which sets `composite: true`. The root `tsconfig.json` lists all packages as project references. This means:

- **Always typecheck from the root** — run `pnpm run typecheck` (which runs `tsc --build --emitDeclarationOnly`). This builds the full dependency graph so that cross-package imports resolve correctly. Running `tsc` inside a single package will fail if its dependencies haven't been built yet.
- **`emitDeclarationOnly`** — we only emit `.d.ts` files during typecheck; actual JS bundling is handled by esbuild/tsx/vite...etc, not `tsc`.
- **Project references** — when package A depends on package B, A's `tsconfig.json` must list B in its `references` array. `tsc --build` uses this to determine build order and skip up-to-date packages.

## Root Scripts

- `pnpm run build` — runs `typecheck` first, then recursively runs `build` in all packages that define it
- `pnpm run typecheck` — runs `tsc --build --emitDeclarationOnly` using project references

## Packages

### `artifacts/api-server` (`@workspace/api-server`)

Express 5 API server. Routes live in `src/routes/` and use `@workspace/api-zod` for request and response validation and `@workspace/db` for persistence.

- Entry: `src/index.ts` — reads `PORT`, starts Express
- App setup: `src/app.ts` — mounts CORS, JSON/urlencoded parsing, routes at `/api`
- Routes: `src/routes/index.ts` mounts sub-routers; `src/routes/health.ts` exposes `GET /health` (full path: `/api/health`)
- Depends on: `@workspace/db`, `@workspace/api-zod`
- `pnpm --filter @workspace/api-server run dev` — run the dev server
- `pnpm --filter @workspace/api-server run build` — production esbuild bundle (`dist/index.cjs`)
- Build bundles an allowlist of deps (express, cors, pg, drizzle-orm, zod, etc.) and externalizes the rest

### `lib/db` (`@workspace/db`)

Database layer using Drizzle ORM with PostgreSQL. Exports a Drizzle client instance and schema models.

- `src/index.ts` — creates a `Pool` + Drizzle instance, exports schema
- `src/schema/index.ts` — barrel re-export of all models
- `src/schema/<modelname>.ts` — table definitions with `drizzle-zod` insert schemas (no models definitions exist right now)
- `drizzle.config.ts` — Drizzle Kit config (requires `DATABASE_URL`, automatically provided by Replit)
- Exports: `.` (pool, db, schema), `./schema` (schema only)

Production migrations are handled by Replit when publishing. In development, we just use `pnpm --filter @workspace/db run push`, and we fallback to `pnpm --filter @workspace/db run push-force`.

### `lib/api-spec` (`@workspace/api-spec`)

Owns the OpenAPI 3.1 spec (`openapi.yaml`) and the Orval config (`orval.config.ts`). Running codegen produces output into two sibling packages:

1. `lib/api-client-react/src/generated/` — React Query hooks + fetch client
2. `lib/api-zod/src/generated/` — Zod schemas

Run codegen: `pnpm --filter @workspace/api-spec run codegen`

### `lib/api-zod` (`@workspace/api-zod`)

Generated Zod schemas from the OpenAPI spec (e.g. `HealthCheckResponse`). Used by `api-server` for response validation.

### `lib/api-client-react` (`@workspace/api-client-react`)

Generated React Query hooks and fetch client from the OpenAPI spec (e.g. `useHealthCheck`, `healthCheck`).

### `scripts` (`@workspace/scripts`)

Utility scripts package. Each script is a `.ts` file in `src/` with a corresponding npm script in `package.json`. Run scripts via `pnpm --filter @workspace/scripts run <script>`. Scripts can import any workspace package (e.g., `@workspace/db`) by adding it as a dependency in `scripts/package.json`.

## Python Bots (standalone, separate from pnpm monorepo)

### Root-level Pocket Partners Stats Bot

- Entry: `main.py` — aiogram 3.x polling bot
- Config: `config.py` — reads `TELEGRAM_BOT_TOKEN`, `PP_LOGIN`, `PP_PASSWORD`, `ALLOWED_USER_IDS` from environment
- Routes: `bot/handlers.py` — /start, inline period buttons, custom range FSM
- Parser: `parser/pocket_parser.py` — Playwright-based scraper for pocketpartners.com dashboard
- Utils: `utils/date_parser.py` — date range parsing helpers
- Workflow: "Telegram Bot" → `python main.py`
- Requires secrets: `TELEGRAM_BOT_TOKEN`, `PP_LOGIN`, `PP_PASSWORD`, `ALLOWED_USER_IDS`

### `signal_bot/` — Pocket Option OTC Signal Bot

Separate standalone Python bot for OTC trading signals on Pocket Option.
Multi-user, admin-approved, supports 100+ concurrent users via candle cache.

- Entry: `signal_bot/main.py` — initializes SQLite DB, starts cache refresher + polling
- Config: `signal_bot/config.py` — reads `SIGNAL_BOT_TOKEN`, `ADMIN_USER_ID`, `PO_LOGIN`, `PO_PASSWORD`, `HEADLESS`
- Database: `signal_bot/db/database.py` — SQLite (aiosqlite), tables: `users`, `signal_outcomes`
- Services:
  - `services/pocket_browser.py` — Playwright login to pocketoption.com; candle collection via WebSocket intercept (binary `updateHistoryNewFast` frames); also captures auth credentials for direct WS client. WS auth saved to `po_ws_auth.json`.
  - `services/po_ws_client.py` — Direct Socket.IO v4 WebSocket client (aiohttp, no browser). Uses auth from `po_ws_auth.json`. Fetches all 10 OTC pairs in one connection in ~3 seconds via `changeSymbol` + `ps` cycling. Background reader task feeds asyncio.Queue; never cancels `ws.receive()`. Used for fast periodic refresh cycles.
  - `services/candle_cache.py` — In-memory cache (TTL=55s, interval=45s). Warm-up: all 10 pairs via browser (~3-4 min, 43-50 candles each). Refresh cycles: WS client (~3 sec, 13-14 new candles) merged with existing cache by timestamp (preserves full history). Browser fallback if WS returns nothing.
  - `services/analysis/scoring_engine.py` — Price action analysis engine v5. Candles → pandas DataFrame. Regime detection, trend analysis, PA patterns, indicator confluence (RSI, EMA, Stochastic, BB, MACD). Confidence = PA×0.60+candles×0.15+regime×0.10+levels×0.08+indicators×0.07. Thresholds: strong≥70, moderate≥58.
  - `services/signal_service.py` — Serves from cache first; falls through to live browser fetch on cache miss.
  - `services/outcome_tracker.py` — Tracks signal WIN/LOSS. Waits for candle expiry, fetches close price, notifies user.
  - `services/access_service.py` — user registration + admin notification
- Bot: `signal_bot/bot/handlers.py` + `keyboards.py` — FSM flow, admin commands (/approve, /deny, /users, /pending, /broadcast), /stats (winrate per strategy)
- Data files: `po_cookies.json` (saved browser cookies), `po_ws_auth.json` (captured WS auth: session + uid)
- Workflow: "Signal Bot" → `python3 signal_bot/main.py`
- Requires secrets: `SIGNAL_BOT_TOKEN`, `ADMIN_USER_ID`, `PO_LOGIN`, `PO_PASSWORD`

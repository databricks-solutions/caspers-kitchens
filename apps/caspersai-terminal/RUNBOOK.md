# Databricks App Terminal Runbook

## Purpose

Operational guide for deploying, validating, and troubleshooting Databricks App Terminal.

## Deploy

```bash
npm run build
databricks apps deploy --profile <PROFILE>
```

Example:

```bash
databricks apps deploy --profile SHARED
```

## Check app status

```bash
databricks apps get databricks-app-terminal --profile <PROFILE> -o json
```

Expected status:
- `app_status.state = RUNNING`
- `active_deployment.status.state = SUCCEEDED`

## Runtime health checks

- `GET /health` -> process/liveness
- `GET /ready` -> readiness (shell + PTY + required services)
- `GET /api/runtime/diagnostics` -> detailed checks and session/runtime stats

## Verify discovered session types

```bash
curl -sS "$APP_URL/api/session-types"
```

Expected:
- includes built-in `terminal`
- includes any valid entries under `terminal-types/*`
- `+` in the UI opens a centered in-terminal launcher showing these types
- first launch with zero sessions also opens the same launcher before any backend session is created
- launcher supports `?` (help panel) and `a` (about panel)

## Verify terminal identity behavior

From a terminal session inside the app:

```bash
databricks auth env
```

This should show Databricks auth context for the app runtime (service principal M2M).

## Verify user session mode

In the UI, click a tab's auth badge from `m2m` to `user`, then run:

```bash
dbx-auth
databricks auth env
```

Expected:
- `dbx-auth` prints `user`
- Databricks CLI resolves auth via `DATABRICKS_HOST` + `DATABRICKS_TOKEN`

To switch in-shell:

```bash
dbx-auth m2m
dbx-auth user
```

If switching to user fails with `USER_ACCESS_TOKEN_MISSING`, confirm app ingress forwards `x-forwarded-access-token` (or set `USER_ACCESS_TOKEN_HEADER`).

## Writable paths and global npm installs

`npm install -g ...` in terminal sessions is redirected to writable paths:

- prefix: `DBX_APP_TERMINAL_NPM_PREFIX`
- cache: `DBX_APP_TERMINAL_NPM_CACHE`
- PATH includes `<prefix>/bin`

If users report global install permission errors, verify these env vars in a session:

```bash
echo "$DBX_APP_TERMINAL_NPM_PREFIX"
echo "$DBX_APP_TERMINAL_NPM_CACHE"
```

## Session cwd safety

Default session cwd is controlled by:

- `SESSION_DEFAULT_CWD`

Recommended Databricks default:
- `/app/python`

This avoids opening directly in `/app/python/source_code` unless explicitly requested.

## Common failures

### 1) `/ready` returns 503
- Call `/api/runtime/diagnostics`
- If shell check fails: verify `SHELL`
- If PTY check fails: verify `node-pty` compatibility in runtime image

### 2) `SESSION_SPAWN_FAILED`
- Usually shell/PTY/runtime compatibility issue
- Validate diagnostics and shell path

### 3) Terminal disconnects
- Check websocket path correctness (`sessionId` present)
- Check for `ws.backpressure_close` in logs

## Logging

Structured logs are emitted to stdout and optional `LOG_PATH`.

Useful events:
- `runtime.startup`
- `server.listen`
- `http.request`
- `api.error`
- `session.create`, `session.exit`, `session.kill`
- `ws.connect`, `ws.disconnect`, `ws.backpressure_close`

## Local smoke test

With local server running:

```bash
npm run smoke
```

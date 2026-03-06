# Databricks App Terminal - Project Guide

This file defines implementation boundaries and contributor rules for this repository.

## Product definition

This project is a Databricks App web terminal.

Core behavior:
- multi-session browser terminal
- PTY-backed shell sessions
- session lifecycle API + websocket streaming
- runs in Databricks App runtime and uses app identity context

## Hard boundaries

Keep this repository focused on terminal runtime concerns.

Do include:
- terminal UI/UX
- PTY session lifecycle and websocket transport
- runtime diagnostics, health/readiness
- operational safety defaults for Databricks runtime

Do not include by default:
- notebook automation/tooling
- browser automation tooling
- tracing/observability vendor lock-in code
- product-specific agent orchestration logic

## Runtime assumptions

In Databricks Apps:
- commands run under app service principal context
- Databricks CLI is available in runtime
- M2M auth context is provided by platform

## Key implementation conventions

- Session IDs are UUIDv7.
- Default session cwd should be safe (`/app/python` in Databricks runtime unless overridden).
- `npm install -g` in sessions must remain writable/non-root via redirected prefix/cache env.
- API errors must use structured envelope from `src/api/types.ts`.
- Websocket and HTTP behavior should remain stable for current routes.

## UI conventions

- Tabs are always visible.
- New tab shortcut: Cmd+T / Ctrl+T.
- Terminal must auto-focus on active/new session.
- Layout should keep terminal area stable while tabs are added.

## Before merging changes

Run:

```bash
npm run check
npm test
npm run build
```

If deploy testing is needed:

```bash
databricks apps deploy --profile <PROFILE>
```

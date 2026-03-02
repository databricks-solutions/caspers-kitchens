# Terminal types

Session types are discovered at startup from this directory.

Current profiles in this repo:
- `claude`
- `codex`
- `pi`

Each custom type must live in its own folder:

```text
terminal-types/<type-id>/
  type.json
  launch.sh
```

## `type.json`

```json
{
  "id": "claude",
  "name": "Claude Code",
  "description": "Launch Claude Code in the terminal",
  "badge": "claude",
  "icon": "✶",
  "authPolicy": "both",
  "default": false,
  "order": 20,
  "entrypoint": "launch.sh"
}
```

Fields:
- `id` (optional): type id (`[a-z0-9][a-z0-9-_]{0,63}`), defaults to folder name
- `name` (required): display name
- `description` (optional): short description
- `badge` (optional): short tab badge label
- `icon` (optional): short icon/logo string (e.g. unicode glyph) used in TUI picker and tab badge
  - can be a private-use glyph when backed by a bundled icon font
- `authPolicy` (optional): auth-mode policy for sessions of this type
  - `both` (default): users can toggle between `m2m` and `user`
  - `user`: pinned to `user` mode (toggle disabled)
  - `m2m`: pinned to `m2m` mode (toggle disabled)
- `default` (optional): marks this type as the default session type
  - if multiple types set `default: true`, the app selects one by ordering rules and logs a warning
  - if omitted for all custom types, built-in `terminal` remains default
- `order` (optional): integer ordering hint for picker/tab ordering
  - lower values appear first
  - ordered types appear before unordered types
  - tie-breakers: `default` then name
- `entrypoint` (optional): launch script path relative to type folder, default `launch.sh`

## `launch.sh`

`launch.sh` is sourced by the base shell startup sequence once, at session boot.

- Use it to set env vars and/or `exec` a CLI.
- If it returns without `exec`, the user stays in a regular shell.
- Session auth mode (`m2m` / `user`) and Databricks auth env handling are already provided by core runtime.
- Shared helpers for Databricks-backed agents live in `terminal-types/_shared/agent-bootstrap.sh` and `terminal-types/_shared/get-token.sh`.
- Keep provider-specific setup in the type's own `launch.sh` for true drop-in extensions.

## Databricks-backed agent launchers

The built-in `claude`, `codex`, and `pi` terminal types source shared helper logic from `terminal-types/_shared/`.

- `agent-bootstrap.sh` provides generic bootstrap primitives (host normalization, OAuth checks, `.databrickscfg`, token exchange/cache).
- `get-token.sh` performs service-principal OAuth token exchange (with `DATABRICKS_TOKEN` fallback).
- Provider-specific CLI config generation lives inside each type's own `launch.sh`.

Launcher env knobs:

- `DBX_APP_TERMINAL_CLAUDE_CMD` (default `claude`)
- `DBX_APP_TERMINAL_CODEX_CMD` (default `codex`)
- `DBX_APP_TERMINAL_PI_CMD` (default `pi`)
- `DBX_APP_TERMINAL_CLAUDE_MODEL` (optional Claude model override)
- `DBX_APP_TERMINAL_CODEX_MODEL` (default `databricks-gpt-5-3-codex`)
- `DBX_APP_TERMINAL_PI_FOOTER_EXTENSION` (optional path override; default `./terminal-types/pi/extensions/top-footer-line/index.ts`)

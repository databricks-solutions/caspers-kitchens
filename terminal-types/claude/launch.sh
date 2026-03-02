#!/usr/bin/env bash
# shellcheck shell=bash

__dbx_terminal_type_name="claude"
__dbx_terminal_type_cmd="${DBX_APP_TERMINAL_CLAUDE_CMD:-claude}"
__dbx_terminal_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
__dbx_terminal_shared_dir="${__dbx_terminal_root}/terminal-types/_shared"

dbx_claude_write_settings() {
  local host_url="$1"
  local token="$2"
  local project_dir="$3"

  mkdir -p "${HOME}/.claude"

  cat > "${HOME}/.claude/settings.json" << SETTINGS
{
  "env": {
    "ANTHROPIC_BASE_URL": "${host_url}/serving-endpoints/anthropic",
    "ANTHROPIC_AUTH_TOKEN": "${token}",
    "ANTHROPIC_DEFAULT_OPUS_MODEL": "databricks-claude-opus-4-6",
    "ANTHROPIC_DEFAULT_SONNET_MODEL": "databricks-claude-sonnet-4-5",
    "ANTHROPIC_DEFAULT_HAIKU_MODEL": "databricks-claude-haiku-4-5",
    "ANTHROPIC_CUSTOM_HEADERS": "x-databricks-use-coding-agent-mode: true",
    "CLAUDE_CODE_DISABLE_EXPERIMENTAL_BETAS": "1"
  },
  "permissions": {
    "allow": [
      "Bash(*)",
      "Read",
      "Edit",
      "Write",
      "Glob",
      "Grep",
      "WebFetch",
      "WebSearch"
    ],
    "deny": []
  }
}
SETTINGS
  chmod 600 "${HOME}/.claude/settings.json"

  cat > "${HOME}/.claude.json" << CLSTATE
{
  "hasCompletedOnboarding": true,
  "projects": {
    "${project_dir}": {
      "allowedTools": [],
      "hasTrustDialogAccepted": true
    }
  }
}
CLSTATE
}

# shellcheck source=../_shared/agent-bootstrap.sh
source "${__dbx_terminal_shared_dir}/agent-bootstrap.sh"

dbx_agent_home
dbx_agent_add_node_path "${__dbx_terminal_root}"
if ! dbx_agent_require_oauth_env; then
  printf '\n[session-type:%s] oauth env bootstrap failed. staying in shell.\n\n' "$__dbx_terminal_type_name"
  return 0
fi

if ! __dbx_host_url="$(dbx_agent_host_url)"; then
  printf '\n[session-type:%s] host bootstrap failed. staying in shell.\n\n' "$__dbx_terminal_type_name"
  return 0
fi

if ! dbx_agent_write_databrickscfg "${__dbx_host_url}"; then
  printf '\n[session-type:%s] databrickscfg bootstrap failed. staying in shell.\n\n' "$__dbx_terminal_type_name"
  return 0
fi

__dbx_bearer_token="$(dbx_agent_exchange_token "${__dbx_terminal_shared_dir}" || true)"
if [[ -z "${__dbx_bearer_token}" ]]; then
  __dbx_bearer_token="$(dbx_agent_read_token_file)"
fi
if [[ -z "${__dbx_bearer_token}" ]]; then
  printf '\n[session-type:%s] token bootstrap failed. staying in shell.\n\n' "$__dbx_terminal_type_name"
  return 0
fi

dbx_agent_write_token_file "${__dbx_bearer_token}"
dbx_claude_write_settings "${__dbx_host_url}" "${__dbx_bearer_token}" "$(pwd)"

if [[ "${DBX_APP_TERMINAL_TYPE_NO_AUTO_EXEC:-0}" != "1" ]] && command -v "$__dbx_terminal_type_cmd" >/dev/null 2>&1; then
  if [[ -n "${DBX_APP_TERMINAL_CLAUDE_MODEL:-}" ]]; then
    export ANTHROPIC_MODEL="${DBX_APP_TERMINAL_CLAUDE_MODEL}"
  fi
  exec "$__dbx_terminal_type_cmd"
fi

printf '\n[session-type:%s] CLI "%s" was not auto-started.\n' "$__dbx_terminal_type_name" "$__dbx_terminal_type_cmd"
printf '[session-type:%s] Staying in shell. Run `%s` when ready.\n\n' "$__dbx_terminal_type_name" "$__dbx_terminal_type_cmd"

unset __dbx_terminal_type_name __dbx_terminal_type_cmd __dbx_terminal_root __dbx_terminal_shared_dir __dbx_host_url __dbx_bearer_token

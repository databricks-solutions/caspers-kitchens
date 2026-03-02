#!/usr/bin/env bash
# shellcheck shell=bash

__dbx_terminal_type_name="codex"
__dbx_terminal_type_cmd="${DBX_APP_TERMINAL_CODEX_CMD:-codex}"
__dbx_terminal_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
__dbx_terminal_shared_dir="${__dbx_terminal_root}/terminal-types/_shared"
__dbx_codex_model="${DBX_APP_TERMINAL_CODEX_MODEL:-databricks-gpt-5-3-codex}"

dbx_codex_write_config() {
  local host_url="$1"
  local default_model="$2"

  mkdir -p "${HOME}/.codex"

  cat > "${HOME}/.codex/config.toml" << CODEXCFG
profile = "default"
# Databricks OpenAI proxy currently rejects web_search tool calls.
web_search = "disabled"

[profiles.default]
model_provider = "proxy"
model = "${default_model}"
model_catalog_json = "${HOME}/.codex/databricks-models.json"

[model_providers.proxy]
name = "Databricks Proxy"
base_url = "${host_url}/serving-endpoints"
env_key = "DATABRICKS_TOKEN"
wire_api = "responses"
CODEXCFG

  cat > "${HOME}/.codex/databricks-models.json" << 'MODELS'
{
  "models": [
    {
      "slug": "databricks-gpt-5-3-codex",
      "display_name": "databricks-gpt-5-3-codex",
      "description": "GPT-5.3 Codex via Databricks Model Serving",
      "context_window": 272000,
      "supported_in_api": true,
      "priority": 0,
      "available_in_plans": ["enterprise"],
      "supports_reasoning_summaries": true,
      "support_verbosity": true,
      "default_verbosity": "low",
      "default_reasoning_level": "medium",
      "supported_reasoning_levels": [
        {"effort": "low", "description": "Fast responses with lighter reasoning"},
        {"effort": "medium", "description": "Balances speed and reasoning depth"},
        {"effort": "high", "description": "Greater reasoning depth for complex problems"},
        {"effort": "xhigh", "description": "Maximum reasoning depth"}
      ],
      "input_modalities": ["text", "image"],
      "supports_parallel_tool_calls": true,
      "prefer_websockets": false,
      "apply_patch_tool_type": "freeform",
      "truncation_policy": {"mode": "tokens", "limit": 10000},
      "reasoning_summary_format": "experimental",
      "shell_type": "shell_command",
      "visibility": "list",
      "minimal_client_version": "0.98.0",
      "upgrade": null,
      "base_instructions": "",
      "model_messages": null,
      "experimental_supported_tools": []
    },
    {
      "slug": "databricks-gpt-5-2",
      "display_name": "databricks-gpt-5-2",
      "description": "GPT-5.2 via Databricks Model Serving",
      "context_window": 272000,
      "supported_in_api": true,
      "priority": 1,
      "available_in_plans": ["enterprise"],
      "supports_reasoning_summaries": true,
      "support_verbosity": true,
      "default_verbosity": "low",
      "default_reasoning_level": "medium",
      "supported_reasoning_levels": [
        {"effort": "low", "description": "Fast responses with lighter reasoning"},
        {"effort": "medium", "description": "Balances speed and reasoning depth"},
        {"effort": "high", "description": "Greater reasoning depth for complex problems"},
        {"effort": "xhigh", "description": "Maximum reasoning depth"}
      ],
      "input_modalities": ["text", "image"],
      "supports_parallel_tool_calls": true,
      "prefer_websockets": false,
      "apply_patch_tool_type": "freeform",
      "truncation_policy": {"mode": "tokens", "limit": 10000},
      "reasoning_summary_format": "experimental",
      "shell_type": "shell_command",
      "visibility": "list",
      "minimal_client_version": "0.98.0",
      "upgrade": null,
      "base_instructions": "",
      "model_messages": null,
      "experimental_supported_tools": []
    }
  ]
}
MODELS
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
dbx_codex_write_config "${__dbx_host_url}" "${__dbx_codex_model}"
export DATABRICKS_TOKEN="${__dbx_bearer_token}"
export DATABRICKS_AUTH_TYPE="pat"
unset DATABRICKS_CLIENT_ID DATABRICKS_CLIENT_SECRET

__dbx_codex_launched=0
if [[ "${DBX_APP_TERMINAL_TYPE_NO_AUTO_EXEC:-0}" != "1" ]] && command -v "$__dbx_terminal_type_cmd" >/dev/null 2>&1; then
  __dbx_codex_launched=1
  if "$__dbx_terminal_type_cmd"; then
    :
  else
    __dbx_codex_exit_code="$?"
    printf '\n[session-type:%s] CLI exited with status %s. Staying in shell.\n\n' \
      "$__dbx_terminal_type_name" "$__dbx_codex_exit_code"
    unset __dbx_codex_exit_code
  fi
fi

if [[ "$__dbx_codex_launched" == "0" ]]; then
  printf '\n[session-type:%s] CLI "%s" was not auto-started.\n' "$__dbx_terminal_type_name" "$__dbx_terminal_type_cmd"
  printf '[session-type:%s] Staying in shell. Run `%s` when ready.\n\n' "$__dbx_terminal_type_name" "$__dbx_terminal_type_cmd"
fi

unset __dbx_terminal_type_name __dbx_terminal_type_cmd __dbx_terminal_root __dbx_terminal_shared_dir __dbx_host_url __dbx_bearer_token __dbx_codex_model __dbx_codex_launched

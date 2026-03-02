#!/usr/bin/env bash
# shellcheck shell=bash

__dbx_terminal_type_name="pi"
__dbx_terminal_type_cmd="${DBX_APP_TERMINAL_PI_CMD:-pi}"
__dbx_terminal_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
__dbx_terminal_shared_dir="${__dbx_terminal_root}/terminal-types/_shared"
__dbx_pi_footer_extension="${DBX_APP_TERMINAL_PI_FOOTER_EXTENSION:-${__dbx_terminal_root}/terminal-types/pi/extensions/top-footer-line/index.ts}"

dbx_pi_enable_databricks_xhigh() {
  local models_js="${__dbx_terminal_root}/node_modules/@mariozechner/pi-ai/dist/models.js"
  [[ -f "${models_js}" ]] || return 0

  if grep -q 'model.id.includes("gpt-5-3")' "${models_js}"; then
    return 0
  fi

  DBX_PI_MODELS_JS="${models_js}" node <<'NODE'
const fs = require("node:fs");

const path = process.env.DBX_PI_MODELS_JS;
const before = 'if (model.id.includes("gpt-5.2") || model.id.includes("gpt-5.3")) {';
const after =
  'if (model.id.includes("gpt-5.2") || model.id.includes("gpt-5.3") || model.id.includes("gpt-5-2") || model.id.includes("gpt-5-3")) {';

let source = fs.readFileSync(path, "utf8");
if (source.includes(after)) process.exit(0);
if (!source.includes(before)) process.exit(3);
source = source.replace(before, after);
fs.writeFileSync(path, source);
NODE

  local patch_rc=$?
  if [[ $patch_rc -ne 0 ]]; then
    printf '[session-type:%s] warning: could not enable xhigh alias for Databricks GPT models (rc=%s).\n' "$__dbx_terminal_type_name" "$patch_rc"
  fi
}

dbx_pi_fix_long_toolcall_ids() {
  local responses_shared_js="${__dbx_terminal_root}/node_modules/@mariozechner/pi-ai/dist/providers/openai-responses-shared.js"
  [[ -f "${responses_shared_js}" ]] || return 0

  if grep -q 'if (itemId && itemId.length > 64)' "${responses_shared_js}"; then
    return 0
  fi

  DBX_PI_RESPONSES_SHARED_JS="${responses_shared_js}" node <<'NODE'
const fs = require("node:fs");

const path = process.env.DBX_PI_RESPONSES_SHARED_JS;
const before = `if (isDifferentModel && itemId?.startsWith("fc_")) {
                        itemId = undefined;
                    }`;
const after = `${before}
                    if (itemId && itemId.length > 64) {
                        itemId = undefined;
                    }`;

let source = fs.readFileSync(path, "utf8");
if (source.includes(after)) process.exit(0);
if (!source.includes(before)) process.exit(3);
source = source.replace(before, after);
fs.writeFileSync(path, source);
NODE

  local patch_rc=$?
  if [[ $patch_rc -ne 0 ]]; then
    printf '[session-type:%s] warning: could not patch long tool-call ids for Responses API (rc=%s).\n' "$__dbx_terminal_type_name" "$patch_rc"
  fi
}

dbx_pi_write_models_config() {
  local host_url="$1"

  mkdir -p "${HOME}/.pi/agent"

  cat > "${HOME}/.pi/agent/models.json" << MODELS
{
  "providers": {
    "databricks": {
      "baseUrl": "${host_url}/serving-endpoints",
      "api": "openai-completions",
      "apiKey": "DATABRICKS_TOKEN",
      "models": [
        {
          "id": "databricks-claude-sonnet-4-6",
          "name": "databricks-claude-sonnet-4-6",
          "reasoning": true,
          "input": ["text", "image"],
          "cost": { "input": 0, "output": 0, "cacheRead": 0, "cacheWrite": 0 },
          "contextWindow": 128000,
          "maxTokens": 8192,
          "compat": {
            "maxTokensField": "max_tokens",
            "supportsStore": false,
            "supportsDeveloperRole": false,
            "supportsReasoningEffort": false,
            "supportsUsageInStreaming": false
          }
        },
        {
          "id": "databricks-claude-sonnet-4-5",
          "name": "databricks-claude-sonnet-4-5",
          "reasoning": true,
          "input": ["text", "image"],
          "cost": { "input": 0, "output": 0, "cacheRead": 0, "cacheWrite": 0 },
          "contextWindow": 128000,
          "maxTokens": 8192,
          "compat": {
            "maxTokensField": "max_tokens",
            "supportsStore": false,
            "supportsDeveloperRole": false,
            "supportsReasoningEffort": false,
            "supportsUsageInStreaming": false
          }
        },
        {
          "id": "databricks-claude-opus-4-6",
          "name": "databricks-claude-opus-4-6",
          "reasoning": true,
          "input": ["text", "image"],
          "cost": { "input": 0, "output": 0, "cacheRead": 0, "cacheWrite": 0 },
          "contextWindow": 128000,
          "maxTokens": 8192,
          "compat": {
            "maxTokensField": "max_tokens",
            "supportsStore": false,
            "supportsDeveloperRole": false,
            "supportsReasoningEffort": false,
            "supportsUsageInStreaming": false
          }
        },
        {
          "id": "databricks-claude-opus-4-5",
          "name": "databricks-claude-opus-4-5",
          "reasoning": true,
          "input": ["text", "image"],
          "cost": { "input": 0, "output": 0, "cacheRead": 0, "cacheWrite": 0 },
          "contextWindow": 128000,
          "maxTokens": 8192,
          "compat": {
            "maxTokensField": "max_tokens",
            "supportsStore": false,
            "supportsDeveloperRole": false,
            "supportsReasoningEffort": false,
            "supportsUsageInStreaming": false
          }
        },
        {
          "id": "databricks-gpt-5-3-codex",
          "name": "databricks-gpt-5-3-codex",
          "api": "openai-responses",
          "reasoning": true,
          "input": ["text", "image"],
          "cost": { "input": 0, "output": 0, "cacheRead": 0, "cacheWrite": 0 },
          "contextWindow": 272000,
          "maxTokens": 10000
        },
        {
          "id": "databricks-gpt-5-2-codex",
          "name": "databricks-gpt-5-2-codex",
          "api": "openai-responses",
          "reasoning": true,
          "input": ["text", "image"],
          "cost": { "input": 0, "output": 0, "cacheRead": 0, "cacheWrite": 0 },
          "contextWindow": 272000,
          "maxTokens": 10000
        },
        {
          "id": "databricks-gpt-5-1-codex-max",
          "name": "databricks-gpt-5-1-codex-max",
          "api": "openai-responses",
          "reasoning": true,
          "input": ["text", "image"],
          "cost": { "input": 0, "output": 0, "cacheRead": 0, "cacheWrite": 0 },
          "contextWindow": 272000,
          "maxTokens": 10000
        },
        {
          "id": "databricks-gpt-5-1-codex-mini",
          "name": "databricks-gpt-5-1-codex-mini",
          "api": "openai-responses",
          "reasoning": true,
          "input": ["text", "image"],
          "cost": { "input": 0, "output": 0, "cacheRead": 0, "cacheWrite": 0 },
          "contextWindow": 272000,
          "maxTokens": 10000
        }
      ]
    }
  }
}
MODELS

  chmod 600 "${HOME}/.pi/agent/models.json"
}

# shellcheck source=../_shared/agent-bootstrap.sh
source "${__dbx_terminal_shared_dir}/agent-bootstrap.sh"

dbx_agent_home
dbx_agent_add_node_path "${__dbx_terminal_root}"
dbx_pi_enable_databricks_xhigh
dbx_pi_fix_long_toolcall_ids
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
dbx_pi_write_models_config "${__dbx_host_url}"
export DATABRICKS_TOKEN="${__dbx_bearer_token}"
export DATABRICKS_AUTH_TYPE="pat"
unset DATABRICKS_CLIENT_ID DATABRICKS_CLIENT_SECRET

if [[ "${DBX_APP_TERMINAL_TYPE_NO_AUTO_EXEC:-0}" != "1" ]] && command -v "$__dbx_terminal_type_cmd" >/dev/null 2>&1; then
  if [[ -f "${__dbx_pi_footer_extension}" ]]; then
    exec "$__dbx_terminal_type_cmd" -e "${__dbx_pi_footer_extension}"
  fi

  exec "$__dbx_terminal_type_cmd"
fi

printf '\n[session-type:%s] CLI "%s" was not auto-started.\n' "$__dbx_terminal_type_name" "$__dbx_terminal_type_cmd"
printf '[session-type:%s] Staying in shell. Run `%s` when ready.\n\n' "$__dbx_terminal_type_name" "$__dbx_terminal_type_cmd"

unset __dbx_terminal_type_name __dbx_terminal_type_cmd __dbx_terminal_root __dbx_terminal_shared_dir __dbx_host_url __dbx_bearer_token __dbx_pi_footer_extension

#!/usr/bin/env bash
# shellcheck shell=bash

dbx_agent_log() {
  printf '[agent-bootstrap] %s\n' "$*"
}

dbx_agent_die() {
  dbx_agent_log "ERROR: $*"
  return 1
}

dbx_agent_normalize_host() {
  local raw="${1:-}"
  raw="${raw#https://}"
  raw="${raw#http://}"
  raw="${raw%%/*}"
  printf '%s' "$raw"
}

dbx_agent_home() {
  export HOME="${HOME:-/home/app}"
  mkdir -p "$HOME"
}

dbx_agent_host_name() {
  local raw="${DATABRICKS_HOST:-${DATABRICKS_SERVER_HOSTNAME:-}}"
  dbx_agent_normalize_host "$raw"
}

dbx_agent_host_url() {
  local host
  host="$(dbx_agent_host_name)"
  [[ -n "$host" ]] || return 1
  printf 'https://%s' "$host"
}

dbx_agent_require_oauth_env() {
  [[ -n "$(dbx_agent_host_name)" ]] || dbx_agent_die "Missing DATABRICKS_HOST (or DATABRICKS_SERVER_HOSTNAME)"
  [[ -n "${DATABRICKS_CLIENT_ID:-}" ]] || dbx_agent_die "Missing DATABRICKS_CLIENT_ID"
  [[ -n "${DATABRICKS_CLIENT_SECRET:-}" ]] || dbx_agent_die "Missing DATABRICKS_CLIENT_SECRET"
}

dbx_agent_add_node_path() {
  local root="$1"
  local app_bin="${root}/node_modules/.bin"
  if [[ -d "$app_bin" ]]; then
    export PATH="${app_bin}:${PATH}"
  fi
}

dbx_agent_write_databrickscfg() {
  local url="$1"
  cat > "${HOME}/.databrickscfg" << DBCFG
[DEFAULT]
host = ${url}
client_id = ${DATABRICKS_CLIENT_ID}
client_secret = ${DATABRICKS_CLIENT_SECRET}

[sandbox]
host = ${url}
client_id = ${DATABRICKS_CLIENT_ID}
client_secret = ${DATABRICKS_CLIENT_SECRET}
DBCFG
  chmod 600 "${HOME}/.databrickscfg"
}

dbx_agent_exchange_token() {
  local shared_dir="$1"
  local host_name
  host_name="$(dbx_agent_host_name)"
  DATABRICKS_HOST="${host_name}" bash "${shared_dir}/get-token.sh"
}

dbx_agent_write_token_file() {
  local token="$1"
  printf '%s' "$token" > "${HOME}/.dbx_bearer_token"
  chmod 600 "${HOME}/.dbx_bearer_token"
}

dbx_agent_read_token_file() {
  if [[ -f "${HOME}/.dbx_bearer_token" ]]; then
    tr -d '\r\n' < "${HOME}/.dbx_bearer_token"
  fi
}

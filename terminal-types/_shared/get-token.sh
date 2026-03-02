#!/usr/bin/env bash
# OAuth token exchange for Databricks App service principal.
# Falls back to DATABRICKS_TOKEN only when OAuth vars are unavailable.
set -euo pipefail

normalize_host() {
  local raw="${1:-}"
  raw="${raw#https://}"
  raw="${raw#http://}"
  raw="${raw%%/*}"
  printf '%s' "$raw"
}

DBX_HOST_RAW="${DATABRICKS_HOST:-${DATABRICKS_SERVER_HOSTNAME:-}}"
DBX_HOST="$(normalize_host "$DBX_HOST_RAW")"

if [[ -n "${DATABRICKS_CLIENT_ID:-}" && -n "${DATABRICKS_CLIENT_SECRET:-}" ]]; then
  if [[ -z "$DBX_HOST" ]]; then
    echo "get-token: missing DATABRICKS_HOST (or DATABRICKS_SERVER_HOSTNAME)" >&2
    exit 1
  fi

  response="$(
    curl -sS --fail-with-body --request POST \
      --url "https://${DBX_HOST}/oidc/v1/token" \
      --user "${DATABRICKS_CLIENT_ID}:${DATABRICKS_CLIENT_SECRET}" \
      --data 'grant_type=client_credentials&scope=all-apis'
  )"

  token="$(printf '%s' "$response" | python3 -c "import json,sys; print(json.load(sys.stdin).get('access_token',''))")"
  if [[ -z "$token" ]]; then
    echo "get-token: token response did not contain access_token" >&2
    exit 1
  fi
  printf '%s\n' "$token"
  exit 0
fi

if [[ -n "${DATABRICKS_TOKEN:-}" ]]; then
  printf '%s\n' "${DATABRICKS_TOKEN}"
  exit 0
fi

echo "get-token: missing OAuth client credentials and DATABRICKS_TOKEN fallback" >&2
exit 1

#!/usr/bin/env bash
set -euo pipefail

BASE_URL="${BASE_URL:-http://127.0.0.1:${PORT:-8080}}"

extract_field() {
  local field_path="$1"
  node -e '
const fieldPath = process.argv[1].split(".");
let raw = "";
process.stdin.on("data", (chunk) => { raw += chunk.toString("utf8"); });
process.stdin.on("end", () => {
  const data = JSON.parse(raw);
  let value = data;
  for (const part of fieldPath) {
    value = value?.[part];
  }
  if (value === undefined) {
    process.exit(2);
  }
  process.stdout.write(String(value));
});
' "$field_path"
}

echo "[smoke] BASE_URL=${BASE_URL}"

echo "[smoke] GET /health"
health_json="$(curl -fsS "${BASE_URL}/health")"
echo "${health_json}" >/dev/null

echo "[smoke] GET /ready"
ready_http_code="$(curl -sS -o /tmp/databricks-app-terminal-ready.json -w '%{http_code}' "${BASE_URL}/ready")"
if [[ "${ready_http_code}" != "200" ]]; then
  echo "[smoke] /ready returned ${ready_http_code}" >&2
  printf '%s\n' "$(</tmp/databricks-app-terminal-ready.json)" >&2
  echo "[smoke] diagnostics:" >&2
  curl -sS "${BASE_URL}/api/runtime/diagnostics" >&2 || true
  exit 1
fi

echo "[smoke] POST /api/sessions"
create_json="$(curl -fsS -X POST "${BASE_URL}/api/sessions" -H 'content-type: application/json' -d '{}')"
session_id="$(printf '%s' "${create_json}" | extract_field 'data.session.sessionId')"

echo "[smoke] session_id=${session_id}"

echo "[smoke] POST /api/sessions/:id/attach"
curl -fsS -X POST "${BASE_URL}/api/sessions/${session_id}/attach" -H 'content-type: application/json' -d '{}' >/dev/null

echo "[smoke] POST /api/sessions/:id/input"
curl -fsS -X POST "${BASE_URL}/api/sessions/${session_id}/input" -H 'content-type: application/json' -d '{"data":"echo smoke\r"}' >/dev/null

echo "[smoke] POST /api/sessions/:id/resize"
curl -fsS -X POST "${BASE_URL}/api/sessions/${session_id}/resize" -H 'content-type: application/json' -d '{"cols":100,"rows":35}' >/dev/null

echo "[smoke] GET /api/sessions"
list_json="$(curl -fsS "${BASE_URL}/api/sessions")"
count="$(printf '%s' "${list_json}" | extract_field 'data.sessions.length')"
echo "[smoke] session_count=${count}"

echo "[smoke] DELETE /api/sessions/:id"
curl -fsS -X DELETE "${BASE_URL}/api/sessions/${session_id}" >/dev/null

echo "[smoke] PASS"

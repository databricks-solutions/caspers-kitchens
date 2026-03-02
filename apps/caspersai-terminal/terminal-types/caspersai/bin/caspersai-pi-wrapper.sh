#!/usr/bin/env bash
set -euo pipefail

export HOME="${HOME:-/home/app}"
__dbx_default_pi_dir="${HOME}/.pi/agent"
__dbx_caspers_pi_dir="${HOME}/.pi/caspersai-agent"

mkdir -p "${__dbx_default_pi_dir}" "${__dbx_caspers_pi_dir}"

if [[ -f "${__dbx_default_pi_dir}/models.json" ]]; then
  cp -f "${__dbx_default_pi_dir}/models.json" "${__dbx_caspers_pi_dir}/models.json"
fi

export PI_CODING_AGENT_DIR="${__dbx_caspers_pi_dir}"
export DBX_CASPERS_SETTINGS_PATH="${PI_CODING_AGENT_DIR}/settings.json"
export PI_SKIP_VERSION_CHECK="${PI_SKIP_VERSION_CHECK:-1}"

python3 <<'PY'
import json
import os

settings_path = os.environ.get("DBX_CASPERS_SETTINGS_PATH")
if not settings_path:
    raise SystemExit(0)

settings = {}
if os.path.exists(settings_path):
    try:
        with open(settings_path, "r", encoding="utf-8") as f:
            raw = f.read().strip()
        if raw:
            loaded = json.loads(raw)
            if isinstance(loaded, dict):
                settings = loaded
    except Exception:
        settings = {}

settings["quietStartup"] = True
settings["collapseChangelog"] = True

os.makedirs(os.path.dirname(settings_path), exist_ok=True)
temp_path = f"{settings_path}.tmp"
with open(temp_path, "w", encoding="utf-8") as f:
    json.dump(settings, f, indent=2)
    f.write("\n")
os.replace(temp_path, settings_path)
PY

exec pi "$@"

#!/usr/bin/env bash
# shellcheck shell=bash

__dbx_caspers_terminal_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
__dbx_caspers_wrapper="${__dbx_caspers_terminal_root}/terminal-types/caspersai/bin/caspersai-pi-wrapper.sh"
__dbx_caspers_branding_extension="${__dbx_caspers_terminal_root}/terminal-types/caspersai/extensions/caspers-branding/index.ts"
__dbx_caspers_footer_extension="${DBX_APP_TERMINAL_CASPERSAI_FOOTER_EXTENSION:-${__dbx_caspers_branding_extension}}"

if [[ -f "${__dbx_caspers_wrapper}" ]]; then
  chmod +x "${__dbx_caspers_wrapper}" 2>/dev/null || true
  export DBX_APP_TERMINAL_PI_CMD="${DBX_APP_TERMINAL_CASPERSAI_CMD:-${__dbx_caspers_wrapper}}"
fi

# Keep startup minimal and suppress npm version-check noise in CaspersAI mode.
export PI_SKIP_VERSION_CHECK="${PI_SKIP_VERSION_CHECK:-1}"
export DBX_APP_TERMINAL_PI_FOOTER_EXTENSION="${__dbx_caspers_footer_extension}"

# shellcheck source=../pi/launch.sh
source "${__dbx_caspers_terminal_root}/terminal-types/pi/launch.sh"

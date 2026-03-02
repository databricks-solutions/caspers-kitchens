#!/usr/bin/env bash
# shellcheck shell=bash

__dbx_caspers_terminal_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
__dbx_caspers_wrapper="${__dbx_caspers_terminal_root}/terminal-types/caspersai/bin/caspersai-pi-wrapper.sh"
__dbx_caspers_footer_extension="${DBX_APP_TERMINAL_CASPERSAI_FOOTER_EXTENSION:-${__dbx_caspers_terminal_root}/terminal-types/pi/extensions/top-footer-line/index.ts}"

if [[ -x "${__dbx_caspers_wrapper}" ]]; then
  export DBX_APP_TERMINAL_PI_CMD="${DBX_APP_TERMINAL_CASPERSAI_CMD:-${__dbx_caspers_wrapper}}"
fi
export DBX_APP_TERMINAL_PI_FOOTER_EXTENSION="${__dbx_caspers_footer_extension}"

# shellcheck source=../pi/launch.sh
source "${__dbx_caspers_terminal_root}/terminal-types/pi/launch.sh"

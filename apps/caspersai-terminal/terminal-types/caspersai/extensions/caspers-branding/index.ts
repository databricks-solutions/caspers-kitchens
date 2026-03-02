import type { ExtensionAPI, ExtensionContext } from "@mariozechner/pi-coding-agent";

const WIDGET_KEY = "caspers-branding";

const BANNER = [
  "   _____    _    ____  ____  _____ ____  ____   ",
  "  / ____|  / \\  / ___||  _ \\| ____|  _ \\/ ___|  ",
  " | |      / _ \\ \\___ \\| |_) |  _| | |_) \\___ \\ ",
  " | |___  / ___ \\ ___) |  __/| |___|  _ < ___) |",
  "  \\____|/_/   \\_\\____/|_|   |_____|_| \\_\\____/ ",
  "                 Caspers × \uE003 AI                 ",
];

function installBranding(ctx: ExtensionContext): void {
  if (!ctx.hasUI) {
    return;
  }

  ctx.ui.setWidget(WIDGET_KEY, BANNER, {
    placement: "aboveEditor",
  });
}

export default function caspersBrandingExtension(pi: ExtensionAPI) {
  pi.on("session_start", async (_event, ctx) => {
    installBranding(ctx);
  });

  pi.on("session_switch", async (_event, ctx) => {
    installBranding(ctx);
  });
}

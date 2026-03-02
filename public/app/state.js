export const LAUNCHER_SESSION_ID = "__launcher__";

const DEFAULT_SESSION_TYPES = [
  {
    id: "terminal",
    name: "Terminal",
    description: "Plain shell session",
    badge: "terminal",
    icon: "⌂",
    default: true,
    builtIn: true,
  },
];

export function createAppState() {
  return {
    sessions: new Map(),
    activeSessionId: null,
    sessionTypes: [...DEFAULT_SESSION_TYPES],
    terminalIconFontReadyPromise: null,
  };
}

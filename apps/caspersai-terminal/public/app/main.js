import { api, wsUrlFromPath } from "./apiClient.js";
import { createLauncherController } from "./launcherController.js";
import { createSessionController } from "./sessionController.js";
import { createSessionTypesModel } from "./sessionTypesModel.js";
import { createAppState } from "./state.js";
import * as tabUi from "./tabUi.js";
import * as textLayout from "./textLayout.js";

export function bootstrapApp() {
  const tabsEl = document.getElementById("tabs");
  const terminalMainEl = document.getElementById("terminal-main");
  const createBtn = document.getElementById("create-session");

  if (!tabsEl || !terminalMainEl || !createBtn) {
    return;
  }

  const state = createAppState();
  const sessionTypesModel = createSessionTypesModel(state);

  const launcher = createLauncherController({
    state,
    sessionTypesModel,
    textLayout,
    api,
  });

  const sessions = createSessionController({
    elements: {
      tabsEl,
      terminalMainEl,
    },
    state,
    apiClient: {
      api,
      wsUrlFromPath,
    },
    sessionTypesModel,
    tabUi,
    launcher,
  });

  launcher.setUiHooks({
    focusSessionTerminal: sessions.focusSessionTerminal,
    sendResize: sessions.sendResize,
  });

  launcher.setSessionActions({
    createSession: sessions.createSession,
    closeSessionUi: sessions.closeSessionUi,
    mountLauncherSession: sessions.mountLauncherSession,
  });

  function loadSessionTypes() {
    return api("GET", "/api/session-types")
      .then((data) => {
        sessionTypesModel.setSessionTypes(data.types || []);
      })
      .catch((error) => {
        console.warn("Loading session types failed:", error.message);
      });
  }

  function handleGlobalKeyDown(event) {
    const key = event.key.toLowerCase();
    const isNewTabShortcut = (event.metaKey || event.ctrlKey) && !event.altKey && key === "t";

    if (!isNewTabShortcut) {
      return;
    }

    event.preventDefault();
    launcher.openInlineSessionTypePicker();
  }

  window.addEventListener("resize", () => {
    sessions.fitActiveSession();
  });

  window.addEventListener("keydown", handleGlobalKeyDown);
  createBtn.addEventListener("click", () => launcher.openInlineSessionTypePicker());

  if (typeof ResizeObserver !== "undefined") {
    const resizeObserver = new ResizeObserver(() => {
      sessions.fitActiveSession();
    });

    resizeObserver.observe(terminalMainEl);
  }

  if (document.fonts && document.fonts.ready) {
    document.fonts.ready.then(() => {
      sessions.fitActiveSession();
    });
  }

  loadSessionTypes().finally(() => {
    sessions.loadExistingSessions();
  });
}

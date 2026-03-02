import { LAUNCHER_SESSION_ID } from "./state.js";
import { renderLauncherScreen } from "./launcherRenderer.js";

export function createLauncherController({ state, sessionTypesModel, textLayout, api }) {
  let sessionActions = null;
  let uiHooks = null;

  function setSessionActions(actions) {
    sessionActions = actions;
  }

  function setUiHooks(hooks) {
    uiHooks = hooks;
  }

  function requireSessionActions() {
    if (!sessionActions) {
      throw new Error("launcher session actions are not configured");
    }
    return sessionActions;
  }

  function requireUiHooks() {
    if (!uiHooks) {
      throw new Error("launcher UI hooks are not configured");
    }
    return uiHooks;
  }

  function isLauncherSession(session) {
    return Boolean(session && session.sessionId === LAUNCHER_SESSION_ID);
  }

  function activeInlinePickerSession() {
    for (const session of state.sessions.values()) {
      if (session.inlinePicker) {
        return session;
      }
    }
    return null;
  }

  function closeInlineSessionTypePicker(session, flushBufferedOutput = true) {
    if (!session || !session.inlinePicker) {
      return;
    }

    const hooks = requireUiHooks();

    const bufferedOutput = session.inlinePicker.bufferedOutput.join("");
    session.inlinePicker = null;

    session.terminal.write("\u001b[?25h\u001b[?1049l");

    if (flushBufferedOutput && bufferedOutput.length > 0) {
      session.terminal.write(bufferedOutput);
    }

    hooks.focusSessionTerminal(session);
  }

  function closeAnyInlineSessionTypePicker(flushBufferedOutput = true) {
    for (const session of state.sessions.values()) {
      if (session.inlinePicker) {
        closeInlineSessionTypePicker(session, flushBufferedOutput);
      }
    }
  }

  function renderInlineSessionTypePicker(session) {
    const picker = session?.inlinePicker;
    if (!picker) {
      return;
    }

    const output = renderLauncherScreen({
      session,
      picker,
      sessionTypes: sessionTypesModel.getAllTypes(),
      sessionTypesModel,
      textLayout,
    });

    session.terminal.write(output);
  }

  function setInlineSessionTypePickerMode(session, mode) {
    if (!session || !session.inlinePicker) {
      return;
    }

    session.inlinePicker.mode = mode;
    renderInlineSessionTypePicker(session);
  }

  function waitForTerminalIconFontReady(timeoutMs = 1500) {
    if (state.terminalIconFontReadyPromise) {
      return state.terminalIconFontReadyPromise;
    }

    if (!document.fonts || typeof document.fonts.load !== "function") {
      state.terminalIconFontReadyPromise = Promise.resolve();
      return state.terminalIconFontReadyPromise;
    }

    const settle = Promise.all([
      document.fonts.load('16px "DBX Term Icons"', "\uE001"),
      document.fonts.load('16px "DBX Term Icons"', "\uE002"),
      document.fonts.load('16px "DBX Term Icons"', "\uE003"),
      document.fonts.ready,
    ]).then(() => undefined).catch(() => undefined);

    const timeout = new Promise((resolve) => {
      setTimeout(resolve, timeoutMs);
    });

    state.terminalIconFontReadyPromise = Promise.race([settle, timeout]).then(() => undefined);
    return state.terminalIconFontReadyPromise;
  }

  function scheduleInlinePickerFirstRender(session) {
    waitForTerminalIconFontReady().finally(() => {
      let remainingPasses = 2;

      const pass = () => {
        if (!session.inlinePicker) {
          return;
        }

        const hooks = requireUiHooks();
        session.fitAddon.fit();
        hooks.sendResize(session);

        if (remainingPasses > 0) {
          remainingPasses -= 1;
          requestAnimationFrame(pass);
          return;
        }

        session.inlinePicker.initialized = true;
        renderInlineSessionTypePicker(session);
        hooks.focusSessionTerminal(session);
      };

      requestAnimationFrame(pass);
    });
  }

  function moveInlineSessionTypePicker(delta) {
    const session = activeInlinePickerSession();
    if (!session || !session.inlinePicker || session.inlinePicker.mode !== "home") {
      return;
    }

    const types = sessionTypesModel.getAllTypes();
    if (types.length === 0) {
      return;
    }

    const count = types.length;
    session.inlinePicker.selectedIndex = ((session.inlinePicker.selectedIndex + delta) % count + count) % count;
    renderInlineSessionTypePicker(session);
  }

  function handleInlineSessionTypePickerKey(session, domEvent) {
    if (!session.inlinePicker) {
      return false;
    }

    const key = domEvent.key;
    const lower = key.toLowerCase();
    const hasModifiers = domEvent.metaKey || domEvent.ctrlKey || domEvent.altKey;
    const mode = session.inlinePicker.mode || "home";
    const blocking = Boolean(session.inlinePicker.blocking);

    if (!session.inlinePicker.initialized) {
      domEvent.preventDefault();
      domEvent.stopPropagation();
      return true;
    }

    if (key === "Escape") {
      if (mode !== "home") {
        setInlineSessionTypePickerMode(session, "home");
      } else if (!blocking) {
        closeInlineSessionTypePicker(session);
      }

      domEvent.preventDefault();
      domEvent.stopPropagation();
      return true;
    }

    if (!hasModifiers && key === "?") {
      setInlineSessionTypePickerMode(session, mode === "help" ? "home" : "help");
      domEvent.preventDefault();
      domEvent.stopPropagation();
      return true;
    }

    if (!hasModifiers && lower === "a") {
      setInlineSessionTypePickerMode(session, mode === "about" ? "home" : "about");
      domEvent.preventDefault();
      domEvent.stopPropagation();
      return true;
    }

    if (mode !== "home") {
      if (key === "Enter" || key === "Backspace" || key === " ") {
        setInlineSessionTypePickerMode(session, "home");
      }
      domEvent.preventDefault();
      domEvent.stopPropagation();
      return true;
    }

    if (key === "ArrowDown" || (!hasModifiers && lower === "j")) {
      moveInlineSessionTypePicker(1);
      domEvent.preventDefault();
      domEvent.stopPropagation();
      return true;
    }

    if (key === "ArrowUp" || (!hasModifiers && lower === "k")) {
      moveInlineSessionTypePicker(-1);
      domEvent.preventDefault();
      domEvent.stopPropagation();
      return true;
    }

    if (key === "Enter") {
      chooseInlineSessionType();
      domEvent.preventDefault();
      domEvent.stopPropagation();
      return true;
    }

    if (/^[1-9]$/.test(key)) {
      chooseInlineSessionType(Number(key) - 1);
      domEvent.preventDefault();
      domEvent.stopPropagation();
      return true;
    }

    domEvent.preventDefault();
    domEvent.stopPropagation();
    return true;
  }

  function chooseInlineSessionType(index) {
    const session = activeInlinePickerSession();
    if (!session || !session.inlinePicker || session.inlinePicker.mode !== "home") {
      return;
    }

    const types = sessionTypesModel.getAllTypes();
    if (types.length === 0) {
      return;
    }

    if (typeof index === "number" && Number.isInteger(index)) {
      if (index < 0 || index >= types.length) {
        return;
      }
      session.inlinePicker.selectedIndex = index;
    }

    const selectedType = types[session.inlinePicker.selectedIndex] || {
      id: sessionTypesModel.defaultTypeId(),
    };

    const replaceOnSelect = Boolean(session.inlinePicker.replaceOnSelect);
    const currentTypeId = session.typeId;
    const launcher = isLauncherSession(session);

    // Prevent the Enter key that confirms picker selection from leaking as input
    // into the currently attached PTY session.
    session.suppressControlInputOnce = true;

    closeInlineSessionTypePicker(session, !replaceOnSelect);

    const actions = requireSessionActions();

    if (replaceOnSelect) {
      if (!launcher && selectedType.id === currentTypeId) {
        return;
      }

      actions.closeSessionUi(session.sessionId, {
        suppressAutoCreate: true,
      });

      if (!launcher) {
        api("DELETE", `/api/sessions/${encodeURIComponent(session.sessionId)}`)
          .catch((error) => {
            if ((error.message || "").toLowerCase().includes("not found")) {
              return;
            }
            console.warn(`Failed to close session ${session.sessionId}:`, error.message);
          });
      }

      actions.createSession(selectedType.id);
      return;
    }

    actions.createSession(selectedType.id);
  }

  function openInlineSessionTypePicker(sessionId = state.activeSessionId, options = {}) {
    const session = sessionId ? state.sessions.get(sessionId) : null;

    if (!session) {
      const actions = requireSessionActions();
      actions.mountLauncherSession(true);
      openInlineSessionTypePicker(LAUNCHER_SESSION_ID, {
        replaceOnSelect: true,
      });
      return;
    }

    if (session.inlinePicker) {
      if (!session.inlinePicker.blocking) {
        closeInlineSessionTypePicker(session);
      }
      return;
    }

    closeAnyInlineSessionTypePicker();

    let selectedIndex = sessionTypesModel.getAllTypes().findIndex((type) => type.default);
    if (selectedIndex < 0) {
      selectedIndex = 0;
    }

    const replaceOnSelect = options.replaceOnSelect === undefined
      ? isLauncherSession(session)
      : Boolean(options.replaceOnSelect);

    const blocking = options.blocking === undefined
      ? (isLauncherSession(session) && replaceOnSelect)
      : Boolean(options.blocking);

    session.inlinePicker = {
      mode: "home",
      selectedIndex,
      bufferedOutput: [],
      replaceOnSelect,
      blocking,
      initialized: false,
    };

    session.terminal.write("\u001b[?1049h\u001b[?25l");
    scheduleInlinePickerFirstRender(session);
  }

  return {
    setSessionActions,
    setUiHooks,
    isLauncherSession,
    activeInlinePickerSession,
    closeInlineSessionTypePicker,
    closeAnyInlineSessionTypePicker,
    renderInlineSessionTypePicker,
    handleInlineSessionTypePickerKey,
    openInlineSessionTypePicker,
  };
}

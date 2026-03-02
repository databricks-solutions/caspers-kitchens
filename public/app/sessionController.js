import { LAUNCHER_SESSION_ID } from "./state.js";

export function createSessionController({
  elements,
  state,
  apiClient,
  sessionTypesModel,
  tabUi,
  launcher,
}) {
  const { tabsEl, terminalMainEl } = elements;
  const { api, wsUrlFromPath } = apiClient;
  const { normalizeAuthMode, updateTabAuth, updateTabType, updateTabTitle, updateTabStatus } = tabUi;

  function sendResize(session) {
    if (!session.socket || session.socket.readyState !== WebSocket.OPEN) {
      return;
    }

    session.socket.send(
      JSON.stringify({
        type: "resize",
        cols: session.terminal.cols,
        rows: session.terminal.rows,
      }),
    );
  }

  function fitAndResizeSession(session) {
    requestAnimationFrame(() => {
      session.fitAddon.fit();
      sendResize(session);

      if (session.inlinePicker && session.inlinePicker.initialized) {
        launcher.renderInlineSessionTypePicker(session);
      }
    });
  }

  function focusSessionTerminal(session) {
    requestAnimationFrame(() => {
      session.terminal.focus();
    });
  }

  function activateSession(sessionId) {
    const pickerSession = launcher.activeInlinePickerSession();
    if (pickerSession && pickerSession.sessionId !== sessionId) {
      launcher.closeInlineSessionTypePicker(pickerSession);
    }

    state.activeSessionId = sessionId;

    for (const [id, session] of state.sessions.entries()) {
      const isActive = id === sessionId;
      session.tabEl.classList.toggle("active", isActive);
      session.paneEl.classList.toggle("active", isActive);
    }

    const active = state.sessions.get(sessionId);
    if (active) {
      fitAndResizeSession(active);
      setTimeout(() => {
        if (state.activeSessionId === sessionId) {
          fitAndResizeSession(active);
        }
      }, 80);
      focusSessionTerminal(active);
    }
  }

  function closeSessionUi(sessionId, options = {}) {
    const session = state.sessions.get(sessionId);
    if (!session) {
      return;
    }

    if (session.inlinePicker) {
      launcher.closeInlineSessionTypePicker(session, false);
    }

    if (session.socket) {
      session.socket.close();
      session.socket = null;
    }

    session.terminal.dispose();
    session.tabEl.remove();
    session.paneEl.remove();
    state.sessions.delete(sessionId);

    if (state.activeSessionId === sessionId) {
      const first = state.sessions.keys().next();
      state.activeSessionId = null;
      if (!first.done) {
        activateSession(first.value);
      }
    }

    if (state.sessions.size === 0 && !options.suppressAutoCreate) {
      mountLauncherSession(true);
      launcher.openInlineSessionTypePicker(LAUNCHER_SESSION_ID, {
        replaceOnSelect: true,
      });
    }
  }

  function killSession(sessionId, options = {}) {
    api("DELETE", `/api/sessions/${encodeURIComponent(sessionId)}`)
      .then(() => {
        closeSessionUi(sessionId, options);
      })
      .catch((error) => {
        if ((error.message || "").toLowerCase().includes("not found")) {
          closeSessionUi(sessionId, options);
          return;
        }
        console.warn(`Failed to close session ${sessionId}:`, error.message);
      });
  }

  function toggleSessionAuthMode(sessionId) {
    const session = state.sessions.get(sessionId);
    if (!session) {
      return;
    }

    const nextMode = session.authMode === "user" ? "m2m" : "user";

    api("POST", `/api/sessions/${encodeURIComponent(sessionId)}/auth-mode`, {
      mode: nextMode,
    })
      .then((data) => {
        session.authMode = normalizeAuthMode(data.authMode);
        updateTabAuth(session);
      })
      .catch((error) => {
        console.warn(`Failed to switch auth mode (${sessionId}):`, error.message);
        session.terminal.writeln(`\r\n[error] ${error.message}`);
      });
  }

  function connectSession(sessionId) {
    const session = state.sessions.get(sessionId);
    if (!session) {
      return;
    }

    api("POST", `/api/sessions/${encodeURIComponent(sessionId)}/attach`, {})
      .then((data) => {
        const socket = new WebSocket(wsUrlFromPath(data.websocketPath));
        session.socket = socket;

        socket.addEventListener("open", () => {
          updateTabStatus(state, sessionId, "connected");
          fitAndResizeSession(session);
        });

        socket.addEventListener("message", (event) => {
          let msg;
          try {
            msg = JSON.parse(event.data);
          } catch {
            return;
          }

          if (msg.type === "ready") {
            updateTabStatus(state, sessionId, "connected");
            fitAndResizeSession(session);
            return;
          }

          if (msg.type === "auth_mode") {
            session.authMode = normalizeAuthMode(msg.mode);
            updateTabAuth(session);
            return;
          }

          if (msg.type === "output") {
            if (session.inlinePicker) {
              session.inlinePicker.bufferedOutput.push(msg.data);
              return;
            }

            session.terminal.write(msg.data);
            return;
          }

          if (msg.type === "exit") {
            if (session.inlinePicker) {
              launcher.closeInlineSessionTypePicker(session);
            }

            updateTabStatus(state, sessionId, "closed");
            session.terminal.writeln(`\r\n[process exited: code=${msg.exitCode}]`);
            return;
          }

          if (msg.type === "error") {
            if (session.inlinePicker) {
              launcher.closeInlineSessionTypePicker(session);
            }
            session.terminal.writeln(`\r\n[error] ${msg.message}`);
          }
        });

        socket.addEventListener("close", () => {
          if (session.inlinePicker) {
            launcher.closeInlineSessionTypePicker(session);
          }

          if (state.sessions.has(sessionId)) {
            updateTabStatus(state, sessionId, "disconnected");
          }
        });

        socket.addEventListener("error", () => {
          if (session.inlinePicker) {
            launcher.closeInlineSessionTypePicker(session);
          }

          if (state.sessions.has(sessionId)) {
            updateTabStatus(state, sessionId, "disconnected");
          }
        });
      })
      .catch((error) => {
        console.warn(`Attach failed (${sessionId}):`, error.message);
      });
  }

  function mountSession(sessionId, authMode = "m2m", typeId = "terminal", activate = true, options = {}) {
    if (state.sessions.has(sessionId)) {
      if (activate) {
        activateSession(sessionId);
      }
      return;
    }

    const TerminalCtor = window.Terminal;
    const FitAddonCtor = window.FitAddon && window.FitAddon.FitAddon;

    if (typeof TerminalCtor !== "function" || typeof FitAddonCtor !== "function") {
      console.error("xterm runtime not loaded");
      return;
    }

    const isLauncher = Boolean(options.launcher);

    const tabEl = document.createElement("div");
    tabEl.className = isLauncher ? "tab launcher" : "tab";

    const closeEl = document.createElement("button");
    closeEl.className = "tab-close";
    closeEl.type = "button";
    closeEl.setAttribute("aria-label", `Close ${sessionId}`);
    closeEl.textContent = "×";

    const authEl = document.createElement("button");
    authEl.className = "tab-auth";
    authEl.type = "button";
    authEl.setAttribute("aria-label", `Toggle auth mode for ${sessionId}`);

    const labelEl = document.createElement("span");
    labelEl.className = "tab-label";

    const statusEl = document.createElement("span");
    statusEl.className = "tab-status disconnected";

    tabEl.appendChild(closeEl);
    tabEl.appendChild(authEl);
    tabEl.appendChild(labelEl);
    tabEl.appendChild(statusEl);
    tabsEl.appendChild(tabEl);

    if (isLauncher) {
      closeEl.tabIndex = -1;
      closeEl.setAttribute("aria-hidden", "true");
      authEl.tabIndex = -1;
      authEl.setAttribute("aria-hidden", "true");
      statusEl.setAttribute("aria-hidden", "true");
    }

    const paneEl = document.createElement("section");
    paneEl.className = "terminal-pane";

    const hostEl = document.createElement("div");
    hostEl.className = "terminal-host";
    paneEl.appendChild(hostEl);
    terminalMainEl.appendChild(paneEl);

    const terminalFontFamily =
      getComputedStyle(document.documentElement).getPropertyValue("--terminal-font-family").trim()
      || '"DBX Term Icons", "Symbols Nerd Font Mono", Menlo, Monaco, Consolas, "Liberation Mono", monospace';

    const terminal = new TerminalCtor({
      cursorBlink: true,
      fontSize: 15,
      fontFamily: terminalFontFamily,
      theme: {
        background: "#0b0d10",
      },
      scrollback: 2000,
    });

    const fitAddon = new FitAddonCtor();
    terminal.loadAddon(fitAddon);
    terminal.open(hostEl);

    const stateEntry = {
      sessionId,
      tabEl,
      closeEl,
      typeEl: null,
      authEl,
      labelEl,
      paneEl,
      statusEl,
      terminal,
      fitAddon,
      socket: null,
      status: isLauncher ? "connected" : "disconnected",
      authMode: normalizeAuthMode(authMode),
      typeId: sessionTypesModel.normalizeTypeId(typeId),
      dynamicTitle: typeof options.dynamicTitle === "string" ? options.dynamicTitle : "",
      inlinePicker: null,
      isLauncher,
      suppressControlInputOnce: false,
    };

    state.sessions.set(sessionId, stateEntry);
    updateTabTitle(stateEntry);
    updateTabType(stateEntry, sessionTypesModel);
    updateTabAuth(stateEntry);

    if (isLauncher) {
      updateTabStatus(state, sessionId, "connected");
    }

    terminal.onData((data) => {
      if (stateEntry.inlinePicker) {
        return;
      }

      if (stateEntry.suppressControlInputOnce) {
        stateEntry.suppressControlInputOnce = false;

        // Enter usually arrives as CR/LF; drop it once after picker selection.
        if (data.replace(/[\r\n]/g, "").length === 0) {
          return;
        }
      }

      if (!stateEntry.socket || stateEntry.socket.readyState !== WebSocket.OPEN) {
        return;
      }

      stateEntry.socket.send(
        JSON.stringify({
          type: "input",
          data,
        }),
      );
    });

    if (typeof terminal.onKey === "function") {
      terminal.onKey(({ domEvent }) => {
        launcher.handleInlineSessionTypePickerKey(stateEntry, domEvent);
      });
    }

    if (typeof terminal.onTitleChange === "function") {
      terminal.onTitleChange((title) => {
        stateEntry.dynamicTitle = title || "";
        updateTabTitle(stateEntry);
      });
    }

    tabEl.addEventListener("click", () => {
      activateSession(sessionId);
    });

    if (!isLauncher) {
      closeEl.addEventListener("click", (event) => {
        event.stopPropagation();
        killSession(sessionId);
      });

      authEl.addEventListener("click", (event) => {
        event.stopPropagation();
        toggleSessionAuthMode(sessionId);
      });
    }

    if (options.connect !== false) {
      connectSession(sessionId);
    }

    if (activate || !state.activeSessionId) {
      activateSession(sessionId);
    }
  }

  function mountLauncherSession(activate = true) {
    if (state.sessions.has(LAUNCHER_SESSION_ID)) {
      if (activate) {
        activateSession(LAUNCHER_SESSION_ID);
      }
      return;
    }

    mountSession(LAUNCHER_SESSION_ID, "m2m", "terminal", activate, {
      launcher: true,
      connect: false,
      dynamicTitle: "New session",
    });
  }

  function createSession(typeId = "terminal", options = {}) {
    const body = {};
    if (sessionTypesModel.normalizeTypeId(typeId) !== "terminal") {
      body.typeId = sessionTypesModel.normalizeTypeId(typeId);
    }

    api("POST", "/api/sessions", body)
      .then((data) => {
        const sessionId = data.session.sessionId;

        mountSession(
          sessionId,
          data.authMode || data.session.authMode || "m2m",
          data.typeId || data.session.typeId || typeId,
          true,
        );

        if (options.openPicker) {
          launcher.openInlineSessionTypePicker(sessionId, {
            replaceOnSelect: Boolean(options.replaceOnSelect),
          });
        }
      })
      .catch((error) => {
        console.warn("Create session failed:", error.message);
      });
  }

  function loadExistingSessions() {
    api("GET", "/api/sessions")
      .then((data) => {
        for (const session of data.sessions) {
          mountSession(session.sessionId, session.authMode || "m2m", session.typeId || "terminal", false);
        }

        if (state.sessions.size > 0) {
          const first = state.sessions.keys().next();
          if (!first.done) {
            activateSession(first.value);
          }
          return;
        }

        mountLauncherSession(true);
        launcher.openInlineSessionTypePicker(LAUNCHER_SESSION_ID, {
          replaceOnSelect: true,
        });
      })
      .catch((error) => {
        console.warn("Loading sessions failed:", error.message);
      });
  }

  function fitActiveSession() {
    if (!state.activeSessionId) {
      return;
    }

    const session = state.sessions.get(state.activeSessionId);
    if (!session) {
      return;
    }

    fitAndResizeSession(session);
  }

  return {
    sendResize,
    fitAndResizeSession,
    focusSessionTerminal,
    activateSession,
    closeSessionUi,
    killSession,
    mountSession,
    mountLauncherSession,
    createSession,
    loadExistingSessions,
    fitActiveSession,
  };
}

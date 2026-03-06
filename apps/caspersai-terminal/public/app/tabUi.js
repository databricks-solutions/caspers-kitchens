export function normalizeAuthMode(mode) {
  return mode === "user" || mode === "user-token" ? "user" : "m2m";
}

function authBadgeText(mode) {
  return mode === "user" ? "user" : "m2m";
}

function shortSessionLabel(sessionId) {
  return sessionId.slice(0, 8);
}

function displayTitle(session) {
  return session.dynamicTitle && session.dynamicTitle.trim().length > 0
    ? session.dynamicTitle.trim()
    : shortSessionLabel(session.sessionId);
}

export function updateTabAuth(session, sessionTypesModel) {
  const mode = normalizeAuthMode(session.authMode);
  session.authMode = mode;

  const policy = sessionTypesModel.authPolicyForType(session.typeId);
  const toggleEnabled = policy === "both";

  session.authEl.textContent = authBadgeText(mode);
  session.authEl.classList.toggle("user", mode === "user");
  session.authEl.classList.toggle("locked", !toggleEnabled);
  session.authEl.disabled = !toggleEnabled;

  if (toggleEnabled) {
    session.authEl.setAttribute("aria-label", `Toggle auth mode for ${session.sessionId}`);
    session.authEl.title = "Toggle auth mode";
  } else {
    const pinnedText = policy === "user" ? "Pinned to user auth" : "Pinned to m2m auth";
    session.authEl.setAttribute("aria-label", pinnedText);
    session.authEl.title = pinnedText;
  }
}

export function updateTabType(session, sessionTypesModel) {
  const type = sessionTypesModel.findType(session.typeId);

  if (type.id === "terminal") {
    if (session.typeEl) {
      session.typeEl.remove();
      session.typeEl = null;
    }
    return;
  }

  if (!session.typeEl) {
    const typeEl = document.createElement("span");
    typeEl.className = "tab-type";
    session.typeEl = typeEl;
    session.tabEl.insertBefore(typeEl, session.authEl);
  }

  session.typeEl.textContent = sessionTypesModel.typeLogo(type);
}

export function updateTabTitle(session) {
  session.labelEl.textContent = displayTitle(session);
}

export function updateTabStatus(state, sessionId, status) {
  const session = state.sessions.get(sessionId);
  if (!session) {
    return;
  }

  session.status = status;
  session.statusEl.classList.remove("connected", "disconnected", "closed");
  session.statusEl.classList.add(status);
}

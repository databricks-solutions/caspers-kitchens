function normalizeTypeId(typeId) {
  return typeof typeId === "string" && typeId.length > 0 ? typeId : "terminal";
}

function normalizeAuthPolicy(policy) {
  return policy === "user" || policy === "m2m" ? policy : "both";
}

function normalizeOrder(value) {
  if (typeof value !== "number" || !Number.isFinite(value)) {
    return undefined;
  }

  return Math.trunc(value);
}

function hasExplicitOrder(type) {
  return typeof type.order === "number" && Number.isFinite(type.order);
}

function compareSessionTypes(a, b) {
  const aOrdered = hasExplicitOrder(a);
  const bOrdered = hasExplicitOrder(b);

  if (aOrdered && bOrdered) {
    const orderDiff = a.order - b.order;
    if (orderDiff !== 0) {
      return orderDiff;
    }
  }

  if (aOrdered !== bOrdered) {
    return aOrdered ? -1 : 1;
  }

  if (a.default !== b.default) {
    return a.default ? -1 : 1;
  }

  const byName = String(a.name || a.id).localeCompare(String(b.name || b.id));
  if (byName !== 0) {
    return byName;
  }

  return String(a.id).localeCompare(String(b.id));
}

function sortedSessionTypes(list) {
  return [...list].sort(compareSessionTypes);
}

function fallbackType(typeId) {
  const normalized = normalizeTypeId(typeId);
  return {
    id: normalized,
    name: normalized,
    description: "",
    badge: normalized,
    icon: undefined,
    authPolicy: "both",
    default: false,
    order: undefined,
    builtIn: false,
  };
}

export function createSessionTypesModel(state) {
  return {
    normalizeTypeId,

    getAllTypes() {
      return state.sessionTypes;
    },

    setSessionTypes(types) {
      if (!Array.isArray(types) || types.length === 0) {
        return;
      }

      state.sessionTypes = sortedSessionTypes(
        types.map((type) => ({
          id: normalizeTypeId(type.id),
          name: type.name || type.id || "Terminal",
          description: type.description || "",
          badge: type.badge || type.id || "terminal",
          icon: typeof type.icon === "string" && type.icon.length > 0 ? type.icon : undefined,
          authPolicy: normalizeAuthPolicy(type.authPolicy),
          default: Boolean(type.default),
          order: normalizeOrder(type.order),
          builtIn: Boolean(type.builtIn),
        })),
      );
    },

    findType(typeId) {
      const normalized = normalizeTypeId(typeId);
      const found = state.sessionTypes.find((type) => type.id === normalized);
      return found || fallbackType(normalized);
    },

    authPolicyForType(typeId) {
      const type = this.findType(typeId);
      return normalizeAuthPolicy(type.authPolicy);
    },

    allowsAuthMode(typeId, mode) {
      const normalizedMode = mode === "user" ? "user" : "m2m";
      const policy = this.authPolicyForType(typeId);
      if (policy === "both") {
        return true;
      }
      return policy === normalizedMode;
    },

    isAuthToggleEnabled(typeId) {
      return this.authPolicyForType(typeId) === "both";
    },

    defaultTypeId() {
      const found = state.sessionTypes.find((type) => type.default);
      if (found) {
        return found.id;
      }

      return state.sessionTypes[0]?.id || "terminal";
    },

    typeLogo(type) {
      if (typeof type.icon === "string" && type.icon.length > 0) {
        return type.icon;
      }

      if (typeof type.badge === "string" && type.badge.length > 0) {
        return type.badge;
      }

      return type.id;
    },
  };
}

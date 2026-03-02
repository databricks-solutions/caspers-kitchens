function normalizeTypeId(typeId) {
  return typeof typeId === "string" && typeId.length > 0 ? typeId : "terminal";
}

function sortedSessionTypes(list) {
  return [...list].sort((a, b) => {
    if (a.default) {
      return -1;
    }
    if (b.default) {
      return 1;
    }
    return String(a.name || a.id).localeCompare(String(b.name || b.id));
  });
}

function fallbackType(typeId) {
  const normalized = normalizeTypeId(typeId);
  return {
    id: normalized,
    name: normalized,
    description: "",
    badge: normalized,
    icon: undefined,
    default: false,
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
          default: Boolean(type.default),
          builtIn: Boolean(type.builtIn),
        })),
      );
    },

    findType(typeId) {
      const normalized = normalizeTypeId(typeId);
      const found = state.sessionTypes.find((type) => type.id === normalized);
      return found || fallbackType(normalized);
    },

    defaultTypeId() {
      const found = state.sessionTypes.find((type) => type.default);
      return found ? found.id : "terminal";
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
